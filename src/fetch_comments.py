"""
YouTube Comment Fetcher with Kafka Streaming
Fetches ALL comments from YouTube videos and streams them to Kafka.
First run: Fetches all historical comments
Subsequent runs: Only fetches new comments since last run
"""

import os
import json
import yaml
import time
import logging
import signal
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from confluent_kafka import Producer
import socket

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fetch_comments.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

shutdown_flag = False

def signal_handler(signum, frame):
    global shutdown_flag
    logger.info("\nReceived shutdown signal. Finishing current video...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class CommentFetcher:
    """Fetch YouTube comments with Kafka streaming."""
    
    def __init__(self, api_key: str, data_dir: str = 'data/raw', enable_kafka: bool = True):
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.quota_used = 0
        self.enable_kafka = enable_kafka
        self.kafka_producer = None
        self.kafka_messages_sent = 0
        
        if enable_kafka:
            try:
                self.kafka_producer = Producer({
                    'bootstrap.servers': 'localhost:9092',
                    'client.id': socket.gethostname(),
                    'acks': 'all',
                    'retries': 3
                })
                logger.info("Kafka producer initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka: {e}")
                self.enable_kafka = False
    
    def send_to_kafka(self, comment_data: Dict, topic: str = 'raw_comments'):
        """Stream comment to Kafka topic."""
        if not self.enable_kafka or not self.kafka_producer:
            return
        
        try:
            self.kafka_producer.produce(
                topic,
                key=comment_data['comment_id'].encode('utf-8'),
                value=json.dumps(comment_data).encode('utf-8')
            )
            self.kafka_producer.poll(0)
            self.kafka_messages_sent += 1
        except Exception as e:
            logger.error(f"Kafka send error: {e}")
    
    def get_existing_comments(self, video_id: str) -> tuple[List[Dict], set]:
        """Load existing comments and return list + set of IDs for fast lookup."""
        json_file = self.data_dir / f"{video_id}.json"
        
        if not json_file.exists():
            return [], set()
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                comments = data.get('comments', [])
                existing_ids = {c['comment_id'] for c in comments}
                return comments, existing_ids
        except Exception as e:
            logger.error(f"Error loading {json_file}: {e}")
            return [], set()
    
    def fetch_video_comments(self, video_id: str, category: str = None, existing_ids: set = None) -> Dict:
        """
        Fetch comments for a video. Uses smart ordering for efficiency.
        
        Args:
            video_id: YouTube video ID
            existing_ids: Set of existing comment IDs (for deduplication)
        
        Returns:
            Dict with success status, comments, and metadata
        """
        if existing_ids is None:
            existing_ids = set()
        
        logger.info(f"Fetching comments for video: {video_id}")
        
        try:
            # Get video metadata
            video_response = self.youtube.videos().list(
                part='snippet,statistics',
                id=video_id
            ).execute()
            self.quota_used += 1
            
            if not video_response['items']:
                return {'success': False, 'error': 'Video not found', 'video_id': video_id}
            
            video_info = video_response['items'][0]
            video_title = video_info['snippet']['title']
            channel_title = video_info['snippet']['channelTitle']
            total_comments_reported = int(video_info['statistics'].get('commentCount', 0))
            
            logger.info(f"Video: '{video_title}' by {channel_title}")
            logger.info(f"Total comments reported: {total_comments_reported:,}")
            
            # Check if incremental mode (has existing comments)
            is_incremental = len(existing_ids) > 0
            
            if is_incremental:
                logger.info(f"Incremental mode: Skip duplicates (already have {len(existing_ids)} comments)")
            else:
                logger.info(f"First run: Fetching all comments")
            
            # Fetch comments with pagination
            new_comments = []
            next_page_token = None
            page_num = 0
            consecutive_empty_pages = 0
            MAX_EMPTY_PAGES = 3  # Stop after 3 pages with no new comments
            
            while True:
                if shutdown_flag:
                    logger.info("Shutdown requested, stopping fetch")
                    break
                
                page_num += 1
                
                try:
                    api_params = {
                        'part': 'snippet',
                        'videoId': video_id,
                        'maxResults': 100,
                        'order': 'time',  # Always chronological (oldest first)
                        'textFormat': 'plainText'
                    }
                    
                    if next_page_token:
                        api_params['pageToken'] = next_page_token
                    
                    comments_response = self.youtube.commentThreads().list(**api_params).execute()
                    self.quota_used += 1
                    
                    page_comments = []
                    page_duplicates = 0
                    
                    for item in comments_response['items']:
                        top_comment = item['snippet']['topLevelComment']['snippet']
                        
                        comment_data = {
                            'comment_id': item['id'],
                            'video_id': video_id,
                            'category': category,
                            'author': top_comment['authorDisplayName'],
                            'text': top_comment['textDisplay'],
                            'like_count': top_comment['likeCount'],
                            'published_at': top_comment['publishedAt'],
                            'updated_at': top_comment.get('updatedAt', top_comment['publishedAt']),
                            'reply_count': item['snippet']['totalReplyCount']
                        }
                        
                        # Skip duplicates (ID-based check)
                        if comment_data['comment_id'] in existing_ids:
                            page_duplicates += 1
                            continue
                        
                        page_comments.append(comment_data)
                        self.send_to_kafka(comment_data)
                    
                    new_comments.extend(page_comments)
                    
                    logger.info(f"Page {page_num}: {len(page_comments)} new, {page_duplicates} duplicates | Total new: {len(new_comments)}")
                    
                    # Early stopping for incremental mode
                    if is_incremental:
                        if len(page_comments) == 0:
                            consecutive_empty_pages += 1
                            if consecutive_empty_pages >= MAX_EMPTY_PAGES:
                                logger.info(f"Early stop: {MAX_EMPTY_PAGES} consecutive pages with no new comments")
                                break
                        else:
                            consecutive_empty_pages = 0
                    
                    # Check if more pages exist
                    next_page_token = comments_response.get('nextPageToken')
                    if not next_page_token:
                        logger.info(f"No more pages. Fetch complete!")
                        break
                    
                    time.sleep(0.5)  # Rate limit courtesy
                
                except HttpError as e:
                    if e.resp.status == 403:
                        logger.error("Quota exceeded or comments disabled")
                        break
                    else:
                        logger.error(f"API error: {e}")
                        break
            
            return {
                'success': True,
                'video_id': video_id,
                'video_title': video_title,
                'channel_title': channel_title,
                'new_comments': new_comments,
                'total_fetched': len(new_comments),
                'fetch_timestamp': datetime.now(timezone.utc).isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error fetching {video_id}: {e}", exc_info=True)
            return {'success': False, 'error': str(e), 'video_id': video_id}
    
    def save_comments(self, result: Dict, merge_with_existing: bool = True):
        """Save comments to JSON file."""
        if not result.get('success'):
            return
        
        video_id = result['video_id']
        json_file = self.data_dir / f"{video_id}.json"
        new_comments = result.get('new_comments', [])
        
        if merge_with_existing and json_file.exists():
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    existing_comments = existing_data.get('comments', [])
                
                # Merge and deduplicate
                all_comments = existing_comments + new_comments
                unique_comments = {c['comment_id']: c for c in all_comments}.values()
                final_comments = sorted(unique_comments, key=lambda x: x['published_at'])
                
                logger.info(f"Merged: {len(existing_comments)} existing + {len(new_comments)} new = {len(final_comments)} total")
            except Exception as e:
                logger.error(f"Error merging comments: {e}")
                final_comments = new_comments
        else:
            final_comments = sorted(new_comments, key=lambda x: x['published_at'])
        
        # Save to file
        output_data = {
            'video_id': video_id,
            'video_title': result.get('video_title', ''),
            'channel_title': result.get('channel_title', ''),
            'category': result.get('category', ''),
            'total_comments': len(final_comments),
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'comments': final_comments
        }
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(final_comments)} comments to {json_file}")
    
    def close(self):
        """Cleanup and flush Kafka producer."""
        if self.kafka_producer:
            logger.info("Flushing Kafka producer...")
            self.kafka_producer.flush(timeout=10)
            logger.info(f"Kafka: Delivered {self.kafka_messages_sent} messages to topic raw_comments")


def load_videos_config(config_path: str = 'config/videos.yaml') -> Dict:
    """Load video configuration from YAML."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def main():
    """Main execution."""
    logger.info("=" * 70)
    logger.info("YouTube Comment Ingestion - Starting")
    logger.info("=" * 70)
    
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        logger.error("YOUTUBE_API_KEY not found in environment")
        return
    
    try:
        config = load_videos_config()
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return
    
    fetcher = CommentFetcher(api_key=api_key, enable_kafka=True)
    logger.info(f"CommentFetcher initialized")
    logger.info(f"Data directory: {fetcher.data_dir}")
    logger.info(f"Kafka streaming: enabled")
    
    total_new_comments = 0
    successful_videos = 0
    total_videos_attempted = 0
    
    try:
        for category_name, videos in config['categories'].items():
            if shutdown_flag:
                break
            
            logger.info(f"\n{'=' * 70}")
            logger.info(f"Processing category: {category_name}")
            logger.info(f"Videos in category: {len(videos)}")
            logger.info(f"{'=' * 70}")
            
            for video_entry in videos:
                if shutdown_flag:
                    break
                
                video_id = video_entry['video_id']
                total_videos_attempted += 1
                
                logger.info(f"\n--- Video {total_videos_attempted}: {video_id} ---")
                
                # Check for existing comments
                existing_comments, existing_ids = fetcher.get_existing_comments(video_id)
                
                if existing_comments:
                    logger.info(f"Found {len(existing_comments)} existing comments")
                    logger.info(f"Mode: Incremental (fetch only new comments)")
                else:
                    logger.info(f"No existing comments")
                    logger.info(f"Mode: Full fetch (get all historical comments)")
                
                # Fetch comments
                result = fetcher.fetch_video_comments(
                    video_id,
                    category=category_name,
                    existing_ids=existing_ids
                )
                
                if result.get('success'):
                    result['category'] = category_name
                    fetcher.save_comments(result, merge_with_existing=True)
                    
                    successful_videos += 1
                    new_count = len(result.get('new_comments', []))
                    total_new_comments += new_count
                    
                    logger.info(f"Success: Added {new_count} new comments")
                else:
                    logger.error(f"Failed: {result.get('error')}")
                
                time.sleep(1)  # Rate limiting
        
        logger.info(f"\n{'=' * 70}")
        logger.info(f"INGESTION COMPLETE")
        logger.info(f"{'=' * 70}")
        logger.info(f"Videos processed: {successful_videos}/{total_videos_attempted}")
        logger.info(f"New comments fetched: {total_new_comments:,}")
        logger.info(f"API quota used: {fetcher.quota_used}")
        logger.info(f"={'=' * 70}")
    
    finally:
        fetcher.close()


if __name__ == '__main__':
    main()
