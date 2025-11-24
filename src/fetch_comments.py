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
import socket
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional
from prefect import task, get_run_logger
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

class CommentFetcher:
    """Fetch YouTube comments with Kafka streaming."""
    
    def __init__(self, api_key: str, data_dir: str = 'data/raw', enable_kafka: bool = True):
        # We use get_run_logger() inside tasks, but if initialized outside a task (testing), fallback to print
        try:
            self.logger = get_run_logger()
        except:
            import logging
            self.logger = logging.getLogger("CommentFetcher")

        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.enable_kafka = enable_kafka
        self.kafka_producer = None
        
        if enable_kafka:
            try:
                self.kafka_producer = Producer({
                    'bootstrap.servers': 'localhost:9092',
                    'client.id': socket.gethostname(),
                    'acks': 'all',
                    'retries': 3
                })
            except Exception as e:
                self.logger.warning(f"Failed to initialize Kafka: {e}. Continuing without streaming.")
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
        except Exception as e:
            self.logger.error(f"Kafka send error: {e}")
    
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
            self.logger.error(f"Error loading {json_file}: {e}")
            return [], set()
    
    def fetch_video_comments(self, video_id: str, category: str = None) -> Dict:
        """
        Fetch comments for a video. Handles incremental logic internally.
        """
        existing_comments, existing_ids = self.get_existing_comments(video_id)
        self.logger.info(f"Fetching video: {video_id} | Existing comments: {len(existing_ids)}")
        
        try:
            video_response = self.youtube.videos().list(
                part='snippet,statistics',
                id=video_id
            ).execute()
            
            if not video_response['items']:
                return {'success': False, 'error': 'Video not found', 'video_id': video_id}
            
            video_info = video_response['items'][0]
            video_title = video_info['snippet']['title']
            channel_title = video_info['snippet']['channelTitle']
            
            is_incremental = len(existing_ids) > 0
            new_comments = []
            next_page_token = None
            consecutive_empty_pages = 0
            MAX_EMPTY_PAGES = 3
            
            while True:
                api_params = {
                    'part': 'snippet',
                    'videoId': video_id,
                    'maxResults': 100,
                    'order': 'time',
                    'textFormat': 'plainText'
                }
                
                if next_page_token:
                    api_params['pageToken'] = next_page_token
                
                comments_response = self.youtube.commentThreads().list(**api_params).execute()
                page_comments = []
                
                for item in comments_response['items']:
                    top_comment = item['snippet']['topLevelComment']['snippet']
                    comment_id = item['id']
                    
                    if comment_id in existing_ids:
                        continue
                    
                    comment_data = {
                        'comment_id': comment_id,
                        'video_id': video_id,
                        'category': category,
                        'author': top_comment['authorDisplayName'],
                        'text': top_comment['textDisplay'],
                        'like_count': top_comment['likeCount'],
                        'published_at': top_comment['publishedAt'],
                        'reply_count': item['snippet']['totalReplyCount']
                    }
                    
                    page_comments.append(comment_data)
                    self.send_to_kafka(comment_data)
                
                new_comments.extend(page_comments)
                
                if is_incremental:
                    if not page_comments:
                        consecutive_empty_pages += 1
                        if consecutive_empty_pages >= MAX_EMPTY_PAGES:
                            self.logger.info(f"Stopping early: {MAX_EMPTY_PAGES} empty pages (Incremental mode).")
                            break
                    else:
                        consecutive_empty_pages = 0
                
                next_page_token = comments_response.get('nextPageToken')
                if not next_page_token:
                    break
                
                time.sleep(0.2) 
            
            return {
                'success': True,
                'video_id': video_id,
                'video_title': video_title,
                'channel_title': channel_title,
                'category': category,
                'new_comments': new_comments,
                'existing_comments': existing_comments
            }
        
        except HttpError as e:
            if e.resp.status == 403:
                self.logger.error(f"Quota exceeded or disabled for {video_id}")
                return {'success': False, 'error': 'Quota/Disabled', 'video_id': video_id}
            raise e
        except Exception as e:
            self.logger.error(f"Error fetching {video_id}: {e}")
            return {'success': False, 'error': str(e), 'video_id': video_id}

    def save_result(self, result: Dict):
        """Merges new comments with existing ones and saves to disk."""
        if not result['success']:
            return None
            
        video_id = result['video_id']
        new_comments = result['new_comments']
        existing_comments = result.get('existing_comments', [])
        
        all_comments = existing_comments + new_comments
        final_comments = sorted(all_comments, key=lambda x: x['published_at'])
        
        output_data = {
            'video_id': video_id,
            'video_title': result.get('video_title'),
            'channel_title': result.get('channel_title'),
            'category': result.get('category'),
            'total_comments': len(final_comments),
            'last_updated': datetime.now(timezone.utc).isoformat(),
            'comments': final_comments
        }
        
        json_file = self.data_dir / f"{video_id}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Saved {video_id}: {len(new_comments)} new comments.")
        return str(json_file)

    def close(self):
        if self.kafka_producer:
            self.kafka_producer.flush(timeout=5)


@task(name="Load Video Config", tags=["config"])
def load_videos_config() -> Dict:
    """Load video configuration from YAML."""
    # Assuming config is in ../config/videos.yaml relative to this script
    current_dir = Path(__file__).parent.resolve()
    config_path = current_dir.parent / "config" / "videos.yaml"

    if not config_path.exists():
        # Fallback for different folder structures
        config_path = Path("config/videos.yaml")

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

@task(name="Fetch Video", retries=2, retry_delay_seconds=10)
def fetch_and_save_video_task(video_entry: Dict, category: str, api_key: str):
    """
    Atomic task to fetch and save comments for ONE video.
    Returns the path to the saved JSON file.
    """
    video_id = video_entry['video_id']
    
    # Initialize fetcher inside the task
    fetcher = CommentFetcher(api_key=api_key, enable_kafka=True)
    
    try:
        result = fetcher.fetch_video_comments(video_id, category=category)
        file_path = fetcher.save_result(result)
        return file_path
    finally:
        fetcher.close()