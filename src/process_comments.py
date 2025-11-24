"""
Kafka consumer that processes YouTube comments and loads into DuckDB.

Consumes from Kafka 'raw_comments' topic, enriches with sentiment analysis,
and stores in DuckDB for analytical queries.

WHY: Decouples data ingestion from processing. The consumer can run
independently, process at its own pace, and be restarted without re-fetching
from YouTube API. This is a core streaming architecture pattern.
"""

import sys
import json
import logging
import duckdb
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List
from prefect import task, get_run_logger

def init_duckdb(db_path: str):
    """
    Initialize DuckDB database and create schema.
    """
    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(db_path)
    
    # Create schema - ADDED 'phrases JSON' here
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id VARCHAR PRIMARY KEY,
            video_id VARCHAR NOT NULL,
            category VARCHAR,
            author VARCHAR,
            text TEXT,
            cleaned_text TEXT, 
            like_count INTEGER,
            reply_count INTEGER,
            published_at TIMESTAMP,
            updated_at TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sentiment_label VARCHAR,
            sentiment_score FLOAT,
            phrases JSON,
            ingested_at TIMESTAMP
        )
    """)
    return conn

def clean_text(text: str) -> str:
    """
    Clean comment text for analysis.
    """
    if not text:
        return ""
    text = text.strip()
    text = ' '.join(text.split())
    return text

@task(name="Ingest to DuckDB", tags=["database"])
def process_file_to_duckdb(file_path: str, db_path: str = 'data/youtube.duckdb') -> str:
    """
    Reads a specific JSON file and upserts it into DuckDB.
    Returns the video_id processed.
    """
    logger = get_run_logger()
    
    path_obj = Path(file_path)
    if not path_obj.exists():
        logger.error(f"File not found: {file_path}")
        return None

    try:
        with open(path_obj, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load JSON {file_path}: {e}")
        return None
        
    comments = data.get('comments', [])
    video_id = data.get('video_id')
    category = data.get('category')
    
    if not comments:
        logger.info(f"No comments to process in {file_path}")
        return video_id

    conn = init_duckdb(db_path)
    records_processed = 0
    
    try:
        for comment in comments:
            raw_text = comment.get('text', '')
            cleaned = clean_text(raw_text)
            
            conn.execute("""
                INSERT INTO comments (
                    comment_id, video_id, category, author, text, cleaned_text,
                    like_count, reply_count, published_at, updated_at,
                    ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (comment_id) DO UPDATE SET
                    like_count = EXCLUDED.like_count,
                    reply_count = EXCLUDED.reply_count,
                    updated_at = EXCLUDED.updated_at,
                    cleaned_text = EXCLUDED.cleaned_text,
                    ingested_at = EXCLUDED.ingested_at
            """, [
                comment['comment_id'],
                video_id,
                category,
                comment['author'],
                raw_text,
                cleaned,
                comment['like_count'],
                comment.get('reply_count', 0),
                comment['published_at'],
                comment.get('updated_at'),
                datetime.now(timezone.utc).isoformat()
            ])
            records_processed += 1
            
        logger.info(f"Upserted {records_processed} comments for video {video_id} into DuckDB.")
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        raise e
    finally:
        conn.close()
        
    return video_id