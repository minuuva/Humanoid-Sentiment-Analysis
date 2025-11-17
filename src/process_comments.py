"""
Kafka consumer that processes YouTube comments and loads into DuckDB.

Consumes from Kafka 'raw_comments' topic, enriches with sentiment analysis,
and stores in DuckDB for analytical queries.

WHY: Decouples data ingestion from processing. The consumer can run
independently, process at its own pace, and be restarted without re-fetching
from YouTube API. This is a core streaming architecture pattern.
"""

import os
import sys
import json
import time
import logging
import signal
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import duckdb
from confluent_kafka import Consumer, KafkaError, KafkaException

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('process_comments.log')
    ]
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
running = True


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global running
    logger.info("Shutdown signal received, finishing current batch...")
    running = False


def init_duckdb(db_path: str = 'data/youtube.duckdb'):
    """
    Initialize DuckDB database and create schema.
    
    WHY: DuckDB provides fast analytical queries over comment data.
    Embedded database means no server to manage.
    
    Args:
        db_path: Path to DuckDB database file
        
    Returns:
        DuckDB connection
    """
    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(db_path)
    
    # Create schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            comment_id VARCHAR PRIMARY KEY,
            video_id VARCHAR NOT NULL,
            category VARCHAR,
            author VARCHAR,
            text TEXT,
            like_count INTEGER,
            reply_count INTEGER,
            published_at TIMESTAMP,
            updated_at TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Sentiment fields (will add later with actual analysis)
            sentiment_label VARCHAR,
            sentiment_score FLOAT,
            
            -- Metadata
            ingested_at TIMESTAMP
        )
    """)
    
    logger.info(f"✅ DuckDB initialized at {db_path}")
    
    # Show current row count
    count = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
    logger.info(f"Current comments in database: {count:,}")
    
    return conn


def clean_text(text: str) -> str:
    """
    Clean comment text.
    
    WHY: Removes noise and normalizes text for better analysis.
    """
    if not text:
        return ""
    
    # Basic cleaning (will enhance later with more sophisticated cleaning)
    text = text.strip()
    text = ' '.join(text.split())  # Normalize whitespace
    
    return text


def insert_comment(conn, comment_data: dict):
    """
    Insert comment into DuckDB with upsert logic.
    
    WHY: Handles duplicate comments gracefully. If we re-process Kafka
    messages, we won't create duplicates.
    
    Args:
        conn: DuckDB connection
        comment_data: Comment dictionary from Kafka
    """
    try:
        # Clean text
        cleaned_text = clean_text(comment_data.get('text', ''))
        
        # For now, sentiment is NULL (will add VADER in next step)
        conn.execute("""
            INSERT INTO comments (
                comment_id, video_id, category, author, text,
                like_count, reply_count, published_at, updated_at,
                ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (comment_id) DO UPDATE SET
                like_count = EXCLUDED.like_count,
                reply_count = EXCLUDED.reply_count,
                updated_at = EXCLUDED.updated_at
        """, [
            comment_data['comment_id'],
            comment_data['video_id'],
            comment_data.get('category'),  # May be None on first pass
            comment_data['author'],
            cleaned_text,
            comment_data['like_count'],
            comment_data['reply_count'],
            comment_data['published_at'],
            comment_data['updated_at'],
            datetime.utcnow().isoformat()
        ])
        
    except Exception as e:
        logger.error(f"Failed to insert comment {comment_data.get('comment_id')}: {e}")


def consume_from_kafka(
    bootstrap_servers: str = 'localhost:9092',
    topic: str = 'raw_comments',
    group_id: str = 'duckdb-loader',
    db_path: str = 'data/youtube.duckdb'
):
    """
    Main consumer loop: Read from Kafka and write to DuckDB.
    
    WHY: Processes comments in real-time as they arrive in Kafka.
    Can be stopped and restarted without losing data (Kafka tracks offset).
    
    Args:
        bootstrap_servers: Kafka broker address
        topic: Kafka topic to consume from
        group_id: Consumer group ID (for offset tracking)
        db_path: Path to DuckDB database
    """
    # Initialize DuckDB
    conn = init_duckdb(db_path)
    
    # Configure Kafka consumer
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Start from beginning if no offset
        'enable.auto.commit': False,  # Manual commit for reliability
        'max.poll.interval.ms': 300000  # 5 minutes
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    
    logger.info(f"✅ Kafka consumer started, listening to topic: {topic}")
    logger.info(f"Consumer group: {group_id}")
    
    messages_processed = 0
    batch_size = 100
    batch = []
    
    try:
        while running:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No message, check if we should commit pending batch
                if batch:
                    logger.info(f"No new messages, committing batch of {len(batch)}")
                    conn.commit()  # DuckDB transaction commit
                    consumer.commit()  # Kafka offset commit
                    batch = []
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()}")
                else:
                    raise KafkaException(msg.error())
                continue
            
            # Decode message
            try:
                comment_data = json.loads(msg.value().decode('utf-8'))
                
                # Insert into DuckDB
                insert_comment(conn, comment_data)
                
                batch.append(comment_data)
                messages_processed += 1
                
                # Commit batch
                if len(batch) >= batch_size:
                    conn.commit()  # DuckDB transaction
                    consumer.commit()  # Kafka offset
                    logger.info(f"Processed {messages_processed} comments (batch committed)")
                    batch = []
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    
    finally:
        # Commit any remaining messages
        if batch:
            logger.info(f"Committing final batch of {len(batch)} messages")
            conn.commit()
            consumer.commit()
        
        logger.info(f"Total messages processed: {messages_processed}")
        consumer.close()
        conn.close()
        logger.info("✅ Consumer shutdown complete")


def main():
    """Main execution."""
    logger.info("=" * 70)
    logger.info("YouTube Comment Processor - Starting")
    logger.info("=" * 70)
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start consuming
    consume_from_kafka()


if __name__ == '__main__':
    main()

