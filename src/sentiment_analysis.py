"""
Sentiment Analysis for YouTube Comments
Analyzes sentiment using VADER and extracts key phrases using TF-IDF
Updates DuckDB in batches - optimized for speed and reliability
"""

import duckdb
import sys
import logging
import json
from datetime import datetime, timezone
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from rake_nltk import Rake

# Download VADER lexicon if not already
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

# Download stopwords for RAKE
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)

# Download punkt tokenizer for RAKE
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('sentiment_analysis.log')
    ]
)
logger = logging.getLogger(__name__)

# Initialize VADER analyzer
sia = SentimentIntensityAnalyzer()


def analyze_sentiment(text: str) -> tuple:
    """
    Analyze sentiment of text using VADER.
    
    Returns:
        (label, score): ('positive'|'negative'|'neutral', float)
    """
    if not text or not text.strip():
        return 'neutral', 0.0
    
    try:
        scores = sia.polarity_scores(text)
        compound = scores['compound']
        
        # Classify based on compound score
        if compound >= 0.05:
            label = 'positive'
        elif compound <= -0.05:
            label = 'negative'
        else:
            label = 'neutral'
        
        return label, compound
    except Exception as e:
        logger.warning(f"Error analyzing text: {e}")
        return 'neutral', 0.0


def extract_phrases_rake(text: str) -> list:
    """
    Extract meaningful phrases using RAKE algorithm.
    Filter out filler phrases and normalize similar phrases.
    
    Returns:
        List of top phrases (strings)
    """
    if not text or not text.strip():
        return []
    
    try:
        # Initialize RAKE - require at least 2 words for more meaningful phrases
        rake = Rake(
            min_length=2,  # Minimum 2 words (filters out single words like "like")
            max_length=4   # Up to 4-word phrases
        )
        rake.extract_keywords_from_text(text)
        
        # Get ranked phrases
        ranked_phrases = rake.get_ranked_phrases()
        
        # Filler phrases to filter out (common meaningless patterns)
        filler_patterns = {
            'looks like', 'look like', 'feel like', 'feels like', 
            'seems like', 'seem like', 'sounds like', 'sound like',
            'kinda like', 'kind like', 'sorta like', 'sort like',
            'looks pretty', 'look pretty', 'pretty much', 'pretty cool',
            'really like', 'really cool', 'really good', 'really nice',
            'thats like', 'thats pretty', 'going like', 'walks like',
            'looks good', 'look good', 'looks bad', 'look bad'
        }
        
        # Filter and normalize phrases
        filtered_phrases = []
        seen_normalized = set()
        
        for phrase in ranked_phrases:
            phrase_lower = phrase.lower().strip()
            
            # Skip filler phrases
            if phrase_lower in filler_patterns:
                continue
            
            # Skip if too short or just numbers
            if len(phrase_lower) < 5 or phrase_lower.replace(' ', '').isdigit():
                continue
            
            # Normalize: remove trailing 's' for deduplication
            # "humanoid robot" and "humanoid robots" → both become "humanoid robot"
            words = phrase_lower.split()
            normalized_words = []
            for word in words:
                # Remove plural 's' but keep words like "this", "was", "has"
                if word.endswith('s') and len(word) > 3 and word not in {'this', 'its', 'was', 'has', 'does', 'boss', 'less', 'pass', 'mass', 'class'}:
                    normalized_words.append(word[:-1])
                else:
                    normalized_words.append(word)
            normalized = ' '.join(normalized_words)
            
            # Skip if we've seen this normalized phrase already
            if normalized in seen_normalized:
                continue
            
            seen_normalized.add(normalized)
            filtered_phrases.append(phrase)
            
            # Return top 5 unique, meaningful phrases
            if len(filtered_phrases) >= 5:
                break
        
        return filtered_phrases
    
    except Exception as e:
        logger.warning(f"Error extracting phrases: {e}")
        return []


def process_batch(conn, batch_data: list):
    """
    Update a batch of comments in DuckDB.
    
    Args:
        conn: DuckDB connection
        batch_data: List of (label, score, phrases_json, comment_id) tuples
    """
    try:
        # Use executemany for batch updates
        conn.executemany(
            """
            UPDATE comments
            SET sentiment_label = ?,
                sentiment_score = ?,
                phrases = ?
            WHERE comment_id = ?
            """,
            batch_data
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Error updating batch: {e}")
        conn.rollback()
        raise


def main():
    """Main sentiment analysis pipeline."""
    logger.info("=" * 70)
    logger.info("Sentiment Analysis - Starting")
    logger.info("=" * 70)
    
    # Connect to DuckDB
    db_path = 'data/youtube.duckdb'
    conn = duckdb.connect(db_path)
    logger.info(f"Connected to {db_path}")
    
    try:
        # Count total unprocessed comments (missing sentiment OR phrases)
        total_unprocessed = conn.execute(
            "SELECT COUNT(*) FROM comments WHERE sentiment_label IS NULL OR phrases IS NULL"
        ).fetchone()[0]
        
        logger.info(f"Found {total_unprocessed:,} comments without sentiment/phrases")
        
        if total_unprocessed == 0:
            logger.info("All comments already processed!")
            return
        
        # Fetch unprocessed comments
        logger.info("Fetching unprocessed comments...")
        comments = conn.execute("""
            SELECT comment_id, text
            FROM comments
            WHERE sentiment_label IS NULL OR phrases IS NULL
        """).fetchall()
        
        # Process in batches
        batch_size = 1000
        batch_texts = []
        batch_ids = []
        processed_count = 0
        start_time = datetime.now(timezone.utc)
        
        logger.info(f"Processing {len(comments):,} comments in batches of {batch_size}")
        
        for comment_id, text in comments:
            batch_ids.append(comment_id)
            batch_texts.append(text)
            
            # Process batch when full
            if len(batch_texts) >= batch_size:
                try:
                    # Analyze sentiment and extract phrases for each comment
                    update_data = []
                    for cid, txt in zip(batch_ids, batch_texts):
                        label, score = analyze_sentiment(txt)
                        phrases = extract_phrases_rake(txt)
                        phrases_json = json.dumps(phrases)
                        update_data.append((label, score, phrases_json, cid))
                    
                    process_batch(conn, update_data)
                    processed_count += len(update_data)
                    
                    # Progress logging
                    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                    rate = processed_count / elapsed if elapsed > 0 else 0
                    remaining = len(comments) - processed_count
                    eta_seconds = remaining / rate if rate > 0 else 0
                    
                    logger.info(
                        f"Progress: {processed_count:,}/{len(comments):,} "
                        f"({processed_count/len(comments)*100:.1f}%) | "
                        f"Rate: {rate:.0f} comments/sec | "
                        f"ETA: {eta_seconds:.0f}s"
                    )
                    
                    batch_texts = []
                    batch_ids = []
                
                except Exception as e:
                    logger.error(f"Error processing batch: {e}")
                    batch_texts = []
                    batch_ids = []
                    continue
        
        # Process remaining batch
        if batch_texts:
            try:
                update_data = []
                for cid, txt in zip(batch_ids, batch_texts):
                    label, score = analyze_sentiment(txt)
                    phrases = extract_phrases_rake(txt)
                    phrases_json = json.dumps(phrases)
                    update_data.append((label, score, phrases_json, cid))
                
                process_batch(conn, update_data)
                processed_count += len(update_data)
                logger.info(f"Processed final batch: {len(update_data)} comments")
            except Exception as e:
                logger.error(f"Error processing final batch: {e}")
        
        # Final statistics
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info("=" * 70)
        logger.info("SENTIMENT ANALYSIS COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Total processed: {processed_count:,} comments")
        logger.info(f"Time elapsed: {elapsed:.1f} seconds")
        logger.info(f"Average rate: {processed_count/elapsed:.0f} comments/sec")
        
        # Show sentiment distribution
        logger.info("\nSentiment Distribution:")
        distribution = conn.execute("""
            SELECT 
                sentiment_label,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
            FROM comments
            WHERE sentiment_label IS NOT NULL
            GROUP BY sentiment_label
            ORDER BY count DESC
        """).fetchall()
        
        for label, count, pct in distribution:
            logger.info(f"  {label:10s}: {count:6,} ({pct:5.1f}%)")
        
        logger.info("=" * 70)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise
    
    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    main()
