"""
Sentiment Analysis for YouTube Comments
Analyzes sentiment using VADER and extracts key phrases using TF-IDF
Updates DuckDB in batches - optimized for speed and reliability
"""

import duckdb
import json
import logging
from datetime import datetime, timezone
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from rake_nltk import Rake
from prefect import task, get_run_logger

try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)


def init_analyzer():
    """Helper to get analyzer instances."""
    return SentimentIntensityAnalyzer()

def analyze_text_vader(sia, text: str) -> tuple:
    """Analyze sentiment of text using VADER."""
    if not text or not text.strip():
        return 'neutral', 0.0
    
    try:
        scores = sia.polarity_scores(text)
        compound = scores['compound']
        
        if compound >= 0.05:
            label = 'positive'
        elif compound <= -0.05:
            label = 'negative'
        else:
            label = 'neutral'
        return label, compound
    except Exception:
        return 'neutral', 0.0

def extract_phrases_rake(text: str) -> list:
    """Extract meaningful phrases using RAKE."""
    if not text or not text.strip():
        return []
    
    try:
        # Initialize RAKE
        rake = Rake(min_length=2, max_length=4)
        rake.extract_keywords_from_text(text)
        ranked_phrases = rake.get_ranked_phrases()
        
        filler_patterns = {
            'looks like', 'look like', 'feel like', 'feels like', 
            'seems like', 'seem like', 'sounds like', 'sound like',
            'really like', 'pretty much'
        }
        
        filtered_phrases = []
        seen = set()
        
        for phrase in ranked_phrases:
            p_lower = phrase.lower().strip()
            if p_lower in filler_patterns: continue
            if len(p_lower) < 5 or p_lower.replace(' ', '').isdigit(): continue
            
            # Simple deduplication
            if p_lower in seen: continue
            
            seen.add(p_lower)
            filtered_phrases.append(phrase)
            if len(filtered_phrases) >= 5: break
            
        return filtered_phrases
    except Exception:
        return []

@task(name="Analyze Sentiment", tags=["ml"])
def analyze_video_sentiment(video_id: str, db_path: str = 'data/youtube.duckdb'):
    """
    Analyzes sentiment ONLY for the specific video_id provided.
    Updates DuckDB in place.
    """
    logger = get_run_logger()
    
    if not video_id:
        logger.warning("No Video ID provided to analyzer.")
        return None

    conn = duckdb.connect(db_path)
    sia = init_analyzer()
    
    try:
        # 1. Select unprocessed comments for this specific video
        # We check for sentiment_label IS NULL to avoid re-work
        query = """
            SELECT comment_id, cleaned_text 
            FROM comments 
            WHERE video_id = ? AND (sentiment_label IS NULL OR phrases IS NULL)
        """
        rows = conn.execute(query, [video_id]).fetchall()
        
        if not rows:
            logger.info(f"No new comments to analyze for video {video_id}")
            return {'video_id': video_id, 'processed': 0}

        logger.info(f"Analyzing {len(rows)} comments for video {video_id}...")
        
        update_data = []
        
        # 2. Process in memory
        for comment_id, text in rows:
            # Fallback to empty string if text is None
            safe_text = text if text else ""
            
            label, score = analyze_text_vader(sia, safe_text)
            phrases = extract_phrases_rake(safe_text)
            
            update_data.append((
                label, 
                score, 
                json.dumps(phrases), 
                comment_id
            ))
            
        # 3. Batch Update
        conn.executemany("""
            UPDATE comments
            SET sentiment_label = ?,
                sentiment_score = ?,
                phrases = ?
            WHERE comment_id = ?
        """, update_data)
        
        # 4. Get Summary Stats for Reporting
        stats = conn.execute("""
            SELECT sentiment_label, COUNT(*) 
            FROM comments 
            WHERE video_id = ? 
            GROUP BY sentiment_label
        """, [video_id]).fetchall()
        
        stats_dict = {row[0]: row[1] for row in stats}
        
        logger.info(f"Analysis Complete for {video_id}. Stats: {stats_dict}")
        
        return {
            'video_id': video_id,
            'processed': len(rows),
            'stats': stats_dict
        }

    except Exception as e:
        logger.error(f"Error in sentiment analysis task: {e}")
        raise e
    finally:
        conn.close()