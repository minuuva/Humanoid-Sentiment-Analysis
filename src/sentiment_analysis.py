import duckdb
import sys
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import logging

# Download VADER lexicon if not already
nltk.download('vader_lexicon')

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler('process_comments.log')]
)
logger = logging.getLogger(__name__)

running = True

# Initialize Sentiment Analyzer
sia = SentimentIntensityAnalyzer()

def analyze_sentiment(text: str):
    """
    Analyze sentiment of the given text using VADER.
    """
    if not text:
        return None, 0.0
    scores = sia.polarity_scores(text)
    compound = scores['compound']
    if compound >= 0.05:
        label = 'positive'
    elif compound <= -0.05:
        label = 'negative'
    else:
        label = 'neutral'
    return label, compound 

def main():
    # Connect to DuckDB
    conn = duckdb.connect('data/youtube.duckdb')
    # Example query to fetch unprocessed comments
    comments = conn.execute("SELECT comment_id, text FROM comments WHERE sentiment_label IS NULL").fetchall()

    logger.info(f"Processing {len(comments)} comments for sentiment analysis.")

    for comment_id, text in comments:
        label, score = analyze_sentiment(text)
        conn.execute("""
            UPDATE comments
            SET sentiment_label = ?, sentiment_score = ?
            WHERE comment_id = ?
        """, (label, score, comment_id))
        logger.info(f"Processed comment {comment_id}: {label} ({score})")

    conn.close()
    logger.info("Sentiment analysis completed.")

if __name__ == "__main__":
    main()