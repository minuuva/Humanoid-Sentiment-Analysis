import os
from prefect import flow, task
from dotenv import load_dotenv
from fetch_comments import load_videos_config, fetch_and_save_video_task
from process_comments import process_file_to_duckdb
from sentiment_analysis import analyze_video_sentiment

load_dotenv()

@task(name="Generate Summary Report")
def print_summary(results: list):
    """
    Prints a consolidated report of the pipeline run.
    """
    print("\n" + "="*60)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*60)
    
    total_processed = 0
    
    for res in results:
        # res might be None if a task failed and returned early
        if not res:
            continue
            
        vid = res.get('video_id', 'Unknown')
        count = res.get('processed', 0)
        stats = res.get('stats', {})
        
        print(f"Video: {vid}")
        print(f"  - New Comments Processed: {count}")
        if count > 0:
            print(f"  - Sentiment: {stats}")
        print("-" * 30)
        
        total_processed += count

    print(f"Total Comments Analyzed in this run: {total_processed}")
    print("="*60 + "\n")

@flow(name="Humanoid Sentiment Pipeline", log_prints=True)
def main_flow():
    # 1. Load Config
    config = load_videos_config()
    api_key = os.getenv("YOUTUBE_API_KEY")
    
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY is missing!")

    results = []

    # 2. Loop through categories and videos
    for category, videos in config['categories'].items():
        print(f"Processing Category: {category}")
        
        for video_entry in videos:
            
            # Fetch and Save Comments (Returns JSON file path)
            json_path = fetch_and_save_video_task(video_entry, category, api_key)
            
            # If fetch failed or no file saved, skip
            if not json_path:
                continue

            # Ingest to DuckDB (Returns video_id)
            video_id = process_file_to_duckdb(json_path)
            
            # Analyze (Returns stats)
            if video_id:
                analysis_result = analyze_video_sentiment(video_id)
                results.append(analysis_result)

    # 3. Final Report
    print_summary(results)

if __name__ == "__main__":
    main_flow().serve(
        name="daily-humanoid-fetch",
        cron="0 8 * * *", 
        tags=["production", "daily"]
    )