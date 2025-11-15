import requests
import os
import csv
import yaml

api = os.getenv("api_key")

def load_video(yaml_file):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config['categories']

def get_comments(video_id, api_key):
    comments = []
    url = "https://www.googleapis.com/youtube/v3/commentThreads"

    params = {
        "part": "snippet",
        "videoId": video_id,
        "key": api_key,
        "maxResults": 100,
        "textFormat": "plainText"
    }

    while True:
        response = requests.get(url, params=params)
        data = response.json()

        if "items" not in data:
            print(f"No comments found or quota exceeded for: {video_id}")
            break

        for item in data["items"]:
            c = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "video_id": video_id,
                "author": c.get("authorDisplayName", ""),
                "text": c.get("textDisplay", ""),
                "published_at": c.get("publishedAt", "")
            })

        token = data.get("nextPageToken")
        if not token:
            break

        params["pageToken"] = token

    return comments

def save_to_csv(comments, filename):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["video_id", "author", "text", "published_at"])
        writer.writeheader()
        writer.writerows(comments)
    print(f"Saved {len(comments)} comments to {filename}")

def run_pipeline():
    categories = load_video("video.yml")

    for category, video_list in categories.items():
        print(f"\n=== Category: {category} ===")

        for item in video_list:
            video_id = item["video_id"]
            print(f"Scraping comments for video: {video_id}")

            comments = get_comments(video_id, api)

            output_filename = f"comments_{category}_{video_id}.csv"
            save_to_csv(comments, output_filename)


if __name__ == "__main__":
    run_pipeline()