import os
import pymongo
import re

# MongoDB connection setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube_analyzer"]
collection = db["videos"]

def parse_file(file_path, crawl_date):
    videos = []
    with open(file_path, 'r') as f:
        for line in f:
            data = line.strip().split("\t")
            # Check if the line has at least 9 elements (video info without related videos)
            if len(data) < 9:
                # print(f"Skipping malformed line in {file_path}: {line}")
                continue
            
            video = {
                "video_id": data[0],
                "uploader": data[1],
                "age": int(data[2]),
                "category": data[3],
                "length": int(data[4]),
                "views": int(data[5]),
                "rating": float(data[6]),
                "ratings_count": int(data[7]),
                "comments_count": int(data[8]),
                "related_ids": data[9:] if len(data) > 9 else [],
                "crawl_date": crawl_date
            }
            videos.append(video)
    return videos

def extract_crawl_date(log_path):
    # Regular expression to match the date pattern (e.g., 080610) (YY\MM\DD)
    date_pattern = re.compile(r"start:\s+(\d{6})")
    try:
        with open(log_path, 'r') as log_file:
            for line in log_file:
                match = date_pattern.search(line)
                if match:
                    return match.group(1)  # Return the matched date
    except FileNotFoundError:
        print(f"Log file not found: {log_path}")
    
    return None

def load_data(directory_path):
    for root, dirs, files in os.walk(directory_path):
        for file in files:
            if file.endswith(".txt") and file != "log.txt":
                file_path = os.path.join(root, file)
                # Extract crawl_date from the log.txt file in the same folder
                log_path = os.path.join(root, "log.txt")
                crawl_date = extract_crawl_date(log_path)
                
                if not crawl_date:
                    print(f"Error reading crawl date from {log_path}. Skipping folder: {root}")
                    continue
                
                videos = parse_file(file_path, crawl_date)
                if videos:
                    collection.insert_many(videos)
                    print(f"Inserted {len(videos)} videos from {file_path}")

load_data(".\\data")
