import os
import pymongo
import re
import time

# MongoDB connection setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube_analyzer"]
collection = db["videos"]

# Ensure video_id is unique
collection.create_index("video_id", unique=True)

def parse_file(file_path, crawl_date):
    videos = []
    with open(file_path, 'r') as f:
        for line in f:
            data = line.strip().split("\t")
            # Check if the line has at least 9 elements (video info without related videos)
            if len(data) < 9:
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
    total_records = 0
    total_time = 0
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
                    start_time = time.time()  # Start time
                    inserted_count = 0
                    for video in videos:
                        try:
                            collection.insert_one(video)
                            inserted_count += 1
                        except pymongo.errors.DuplicateKeyError:
                            print(f"Duplicate video_id detected, skipping: {video['video_id']}")
                    end_time = time.time()  # End time
                    
                    elapsed_time = end_time - start_time
                    total_records += inserted_count
                    total_time += elapsed_time

                    print(f"Processed {file_path}")
                    print(f"Inserted {inserted_count} records in {elapsed_time:.2f} seconds")
                    print(f"Throughput: {inserted_count / elapsed_time:.2f} records/second\n")
    
    print(f"Total records inserted: {total_records}")
    print(f"Total time taken: {total_time:.2f} seconds")
    print(f"Overall throughput: {total_records / total_time:.2f} records/second" if total_time > 0 else "No records inserted.")

# Load data
load_data("./data")
