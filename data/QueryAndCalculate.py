import pymongo
import time

# MongoDB connection setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube_analyzer"]
collection = db["videos"]

# Example queries
def query_examples():
    queries = [
        # Find all videos in the "Comedy" category
        {"category": "Comedy"},
        # Find all videos with views greater than 1000
        {"views": {"$gt": 1000}},
        # Find all videos uploaded by a specific uploader
        {"uploader": "PIEGUYRULZ"},
        # Find all videos with a rating of 5.0
        {"rating": 5.0}
    ]

    total_time = 0
    for query in queries:
        start_time = time.time()  # Record start time
        result_count = collection.count_documents(query)  # Count the number of documents matching the query
        end_time = time.time()  # Record end time
        elapsed_time = end_time - start_time
        total_time += elapsed_time

        print(f"Query: {query}")
        print(f"Matched documents: {result_count}")
        print(f"Time taken: {elapsed_time:.4f} seconds\n")
    
    average_time = total_time / len(queries)
    print(f"Average query response time: {average_time:.4f} seconds")

# Run the queries
query_examples()
