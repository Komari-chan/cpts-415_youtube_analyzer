import pymongo
import time

# MongoDB connection setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube_analyzer"]
collection = db["videos"]

# Example queries
def query_examples():
    queries = [
        {"category": "Comedy"},
        {"views": {"$gt": 1000}},
        {"uploader": "PIEGUYRULZ"},
        {"rating": 5.0}
    ]

    total_time = 0
    for query in queries:
        start_time = time.time()
        result_count = collection.count_documents(query)
        end_time = time.time()
        elapsed_time = end_time - start_time
        total_time += elapsed_time

        print(f"Query: {query}")
        print(f"Matched documents: {result_count}")
        print(f"Time taken: {elapsed_time:.4f} seconds\n")
    
    average_time = total_time / len(queries)
    print(f"Average query response time: {average_time:.4f} seconds")

# Optimize indexes for queries
collection.create_index("category")
collection.create_index("views")
collection.create_index("uploader")
collection.create_index("rating")

# Run the queries
query_examples()
