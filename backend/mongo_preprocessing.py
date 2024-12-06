from pymongo import MongoClient

def get_mongo_collection(db_name, collection_name, uri="mongodb://localhost:27017/"):
    """
    Connect to MongoDB and return the specified collection.
    :param db_name: Name of the database
    :param collection_name: Name of the collection
    :param uri: MongoDB connection URI
    :return: Collection object
    """
    client = MongoClient(uri)
    db = client[db_name]
    return db[collection_name]

def fetch_filtered_data(collection, filters=None):
    """
    Fetch filtered data directly from MongoDB using aggregation pipeline.
    :param collection: MongoDB collection object
    :param filters: Dictionary containing filter criteria (e.g., views, ratings)
    :return: List of filtered documents
    """
    pipeline = []

    # Add filters to the pipeline
    if filters:
        match_stage = {"$match": {}}
        if "views" in filters:
            match_stage["$match"].update({"views": {"$gte": filters["views"][0], "$lte": filters["views"][1]}})
        if "rating" in filters:
            match_stage["$match"].update({"rating": {"$gte": filters["rating"][0], "$lte": filters["rating"][1]}})
        if "category" in filters:
            match_stage["$match"].update({"category": {"$in": filters["category"]}})
        pipeline.append(match_stage)

    # Projection to include only relevant fields
    projection_stage = {
        "$project": {
            "video_id": 1,
            "views": 1,
            "rating": 1,
            "comments_count": 1,
            "category": 1,
            "length": 1,
            "age": 1,
            "upload_date": {
                "$subtract": [
                    {"$dateFromString": {"dateString": "2008-01-01"}},
                    {"$multiply": ["$age", 86400000]}
                ]
            }
        }
    }
    pipeline.append(projection_stage)

    return list(collection.aggregate(pipeline))

def precompute_category_statistics(collection, output_collection_name):
    """
    Precompute and store average views and ratings by category into a new collection.
    :param collection: MongoDB collection object
    :param output_collection_name: Name of the output collection to store results
    """
    pipeline = [
        {
            "$group": {
                "_id": "$category",
                "average_views": {"$avg": "$views"},
                "average_rating": {"$avg": "$rating"}
            }
        },
        {
            "$out": output_collection_name  # Save results to a new collection
        }
    ]
    collection.aggregate(pipeline)

def bin_video_lengths(collection, output_collection_name):
    """
    Bin video lengths into predefined ranges and store the counts into a new collection.
    :param collection: MongoDB collection object
    :param output_collection_name: Name of the output collection to store results
    """
    pipeline = [
        {
            "$bucket": {
                "groupBy": "$length",
                "boundaries": [0, 60, 300, 600, 1200, 1800, 3600],
                "default": "Other",
                "output": {"count": {"$sum": 1}}
            }
        },
        {
            "$out": output_collection_name  # Save results to a new collection
        }
    ]
    collection.aggregate(pipeline)

def precompute_top_videos(collection, output_collection_name, top_n=10):
    """
    Precompute top-N videos by views, ratings, and comments and store results in a new collection.
    :param collection: MongoDB collection object
    :param output_collection_name: Name of the output collection to store results
    :param top_n: Number of top videos to retrieve
    """
    pipeline = [
        {
            "$project": {
                "video_id": 1,
                "views": 1,
                "rating": 1,
                "comments_count": 1
            }
        },
        {"$sort": {"views": -1}},  # Sort by views
        {"$limit": top_n},  # Get top-N
        {
            "$out": output_collection_name  # Save results to a new collection
        }
    ]
    collection.aggregate(pipeline)

if __name__ == "__main__":
    # Test case
    db_name = "youtube_analyzer"
    videos_collection_name = "videos"

    collection = get_mongo_collection(db_name, videos_collection_name)

    # Precompute statistics
    precompute_category_statistics(collection, "category_statistics")
    bin_video_lengths(collection, "video_length_bins")
    precompute_top_videos(collection, "top_videos", top_n=10)

    # Fetch filtered data
    filters = {
        "views": [1000, 100000],
        "rating": [4.0, 5.0],
        "category": ["Education", "Entertainment"]
    }
    filtered_data = fetch_filtered_data(collection, filters)
    print(f"Fetched {len(filtered_data)} filtered records.")
