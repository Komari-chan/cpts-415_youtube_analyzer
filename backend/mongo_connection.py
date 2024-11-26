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
