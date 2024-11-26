from pymongo import MongoClient
from pyspark.sql import SparkSession

def fetch_mongo_data():
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017")
    db = client["youtube_analyzer"]
    collection = db["videos"]

    # Fetch data
    data = list(collection.find())

    # Convert to a Spark DataFrame
    spark = SparkSession.builder.appName("YouTubeDataAnalysis").getOrCreate()
    df = spark.read.json(spark.sparkContext.parallelize(data))
    return df

def main():
    df = fetch_mongo_data()
    df.show(5)

if __name__ == "__main__":
    main()
