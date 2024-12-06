from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, collect_list

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PageRankWithMongoDB") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/pagerank_db.pages") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/pagerank_db.pages") \
    .getOrCreate()

# Parameters
damping_factor = 0.85
iterations = 10

# Load data from MongoDB
df = spark.read.format("mongodb").load()

# Initialize PageRank
total_pages = df.count()
initial_rank = 1.0 / total_pages

df = df.withColumn("pagerank", lit(initial_rank))

for _ in range(iterations):
    # Explode links to compute contributions
    contributions = df.select(
        col("_id").alias("source"),
        explode(col("links")).alias("target"),
        col("pagerank") / col("links").cast("int").alias("contribution")
    )

    # Sum contributions for each target page
    new_ranks = contributions.groupBy("target").sum("contribution").withColumnRenamed("sum(contribution)", "new_pagerank")

    # Apply damping factor
    new_ranks = new_ranks.withColumn("pagerank", (1 - damping_factor) + damping_factor * col("new_pagerank"))

    # Join with original DataFrame to retain structure
    df = df.join(new_ranks.select("target", "pagerank"), df["_id"] == new_ranks["target"], "left_outer").drop("target")

    # Fill missing ranks (for pages with no inbound links)
    df = df.withColumn("pagerank", col("pagerank").fillna((1 - damping_factor)))

# Save results back to MongoDB
df.write.format("mongodb").mode("overwrite").save()