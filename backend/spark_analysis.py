from pyspark.sql.functions import avg, col, date_sub, to_date, lit, desc, concat_ws
import os


def spark_analyze_data(spark, df_videos, output_folder):
    """
    Perform data analysis and save results to the output folder.
    """
    # Cache cleaned data as Parquet
    parquet_path = os.path.join(output_folder, "cached_cleaned_data.parquet")
    df_videos.write.mode("overwrite").parquet(parquet_path)

    # Use the cached data for subsequent analysis
    cleaned_data = spark.read.parquet(parquet_path)

    # Save Top-10 videos by views and ratings
    cleaned_data.orderBy(desc("views")).limit(10).write.mode("overwrite").csv(
        os.path.join(output_folder, "top_10_views"), header=True)
    cleaned_data.orderBy(desc("rating")).limit(10).write.mode("overwrite").csv(
        os.path.join(output_folder, "top_10_ratings"), header=True)

    # Compute category statistics
    category_stats = cleaned_data.groupBy("category").agg(
        avg("views").alias("avg_views"),
        avg("rating").alias("avg_rating")
    )
    category_stats.write.mode("overwrite").csv(
        os.path.join(output_folder, "category_stats"), header=True)

    # Compute trends over time
    cleaned_data = cleaned_data.withColumn(
        "upload_date", to_date(date_sub(to_date(lit("2008-01-01")), col("age")))
    )
    trends = cleaned_data.groupBy("upload_date").agg(
        avg("views").alias("avg_views"),
        avg("rating").alias("avg_rating")
    )
    trends.write.mode("overwrite").csv(
        os.path.join(output_folder, "trends"), header=True)


def analyze_related_videos(df_videos, output_folder):
    """
    Analyze related video data and save results to the output folder.
    """
    if "related_ids" in df_videos.columns:
        # Convert the array to a comma-separated string
        related_videos = df_videos.withColumn(
            "related_ids",
            concat_ws(",", col("related_ids")).alias("related_ids")
        ).withColumn(
            "related_ids",
            col("related_ids").cast("string").alias("related_ids")
        )
    else:
        print("`related_ids` column does not exist. Skipping analysis.")
        return

    # Save the analysis of related videos
    related_videos.write.csv(os.path.join(output_folder, "related_analysis"), header=True)
