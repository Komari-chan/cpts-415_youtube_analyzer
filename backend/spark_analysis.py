from pyspark.sql.functions import avg, col, date_sub, to_date, lit, desc, concat_ws

def analyze_data(df_videos, output_folder):
    """
    Perform data analysis and save results to the output folder.
    """
    # Convert `related_ids` column to a comma-separated string with double quotes
    if "related_ids" in df_videos.columns:
        df_videos = df_videos.withColumn(
            "related_ids",
            concat_ws(",", col("related_ids")).alias("related_ids")
        ).withColumn(
            "related_ids",
            col("related_ids").cast("string").alias("related_ids")
        )

    # Save Top-10 videos by views and ratings
    df_videos.orderBy(desc("views")).limit(10).write.csv(f"{output_folder}/top_10_views", header=True)
    df_videos.orderBy(desc("rating")).limit(10).write.csv(f"{output_folder}/top_10_ratings", header=True)

    # Compute category statistics
    category_stats = df_videos.groupBy("category").agg(
        avg("views").alias("avg_views"),
        avg("rating").alias("avg_rating")
    )
    category_stats.write.csv(f"{output_folder}/category_stats", header=True)

    # Compute trends over time
    df_videos = df_videos.withColumn(
        "upload_date", to_date(date_sub(to_date(lit("2008-01-01")), col("age")))
    )
    trends = df_videos.groupBy("upload_date").agg(
        avg("views").alias("avg_views"),
        avg("rating").alias("avg_rating")
    )
    trends.write.csv(f"{output_folder}/trends", header=True)


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
    related_videos.write.csv(f"{output_folder}/related_analysis", header=True)
