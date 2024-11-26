import os
import shutil
import glob
import pandas as pd
from pyspark.sql import SparkSession
from spark_analysis import analyze_data, analyze_related_videos
from spark_visualization import (
    generate_visualizations,
    plot_top_videos,
    plot_related_video_analysis,
    plot_trends_views,
    plot_trends_ratings,
    plot_category_statistics,
    plot_trends_over_time
)


def clean_and_prepare_data(df_videos):
    """
    Cleans and prepares the data from MongoDB by filling null values and handling fields.

    :param df_videos: Spark DataFrame containing raw data from MongoDB.
    :return: Cleaned Spark DataFrame.
    """
    return df_videos.na.fill({
        "views": 0,
        "rating": 0,
        "comments_count": 0,
        "length": 0,
        "age": -1
    })


def merge_and_rename_spark_output(output_folder, file_name):
    """
    Merges Spark output files in a directory and saves them as a single CSV file.

    :param output_folder: Path to the output folder.
    :param file_name: Name of the file (without .csv extension) to process and rename.
    """
    # Path to the directory containing partition files
    part_folder = os.path.join(output_folder, file_name)
    output_file = os.path.join(output_folder, f"{file_name}.csv")

    if not os.path.exists(part_folder) or not os.path.isdir(part_folder):
        raise FileNotFoundError(f"Output directory not found: {part_folder}")

    # Find all part files (partitioned CSV files)
    part_files = glob.glob(os.path.join(part_folder, "part-*"))

    if not part_files:
        raise FileNotFoundError(f"No part files found in directory: {part_folder}")

    # Merge the partition files into a single CSV
    with open(output_file, "w") as merged_file:
        for idx, part_file in enumerate(sorted(part_files)):
            with open(part_file, "r") as pf:
                for line in pf:
                    # Skip the header for all but the first file
                    if idx > 0 and line.startswith("header_name1,header_name2"):  # Replace with actual column headers
                        continue
                    merged_file.write(line)

    # Clean up the original partition folder
    shutil.rmtree(part_folder, ignore_errors=True)
    print(f"Merged output saved to: {output_file}")


def main():
    """
    Main function to perform data analysis and generate visualizations.
    """
    # Define the output folder
    output_folder = "spark_output"

    # Clean the output folder if it exists
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder)

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("YouTubeDataAnalysis") \
        .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
        .config("spark.mongodb.read.database", "youtube_analyzer") \
        .config("spark.mongodb.read.collection", "videos") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.4.0,org.mongodb:bson:5.3.0") \
        .getOrCreate()

    try:
        print("Loading data from MongoDB...")
        df_videos = spark.read.format("mongodb").load()

        print("Cleaning and processing data...")
        df_videos = clean_and_prepare_data(df_videos)

        print("Schema after cleaning:")
        df_videos.printSchema()

        print("Sample data after cleaning:")
        df_videos.show(5, truncate=False)

        # Analyze data
        print("Analyzing data...")
        analyze_data(df_videos, output_folder)

        # Analyze related videos
        print("Analyzing related videos...")
        analyze_related_videos(df_videos, output_folder)

        # Merge Spark outputs
        print("Merging and renaming Spark output files...")
        merge_and_rename_spark_output(output_folder, "trends")
        merge_and_rename_spark_output(output_folder, "category_stats")
        merge_and_rename_spark_output(output_folder, "top_10_views")
        merge_and_rename_spark_output(output_folder, "related_analysis")
        merge_and_rename_spark_output(output_folder, "top_10_ratings")

        # Handle inconsistent CSV rows while reading
        print("Reading trends.csv with error handling...")
        try:
            trends_data = pd.read_csv(
                os.path.join(output_folder, "trends.csv"), 
                on_bad_lines='skip',  # Skip problematic rows
                engine='python'       # More robust CSV parsing
            )
        except Exception as e:
            print(f"Error reading trends.csv: {e}")
            return

        # Generate visualizations
        print("Generating visualizations...")
        generate_visualizations(df_videos, output_folder)

        trends_data = pd.read_csv(
            os.path.join(output_folder, "trends.csv"),
            on_bad_lines="skip",
            engine="python"
        )
        trends_data = trends_data.dropna()
        trends_data["upload_date"] = pd.to_datetime(trends_data["upload_date"])
        trends_data = trends_data.sort_values("upload_date")


        print("Generating additional visualizations...")
        plot_top_videos(output_folder)
        plot_related_video_analysis(output_folder)
        plot_trends_views(trends_data, output_folder)
        plot_trends_ratings(trends_data, output_folder)
        category_stats = pd.read_csv(f"{output_folder}/category_stats.csv")
        plot_category_statistics(category_stats, output_folder)
        # plot_trends_over_time(trends_data, output_folder)

        print(f"All results saved to '{output_folder}'.")
    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
