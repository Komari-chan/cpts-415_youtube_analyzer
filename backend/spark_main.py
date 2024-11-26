import os
import shutil
import glob
from pyspark.sql import SparkSession
from backend.spark_analysis import analyze_data, analyze_related_videos
from backend.spark_visualization import generate_visualizations

def clean_and_prepare_data(df_videos):
    """
    Cleans and prepares the data from MongoDB by filling null values and handling fields.

    :param df_videos: Spark DataFrame containing raw data from MongoDB.
    :return: Cleaned Spark DataFrame.
    """
    if "related_ids" in df_videos.columns:
        df_videos = df_videos.drop("related_ids")
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
    Entry point for Spark-based analysis.
    """
    # 定义路径
    root_dir = os.path.dirname(os.path.abspath(__file__))  # 根目录
    jar_dir = os.path.join(root_dir, "jar")  # jar 文件目录
    output_folder = os.path.join(root_dir, "output")  # 输出文件夹

    # 清理旧的 output 文件夹
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder, exist_ok=True)

    # Spark JAR 路径
    jar_files = ",".join([
        os.path.join(jar_dir, jar_name)
        for jar_name in ["mongo-spark-connector-10.4.0.jar", "bson-4.10.0.jar",
                         "mongodb-driver-core-4.10.0.jar", "mongodb-driver-sync-4.10.0.jar"]
    ])

    # 初始化 SparkSession
    spark = SparkSession.builder \
        .appName("YouTubeDataAnalysis") \
        .config("spark.jars", jar_files) \
        .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
        .config("spark.mongodb.read.database", "youtube_analyzer") \
        .config("spark.mongodb.read.collection", "videos") \
        .getOrCreate()

    try:
        print("Loading data from MongoDB...")
        df_videos = spark.read.format("mongodb").load()

        print("Cleaning and processing data...")
        df_videos = clean_and_prepare_data(df_videos)

        print("Schema after cleaning:")
        df_videos.printSchema()

        print("Analyzing data...")
        analyze_data(df_videos, output_folder)

        print("Analyzing related videos...")
        analyze_related_videos(df_videos, output_folder)

        print("Merging and renaming Spark output files...")
        for file_name in ["trends", "category_stats", "top_10_views", "related_analysis", "top_10_ratings"]:
            merge_and_rename_spark_output(output_folder, file_name)

        print("Generating visualizations...")
        generate_visualizations(df_videos, output_folder)

        print("Spark analysis completed successfully.")
    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
