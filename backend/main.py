import os
import shutil
from mongo_connection import get_mongo_collection
from data_analysis import fetch_video_data, compute_statistics
from visualization import plot_trend_of_views_and_ratings, plot_correlation_table, plot_views_distribution, plot_category_statistics, compute_top_videos, plot_views_by_length_range, plot_combined_trend_with_dual_axes

def main():
    # Set up the output folder
    output_folder = "output"

    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder, exist_ok=True)

    # Connect to MongoDB
    collection = get_mongo_collection("youtube_analyzer", "videos")
    
    # Step 1: Fetch video data
    print("Fetching video data...")
    df_videos = fetch_video_data(collection)
    print(f"Fetched {len(df_videos)} video records.")

    # Step 2: Compute and save statistics
    print("Computing statistics...")
    compute_statistics(df_videos, output_folder)

    # Step 3: Generate visualizations
    print("Generating visualizations...")
    plot_views_distribution(df_videos, output_folder)
    plot_views_by_length_range(df_videos, output_folder)
    plot_category_statistics(df_videos, output_folder)
    plot_correlation_table(df_videos, output_folder)
    plot_trend_of_views_and_ratings(df_videos, output_folder)
    plot_combined_trend_with_dual_axes(df_videos, output_folder)

    # Step 4: Compute and save Top-N videos
    print("Saving Top-N video data...")
    compute_top_videos(df_videos, output_folder)

    print(f"All results saved to the '{output_folder}' folder.")
    
    
if __name__ == "__main__":
    main()
