import pandas as pd

import pandas as pd

def fetch_video_data(collection, progress_callback=None):
    """
    Fetch relevant video data from MongoDB with optional progress updates.
    """
    pipeline = [
        {"$project": {"video_id": 1, "views": 1, "rating": 1, "comments_count": 1, "category": 1, "length": 1, "age": 1}}
    ]
    video_data = list(collection.aggregate(pipeline))
    total_records = len(video_data)

    data = []
    last_progress = 0  # Track the last emitted progress percentage
    for i, record in enumerate(video_data):
        data.append(record)
        
        if progress_callback and total_records > 0:
            current_progress = int((i + 1) / total_records * 100)
            if current_progress > last_progress:  # Emit progress every 1%
                last_progress = current_progress
                progress_callback(current_progress)

    df_videos = pd.DataFrame(data)
    
    # Replace invalid or missing values with defaults
    df_videos["views"] = pd.to_numeric(df_videos["views"], errors="coerce").fillna(0).astype(int)
    df_videos["rating"] = pd.to_numeric(df_videos["rating"], errors="coerce").fillna(0)
    df_videos["comments_count"] = pd.to_numeric(df_videos["comments_count"], errors="coerce").fillna(0).astype(int)
    df_videos["length"] = pd.to_numeric(df_videos["length"], errors="coerce").fillna(0).astype(int)
    df_videos["age"] = pd.to_numeric(df_videos["age"], errors="coerce").fillna(-1).astype(int)
    
    return df_videos



def compute_statistics(df_videos, output_folder):
    """
    Compute basic statistics for video data and save to CSV.
    :param df_videos: DataFrame containing video data
    :param output_folder: Path to the output folder
    :return: Dictionary of computed statistics
    """
    stats = {
        "Total Videos": len(df_videos),
        "Average Views": df_videos["views"].mean(),
        "Median Views": df_videos["views"].median(),
        "Top Rated Video": df_videos.loc[df_videos["rating"].idxmax()].to_dict(),
        "Most Watched Video": df_videos.loc[df_videos["views"].idxmax()].to_dict()
    }
    
    # Save statistics to a CSV file
    stats_df = pd.DataFrame.from_dict(stats, orient='index', columns=['Value'])
    stats_df.to_csv(f"{output_folder}/statistics.csv", index=True)
    
    return stats

def compute_top_videos(df_videos, output_folder, top_n=10):
    """
    Compute and save Top-N videos by views, ratings, and comments.
    :param df_videos: DataFrame containing video data
    :param output_folder: Path to the output folder
    :param top_n: Number of top videos to retrieve
    """
    # Top-N by views
    top_views = df_videos.nlargest(top_n, "views")
    top_views.to_csv(f"{output_folder}/top_views.csv", index=False)

    # Top-N by rating
    top_ratings = df_videos.nlargest(top_n, "rating")
    top_ratings.to_csv(f"{output_folder}/top_ratings.csv", index=False)

    # Top-N by comments
    top_comments = df_videos.nlargest(top_n, "comments_count")
    top_comments.to_csv(f"{output_folder}/top_comments.csv", index=False)

