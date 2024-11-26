import pandas as pd

def fetch_video_data(collection):
    """
    Fetch relevant video data from MongoDB.
    :param collection: MongoDB collection
    :return: DataFrame containing video data
    """
    pipeline = [
        {
            "$project": {
                "video_id": 1,
                "views": 1,
                "rating": 1,
                "comments_count": 1,
                "category": 1,
                "length": 1,
                "age": 1  # Ensure 'age' is included for time-based analysis
            }
        }
    ]
    video_data = list(collection.aggregate(pipeline))
    df_videos = pd.DataFrame(video_data)

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

