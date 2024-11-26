import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

def plot_views_distribution(df_videos, output_folder):
    """
    Plot distribution of video views and save to file.
    :param df_videos: DataFrame containing video data
    :param output_folder: Path to the output folder
    """
    plt.figure(figsize=(10, 6))
    sns.histplot(df_videos["views"], bins=50, kde=True, log_scale=True)
    plt.title("Distribution of Video Views")
    plt.xlabel("Views (Log Scale)")
    plt.ylabel("Frequency")
    plt.savefig(f"{output_folder}/views_distribution.png")
    plt.close()

def plot_category_statistics(df_videos, output_folder):
    """
    Plot average views and ratings by category and save to files.
    :param df_videos: DataFrame containing video data
    :param output_folder: Path to the output folder
    """
    # Filter out empty or null categories
    df_videos = df_videos[df_videos["category"].notnull()]
    category_stats = df_videos.groupby("category").agg({
        "views": "mean",
        "rating": "mean"
    }).sort_values("views", ascending=False)

    # Plot average views by category
    plt.figure(figsize=(15, 8))  # Increase figure size for better label spacing
    category_stats["views"].plot(kind="bar", color="skyblue", alpha=0.7)
    plt.title("Average Views by Category")
    plt.xlabel("Category")
    plt.ylabel("Average Views")
    plt.xticks(rotation=45, fontsize=10, ha='right')  # Rotate labels and align them to the right
    plt.tight_layout()  # Ensure everything fits within the figure
    plt.savefig(f"{output_folder}/category_views.png")
    plt.close()

    # Plot average ratings by category
    plt.figure(figsize=(15, 8))  # Increase figure size for better label spacing
    category_stats["rating"].plot(kind="bar", color="orange", alpha=0.7)
    plt.title("Average Ratings by Category")
    plt.xlabel("Category")
    plt.ylabel("Average Rating")
    plt.xticks(rotation=45, fontsize=10, ha='right')  # Rotate labels and align them to the right
    plt.tight_layout()  # Ensure everything fits within the figure
    plt.savefig(f"{output_folder}/category_ratings.png")
    plt.close()

def compute_top_videos(df_videos, output_folder, top_n=10):
    """
    Compute and save Top-N videos by views, ratings, and comments.
    :param df_videos: DataFrame containing video data
    :param output_folder: Path to the output folder
    :param top_n: Number of top videos to retrieve
    """
    # Top-N by views
    top_views = df_videos.nlargest(top_n, "views")
    top_views.to_csv(f"{output_folder}/top_{top_n}_views.csv", index=False)

    # Top-N by rating
    top_ratings = df_videos.nlargest(top_n, "rating")
    top_ratings.to_csv(f"{output_folder}/top_{top_n}_ratings.csv", index=False)

    # Top-N by comments
    top_comments = df_videos.nlargest(top_n, "comments_count")
    top_comments.to_csv(f"{output_folder}/top_{top_n}_comments.csv", index=False)

def plot_views_by_length_range(df_videos, output_folder):
    """
    Plot video views by binned video length ranges using a box plot and filter out outliers.
    :param df_videos: DataFrame containing video data
    :param output_folder: Path to the output folder
    """
    # Filter out invalid or extreme lengths
    df_videos = df_videos[(df_videos["length"] > 0) & (df_videos["length"] < 3600)]

    # Filter out views outliers (retain 1st to 99th percentile)
    lower_bound = df_videos["views"].quantile(0.01)
    upper_bound = df_videos["views"].quantile(0.99)
    df_videos = df_videos[(df_videos["views"] >= lower_bound) & (df_videos["views"] <= upper_bound)]

    # Bin video lengths into ranges
    df_videos["length_range"] = pd.cut(
        df_videos["length"],
        bins=[0, 60, 300, 600, 1200, 1800, 3600],
        labels=["0-1 min", "1-5 min", "5-10 min", "10-20 min", "20-30 min", "30-60 min"]
    )

    # Create a violin plot for views across length ranges
    plt.figure(figsize=(10, 6))
    sns.violinplot(x="length_range", y="views", data=df_videos, density_norm="width")  # Updated parameter
    plt.title("Video Views by Length Range (Filtered Outliers)")
    plt.xlabel("Video Length Range")
    plt.ylabel("Views (Filtered)")
    plt.yscale("log")  # Use log scale for better visualization
    plt.tight_layout()
    plt.savefig(f"{output_folder}/views_by_length_range_filtered.png")
    plt.close()

def plot_correlation_table(df_videos, output_folder):
    """
    Display top correlations between numerical fields in a table format.
    :param df_videos: DataFrame containing video data
    :param output_folder: Path to the output folder
    """
    # Calculate correlation matrix
    correlation = df_videos[["views", "rating", "comments_count", "length"]].corr()

    # Extract upper triangle (avoid duplicate pairs)
    corr_pairs = correlation.where(
        np.triu(np.ones(correlation.shape), k=1).astype(bool)
    ).stack().reset_index()
    corr_pairs.columns = ["Feature 1", "Feature 2", "Correlation"]

    # Sort correlations by absolute value
    corr_pairs["Absolute Correlation"] = corr_pairs["Correlation"].abs()
    corr_pairs = corr_pairs.sort_values("Absolute Correlation", ascending=False)

    # Create a simple text-based table visualization
    plt.figure(figsize=(10, 5))
    plt.axis("tight")
    plt.axis("off")
    table = plt.table(
        cellText=corr_pairs.values,
        colLabels=corr_pairs.columns,
        loc="center",
        cellLoc="center"
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.auto_set_column_width(col=list(range(len(corr_pairs.columns))))
    plt.title("Top Correlations Between Features")
    plt.savefig(f"{output_folder}/top_correlations_table.png")
    plt.close()

def plot_combined_trend_with_dual_axes(df_videos, output_folder):
    """
    Plot combined trends of average views and ratings over time using dual y-axes.
    :param df_videos: DataFrame containing video data
    :param output_folder: Path to the output folder
    """
    # Filter valid 'age' data and compute upload date
    df_videos = df_videos[df_videos["age"].notnull() & (df_videos["age"] >= 0)]
    df_videos["upload_date"] = pd.to_datetime("2008-01-01") - pd.to_timedelta(df_videos["age"], unit="d")

    # Group by monthly upload date and calculate averages
    trend_data = df_videos.groupby(df_videos["upload_date"].dt.to_period("M")).agg({
        "views": "mean",
        "rating": "mean",
    })
    trend_data.index = trend_data.index.to_timestamp()

    # Create the figure and axis
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Plot average views on the left y-axis
    ax1.plot(trend_data.index, trend_data["views"], color="blue", label="Average Views")
    ax1.set_xlabel("Time")
    ax1.set_ylabel("Average Views", color="blue")
    ax1.tick_params(axis="y", labelcolor="blue")
    ax1.set_title("Trend of Average Views and Ratings Over Time (Dual Axes)")

    # Create a second y-axis for ratings
    ax2 = ax1.twinx()
    ax2.plot(trend_data.index, trend_data["rating"], color="orange", label="Average Ratings")
    ax2.set_ylabel("Average Ratings", color="orange")
    ax2.tick_params(axis="y", labelcolor="orange")

    # Add legends for both y-axes
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")

    # Save the figure
    plt.tight_layout()
    plt.savefig(f"{output_folder}/trend_combined_dual_axes.png")
    plt.close()

def plot_trend_of_views_and_ratings(df_videos, output_folder):
    """
    Plot trends of average views and average ratings over time in separate and combined graphs.
    :param df_videos: DataFrame containing video data
    :param output_folder: Path to the output folder
    """
    # Filter valid 'age' data and compute upload date
    df_videos = df_videos[df_videos["age"].notnull() & (df_videos["age"] >= 0)]
    df_videos["upload_date"] = pd.to_datetime("2008-01-01") - pd.to_timedelta(df_videos["age"], unit="d")

    # Group by monthly upload date and calculate averages
    trend_data = df_videos.groupby(df_videos["upload_date"].dt.to_period("M")).agg({
        "views": "mean",
        "rating": "mean",
    })
    trend_data.index = trend_data.index.to_timestamp()

    # Plot average views
    plt.figure(figsize=(12, 6))
    plt.plot(trend_data.index, trend_data["views"], color="blue", label="Average Views")
    plt.title("Trend of Average Views Over Time")
    plt.xlabel("Time")
    plt.ylabel("Average Views")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_folder}/trend_of_average_views.png")
    plt.close()

    # Plot average ratings
    plt.figure(figsize=(12, 6))
    plt.plot(trend_data.index, trend_data["rating"], color="orange", label="Average Ratings")
    plt.title("Trend of Average Ratings Over Time")
    plt.xlabel("Time")
    plt.ylabel("Average Ratings")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{output_folder}/trend_of_average_ratings.png")
    plt.close()
