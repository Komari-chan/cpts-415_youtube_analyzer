import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import matplotlib.dates as mdates

def generate_visualizations(df_videos, output_folder):
    """
    Generate core visualizations based on analysis results.
    :param df_videos: Spark DataFrame containing video data
    :param output_folder: Path to save output files
    """
    trends = pd.read_csv(f"{output_folder}/trends.csv")
    category_stats = pd.read_csv(f"{output_folder}/category_stats.csv")

    plot_category_statistics(category_stats, output_folder)
    plot_trends_views(trends, output_folder)
    plot_trends_ratings(trends, output_folder)


def plot_category_statistics(category_stats, output_folder):
    """
    Plot average views by category as a bar chart.
    """
    plt.figure(figsize=(12, 6))
    category_stats.sort_values("avg_views").plot.bar(
        x="category", y="avg_views", color="skyblue", legend=False
    )
    plt.title("Average Views by Category", loc="center")
    plt.xlabel("Category")
    plt.ylabel("Average Views")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(f"{output_folder}/category_views.png")
    plt.close()


import matplotlib.dates as mdates

def plot_trends_views(trends, output_folder):
    """
    Plot trends of average views over time as a line chart with simplified x-axis labels.
    """
    plt.figure(figsize=(12, 6))
    plt.plot(trends["upload_date"], trends["avg_views"], label="Average Views", color="blue")
    plt.title("Trends of Average Views Over Time")
    plt.xlabel("Upload Date")
    plt.ylabel("Average Views")
    plt.xticks(rotation=45)

    # Format x-axis to show fewer ticks
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))  # Every 6 months
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))  # Format as Year-Month

    plt.tight_layout()
    plt.savefig(f"{output_folder}/trends_views_fixed.png")
    plt.close()


def plot_trends_ratings(trends, output_folder):
    """
    Plot trends of average ratings over time as a line chart with simplified x-axis labels.
    """
    plt.figure(figsize=(12, 6))
    plt.plot(trends["upload_date"], trends["avg_rating"], label="Average Ratings", color="orange")
    plt.title("Trends of Average Ratings Over Time")
    plt.xlabel("Upload Date")
    plt.ylabel("Average Ratings")
    plt.xticks(rotation=45)

    # Format x-axis to show fewer ticks
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))  # Every 6 months
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))  # Format as Year-Month

    plt.tight_layout()
    plt.savefig(f"{output_folder}/trends_ratings_fixed.png")
    plt.close()




def plot_top_videos(output_folder):
    """
    Plot the top 10 videos by views as a bar chart using video_id.
    """
    top_videos = pd.read_csv(f"{output_folder}/top_10_views.csv")
    plt.figure(figsize=(12, 6))
    plt.bar(top_videos["video_id"], top_videos["views"], color="skyblue")
    plt.title("Top 10 Videos by Views", loc="center")
    plt.xlabel("Video ID")
    plt.ylabel("Views")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(f"{output_folder}/top_10_videos.png")
    plt.close()




def plot_related_video_analysis(output_folder):
    """
    Plot a heatmap of the related videos' correlation matrix.
    """
    related_stats = pd.read_csv(f"{output_folder}/related_analysis.csv")
    plt.figure(figsize=(10, 8))
    sns.heatmap(related_stats.corr(), annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Between Related Video Metrics", loc="center")
    plt.tight_layout()
    plt.savefig(f"{output_folder}/related_videos_correlation.png")
    plt.close()

def plot_trends_over_time(trends, output_folder):
    """
    Plot trends of views and ratings over time separately.
    """
    trends["upload_date"] = pd.to_datetime(trends["upload_date"])
    trends = trends.groupby("upload_date").mean()  # Aggregate data

    # Plot Average Views Over Time
    plt.figure(figsize=(12, 6))
    plt.plot(trends.index, trends["avg_views"], label="Average Views", color="blue")
    plt.title("Trends of Average Views Over Time")
    plt.xlabel("Upload Date")
    plt.ylabel("Average Views")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_folder}/trends_views_fixed.png")
    plt.close()

    # Plot Average Ratings Over Time
    plt.figure(figsize=(12, 6))
    plt.plot(trends.index, trends["avg_rating"], label="Average Ratings", color="orange")
    plt.title("Trends of Average Ratings Over Time")
    plt.xlabel("Upload Date")
    plt.ylabel("Average Ratings")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_folder}/trends_ratings_fixed.png")
    plt.close()
