import pandas as pd

def filter_videos(df, age_range=None, views_range=None, rating_range=None):
    """
    Filter the videos DataFrame based on age range, views range, and rating range.
    
    :param df: DataFrame containing video data
    :param age_range: Tuple (min_age, max_age) for filtering by age
    :param views_range: Tuple (min_views, max_views) for filtering by views
    :param rating_range: Tuple (min_rating, max_rating) for filtering by rating
    :return: Filtered DataFrame
    """
    try:
        # Start with the original DataFrame
        filtered_df = df.copy()

        # Apply age range filter if provided
        if age_range:
            min_age, max_age = age_range
            filtered_df = filtered_df[(filtered_df['age'] >= min_age) & (filtered_df['age'] <= max_age)]

        # Apply views range filter if provided
        if views_range:
            min_views, max_views = views_range
            filtered_df = filtered_df[(filtered_df['views'] >= min_views) & (filtered_df['views'] <= max_views)]

        # Apply rating range filter if provided
        if rating_range:
            min_rating, max_rating = rating_range
            filtered_df = filtered_df[(filtered_df['rating'] >= min_rating) & (filtered_df['rating'] <= max_rating)]

        return filtered_df

    except Exception as e:
        raise ValueError(f"Error during filtering: {e}")
