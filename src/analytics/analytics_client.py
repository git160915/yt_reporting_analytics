"""
analytics_client.py

Handles interactions with the YouTube Analytics API.
Includes functionality for authenticating and fetching daily video performance metrics
(such as views, watch time, average view duration, and subscriber gains).
"""

import datetime
import logging

from googleapiclient.discovery import build
from utils.auth import get_credentials

logger = logging.getLogger("yt_data_ingestion.analytics")


def get_analytics_service():
    """
    Build and return an authenticated YouTube Analytics API client.

    Returns:
        googleapiclient.discovery.Resource: Authenticated analytics client.
    """
    creds = get_credentials("token_analytics.pickle")
    return build("youtubeAnalytics", "v2", credentials=creds)


def fetch_video_stats(video_id):
    """
    Fetch daily video performance metrics for the past 90 days.

    Args:
        video_id (str): The ID of the YouTube video to retrieve data for.

    Returns:
        dict: Raw response from the YouTube Analytics API.
    """
    logger.info(f"Fetching analytics for video ID: {video_id}")
    analytics = get_analytics_service()
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=90)

    response = analytics.reports().query(
        ids="channel==MINE",
        startDate=start_date.isoformat(),
        endDate=end_date.isoformat(),
        metrics="views,estimatedMinutesWatched,averageViewDuration,subscribersGained",
        dimensions="day",
        filters=f"video=={video_id}",
        maxResults=100,
    ).execute()

    return response
