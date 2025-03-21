"""
auth.py

Provides shared authentication utilities for accessing Google APIs (YouTube Analytics & Reporting).
Handles OAuth2 token loading, refreshing, and persistence.

Scopes are limited to read-only access for YouTube Analytics.

Expected:
- A valid `client_secret.json` file in the root directory.
- Token files are persisted separately for analytics and reporting services.
"""

import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Define the read-only scope for YouTube Analytics API
SCOPES = ["https://www.googleapis.com/auth/yt-analytics.readonly"]

def get_credentials(token_file):
    """
    Retrieve or refresh credentials from the specified token file.

    If no valid token is found, initiates the OAuth flow to authorize access.

    Args:
        token_file (str): Filename to load/save credentials (e.g., 'token_analytics.pickle').

    Returns:
        google.auth.credentials.Credentials: Authenticated credentials.
    """
    creds = None

    # Attempt to load existing credentials.
    if os.path.exists(token_file):
        with open(token_file, "rb") as token:
            creds = pickle.load(token)

    # Refresh or generate new credentials if necessary.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Initiate browser-based OAuth flow using the client secrets file.
            flow = InstalledAppFlow.from_client_secrets_file("client_secret.json", SCOPES)
            creds = flow.run_local_server(port=0)

        # Save credentials for future use.
        with open(token_file, "wb") as token:
            pickle.dump(creds, token)

    return creds
