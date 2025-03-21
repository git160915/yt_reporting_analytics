"""
reporting_client.py

Provides helper functions for interacting with the YouTube Reporting API.
Includes job creation, polling report availability, downloading CSV content,
and parsing reports into structured data.
"""

import time
import io
import csv
import logging
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import AuthorizedSession
from utils.auth import get_credentials

logger = logging.getLogger("yt_data_ingestion.reporting")


def get_reporting_service():
    """
    Build and return an authenticated YouTube Reporting API client.

    Returns:
        googleapiclient.discovery.Resource: Authenticated reporting service.
    """
    creds = get_credentials("token_reporting.pickle")
    return build("youtubereporting", "v1", credentials=creds)


def list_reporting_jobs(service):
    """
    List all existing reporting jobs associated with the account.

    Args:
        service: YouTube Reporting API client.

    Returns:
        list: Reporting jobs.
    """
    return service.jobs().list().execute().get("jobs", [])


def create_reporting_job(service, report_type_id, name):
    """
    Create a new reporting job or retrieve an existing one for a given report type.

    Args:
        service: YouTube Reporting API client.
        report_type_id (str): The report type identifier.
        name (str): Friendly name for the job.

    Returns:
        dict: The created or existing job object.
    """
    try:
        return service.jobs().create(body={"reportTypeId": report_type_id, "name": name}).execute()
    except HttpError as e:
        if e.resp.status == 409:
            for job in list_reporting_jobs(service):
                if job.get("reportTypeId") == report_type_id:
                    return job
        raise e


def list_report_files(service, job_id):
    """
    List available report files for a given reporting job.

    Args:
        service: YouTube Reporting API client.
        job_id (str): Reporting job ID.

    Returns:
        list: Report metadata objects.
    """
    return service.jobs().reports().list(jobId=job_id).execute().get("reports", [])


def download_report_file(download_url, creds):
    """
    Download the report content using an authorized session.

    Args:
        download_url (str): URL to download the report file.
        creds: OAuth credentials.

    Returns:
        str: CSV content of the report.
    """
    session = AuthorizedSession(creds)
    response = session.get(download_url)
    response.raise_for_status()
    return response.text


def poll_and_download_reports(job_id, poll_interval, max_attempts, start_date=None, end_date=None):
    """
    Poll for available report files over time and download those within a specified date range.

    Args:
        job_id (str): Reporting job ID.
        poll_interval (int): Time (seconds) between poll attempts.
        max_attempts (int): Maximum number of attempts.
        start_date (datetime, optional): Filter start date.
        end_date (datetime, optional): Filter end date.

    Returns:
        list: List of CSV content strings.
    """
    service = get_reporting_service()
    creds = get_credentials("token_reporting.pickle")
    downloaded = []

    for _ in range(max_attempts):
        reports = list_report_files(service, job_id)
        for report in reports:
            end_time_ms = int(report.get("endTimeMs", 0))
            report_date = datetime.utcfromtimestamp(end_time_ms / 1000)
            if (not start_date or report_date >= start_date) and (not end_date or report_date <= end_date):
                content = download_report_file(report["downloadUrl"], creds)
                downloaded.append(content)
        if downloaded:
            break
        logger.info("No report files yet. Waiting for the next poll interval...")
        time.sleep(poll_interval)
    return downloaded


def parse_csv(content):
    """
    Convert CSV text into a list of dictionaries.

    Args:
        content (str): CSV-formatted string.

    Returns:
        list: Parsed rows as dictionaries.
    """
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)
