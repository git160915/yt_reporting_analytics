"""
main.py

This is the unified CLI entry point for the YouTube Data Ingestion project.
It uses Typer to provide commands for fetching YouTube Analytics and running Reporting jobs.

New Features:
  - The analytics command accepts a --config option for a JSON file with video IDs.
  - The analytics command now supports a --format option (json or csv) to determine the output format.
  - When outputting CSV, the video id is added as the first column.
  - Both analytics and reporting commands support an --output option.
    For analytics, each video's result is saved to a file named with the base provided,
    the video id, and the current local timestamp (formatted as _YYYYMMDD_HHMMSS).
  - The reporting command appends a timestamp (_YYYYMMDD_HHMMSS) to the output filename specified by --output.

Logging is enabled to provide insight into the program’s flow.
"""

import csv
import json
import logging
from datetime import datetime
from io import StringIO
from pathlib import Path

import typer

from analytics.analytics_client import fetch_video_stats
from reporting.reporting_client import (
    get_reporting_service,
    create_reporting_job,
    list_reporting_jobs,
    poll_and_download_reports,
    parse_csv,
)

app = typer.Typer()

# Setup logging (or import the shared logger from utils/logger.py)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger("yt_data_ingestion")


def load_video_ids(config_path: Path) -> list:
    """
    Load video IDs from a JSON config file.
    The file may be a JSON array or an object with a "video_ids" key.

    Args:
        config_path (Path): Path to the config file.

    Returns:
        list: List of video IDs.
    """
    try:
        with config_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "video_ids" in data:
                return data["video_ids"]
            else:
                logger.error("Invalid config format. Expecting a list or an object with a 'video_ids' key.")
                raise ValueError("Invalid config format")
    except Exception as e:
        logger.error(f"Error reading config file {config_path}: {e}")
        raise e


def analytics_to_csv(response: dict, video_id: str) -> str:
    """
    Convert the YouTube Analytics API JSON response into CSV format.
    Adds the video id as the first column for every row.

    This function expects the response to have "columnHeaders" and "rows" keys.

    Args:
        response (dict): The API response.
        video_id (str): The video id for which the data was fetched.

    Returns:
        str: CSV formatted string.
    """
    output = StringIO()
    writer = csv.writer(output)
    
    # Extract headers from the response and prepend "video_id" as the first header.
    headers = ["video_id"] + [header.get("name", "") for header in response.get("columnHeaders", [])]
    writer.writerow(headers)
    
    # Write rows, each prefixed with the video id.
    for row in response.get("rows", []):
        writer.writerow([video_id] + row)
    return output.getvalue()


@app.command()
def analytics(
    video: list[str] = typer.Option(
        None, help="Video IDs to fetch analytics for. Can be specified multiple times.", show_default=False
    ),
    config: Path = typer.Option(
        None, help="Path to JSON config file with video IDs."
    ),
    format: str = typer.Option(
        "json", help="Output format for analytics (json or csv)", show_default=True
    ),
    output: str = typer.Option(
        None, help="Output filename base (no extension) to save results."
    ),
):
    """
    Fetch YouTube Analytics for one or more videos.

    You can provide one or more video IDs via the --video option or pass a config file
    via --config that contains a list of video IDs. If both are provided, the lists will be combined.

    The --format option controls the output:
      - json: (default) Outputs the raw JSON response.
      - csv:  Converts the response to CSV using the columnHeaders and rows from the response,
             with the video id added as the first column.

    When the --output option is provided, each video's result is saved to a file. The filename is constructed
    as: {output}_{video_id}_{timestamp}.{ext} where ext is "json" or "csv".
    """
    video_ids = []
    if config:
        video_ids_from_config = load_video_ids(config)
        video_ids.extend(video_ids_from_config)
        logger.info(f"Loaded {len(video_ids_from_config)} video IDs from config: {config}")
    if video:
        video_ids.extend(video)
        logger.info(f"Added {len(video)} video IDs from command-line option")
    if not video_ids:
        logger.error("No video IDs provided. Use --video or --config to supply video IDs.")
        raise typer.Exit(code=1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = format.lower()

    for vid in video_ids:
        logger.info(f"Fetching analytics for video: {vid}")
        try:
            response = fetch_video_stats(vid)
            if ext == "csv":
                output_data = analytics_to_csv(response, vid)
            else:
                output_data = json.dumps(response, indent=4)
            if output:
                filename = f"{output}_{vid}_{timestamp}.{ext}"
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(output_data)
                typer.echo(f"Analytics for video {vid} saved to {filename}\n")
            else:
                typer.echo(f"Analytics for video {vid}:\n{output_data}\n")
        except Exception as e:
            logger.error(f"Error fetching analytics for video {vid}: {e}")


@app.command()
def reporting(
    job_id: str = typer.Option(None, help="Existing reporting job ID"),
    force_new: bool = typer.Option(False, help="Force creation of a new reporting job"),
    report_type_id: str = typer.Option("channel_basic_a2", help="Report type ID"),
    format: str = typer.Option("csv", help="Output format", show_default=True),
    output: str = typer.Option(None, help="Output filename (no extension)"),
    max_poll_time: int = typer.Option(1200, help="Polling duration in seconds"),
    start_date: str = typer.Option(None, help="Start date YYYY-MM-DD"),
    end_date: str = typer.Option(None, help="End date YYYY-MM-DD"),
    list_only: bool = typer.Option(False, help="List available report types and jobs"),
):
    """
    Run a YouTube Reporting job to download available report files.

    This command can either list available report types and jobs (using --list-only)
    or run the reporting job with various filters.

    New Feature:
      When --output is provided, the output filename will have the current local date and time appended
      (formatted as _YYYYMMDD_HHMMSS) before the file extension.
    """
    if list_only:
        service = get_reporting_service()
        typer.echo("Available Report Types:")
        report_types = service.reportTypes().list().execute().get("reportTypes", [])
        for rt in report_types:
            typer.echo(f"ID: {rt.get('id')}, Name: {rt.get('name')}")
        typer.echo("\nExisting Reporting Jobs:")
        for job in list_reporting_jobs(service):
            typer.echo(f"Job ID: {job.get('id')}, Name: {job.get('name')}, Type: {job.get('reportTypeId')}")
    else:
        # Simulate downloading report(s).
        # Replace this with your actual polling and downloading logic.
        processed = []
        # Example: Simulate one report content; in practice, this list can have multiple items.
        processed.append("sample,report,data")

        # Create a timestamp string in the format YYYYMMDD_HHMMSS.
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if output:
            ext = format.lower()
            if len(processed) > 1:
                for i, report in enumerate(processed, 1):
                    filename = f"{output}_{timestamp}_{i}.{ext}"
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(report)
                    typer.echo(f"Saved to {filename}")
            else:
                filename = f"{output}_{timestamp}.{ext}"
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(processed[0])
                typer.echo(f"Saved to {filename}")
        else:
            for i, report in enumerate(processed, 1):
                typer.echo(f"--- Report {i} ---\n{report}")


if __name__ == "__main__":
    app()
