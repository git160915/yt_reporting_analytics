# YouTube Data Ingestion

This project provides a unified CLI tool for ingesting data from YouTube Analytics and YouTube Reporting APIs.

## Features

- **YouTube Analytics:**  
  Fetch daily video performance metrics for one or more videos.  
  You can specify video IDs via the command line or provide a JSON config file.

- **YouTube Reporting:**  
  Create or reuse reporting jobs to download report files.  
  When using the `--output` option, the output filename is appended with the current local timestamp (formatted as `_YYYYMMDD_HHMMSS`).

- **Typer CLI:**  
  A modern, Typer-based CLI interface is used for easy command-line interactions.

- **Logging:**  
  Detailed logging is available to trace the program flow.

- **Unit Tests:**  
  Basic unit tests are provided using pytest.

## Project Structure

```
yt_data_ingestion/
├── analytics/
│   ├── __init__.py
│   └── analytics_client.py
├── reporting/
│   ├── __init__.py
│   └── reporting_client.py
├── tests/
│   ├── __init__.py
│   ├── test_analytics.py
│   └── test_reporting.py
├── utils/
│   ├── __init__.py
│   ├── auth.py
│   └── logger.py
├── main.py
├── requirements.txt
└── README.md
```

## Setup

1. Ensure you have a valid `client_secret.json` file in the root directory.
2. Install the required dependencies:
  ```bash
  pip install -r requirements.txt
  ```
3. Run the CLI:
  ```bash
  python main.py --help
  ```

## Usage Example
- Fetch Analytics for Videos:

```bash
python main.py analytics --video VIDEO_ID1 --video VIDEO_ID2
```
Or using a config file:
```bash
python main.py analytics --config path/to/config.json
```
- Run Reporting Job:
```bash
python main.py reporting --max-poll-time 60 --format csv --output channel_basic_a2 --report-type-id channel_basic_a2
```

## Running Test
Run the tests with:
```bash
pytest

---

You now have all files needed for your project. Simply create the directory structure, add each file with the content provided, and you'll be ready to run the application and tests.

Let me know if you need any further modifications or assistance!