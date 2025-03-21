"""
logger.py

Sets up and configures the logger for the YouTube Data Ingestion project.
This allows for centralized logging configuration across all modules.
"""

import logging

def setup_logger(name: str, level=logging.INFO):
    """
    Create and return a logger with the specified name and level.

    Args:
        name (str): Name of the logger.
        level: Logging level (default is logging.INFO).

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger
