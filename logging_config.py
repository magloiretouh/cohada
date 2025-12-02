"""
Logging Configuration for OHADA API Server
Handles application logs with rotation and proper formatting
"""

import logging
import logging.handlers
import os
from datetime import datetime

def setup_logging():
    """
    Configure comprehensive logging for the Flask application.
    Logs to both file and console with rotation.
    """

    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Log file paths
    app_log_file = os.path.join(log_dir, "app.log")
    error_log_file = os.path.join(log_dir, "error.log")
    cache_log_file = os.path.join(log_dir, "cache.log")

    # Format
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # ===== GENERAL APP LOGS =====
    app_handler = logging.handlers.RotatingFileHandler(
        app_log_file,
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5  # Keep 5 backups
    )
    app_handler.setLevel(logging.INFO)
    app_handler.setFormatter(formatter)
    logger.addHandler(app_handler)

    # ===== ERROR LOGS =====
    error_handler = logging.handlers.RotatingFileHandler(
        error_log_file,
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5  # Keep 5 backups
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    # ===== CACHE LOGS =====
    cache_handler = logging.handlers.RotatingFileHandler(
        cache_log_file,
        maxBytes=5*1024*1024,  # 5 MB
        backupCount=3  # Keep 3 backups
    )
    cache_handler.setLevel(logging.INFO)
    cache_handler.setFormatter(formatter)
    cache_logger = logging.getLogger('cache')
    cache_logger.addHandler(cache_handler)

    # ===== CONSOLE LOGS =====
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Log startup info
    logger.info("=" * 70)
    logger.info("OHADA Reporting API Server Started")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Log Directory: {os.path.abspath(log_dir)}")
    logger.info(f"App Log: {os.path.abspath(app_log_file)}")
    logger.info(f"Error Log: {os.path.abspath(error_log_file)}")
    logger.info(f"Cache Log: {os.path.abspath(cache_log_file)}")
    logger.info("=" * 70)

    return logger


def get_logger(name):
    """Get a logger instance for a specific module."""
    return logging.getLogger(name)
