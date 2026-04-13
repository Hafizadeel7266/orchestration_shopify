"""Logging configuration for Shopify pipeline."""

import logging
from logging.handlers import RotatingFileHandler
import os

# ============================================
# LOGGING CONFIGURATION
# ============================================

LOG_DIRECTORY = "./logs/dlt"
LOG_FILENAME = "shopify_pipeline.log"
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5

# Create log directory if it doesn't exist
os.makedirs(LOG_DIRECTORY, exist_ok=True)

# Create formatter
formatter = logging.Formatter(
    "%(asctime)s|%(levelname)s|%(name)s|%(filename)s|%(funcName)s:%(lineno)d|%(message)s"
)

# Only configure if not already configured
if not logging.getLogger("dlt").handlers:
    # File handler (rotating)
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIRECTORY, LOG_FILENAME),
        maxBytes=MAX_LOG_SIZE,
        backupCount=BACKUP_COUNT,
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # dlt logger
    logger = logging.getLogger("dlt")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # app logger
    app_logger = logging.getLogger("shopify_pipeline")
    app_logger.setLevel(logging.DEBUG)
    app_logger.addHandler(file_handler)
    app_logger.addHandler(console_handler)
else:
    logger = logging.getLogger("dlt")
    app_logger = logging.getLogger("shopify_pipeline")

def get_logger(name: str = None):
    """Get a logger instance."""
    if name:
        return logging.getLogger(f"shopify_pipeline.{name}")
    return app_logger

__all__ = ['logger', 'app_logger', 'get_logger']