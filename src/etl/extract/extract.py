"""
Main data extraction module.

This module serves as the primary entry point for the data extraction process,
coordinating the extraction of various datasets, including fitness statistics.
It provides a unified interface for extracting all required data for the
ETL pipeline.
"""

import pandas as pd
from src.utils.logging_utils import setup_logger
from src.etl.extract.extract_fitness_stats import extract_fitness_stats

# Initialize the logger for the extraction process
logger = setup_logger("extract_data", "extract_data.log")


def extract_data() -> pd.DataFrame:
    """
    Extract all required data for the ETL pipeline.

    This function orchestrates the extraction of all necessary datasets,
    currently handling fitness statistics. It can be expanded to extract
    additional datasets as needed.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the extracted data.

    Raises:
        Exception: If any part of the extraction process fails.
    """
    try:
        logger.info("Starting data extraction process")

        # Extract fitness statistics data
        fitness_stats = extract_fitness_stats()

        # Log successful completion with dataset shape information
        logger.info(
            f"Data extraction completed successfully - "
            f"Fitness stats: {fitness_stats.shape}"
        )

        return fitness_stats
    except Exception as e:
        # Log and re-raise exceptions for proper error handling upstream
        logger.error(f"Data extraction failed: {str(e)}")
        raise
