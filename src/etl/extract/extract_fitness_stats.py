"""
Extract fitness statistics from raw data files.

This module handles the extraction of fitness statistics data from a CSV file,
providing functionality to read, validate, and return the data as a
pandas DataFrame. It includes performance monitoring and comprehensive logging.
"""

import os
import logging
import pandas as pd
import timeit
from src.utils.logging_utils import setup_logger, log_extract_success

# Define the file path for the fitness dataset
# Use __file__ to get the path relative to this Python file's location
FILE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "..",
    "data",
    "raw",
    "fitness_stats",
    "unclean_fitness_dataset.csv",
)

# Initialise the logger with specific configuration for this module
logger = setup_logger(
    __name__, "extract_fitness_stats.log", level=logging.DEBUG
)

# Performance benchmark in seconds for monitoring extraction efficiency
EXPECTED_PERFORMANCE = 0.6  # seconds

# Define the type of data being extracted for logging purposes
TYPE = "fitness_stats"


def extract_fitness_stats() -> pd.DataFrame:
    """
    Extract fitness statistics data from the source CSV file.

    This function reads the fitness statistics dataset from a predefined
    file path, logs the extraction process details, including performance
    metrics, and handles any exceptions that may occur during extraction.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the fitness statistics data.

    Raises:
        Exception: If the CSV file cannot be loaded or processed.
    """
    # Start timing the extraction process for performance monitoring
    start_time = timeit.default_timer()
    try:
        logger.info("Starting fitness stats data extraction process")

        # Read the CSV file into a pandas DataFrame
        fitness_stats = pd.read_csv(FILE_PATH)

        # Calculate execution time for performance logging
        extraction_execution_time = timeit.default_timer() - start_time

        # Log successful extraction with detailed metrics
        log_extract_success(
            logger,
            TYPE,
            fitness_stats.shape,  # Dimensions of the dataset
            fitness_stats.isnull().sum().sum(),  # Count of null values
            fitness_stats.duplicated().sum(),  # Count of duplicate rows
            fitness_stats.dtypes.apply(
                lambda x: x.name
            ).to_dict(),  # Column data types
            extraction_execution_time,  # Actual execution time
            EXPECTED_PERFORMANCE,  # Expected performance benchmark
        )

        return fitness_stats
    except Exception as e:
        # Log and re-raise exceptions for proper error handling
        logger.error(f"{FILE_PATH} data extraction failed: {e}")
        raise Exception(f"Failed to load the CSV file at: {FILE_PATH}")
