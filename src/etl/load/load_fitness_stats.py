"""
Load processed fitness statistics to Parquet files.

This module provides functionality for loading the transformed fitness
statistics data to a Parquet adn CSV file in the designated output directory.
It handles the file path configuration and delegates the actual file writing to
utility functions.
"""

import os
from src.utils.file_utils import (
    save_dataframe_to_parquet,
    save_dataframe_to_csv,
)
from src.utils.logging_utils import setup_logger

# Initialize logger
logger = setup_logger(__name__, "load_fitness_stats.log")

# Define output location for the processed fitness statistics data
OUTPUT_DIR = os.path.join("data", "output")
MAIN_FILE_NAME = "loaded_fitness_stats.csv"  # For compatibility with tests
PARQUET_FILE_NAME = "clean_fitness_stats.parquet"


def load_fitness_stats(df):
    """
    Save the processed fitness statistics DataFrame to Parquet and CSV file.

    This function takes the processed DataFrame and saves it to the predefined
    output location in Parquet format, which is more efficient for storage and
    retrieval.

    Args:
        df (pd.DataFrame): The cleaned DataFrame to save.
    """
    logger.info("Saving cleaned fitness statistics data to Parquet.")

    # Save the DataFrame to a Parquet file (efficient format)
    save_dataframe_to_parquet(df, OUTPUT_DIR, PARQUET_FILE_NAME)

    # Also save as CSV for compatibility with tests and legacy code
    save_dataframe_to_csv(df, OUTPUT_DIR, MAIN_FILE_NAME)

    logger.info("Finished saving fitness statistics data.")
