"""
Main data loading module.

This module serves as the primary entry point for the data loading process,
handling the loading of processed data to output files. It provides
error handling and logging for the loading process.
"""

from src.utils.logging_utils import setup_logger
from src.etl.load.load_fitness_stats import load_fitness_stats

# Initialize logger for the loading process
logger = setup_logger("load_data", "load_data.log")


def load_data(data_dict):
    """
    Load processed data to output destinations in Parquet and CSV format.

    This function orchestrates the loading of processed data to its final
    destinations, currently handling fitness statistics. It includes
    error handling and logging of the loading process.

    Args:
        data_dict (dict): Dictionary containing cleaned DataFrame and other data.
                         Expected to have a 'cleaned_df' key with the DataFrame to load.

    Raises:
        Exception: If any part of the loading process fails.
    """
    try:
        logger.info("Starting data loading process.")

        # Check if we received a dictionary or DataFrame
        if isinstance(data_dict, dict) and "cleaned_df" in data_dict:
            # Extract the DataFrame from the dictionary
            df = data_dict["cleaned_df"]
        else:
            # Assume it's already a DataFrame
            df = data_dict

        # Load the processed fitness statistics data
        load_fitness_stats(df)

        logger.info("Data loading process completed successfully.")
    except Exception as e:
        # Log and re-raise exceptions for proper error handling upstream
        logger.error(f"Error during data loading process: {e}")
        raise
