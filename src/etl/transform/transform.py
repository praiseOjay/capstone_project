import pandas as pd
from src.utils.logging_utils import setup_logger
from src.etl.transform.clean_fitness_stats import clean_fitness_stats
from src.etl.transform.filter_fitness_stats import prepare_visualisation_data

# Initialize logger
logger = setup_logger(__name__, "transform_fitness_stats.log")


def transform_data(input_df: pd.DataFrame) -> dict:
    """
    Transforms the fitness statistics DataFrame by cleaning it and preparing visualisation data.

    Args:
        input_df (pd.DataFrame): Raw fitness statistics DataFrame.

    Returns:
        dict: Dictionary containing cleaned DataFrame and prepared visualisation data.
    """
    try:
        logger.info("Starting transformation of fitness statistics data.")

        # Clean the data using the cleaning function
        cleaned_df = clean_fitness_stats(input_df)
        logger.info("Completed cleaning of fitness statistics data.")

        # Prepare data for visualisations
        visualisation_data = prepare_visualisation_data(cleaned_df)
        logger.info("Completed preparation of visualisation data.")

        # Create result dictionary with both datasets
        result = {
            "cleaned_df": cleaned_df,
            "visualisation_data": visualisation_data,
        }

        logger.info("Completed transformation of fitness statistics data.")
        return result
    except Exception as e:
        logger.error(
            f"Error during transformation of fitness statistics data: {e}"
        )
        raise
