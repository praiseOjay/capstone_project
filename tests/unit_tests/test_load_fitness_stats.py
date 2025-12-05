"""
Unit tests for the load_fitness_stats module.

This module contains unit tests for the load_fitness_stats module's
functionality, focusing on verifying that the fitness statistics data is
properly saved to the expected output location.
"""

import pandas as pd
import pytest
from src.etl.load.load_fitness_stats import (
    load_fitness_stats,
    OUTPUT_DIR,
    MAIN_FILE_NAME,
    PARQUET_FILE_NAME,
)


def test_load_fitness_stats(mocker):
    """
    Test that load_fitness_stats correctly calls save functions.

    This test verifies that the load_fitness_stats function correctly calls
    both the save_dataframe_to_csv and save_dataframe_to_parquet functions
    with the appropriate parameters.

    Args:
        mocker: pytest-mock fixture for creating mock objects
    """
    # Sample DataFrame for testing
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
            "steps": [1000, 2000],
            "calories_burned": [250, 500],
        }
    )

    # Mock both save functions
    mock_save_csv = mocker.patch(
        "src.etl.load.load_fitness_stats.save_dataframe_to_csv"
    )
    mock_save_parquet = mocker.patch(
        "src.etl.load.load_fitness_stats.save_dataframe_to_parquet"
    )

    # Call the function under test
    load_fitness_stats(df)

    # Assert
    mock_save_csv.assert_called_once_with(df, OUTPUT_DIR, MAIN_FILE_NAME)
    mock_save_parquet.assert_called_once_with(
        df, OUTPUT_DIR, PARQUET_FILE_NAME
    )
