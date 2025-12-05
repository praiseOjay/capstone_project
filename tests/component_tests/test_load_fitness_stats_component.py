"""
Component tests for the load_fitness_stats module.

This module contains component tests for the load_fitness_stats module,
verifying that it correctly handles saving fitness statistics data
to output files under various conditions.
"""

import pandas as pd
import pytest
import os
from unittest.mock import patch
from src.etl.load.load_fitness_stats import (
    load_fitness_stats,
    OUTPUT_DIR,
    MAIN_FILE_NAME,
    PARQUET_FILE_NAME,
)


def test_load_fitness_stats_success(mocker):
    """
    Test that load_fitness_stats correctly saves data when successful.

    This test verifies that the load_fitness_stats function properly calls
    the save utility functions with the correct parameters when
    given a valid DataFrame.

    Args:
        mocker: pytest-mock fixture for creating mock objects
    """
    # Create a sample DataFrame to pass to the function
    sample_data = {
        "participant_id": [1, 2, 3],
        "steps": [1000, 1500, 2000],
        "calories_burned": [200, 250, 300],
    }
    sample_df = pd.DataFrame(sample_data)

    # Mock both save functions
    mock_save_csv = mocker.patch(
        "src.etl.load.load_fitness_stats.save_dataframe_to_csv"
    )
    mock_save_parquet = mocker.patch(
        "src.etl.load.load_fitness_stats.save_dataframe_to_parquet"
    )

    # Call the function under test
    load_fitness_stats(sample_df)

    # Assert
    mock_save_csv.assert_called_once_with(
        sample_df, OUTPUT_DIR, MAIN_FILE_NAME
    )
    mock_save_parquet.assert_called_once_with(
        sample_df, OUTPUT_DIR, PARQUET_FILE_NAME
    )
