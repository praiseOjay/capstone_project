"""
Component tests for the load module's main loading function.

This module contains component tests for the load_data function, which is the
main entry point for the data loading process. It verifies that the function
correctly saves processed data to the expected output location.
"""

import pandas as pd
import pytest
from unittest.mock import patch
from src.etl.load.load import load_data


@pytest.fixture
def sample_processed_dataset():
    """
    Fixture that creates a sample processed dataset for testing.

    Returns:
        pd.DataFrame: A sample processed DataFrame for testing
    """
    # Using minimal columns for this simple test
    return pd.DataFrame(
        {
            "participant_id": [1, 2, 3, 4, 5],
            "date": pd.to_datetime(
                [
                    "2023-01-01",
                    "2023-01-02",
                    "2023-01-03",
                    "2023-01-04",
                    "2023-01-05",
                ]
            ),
            "daily_steps": [8000, 10000, 12000, 6000, 9500],
            "calories_burned": [300, 400, 500, 250, 380],
        }
    )


def test_load_data_with_dataframe(sample_processed_dataset, mocker):
    """
    Test that load_data correctly handles a DataFrame input.

    Args:
        sample_processed_dataset: Fixture with sample processed data
        mocker: pytest-mock fixture for creating mock objects
    """
    # Mock the load_fitness_stats function
    mock_load_fitness_stats = mocker.patch(
        "src.etl.load.load.load_fitness_stats"
    )

    # Call the function under test with a DataFrame
    load_data(sample_processed_dataset)

    # Assert
    mock_load_fitness_stats.assert_called_once_with(sample_processed_dataset)


def test_load_data_with_dictionary(sample_processed_dataset, mocker):
    """
    Test that load_data correctly handles a dictionary input with 'cleaned_df' key.

    Args:
        sample_processed_dataset: Fixture with sample processed data
        mocker: pytest-mock fixture for creating mock objects
    """
    # Create a dictionary with the cleaned_df key
    data_dict = {
        "cleaned_df": sample_processed_dataset,
        "visualisation_data": {"some": "visualization data"},
    }

    # Mock the load_fitness_stats function
    mock_load_fitness_stats = mocker.patch(
        "src.etl.load.load.load_fitness_stats"
    )

    # Call the function under test with the dictionary
    load_data(data_dict)

    # Assert
    mock_load_fitness_stats.assert_called_once_with(sample_processed_dataset)
