"""
Tests for the extract module's main extraction function.

This module contains tests for the extract_data function, which is the main
entry point for the data extraction process. It verifies that the function
correctly returns the expected data from the extraction sources.
"""

import pandas as pd
import pytest
from unittest.mock import patch
from src.etl.extract.extract import extract_data


@pytest.fixture
def expected_unclean_data_csv():
    """
    Fixture that loads the raw data directly from the source file.

    Returns:
        pd.DataFrame: The raw data as a DataFrame
    """
    return pd.read_csv("data/raw/fitness_stats/fitness_dataset.csv")


def test_extract_data(expected_unclean_data_csv):
    """
    Test that extract_data returns the correct data.

    This test verifies that the extract_data function correctly returns the
    expected data from all extraction sources by mocking pandas.read_csv.

    Args:
        expected_unclean_data_csv: Fixture with the expected raw data
    """
    # Mock the pandas read_csv function to return the expected unclean data
    with patch("pandas.read_csv", return_value=expected_unclean_data_csv):
        # Call the function under test
        df = extract_data()

        # Assert
        assert isinstance(df, pd.DataFrame)
        pd.testing.assert_frame_equal(df, expected_unclean_data_csv)
        assert df.equals(expected_unclean_data_csv)
        assert df.shape == expected_unclean_data_csv.shape
        assert (
            df.columns.tolist() == expected_unclean_data_csv.columns.tolist()
        )
