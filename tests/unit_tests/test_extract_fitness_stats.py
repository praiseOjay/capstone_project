"""
Unit tests for the extract_fitness_stats module.

This module contains unit tests that verify the functionality of the
extract_fitness_stats module, focusing on mocking external dependencies to
isolate the tests. The tests cover successful data extraction,
logging behaviour, and exception handling.
"""

import pandas as pd
import pytest
import re
from src.etl.extract.extract_fitness_stats import (
    extract_fitness_stats,
    TYPE,
    FILE_PATH,
    EXPECTED_PERFORMANCE,
)


@pytest.fixture
def mock_log_extract_success(mocker):
    """
    Mock the log_extract_success function used in the extract_fitness_stats
    module.

    Args:
        mocker: pytest-mock fixture for creating mock objects

    Returns:
        MagicMock: A mock object for the log_extract_success function
    """
    return mocker.patch(
        "src.etl.extract.extract_fitness_stats.log_extract_success"
    )


@pytest.fixture
def mock_setup_logger(mocker):
    """
    Mock the logger used in the extract_fitness_stats module.

    Args:
        mocker: pytest-mock fixture for creating mock objects

    Returns:
        MagicMock: A mock object for the logger
    """
    return mocker.patch("src.etl.extract.extract_fitness_stats.logger")


def test_extract_fitness_stats_csv_to_dataframe(mocker):
    """
    Test that pd.read_csv call returns a DataFrame properly.

    This test verifies that extract_fitness_stats correctly returns the
    DataFrame that is returned by pd.read_csv.

    Args:
        mocker: pytest-mock fixture for creating mock objects
    """
    # Sample DataFrame to be returned by the mocked pd.read_csv
    sample_df = pd.DataFrame(
        {
            "participant_id": [1, 2, 3],
            "steps": [1000, 2000, 3000],
            "calories_burned": [100, 200, 300],
        }
    )

    # Mock pd.read_csv to return the sample DataFrame
    mocker.patch(
        "src.etl.extract.extract_fitness_stats.pd.read_csv",
        return_value=sample_df,
    )

    # Call the function under test
    result = extract_fitness_stats()

    # Assert
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, sample_df)


def test_log_extract_success_fitness_stats(
    mocker, mock_log_extract_success, mock_setup_logger
):
    """
    Test that log_extract_success is called with correct parameters.

    This test verifies that extract_fitness_stats correctly logs extraction
    metrics using the log_extract_success function with the expected parameters.

    Args:
        mocker: pytest-mock fixture for creating mock objects
        mock_log_extract_success: Mock for log_extract_success function
        mock_setup_logger: Mock for the logger
    """
    mock_execution_time = 0.5  # Mock execution time
    # Mock pd.read_csv to return a sample DataFrame
    sample_df = pd.DataFrame(
        {
            "participant_id": [1, 2, 3],
            "steps": [1000, 2000, 3000],
            "calories_burned": [100, 200, 300],
        }
    )
    mocker.patch(
        "src.etl.extract.extract_fitness_stats.pd.read_csv",
        return_value=sample_df,
    )

    # Mock timeit.default_timer to control execution time measurement
    mock_start_time = 100.0
    mock_end_time = 100.5
    mocker.patch(
        "src.etl.extract.extract_fitness_stats.timeit.default_timer",
        side_effect=[mock_start_time, mock_end_time],
    )

    # Call the function under test
    df = extract_fitness_stats()

    # Assert
    mock_log_extract_success.assert_called_once_with(
        mock_setup_logger,
        TYPE,
        df.shape,
        df.isnull().sum().sum(),
        df.duplicated().sum(),
        df.dtypes.apply(lambda x: x.name).to_dict(),
        mock_execution_time,
        EXPECTED_PERFORMANCE,
    )


def test_extract_fitness_stats_exception_handling(mocker, mock_setup_logger):
    """
    Test exception handling when pd.read_csv raises an error.

    This test verifies that extract_fitness_stats properly handles exceptions
    that occur during the data extraction process, logs the error, and re-raises
    the exception with an appropriate message.

    Args:
        mocker: pytest-mock fixture for creating mock objects
        mock_setup_logger: Mock for the logger
    """
    # Mock pd.read_csv to raise an exception
    mocker.patch(
        "src.etl.extract.extract_fitness_stats.pd.read_csv",
        side_effect=Exception(f"Failed to load CSV file at: {FILE_PATH}"),
    )

    # Call the function under test and assert that it raises an exception
    with pytest.raises(
        Exception,
        match=f"Failed to load the CSV file at: {re.escape(FILE_PATH)}",
    ):
        extract_fitness_stats()

    # Assert
    mock_setup_logger.error.assert_called_once_with(
        f"{FILE_PATH} data extraction failed: Failed to load CSV file at: {FILE_PATH}"
    )
