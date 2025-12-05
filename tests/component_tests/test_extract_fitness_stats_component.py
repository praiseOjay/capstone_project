"""
Component tests for the extract_fitness_stats module.

This module contains component tests that verify the functionality of the
extract_fitness_stats module using actual data files rather than mocks.
It tests both the correctness of the extraction and the performance against
expected benchmarks.
"""

import pandas as pd
import pytest
import timeit
from unittest.mock import patch
from src.etl.extract.extract_fitness_stats import (
    extract_fitness_stats,
    EXPECTED_PERFORMANCE,
    FILE_PATH,
)


@pytest.fixture
def expected_unclean_fitness_stats_csv():
    """
    Fixture that loads the raw fitness stats data directly from the source file.

    Returns:
        pd.DataFrame: The raw fitness statistics data as a DataFrame
    """
    # Use the same file path that's defined in the implementation
    return pd.read_csv(FILE_PATH)


def test_extract_fitness_stats(
    expected_unclean_fitness_stats_csv,
):
    """
    Test that extract_fitness_stats returns the correct data.

    This test verifies that the function correctly extracts data from the
    source file and returns it as a DataFrame that matches the expected
    raw data.

    Args:
        expected_unclean_fitness_stats_csv: Fixture with the expected raw data
    """
    # Call the function under test
    df = extract_fitness_stats()

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert df.shape == expected_unclean_fitness_stats_csv.shape
    pd.testing.assert_frame_equal(df, expected_unclean_fitness_stats_csv)


def test_extract_fitness_stats_performance():
    """
    Test that extract_fitness_stats meets performance expectations.

    This test measures the execution time of the extract_fitness_stats function
    and verifies that it meets the expected performance benchmark on a
    per-row basis.
    """
    # Measure the execution time of the extraction function
    execution_time = timeit.timeit(
        extract_fitness_stats, globals=globals(), number=1
    )

    df = extract_fitness_stats()

    # Calculate execution time per row and compare to benchmark
    execution_time_per_row = execution_time / df.shape[0]
    # Assert
    assert execution_time_per_row <= EXPECTED_PERFORMANCE, (
        f"Performance test failed: {execution_time_per_row} seconds per row "
        f"exceeds expected {EXPECTED_PERFORMANCE} seconds per row."
    )


@patch(
    "src.etl.extract.extract_fitness_stats.FILE_PATH",
    "invalid/path/to/fitness_dataset.csv",
)
def test_extract_fitness_stats_file_not_found():
    """
    Test that extract_fitness_stats handles missing files appropriately.

    This test uses patching to simulate a missing file scenario and verifies
    that the function raises the expected exception when the file is not found.
    """
    with pytest.raises(Exception, match="Failed to load the CSV file at:"):
        extract_fitness_stats()
