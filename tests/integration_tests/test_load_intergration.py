"""
Integration Tests for Load Pipeline Component

These tests validate that the load component works correctly with different
data inputs and properly saves CSV files.
"""

import pytest
import pandas as pd
import os
import tempfile
import shutil

from src.etl.load.load import load_data
from src.etl.load.load_fitness_stats import load_fitness_stats


@pytest.fixture
def sample_transformed_data():
    """Fixture providing sample transformed data ready for loading."""
    # Create sample data that matches the expected format after transformation
    cleaned_df = pd.DataFrame(
        {
            "participant_id": [1, 2, 3],
            "date": pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"]),
            "gender": pd.Categorical(["Male", "Female", "Male"]),
            "age": [28, 35, 42],
            "weight_kg": [75.5, 62.3, 88.1],
            "height_cm": [180, 165, 175],
            "activity_type": pd.Categorical(["Running", "Yoga", "Cycling"]),
            "duration_minutes": [45, 60, 30],
            "calories_burned": [450, 200, 300],
            "heart_rate_bpm": [140, 120, 135],
            "intensity": pd.Categorical(["High", "Medium", "High"]),
            "health_condition": pd.Categorical(
                ["None", "No Condition", "Asthma"]
            ),
            "smoking_status": pd.Categorical(
                ["Non-smoker", "Former smoker", "Smoker"]
            ),
            "fitness_level": [7, 6, 5],
        }
    )

    # Return both a DataFrame and a dict with the DataFrame
    transformed_dict = {
        "cleaned_df": cleaned_df,
        "visualisation_data": {"mock": "visualization data"},
    }

    return cleaned_df, transformed_dict


@pytest.fixture
def setup_temp_directory():
    """Create a temporary directory structure for testing file operations."""
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()

    # Create output directory structure
    output_dir = os.path.join(temp_dir, "data", "output")
    os.makedirs(output_dir, exist_ok=True)

    yield temp_dir

    # Clean up after test
    shutil.rmtree(temp_dir)


@pytest.mark.integration
def test_load_integration_with_dataframe(
    sample_transformed_data, setup_temp_directory, monkeypatch, mocker
):
    """
    Test that the load component correctly handles DataFrame input.
    """
    cleaned_df, _ = sample_transformed_data
    test_dir = setup_temp_directory

    # Mock the load_fitness_stats function
    mock_load_fitness = mocker.patch("src.etl.load.load.load_fitness_stats")

    # Mock project root to use the test directory
    monkeypatch.setattr(
        "src.utils.file_utils.find_project_root", lambda: test_dir
    )

    # Run load function with DataFrame input
    load_data(cleaned_df)

    # Assert
    mock_load_fitness.assert_called_once_with(cleaned_df)


@pytest.mark.integration
def test_load_integration_with_dictionary(
    sample_transformed_data, setup_temp_directory, monkeypatch, mocker
):
    """
    Test that the load component correctly handles dictionary input.
    """
    cleaned_df, transformed_dict = sample_transformed_data
    test_dir = setup_temp_directory

    # Mock the load_fitness_stats function
    mock_load_fitness = mocker.patch("src.etl.load.load.load_fitness_stats")

    # Mock project root to use the test directory
    monkeypatch.setattr(
        "src.utils.file_utils.find_project_root", lambda: test_dir
    )

    # Run load function with dictionary input
    load_data(transformed_dict)

    # Assert
    mock_load_fitness.assert_called_once_with(cleaned_df)


@pytest.mark.integration
def test_load_fitness_stats_csv_output(
    sample_transformed_data, setup_temp_directory, monkeypatch, mocker
):
    """
    Test that load_fitness_stats correctly writes to CSV and Parquet files.
    """
    cleaned_df, _ = sample_transformed_data
    test_dir = setup_temp_directory

    # Mock project root to use the test directory
    monkeypatch.setattr(
        "src.utils.file_utils.find_project_root", lambda: test_dir
    )

    # Mock the save functions to verify they're called correctly
    mock_save_csv = mocker.patch(
        "src.etl.load.load_fitness_stats.save_dataframe_to_csv"
    )
    mock_save_parquet = mocker.patch(
        "src.etl.load.load_fitness_stats.save_dataframe_to_parquet"
    )

    # Run the load_fitness_stats function
    load_fitness_stats(cleaned_df)

    # Assert
    mock_save_csv.assert_called_once()
    mock_save_parquet.assert_called_once()
