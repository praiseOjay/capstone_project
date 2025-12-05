"""
Component Tests for clean_fitness_stats Module

These tests validate the clean_fitness_stats module's functionality,
ensuring proper data cleaning and transformation.
"""

import pandas as pd
import pytest
import tempfile
import shutil
from src.etl.transform.clean_fitness_stats import (
    clean_fitness_stats,
    handle_missing_values,
    convert_data_types,
    standardize_dates,
    OUTPUT_DIR,
    FILE_NAME,
)


@pytest.fixture
def test_data_environment():
    """Create a temporary environment with test data"""
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()

    # Create sample test data with common issues to test cleaning
    raw_data = pd.DataFrame(
        {
            "participant_id": [
                1,
                2,
                3,
            ],
            "date": [
                "2024-01-15",
                "01/16/2024",
                "17-01-2024",
            ],
            "age": [28, "35", 42],
            "gender": ["Male", "female", "MALE"],
            "height_cm": [180, 165, 175],
            "weight_kg": [75.5, 62.3, 88.1],
            "activity_type": ["Running", "Yoga", "Cycling"],
            "duration_minutes": [45, 60, None],
            "intensity": ["High", "medium", "H"],
            "calories_burned": [450, 200, 300],
            "avg_heart_rate": [140, 120, 135],
            "hours_sleep": [7, 8, 6],
            "stress_level": [3, 2, 4],
            "daily_steps": [8000, 5000, 7000],
            "hydration_level": [3, 4, 3],
            "bmi": [23.2, 22.8, 28.7],
            "resting_heart_rate": [65, 60, 70],
            "blood_pressure_systolic": [120, 115, 130],
            "blood_pressure_diastolic": [80, 75, 85],
            "health_condition": ["None", None, "Asthma"],
            "smoking_status": [
                "Non-smoker",
                "Former smoker",
                "non-smoker",
            ],
            "fitness_level": [7, 6, 5],
        }
    )

    # Create expected cleaned data (how the data should look after cleaning)
    expected_cleaned = pd.DataFrame(
        {
            "participant_id": [1, 2, 3],
            "date": pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"]),
            "age": [28, 35, 42],
            "gender": ["M", "F", "M"],
            "height_cm": [180, 165, 175],
            "weight_kg": [75.5, 62.3, 88.1],
            "activity_type": ["Running", "Yoga", "Cycling"],
            "duration_minutes": [45, 60, 45],
            "intensity": ["High", "Medium", "High"],
            "calories_burned": [450, 200, 300],
            "avg_heart_rate": [140, 120, 135],
            "hours_sleep": [7, 8, 6],
            "stress_level": [3, 2, 4],
            "daily_steps": [8000, 5000, 7000],
            "hydration_level": [3, 4, 3],
            "bmi": [23.2, 22.8, 28.7],
            "resting_heart_rate": [65, 60, 70],
            "blood_pressure_systolic": [120, 115, 130],
            "blood_pressure_diastolic": [80, 75, 85],
            "health_condition": [
                "No Condition",
                "No Condition",
                "Asthma",
            ],
            "smoking_status": ["Never", "Former", "Never"],
            "fitness_level": [7, 6, 5],
        }
    )

    yield raw_data, expected_cleaned

    # Clean up
    shutil.rmtree(temp_dir)


def test_clean_fitness_stats(test_data_environment, mocker):
    """
    Test the clean_fitness_stats function with sample test data.
    Verifies that the function properly orchestrates the cleaning process.
    """
    # Mock the save_dataframe_to_csv function to avoid file I/O
    mock_save = mocker.patch(
        "src.etl.transform.clean_fitness_stats.save_dataframe_to_csv"
    )

    # Get test data
    raw_data, expected_cleaned = test_data_environment

    # Call the function to clean the data
    cleaned_df = clean_fitness_stats(raw_data)

    # Assert
    mock_save.assert_called_once_with(cleaned_df, OUTPUT_DIR, FILE_NAME)
    assert (
        cleaned_df.shape[0] == expected_cleaned.shape[0]
    ), "Row count should match"
    for col in expected_cleaned.columns:
        assert (
            col in cleaned_df.columns
        ), f"Column '{col}' missing from cleaned data"
    assert pd.api.types.is_datetime64_any_dtype(
        cleaned_df["date"]
    ), "Date column should be datetime"
    assert (
        not cleaned_df["health_condition"].isna().any()
    ), "Health condition should have no null values"
    assert (
        "bmi_category" in cleaned_df.columns
    ), "BMI category field should be added"
    assert "age_group" in cleaned_df.columns, "Age group field should be added"
    assert "season" in cleaned_df.columns, "Season field should be added"


def test_handle_missing_values(test_data_environment):
    """
    Test the handle_missing_values function specifically.
    Ensures that missing values are properly filled with appropriate defaults.
    """
    # Get test data
    raw_data, _ = test_data_environment

    # Assert
    null_count_before = raw_data["health_condition"].isna().sum()
    assert (
        null_count_before > 0
    ), "Test data should have at least one null health_condition value"

    # Call the function to handle missing values
    cleaned_df = handle_missing_values(raw_data)

    # Assert
    assert (
        cleaned_df["health_condition"].isna().sum() == 0
    ), "Health condition should have no null values after cleaning"

    no_condition_count = cleaned_df[
        cleaned_df["health_condition"] == "No Condition"
    ].shape[0]
    assert (
        no_condition_count >= null_count_before
    ), "At least all null values should be filled with 'No Condition'"


def test_convert_data_types(test_data_environment):
    """
    Test the convert_data_types function specifically.
    Ensures proper type conversion of columns, particularly handling mixed types.
    """
    # Get test data
    raw_data, _ = test_data_environment

    # Assert
    assert not pd.api.types.is_numeric_dtype(
        raw_data["age"]
    ), "Age column should be mixed type for this test"

    # Call the function to convert data types
    cleaned_df = convert_data_types(raw_data)

    # Assert
    assert pd.api.types.is_integer_dtype(
        cleaned_df["age"]
    ), "Age should be converted to integer"
    assert pd.api.types.is_integer_dtype(
        cleaned_df["daily_steps"]
    ), "Daily steps should be integer"

    assert (
        cleaned_df["weight_kg"]
        .apply(lambda x: len(str(x).split(".")[1]) <= 1)
        .all()
    ), "Float values should be rounded to 1 decimal"

    assert pd.api.types.is_object_dtype(
        cleaned_df["gender"]
    ), "Gender should remain as string/object type"
    assert pd.api.types.is_object_dtype(
        cleaned_df["health_condition"]
    ), "Health condition should remain as string/object type"


def test_standardize_dates(test_data_environment):
    """
    Test the standardize_dates function specifically.
    Ensures that various date formats are properly converted to datetime.
    """
    # Get test data with mixed date formats
    raw_data, _ = test_data_environment

    # Assert
    assert isinstance(raw_data["date"][0], str), "First date should be string"
    assert isinstance(raw_data["date"][1], str), "Second date should be string"

    # Call the function to standardise dates
    cleaned_df = standardize_dates(raw_data)

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(
        cleaned_df["date"]
    ), "All dates should be converted to datetime"

    assert cleaned_df["date"][0].year == 2024, "First date year should be 2024"
    assert (
        cleaned_df["date"][1].month == 1
    ), "Second date month should be January"
    assert cleaned_df["date"][2].day == 17, "Third date day should be 17"
