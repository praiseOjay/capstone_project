"""
Unit tests for the clean_fitness_stats module.

This module contains unit tests for the functions in the clean_fitness_stats
module, focusing on validating the data cleaning functionality using simple
test cases.
"""

import pandas as pd
import pytest
from src.etl.transform.clean_fitness_stats import (
    clean_fitness_stats,
    handle_missing_values,
    convert_data_types,
    standardize_dates,
    OUTPUT_DIR,
    FILE_NAME,
)


@pytest.fixture
def mock_save_dataframe_to_csv(mocker):
    """
    Mock the save_dataframe_to_csv function to avoid file I/O during tests.
    """
    return mocker.patch(
        "src.etl.transform.clean_fitness_stats.save_dataframe_to_csv"
    )


@pytest.fixture
def sample_fitness_data():
    """
    Create a small sample dataset with typical issues for testing the
    cleaning functions.
    """
    return pd.DataFrame(
        {
            "participant_id": [1, 2, 3],
            "date": ["2023-01-01", "01/02/2023", "2023-03-15"],
            "gender": ["Male", "Female", "male"],
            "age": [30, 25, "40"],
            "activity_type": ["Running", "Cycling", "Yoga"],
            "intensity": [
                "High",
                "Medium",
                "low",
            ],
            "health_condition": ["Diabetes", None, ""],
            "smoking_status": [
                "Never",
                "Current",
                "Non-smoker",
            ],
            "weight_kg": [75.5, 62.3, 80.0],
            "height_cm": [180, 165, 175],
            "bmi": [23.3, 22.9, 26.1],
        }
    )


def test_handle_missing_values(sample_fitness_data):
    """
    Test the handle_missing_values function fills missing values with
    appropriate defaults.
    Particularly checking health_condition missing values are handled correctly.
    """
    # Count missing values before cleaning
    missing_before = sample_fitness_data["health_condition"].isna().sum()

    # Call the function under test
    cleaned_df = handle_missing_values(sample_fitness_data)

    # Assert
    assert (
        cleaned_df["health_condition"].isna().sum() == 0
    ), "Missing values should be filled"
    assert (
        "No Condition" in cleaned_df["health_condition"].values
    ), "Empty strings should be converted to 'No Condition'"
    filled_values = (cleaned_df["health_condition"] == "No Condition").sum()
    assert (
        filled_values >= missing_before
    ), "All missing values should be filled with 'No Condition'"


def test_convert_data_types(sample_fitness_data):
    """
    Test the convert_data_types function properly converts data types.
    Verifies mixed type columns are handled correctly.
    """
    # Call the function under test
    converted_df = convert_data_types(sample_fitness_data)

    # Assert
    assert pd.api.types.is_integer_dtype(
        converted_df["age"]
    ), "Age should be converted to integer type"
    assert (
        converted_df.loc[2, "age"] == 40
    ), "String age should be converted to integer"
    assert (
        round(converted_df["weight_kg"][0], 1) == converted_df["weight_kg"][0]
    ), "Weight should be rounded to 1 decimal"


def test_standardize_dates(sample_fitness_data):
    """
    Test the standardize_dates function correctly parses various date formats.
    """
    # Call the function under test
    date_df = standardize_dates(sample_fitness_data)

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(
        date_df["date"]
    ), "Dates should be converted to datetime"
    assert (
        date_df["date"].dt.year.unique()[0] == 2023
    ), "Year should be parsed correctly"
    assert (
        not date_df["date"].isna().any()
    ), "All dates should be parsed successfully"


def test_clean_fitness_stats(sample_fitness_data, mock_save_dataframe_to_csv):
    """
    Test the main clean_fitness_stats function integrates all cleaning steps correctly.
    Verifies the full cleaning process works end-to-end.
    """
    # Call the function under test
    cleaned_df = clean_fitness_stats(sample_fitness_data)

    # Verify various aspects of cleaning were applied

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(
        cleaned_df["date"]
    ), "Dates should be converted"
    assert (
        not cleaned_df["health_condition"].isna().any()
    ), "Missing health conditions should be filled"
    expected_bmi = round(
        sample_fitness_data["weight_kg"][0]
        / ((sample_fitness_data["height_cm"][0] / 100) ** 2),
        1,
    )
    assert cleaned_df["bmi"][0] == expected_bmi, "BMI should be recalculated"
    assert (
        "bmi_category" in cleaned_df.columns
    ), "New calculated fields should be added"
    assert "age_group" in cleaned_df.columns, "Age groups should be added"
    mock_save_dataframe_to_csv.assert_called_once_with(
        cleaned_df, OUTPUT_DIR, FILE_NAME
    )
