"""
Integration Tests for Extract-Transform Pipeline

These tests validate the integration between data extraction and
transformation processes, ensuring that data flows correctly from the extraction
phase through to transformation.
Tests verify that the complete pipeline produces properly formatted data
ready for visualisation.
"""

import pytest
import pandas as pd
from src.etl.transform.transform import transform_data
from src.etl.extract.extract import extract_data


def test_extract_transform_file_integration(mocker):
    """
    Test the complete extract-transform integration to ensure data flows
    properly through the pipeline.

    This test validates that:
    1. Data can be extracted from a source
    2. The extracted data can be successfully transformed
    3. The transformation produces properly formatted data with expected features
    """
    # Create sample data with exactly 3 rows
    sample_data = pd.DataFrame(
        {
            "participant_id": [1, 2, 3],
            "date": ["2024-01-15", "2024-01-16", "2024-01-17"],
            "age": [28, 35, 42],
            "gender": ["Male", "Female", "Male"],
            "height_cm": [180, 165, 175],
            "weight_kg": [75.5, 62.3, 88.1],
            "activity_type": ["Running", "Yoga", "Cycling"],
            "duration_minutes": [45, 60, 30],
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
            "health_condition": ["None", None, "Asthma"],
            "smoking_status": ["Non-smoker", "Former smoker", "Smoker"],
        }
    )

    # Mock extract_fitness_stats where it's imported in extract.py
    mocker.patch(
        "src.etl.extract.extract.extract_fitness_stats",
        return_value=sample_data,
    )

    # Mock save_dataframe_to_csv to avoid file I/O during testing
    mocker.patch("src.etl.transform.clean_fitness_stats.save_dataframe_to_csv")

    # Extract data using the function with the mocked dependencies
    extracted_df = extract_data()

    # Assert
    assert len(extracted_df) == 3, "Extract should return exactly 3 rows"

    # Now transform the extracted data
    transformed_result = transform_data(extracted_df)

    # Assert
    assert isinstance(transformed_result, dict)
    assert "cleaned_df" in transformed_result
    assert "visualisation_data" in transformed_result

    # Check key aspects of transformed data
    cleaned_df = transformed_result["cleaned_df"]

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(
        cleaned_df["date"]
    ), "Dates should be converted to datetime"
    assert (
        not cleaned_df["health_condition"].isna().any()
    ), "Health condition should have no null values"
    assert set(cleaned_df["gender"].unique()).issubset(
        {"M", "F"}
    ), "Gender should be standardised to M/F"

    assert True
