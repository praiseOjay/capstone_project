"""
Component Tests for transform_data() Function

These tests validate the transform_data() function as a component,
testing the complete transformation pipeline coordination.
"""

import pytest
import pandas as pd
from src.etl.transform.transform import transform_data


@pytest.fixture
def sample_raw_fitness_data():
    """
    Fixture providing sample raw fitness data for transformation testing.
    """
    return pd.DataFrame(
        {
            "participant_id": [
                1,
                2,
                3,
            ],
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
            "fitness_level": [7, 6, 5],
        }
    )


def test_transform_data_success(sample_raw_fitness_data, mocker):
    """
    Test successful transformation of fitness statistics data.
    Verifies that the complete pipeline processes data correctly.
    """
    # Mock the save_dataframe_to_csv function to avoid file I/O during testing
    mock_save = mocker.patch(
        "src.etl.transform.clean_fitness_stats.save_dataframe_to_csv"
    )

    # Mock the visualisation data functions to simplify testing
    mock_vis_data = {"mock": "visualisation data"}
    mocker.patch(
        "src.etl.transform.transform.prepare_visualisation_data",
        return_value=mock_vis_data,
    )

    # Execute the transformation
    result = transform_data(sample_raw_fitness_data)

    # Assert
    assert isinstance(result, dict)
    assert "cleaned_df" in result
    assert "visualisation_data" in result

    # Validate the cleaned DataFrame
    cleaned_df = result["cleaned_df"]
    assert isinstance(cleaned_df, pd.DataFrame)
    assert len(cleaned_df) == len(sample_raw_fitness_data)
    assert result["visualisation_data"] == mock_vis_data
    assert pd.api.types.is_datetime64_any_dtype(
        cleaned_df["date"]
    ), "Date should be datetime"
    assert isinstance(
        cleaned_df["gender"].dtype, object
    ), "Gender should be string type"
    assert isinstance(
        cleaned_df["activity_type"].dtype, object
    ), "Activity type should be string type"
    assert (
        not cleaned_df["health_condition"].isna().any()
    ), "Health condition should have no missing values"
    assert "bmi_category" in cleaned_df.columns, "BMI category should be added"
    assert "age_group" in cleaned_df.columns, "Age group should be added"
    assert "season" in cleaned_df.columns, "Season should be added"
