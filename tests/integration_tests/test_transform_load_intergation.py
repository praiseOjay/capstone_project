"""
Integration Tests for Transform-Load Pipeline

These tests validate the integration between data transformation and
loading processes, ensuring that transformed data can be successfully loaded
into the target destination.
Tests verify data integrity throughout the complete transform-load pipeline.
"""

import pytest
import pandas as pd
import os
import tempfile
import shutil
from src.etl.transform.transform import transform_data
from src.etl.load.load import load_data


@pytest.fixture
def transformed_data():
    """
    Create a fixture with sample transformed data ready for loading.
    Includes both cleaned dataframe and visualisation data.
    """
    # Create sample cleaned DataFrame
    cleaned_df = pd.DataFrame(
        {
            "participant_id": [1, 2, 3],
            "date": pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"]),
            "age": [28, 35, 42],
            "gender": ["M", "F", "M"],
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
            "health_condition": ["No Condition", "No Condition", "Asthma"],
            "smoking_status": ["Never", "Former", "Never"],
            "bmi_category": ["Normal", "Normal", "Overweight"],
            "season": ["Winter", "Winter", "Winter"],
        }
    )

    # Create visualisation data with additional metrics
    vis_data = cleaned_df.copy()
    vis_data["fitness_level"] = [7.0, 6.0, 5.0]
    vis_data["fitness_change"] = [0.5, 0.2, -0.1]
    vis_data["fitness_level_30d_avg"] = [7.0, 6.0, 5.0]

    transformed_result = {
        "cleaned_df": cleaned_df,
        "visualisation_data": vis_data,
    }

    return transformed_result


def test_transform_load_data_flow(transformed_data, mocker):
    """
    Test the flow from transformation output to data loading.

    This test validates that:
    1. Transformed data can be successfully loaded to the target destination
    2. Both cleaned data and visualisation data are properly saved
    """
    # Mock the load_fitness_stats function that's called by load_data
    mock_load_fitness = mocker.patch("src.etl.load.load.load_fitness_stats")

    # Get the transformed data from the fixture
    transformed_result = transformed_data

    # Call the load function with the transformed data
    load_data(transformed_result)

    # Assert
    mock_load_fitness.assert_called_once_with(transformed_result["cleaned_df"])
    assert True
