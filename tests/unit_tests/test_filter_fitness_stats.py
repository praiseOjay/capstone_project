"""
Unit tests for the filter_fitness_stats module.

This module contains unit tests for the functions in the filter_fitness_stats
module, focusing on validating the data preparation for visualisation.
"""

import pandas as pd
import numpy as np
import pytest
from src.etl.transform.filter_fitness_stats import (
    prepare_visualisation_data,
    add_participant_metrics,
    add_weekly_metrics,
)


@pytest.fixture
def sample_cleaned_fitness_data():
    """
    Create sample cleaned fitness data for testing.

    Creates a dataset with multiple entries per participant for testing
    metrics calculation.
    """
    # Create sample data with multiple entries per participant
    return pd.DataFrame(
        {
            "participant_id": [
                1,
                1,
                1,
                2,
                2,
                2,
                3,
                3,
            ],
            "date": pd.to_datetime(
                [
                    "2023-01-01",
                    "2023-01-15",
                    "2023-01-30",
                    "2023-01-05",
                    "2023-01-20",
                    "2023-02-05",
                    "2023-01-10",
                    "2023-01-25",
                ]
            ),
            "gender": [
                "Male",
                "Male",
                "Male",
                "Female",
                "Female",
                "Female",
                "Male",
                "Male",
            ],
            "age": [30, 30, 30, 25, 25, 25, 40, 40],
            "weight_kg": [80, 79, 78, 65, 64, 63, 90, 89],
            "height_cm": [180, 180, 180, 165, 165, 165, 175, 175],
            "bmi": [24.7, 24.4, 24.1, 23.9, 23.5, 23.1, 29.4, 29.1],
            "activity_type": [
                "Running",
                "Cycling",
                "Running",
                "Yoga",
                "Swimming",
                "Yoga",
                "Cycling",
                "Swimming",
            ],
            "duration_minutes": [45, 50, 55, 60, 65, 70, 30, 35],
            "calories_burned": [450, 470, 490, 200, 220, 240, 300, 320],
            "avg_heart_rate": [140, 135, 130, 110, 105, 100, 130, 125],
            "fitness_level": [
                7,
                7.5,
                8,
                6,
                6.5,
                7,
                5,
                5.5,
            ],
            "intensity": [
                "High",
                "High",
                "Medium",
                "Medium",
                "High",
                "Low",
                "High",
                "Medium",
            ],
            "stress_level": [3, 2, 2, 2, 3, 2, 4, 3],
            "hydration_level": [8, 9, 8, 7, 8, 7, 6, 7],
            "hours_sleep": [7, 7.5, 8, 8, 8.5, 7.5, 6, 6.5],
            "daily_steps": [8000, 8500, 9000, 6000, 6500, 7000, 5000, 5500],
            "resting_heart_rate": [65, 63, 61, 60, 58, 56, 70, 68],
            "blood_pressure_systolic": [
                120,
                118,
                116,
                118,
                115,
                112,
                130,
                127,
            ],
            "blood_pressure_diastolic": [80, 78, 76, 75, 73, 71, 85, 83],
            "health_condition": [
                "None",
                "None",
                "None",
                "None",
                "None",
                "None",
                "Asthma",
                "Asthma",
            ],
            "smoking_status": [
                "Non-smoker",
                "Non-smoker",
                "Non-smoker",
                "Non-smoker",
                "Non-smoker",
                "Non-smoker",
                "Former smoker",
                "Former smoker",
            ],
        }
    )


def test_prepare_visualisation_data(sample_cleaned_fitness_data, mocker):
    """
    Test the prepare_visualisation_data function returns a properly
    processed DataFrame.

    Verifies the function processes data correctly and returns a DataFrame
    with additional metrics.
    """
    # Mock the subfunctions to isolate this test
    mock_participant = mocker.patch(
        "src.etl.transform.filter_fitness_stats.add_participant_metrics",
        return_value=sample_cleaned_fitness_data.copy(),
    )
    mock_weekly = mocker.patch(
        "src.etl.transform.filter_fitness_stats.add_weekly_metrics",
        return_value=sample_cleaned_fitness_data.copy(),
    )

    # Call the function under test
    result_df = prepare_visualisation_data(sample_cleaned_fitness_data)

    # Assert
    assert isinstance(result_df, pd.DataFrame), "Should return a DataFrame"
    mock_participant.assert_called_once()
    mock_weekly.assert_called_once()
    assert pd.api.types.is_datetime64_any_dtype(
        result_df["date"]
    ), "Date should be datetime"


def test_add_participant_metrics(sample_cleaned_fitness_data, mocker):
    """
    Test the add_participant_metrics function calculates participant-level
    metrics correctly.

    Verifies metrics like fitness_trend and consistency_score are calculated
    properly.
    """
    # Mock numpy.polyfit to control its behaviour
    mock_polyfit = mocker.patch(
        "numpy.polyfit", return_value=np.array([0.5, 7.0])
    )  # Slope 0.5, intercept 7.0

    # Call the function under test
    result_df = add_participant_metrics(sample_cleaned_fitness_data)

    # Assert
    new_columns = [
        "fitness_trend",
        "fitness_change",
        "fitness_change_pct",
        "workouts_per_week",
        "consistency_score",
    ]
    for col in new_columns:
        assert col in result_df.columns, f"Column {col} should be added"
    p1_data = result_df[result_df["participant_id"] == 1]
    assert (
        p1_data["fitness_change"].iloc[0] > 0
    ), "Participant 1 should have positive fitness change"
    assert (
        mock_polyfit.call_count > 0
    ), "numpy.polyfit should be called for trend calculation"


def test_add_weekly_metrics(sample_cleaned_fitness_data, mocker):
    """
    Test the add_weekly_metrics function adds weekly aggregation data.

    Verifies that week numbers and rolling averages are added correctly.
    """
    # Call the function under test
    result_df = add_weekly_metrics(sample_cleaned_fitness_data)

    # Assert
    assert "week_of_year" in result_df.columns, "Week of year should be added"
    assert result_df["week_of_year"].min() >= 1, "Week numbers should be valid"
    assert (
        result_df["week_of_year"].max() <= 53
    ), "Week numbers should be valid"
    assert (
        "fitness_level_30d_avg" in result_df.columns
    ), "30-day average column should be added"
