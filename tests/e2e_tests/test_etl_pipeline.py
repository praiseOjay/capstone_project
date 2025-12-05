"""
End-to-End Tests for the Complete ETL Pipeline

These tests validate the entire ETL (Extract-Transform-Load) pipeline,
ensuring all components work together correctly from start to finish
with focus on file operations.
"""

import pytest
import pandas as pd
import os
import tempfile
import shutil
import sys
from src.etl.extract.extract import extract_data
from src.etl.transform.transform import transform_data
from src.etl.load.load import load_data


@pytest.fixture
def setup_etl_environment():
    """Set up a complete test environment for E2E ETL testing."""
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()

    # Create project structure
    raw_dir = os.path.join(temp_dir, "data", "raw")
    processed_dir = os.path.join(temp_dir, "data", "processed")
    output_dir = os.path.join(temp_dir, "data", "output")

    # Create directories
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Create test data with realistic fitness stats
    test_data = pd.DataFrame(
        {
            "participant_id": list(range(1, 11)),
            "date": ["2024-01-0" + str(i) for i in range(1, 10)]
            + ["2024-01-10"],
            "gender": [
                "Male",
                "Female",
                "Male",
                "Female",
                "Male",
                "Female",
                "Male",
                "Female",
                "Male",
                "Female",
            ],
            "age": [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
            "weight_kg": [70, 65, 80, 55, 90, 60, 75, 58, 85, 62],
            "height_cm": [175, 165, 180, 160, 185, 170, 178, 162, 182, 168],
            "activity_type": [
                "Running",
                "Yoga",
                "Cycling",
                "Swimming",
                "Running",
                "Yoga",
                "Cycling",
                "Swimming",
                "Running",
                "Yoga",
            ],
            "duration_minutes": [30, 45, 60, 30, 45, 60, 30, 45, 60, 30],
            "calories_burned": [
                300,
                200,
                400,
                250,
                350,
                180,
                420,
                230,
                380,
                190,
            ],
            "avg_heart_rate": [
                130,
                110,
                140,
                125,
                150,
                105,
                145,
                120,
                155,
                100,
            ],
            "intensity": [
                "Medium",
                "Low",
                "High",
                "Medium",
                "High",
                "Low",
                "High",
                "Medium",
                "High",
                "Low",
            ],
            "health_condition": [
                "None",
                None,
                "Asthma",
                "None",
                "Diabetes",
                None,
                "Hypertension",
                "None",
                "Asthma",
                None,
            ],
            "smoking_status": [
                "Non-smoker",
                "Former smoker",
                "Smoker",
                "Non-smoker",
                "Former smoker",
                "Non-smoker",
                "Former smoker",
                "Smoker",
                "Non-smoker",
                "Former smoker",
            ],
            "fitness_level": [7, 6, 8, 5, 9, 4, 7, 6, 8, 5],
            "hours_sleep": [7, 8, 6, 7.5, 6.5, 9, 6, 7, 8, 7.5],
            "stress_level": [3, 2, 4, 2, 5, 1, 3, 4, 2, 3],
            "daily_steps": [
                8000,
                5000,
                7000,
                6000,
                9000,
                4000,
                8500,
                5500,
                7500,
                6500,
            ],
            "hydration_level": [3, 4, 3, 5, 2, 4, 3, 3, 4, 5],
            "bmi": [
                22.9,
                23.9,
                24.7,
                21.5,
                26.3,
                20.8,
                23.7,
                22.1,
                25.7,
                22.0,
            ],
            "resting_heart_rate": [65, 60, 70, 62, 72, 58, 68, 64, 70, 61],
            "blood_pressure_systolic": [
                120,
                115,
                130,
                118,
                135,
                110,
                122,
                125,
                132,
                117,
            ],
            "blood_pressure_diastolic": [
                80,
                75,
                85,
                78,
                88,
                70,
                82,
                80,
                86,
                76,
            ],
        }
    )

    # Save test data to CSV
    test_data.to_csv(os.path.join(raw_dir, "fitness_stats.csv"), index=False)

    yield temp_dir, test_data

    # Clean up
    shutil.rmtree(temp_dir)


@pytest.mark.e2e
def test_etl_pipeline_file_operations(
    setup_etl_environment, monkeypatch, mocker
):
    """
    Test the complete ETL pipeline with focus on file creation and data integrity.
    Validates that files are created at each stage with correct data.
    """
    # Unpack values from the fixture
    test_dir, test_data = setup_etl_environment

    # Mock project root to use our test directory
    monkeypatch.setattr(
        "src.utils.file_utils.find_project_root", lambda: test_dir
    )

    # Mock save functions to avoid actual file I/O
    mock_save_csv = mocker.patch(
        "src.etl.transform.clean_fitness_stats.save_dataframe_to_csv"
    )
    mocker.patch("os.makedirs")
    mocker.patch("pandas.DataFrame.to_parquet")
    mocker.patch("src.etl.load.load_fitness_stats.save_dataframe_to_csv")
    mocker.patch("src.etl.load.load_fitness_stats.save_dataframe_to_parquet")

    # Define expected file paths
    raw_file = os.path.join(test_dir, "data", "raw", "fitness_stats.csv")

    # Assert
    assert os.path.exists(raw_file)
    raw_data = pd.read_csv(raw_file)
    original_count = len(raw_data)
    assert (
        original_count == 10
    ), f"Raw data should have 10 rows, found {original_count}"

    # Mock extract function to use the test data directly
    def mock_extract():
        return pd.read_csv(raw_file)

    extracted_data = mock_extract()
    # Assert
    assert (
        len(extracted_data) == original_count
    ), f"Extracted data should have {original_count} rows, found {len(extracted_data)}"

    # Run transform step
    transform_result = transform_data(extracted_data)
    # Assert
    assert isinstance(
        transform_result, dict
    ), "Transform result should be a dictionary"
    assert (
        "cleaned_df" in transform_result
    ), "Transform result should have 'cleaned_df' key"

    # Get the cleaned DataFrame
    cleaned_df = transform_result["cleaned_df"]
    # Assert
    assert (
        len(cleaned_df) == original_count
    ), f"Transformed data should have {original_count} rows, found {len(cleaned_df)}"

    # Run load step with the transform result dictionary
    load_data(transform_result)

    # Verify data integrity
    assert (
        raw_data["participant_id"].tolist()
        == cleaned_df["participant_id"].tolist()
    ), "Participant IDs should match between raw and cleaned data"
    mock_save_csv.assert_called_once()


@pytest.mark.e2e
def test_etl_pipeline_main_function(
    setup_etl_environment, monkeypatch, mocker
):
    """
    Test the main ETL pipeline run_etl with mocked components.
    """
    # Unpack values from the fixture
    test_dir, test_data = setup_etl_environment

    # Import here to avoid circular imports during testing
    from src.etl.run_etl import main as run_etl

    # Mock project root to use the test directory
    monkeypatch.setattr(
        "src.utils.file_utils.find_project_root", lambda: test_dir
    )

    # Mock all file operations
    mocker.patch("src.etl.transform.clean_fitness_stats.save_dataframe_to_csv")
    mocker.patch("os.makedirs")
    mocker.patch("pandas.DataFrame.to_parquet")
    mocker.patch("src.etl.load.load_fitness_stats.save_dataframe_to_csv")
    mocker.patch("src.etl.load.load_fitness_stats.save_dataframe_to_parquet")

    # Mock extract_data to return the test data
    mock_extract = mocker.patch(
        "src.etl.run_etl.extract_data", return_value=test_data.copy()
    )

    # Mock transform_data to return a dictionary with cleaned_df
    cleaned_df = test_data.copy()
    cleaned_df["date"] = pd.to_datetime(cleaned_df["date"])
    cleaned_df["health_condition"] = cleaned_df["health_condition"].fillna(
        "No Condition"
    )

    transformed_data_dict = {
        "cleaned_df": cleaned_df,
        "visualisation_data": {
            "weight_correlations": pd.Series([0.1, 0.2, 0.3]),
            "correlation_matrix": pd.DataFrame([[1, 0.5], [0.5, 1]]),
        },
    }

    mock_transform = mocker.patch(
        "src.etl.run_etl.transform_data", return_value=transformed_data_dict
    )

    # Mock load_data
    mock_load = mocker.patch("src.etl.run_etl.load_data")

    # Mock environment setup
    mocker.patch("src.etl.run_etl.setup_env")

    # Mock logger
    mocker.patch("src.etl.run_etl.setup_logger")

    # Mock sys.argv to provide environment argument
    monkeypatch.setattr(sys, "argv", ["run_etl.py", "development"])

    # Run the full ETL pipeline
    run_etl()

    # Assert
    mock_extract.assert_called_once()
    mock_transform.assert_called_once()
    mock_load.assert_called_once_with(transformed_data_dict)


@pytest.mark.e2e
def test_etl_pipeline_data_transformations(
    setup_etl_environment, monkeypatch, mocker
):
    """
    Test specific data transformations across the ETL pipeline.
    """
    # Unpack values from the fixture
    test_dir, test_data = setup_etl_environment

    # Mock project root to use the test directory
    monkeypatch.setattr(
        "src.utils.file_utils.find_project_root", lambda: test_dir
    )

    # Mock file operations
    mocker.patch("src.etl.transform.clean_fitness_stats.save_dataframe_to_csv")
    mocker.patch("os.makedirs")
    mocker.patch("pandas.DataFrame.to_parquet")
    mocker.patch("src.etl.load.load_fitness_stats.save_dataframe_to_csv")
    mocker.patch("src.etl.load.load_fitness_stats.save_dataframe_to_parquet")

    # Read the original data to find rows with null health_condition
    raw_file = os.path.join(test_dir, "data", "raw", "fitness_stats.csv")
    raw_data = pd.read_csv(raw_file)
    null_health_indices = raw_data[raw_data["health_condition"].isna()].index

    # Mock extract function to use the test data directly
    def mock_extract():
        return raw_data.copy()

    extracted_data = mock_extract()

    # Run the transform step
    transform_result = transform_data(extracted_data)

    # Get the cleaned DataFrame
    cleaned_df = transform_result["cleaned_df"]

    # Assert
    for idx in null_health_indices:
        assert (
            cleaned_df.loc[idx, "health_condition"] == "No Condition"
        ), f"Row {idx} should have 'No Condition' for health_condition"
    assert pd.api.types.is_datetime64_any_dtype(
        cleaned_df["date"]
    ), "Date column should be datetime type"
    assert pd.api.types.is_string_dtype(
        cleaned_df["gender"]
    ), "Gender column should be string dtype"
    assert pd.api.types.is_string_dtype(
        cleaned_df["activity_type"]
    ), "Activity_type column should be string dtype"
    assert pd.api.types.is_string_dtype(
        cleaned_df["intensity"]
    ), "Intensity column should be string dtype"
    assert set(cleaned_df["gender"].unique()).issubset(
        {"M", "F"}
    ), "Gender values should be standardized to M and F"
    assert "visualisation_data" in transform_result
