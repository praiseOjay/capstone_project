import os
import pandas as pd
import numpy as np
from datetime import datetime
from src.utils.file_utils import save_dataframe_to_csv

OUTPUT_DIR = os.path.join("data", "processed")
FILE_NAME = "cleaned_fitness_stats.csv"


# Cleaning function for health and fitness statistics
def clean_fitness_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Master function that orchestrates the cleaning of fitness statistics data

    Args:
        df: Raw fitness statistics DataFrame

    Returns:
        Cleaned DataFrame with corrected data types, standardized values,
        no duplicates, and additional calculated fields
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # 1. DATA CLEANING
    # Handle string null values first (convert to actual NaN)
    df = handle_string_nulls(df)

    # Remove duplicates
    df = remove_duplicates(df)

    # Standardize formatting (strip whitespace)
    df = standardize_formatting(df)

    # Standardize categorical values
    df = standardize_categorical_values(df)

    # Standardize date formats
    df = standardize_dates(df)

    # Handle missing values (before converting to categorical)
    df = handle_missing_values(df)

    # Convert data types
    df = convert_data_types(df)

    # Recalculate BMI
    df = recalculate_bmi(df)

    # 2. DATA ENRICHMENT
    # Add calculated fields and derived metrics
    df = add_calculated_fields(df)

    # Reset index for clean output
    df.reset_index(drop=True, inplace=True)

    # Save the cleaned DataFrame to a CSV file
    save_dataframe_to_csv(df, OUTPUT_DIR, FILE_NAME)

    return df


def handle_string_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Replace string representations of null values with actual NaN"""
    null_values = ["N/A", "NA", "null", "None", "n/a", "-", "", "NA", "nan"]

    for column in df.columns:
        if df[column].dtype == "object":
            df[column] = df[column].replace(null_values, np.nan)

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate records from the dataset"""
    # Count duplicates before removal
    duplicate_count = df.duplicated().sum()
    print(f"Removed {duplicate_count} duplicate records")

    # Remove duplicates and keep the first occurrence
    return df.drop_duplicates()


def standardize_formatting(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize formatting by removing whitespace"""
    # Strip whitespace from string columns
    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = (
            df[column].str.strip()
            if hasattr(df[column], "str")
            else df[column]
        )
    return df


def standardize_categorical_values(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize values in categorical columns to ensure consistency"""
    # Standardize gender values to 'M' and 'F'
    if "gender" in df.columns:
        gender_mapping = {
            "male": "M",
            "Male": "M",
            "MALE": "M",
            "m": "M",
            "M": "M",
            "female": "F",
            "Female": "F",
            "FEMALE": "F",
            "f": "F",
            "F": "F",
        }
        df["gender"] = df["gender"].map(
            lambda x: gender_mapping.get(str(x), x) if pd.notna(x) else x
        )

    # Standardize intensity values
    if "intensity" in df.columns:
        intensity_mapping = {
            "low": "Low",
            "LOW": "Low",
            "L": "Low",
            "l": "Low",
            "medium": "Medium",
            "MEDIUM": "Medium",
            "Med": "Medium",
            "M": "Medium",
            "m": "Medium",
            "high": "High",
            "HIGH": "High",
            "H": "High",
            "h": "High",
        }
        df["intensity"] = df["intensity"].map(
            lambda x: intensity_mapping.get(str(x), x) if pd.notna(x) else x
        )

    # Standardize smoking status
    if "smoking_status" in df.columns:
        smoking_mapping = {
            "non-smoker": "Never",
            "nonsmoker": "Never",
            "former smoker": "Former",
            "current smoker": "Current",
        }
        df["smoking_status"] = df["smoking_status"].map(
            lambda x: (
                smoking_mapping.get(str(x).lower(), x) if pd.notna(x) else x
            )
        )

    # Standardize health_condition
    if "health_condition" in df.columns:
        # Standardize 'None' values to 'No Condition'
        health_mapping = {
            "none": "No Condition",
            "None": "No Condition",
            "NONE": "No Condition",
        }
        df["health_condition"] = df["health_condition"].map(
            lambda x: (
                health_mapping.get(str(x).lower(), x) if pd.notna(x) else x
            )
        )
    return df


def standardize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize date formats by attempting to parse various formats into datetime
    """
    if "date" in df.columns:
        # Try multiple date formats
        date_formats = [
            "%Y-%m-%d",  # 2024-01-15
            "%m/%d/%Y",  # 01/15/2024
            "%d-%m-%Y",  # 15-01-2024
            "%Y/%m/%d",  # 2024/01/15
            "%d %B %Y",  # 15 January 2024
        ]

        # Convert date column to datetime with multiple format support
        df["date"] = pd.to_datetime(
            df["date"], errors="coerce", format="mixed", dayfirst=False
        )

        # For dates that failed to parse with default method, try explicit formats
        mask = df["date"].isna()
        if mask.any():
            for date_format in date_formats:
                # Only process rows with NA dates
                still_na = mask & df["date"].isna()
                if not still_na.any():
                    break

                df.loc[still_na, "date"] = pd.to_datetime(
                    df.loc[still_na, "date"],
                    format=date_format,
                    errors="coerce",
                )

    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values with appropriate strategies for each column"""
    # For categorical columns, fill with mode or specific value
    if "health_condition" in df.columns:
        # Add the "No Condition" value to the column before making it categorical
        df["health_condition"] = df["health_condition"].fillna("No Condition")

        # Also handle empty strings or 'none' text values
        df["health_condition"] = df["health_condition"].replace(
            ["", "none", "None", "NONE"], "No Condition"
        )

    if "smoking_status" in df.columns:
        df["smoking_status"] = df["smoking_status"].fillna("Never")

    # For numeric columns, use median for measurements, mean for calculated fields
    median_fill_cols = [
        "weight_kg",
        "height_cm",
        "resting_heart_rate",
        "blood_pressure_systolic",
        "blood_pressure_diastolic",
        "hydration_level",
    ]

    mean_fill_cols = ["calories_burned", "avg_heart_rate", "daily_steps"]

    for column in median_fill_cols:
        if column in df.columns:
            df[column] = df[column].fillna(df[column].median())

    for column in mean_fill_cols:
        if column in df.columns:
            df[column] = df[column].fillna(df[column].mean())

    # For hours_sleep, use median by age group if possible
    if "hours_sleep" in df.columns and "age" in df.columns:
        # Ensure age is numeric for binning
        if df["age"].dtype == "object":
            df["age"] = pd.to_numeric(df["age"], errors="coerce")

        # Create age bins for more accurate imputation
        bins = [0, 18, 35, 50, 65, 100]
        labels = ["<18", "18-34", "35-49", "50-64", "65+"]
        df["age_group_temp"] = pd.cut(df["age"], bins=bins, labels=labels)

        # Fill missing sleep hours with median by age group
        for age_group in df["age_group_temp"].unique():
            if pd.isna(age_group):
                continue
            median_sleep = df[df["age_group_temp"] == age_group][
                "hours_sleep"
            ].median()
            idx = (df["age_group_temp"] == age_group) & (
                df["hours_sleep"].isna()
            )
            df.loc[idx, "hours_sleep"] = median_sleep

        # Drop temporary column
        df.drop("age_group_temp", axis=1, inplace=True)

        # Fill any remaining NAs with overall median
        df["hours_sleep"] = df["hours_sleep"].fillna(
            df["hours_sleep"].median()
        )

    # Fill intensity with most common value if missing
    if "intensity" in df.columns:
        # Fill with most common value
        most_common_intensity = df["intensity"].mode()[0]
        df["intensity"] = df["intensity"].fillna(most_common_intensity)

    # Fill duration_minutes with median if missing
    if "duration_minutes" in df.columns:
        df["duration_minutes"] = df["duration_minutes"].fillna(
            df["duration_minutes"].median()
        )

    return df


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert specific columns to integers and round floats to 1 decimal place
    """
    # First, convert any mixed type columns to numeric
    numeric_columns = [
        "age",
        "weight_kg",
        "height_cm",
        "duration_minutes",
        "calories_burned",
        "avg_heart_rate",
        "resting_heart_rate",
        "blood_pressure_systolic",
        "blood_pressure_diastolic",
        "daily_steps",
        "hydration_level",
        "stress_level",
        "hours_sleep",
        "bmi",
    ]

    for column in numeric_columns:
        if column in df.columns:
            # First ensure it's a numeric type
            df[column] = pd.to_numeric(df[column], errors="coerce")

            # Fill any NaN values created by coercion
            if column in [
                "age",
                "duration_minutes",
                "avg_heart_rate",
                "stress_level",
                "daily_steps",
            ]:
                df[column] = df[column].fillna(df[column].median())

    # Columns to convert to integers
    int_columns = [
        "age",
        "duration_minutes",
        "avg_heart_rate",
        "stress_level",
        "daily_steps",
    ]

    for column in int_columns:
        if column in df.columns:
            # Convert to integer (first round to avoid truncation)
            df[column] = df[column].round().astype(int)

    # Identify float columns (exclude integer columns and non-numeric columns)
    float_columns = df.select_dtypes(include=["float"]).columns.tolist()

    # Round all float columns to 1 decimal place
    for column in float_columns:
        if column in df.columns:
            df[column] = df[column].round(1)

    # Keep categorical columns as regular strings, NOT categorical dtype
    categorical_columns = [
        "gender",
        "activity_type",
        "intensity",
        "smoking_status",
        "health_condition",
    ]

    for column in categorical_columns:
        if column in df.columns:
            # Ensure these are strings, not categorical types
            df[column] = df[column].astype(str)

    return df


def recalculate_bmi(df: pd.DataFrame) -> pd.DataFrame:
    """Recalculate BMI to ensure accuracy"""
    if "weight_kg" in df.columns and "height_cm" in df.columns:
        # BMI formula: weight (kg) / (height (m))²
        height_m = df["height_cm"] / 100
        df["bmi"] = df["weight_kg"] / (height_m**2)

        # Round to 1 decimal place
        df["bmi"] = df["bmi"].round(1)

    return df


def add_calculated_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Add new calculated fields to enrich the dataset"""
    # 1. Add BMI category
    if "bmi" in df.columns:
        bmi_categories = [
            (df["bmi"] < 18.5, "Underweight"),
            ((df["bmi"] >= 18.5) & (df["bmi"] < 25), "Normal"),
            ((df["bmi"] >= 25) & (df["bmi"] < 30), "Overweight"),
            (df["bmi"] >= 30, "Obese"),
        ]

        df["bmi_category"] = np.select(
            [x[0] for x in bmi_categories],
            [x[1] for x in bmi_categories],
            default="Unknown",
        )

    # 2. Add age groups
    if "age" in df.columns:
        age_bins = [0, 18, 35, 50, 65, 100]
        age_labels = ["Under 18", "18-34", "35-49", "50-64", "65+"]

        # Create age groups using cut - handle out-of-range values
        df["age"] = df["age"].clip(
            0, 100
        )  # Clip age to prevent out-of-range values

        # Use pd.cut with ordered=False to avoid categorical dtype
        age_groups = pd.cut(
            df["age"], bins=age_bins, labels=age_labels, ordered=False
        )

        # Convert to string and handle any NaN values
        df["age_group"] = age_groups.astype(str)
        df["age_group"] = df["age_group"].replace("nan", "Unknown")

        # Ensure no NaN values remain
        if df["age_group"].isna().any():
            df["age_group"] = df["age_group"].fillna("Unknown")

    # 3. Add date-related features
    if "date" in df.columns:
        df["day_of_week"] = df["date"].dt.day_name()
        df["month"] = df["date"].dt.month_name()
        df["year"] = df["date"].dt.year
        df["week_of_year"] = df["date"].dt.isocalendar().week

        # Add season
        month = df["date"].dt.month
        df["season"] = np.select(
            [
                (month >= 3) & (month <= 5),
                (month >= 6) & (month <= 8),
                (month >= 9) & (month <= 11),
                (month == 12) | (month <= 2),
            ],
            ["Spring", "Summer", "Fall", "Winter"],
            default="Unknown",
        )

        # Add weekend/weekday flag
        df["is_weekend"] = df["date"].dt.dayofweek >= 5

    # 4. Add fitness level indicator (based on heart rate, activity duration, intensity)
    if all(
        col in df.columns
        for col in ["avg_heart_rate", "duration_minutes", "intensity"]
    ):
        # Create a numeric intensity value
        intensity_values = {"Low": 1, "Medium": 2, "High": 3}

        # Convert intensity to numeric safely (as it's now a string)
        df["intensity_numeric"] = (
            df["intensity"].map(intensity_values).fillna(1)
        )

        # Calculate fitness score (a float value between 0-10)
        df["fitness_level"] = (
            (
                (df["duration_minutes"] / 30)
                * df["intensity_numeric"]
                * (1 - (df["avg_heart_rate"] - 70) / 130)
            )
            .clip(0, 10)
            .round(1)
        )

        # Create fitness level categories as strings (NOT categorical type)
        conditions = [
            (df["fitness_level"] < 3),
            (df["fitness_level"] >= 3) & (df["fitness_level"] < 6),
            (df["fitness_level"] >= 6) & (df["fitness_level"] < 8),
            (df["fitness_level"] >= 8),
        ]

        choices = ["Low", "Moderate", "Good", "Excellent"]

        df["fitness_category"] = np.select(
            conditions, choices, default="Unknown"
        )

        # Clean up intermediate column
        df.drop("intensity_numeric", axis=1, inplace=True)

    return df
