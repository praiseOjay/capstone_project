"""
Filter fitness statistics data for visualisation.

This module prepares a single consolidated parquet file for dashboard visualizations.
This approach simplifies data management while providing one source of truth.
"""

import pandas as pd
import numpy as np
import os
from src.utils.logging_utils import setup_logger

# Initialise logger
logger = setup_logger(__name__, "filter_fitness_stats.log")


def prepare_visualisation_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a consolidated dataset for visualizations.

    Args:
        df (pd.DataFrame): Cleaned fitness statistics DataFrame.
    """
    logger.info("Starting preparation of data for dashboard visualisations.")

    # Create a copy of the dataframe to avoid modifying the original
    clean_df = df.copy()

    # Ensure date is datetime
    clean_df["date"] = pd.to_datetime(clean_df["date"])

    # Add any participant-level calculated metrics
    clean_df = add_participant_metrics(clean_df)

    # Add any week-level aggregation data
    clean_df = add_weekly_metrics(clean_df)

    return clean_df


def add_participant_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate participant-specific metrics like fitness trends and consistency.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with added participant metrics.
    """
    # Calculate metrics that need to be done at participant level
    result_df = df.copy()

    participant_metrics = []

    # Track progress for large datasets
    total_participants = df["participant_id"].nunique()
    logger.info(f"Calculating metrics for {total_participants} participants")

    # Process each participant
    for idx, participant_id in enumerate(df["participant_id"].unique()):
        if idx % 1000 == 0:
            logger.info(f"Processing participant {idx}/{total_participants}")

        user_df = df[df["participant_id"] == participant_id].sort_values(
            "date"
        )

        if len(user_df) < 2:
            continue

        # Calculate fitness level trend
        try:
            x = np.arange(len(user_df))
            y = user_df["fitness_level"].values

            if len(x) > 1:
                coeffs = np.polyfit(x, y, 1)
                slope = coeffs[0]

                # Calculate overall fitness change
                initial_fitness = user_df["fitness_level"].iloc[0]
                current_fitness = user_df["fitness_level"].iloc[-1]
                fitness_change = current_fitness - initial_fitness
                fitness_change_pct = (
                    (fitness_change / initial_fitness * 100)
                    if initial_fitness > 0
                    else 0
                )

                # Calculate consistency metrics
                days_span = (
                    user_df["date"].max() - user_df["date"].min()
                ).days
                if days_span > 0:
                    workouts_per_week = (
                        len(user_df) / (days_span / 7) if days_span > 0 else 0
                    )
                    consistency_score = (
                        (len(user_df) / days_span) * 100
                        if days_span > 0
                        else 0
                    )
                else:
                    workouts_per_week = 0
                    consistency_score = 0

                participant_metrics.append(
                    {
                        "participant_id": participant_id,
                        "fitness_trend": slope,
                        "fitness_change": fitness_change,
                        "fitness_change_pct": fitness_change_pct,
                        "workouts_per_week": workouts_per_week,
                        "consistency_score": consistency_score,
                        "total_workouts": len(user_df),
                        "initial_fitness": initial_fitness,
                        "current_fitness": current_fitness,
                        "total_days": days_span,
                        "total_calories": user_df["calories_burned"].sum(),
                    }
                )
        except:
            # Skip if calculations fail
            logger.warning(
                f"Failed to calculate metrics for participant {participant_id}"
            )
            pass

    # Convert participant metrics to DataFrame
    if participant_metrics:
        participant_df = pd.DataFrame(participant_metrics)

        # Merge these metrics back to the main dataframe
        result_df = result_df.merge(
            participant_df, on="participant_id", how="left"
        )

    return result_df


def add_weekly_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add weekly aggregation metrics for time-based analysis.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with added weekly metrics.
    """
    result_df = df.copy()
    # Add week of year column
    result_df["week_of_year"] = result_df["date"].dt.isocalendar().week

    # Calculate 30-day rolling averages for fitness level
    logger.info("Calculating rolling averages by participant")

    # This would be extremely memory-intensive to do for all participants
    # Instead we'll pre-calculate for a sample of participants (up to 1000)
    sample_participants = (
        df["participant_id"]
        .sample(min(1000, df["participant_id"].nunique()), random_state=42)
        .unique()
    )

    # Create a new column for 30-day average (will be NaN for most participants)
    result_df["fitness_level_30d_avg"] = np.nan

    for participant_id in sample_participants:
        # Get indices for this participant
        participant_mask = result_df["participant_id"] == participant_id
        participant_indices = result_df.index[participant_mask]

        if (
            len(participant_indices) >= 5
        ):  # Only calculate for participants with enough data
            # Sort values for this participant
            temp_df = result_df.loc[participant_indices].sort_values("date")

            # Calculate rolling average
            rolling_avg = (
                temp_df["fitness_level"]
                .rolling(window=30, min_periods=1)
                .mean()
            )

            # Store back in the original dataframe at the correct indices
            result_df.loc[temp_df.index, "fitness_level_30d_avg"] = (
                rolling_avg.values
            )

    return result_df
