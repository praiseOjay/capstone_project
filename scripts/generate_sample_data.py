import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.utils.file_utils import find_project_root


def generate_sample_dataset() -> str:
    root_dir = find_project_root()
    target_dir = os.path.join(root_dir, "data", "raw", "fitness_stats")
    target_file = os.path.join(target_dir, "unclean_fitness_dataset.csv")

    os.makedirs(target_dir, exist_ok=True)

    if not os.path.exists(target_file):
        print(f"Generating sample raw dataset at: {target_file}")
        sample_data = pd.DataFrame(
            {
                "participant_id": list(range(1, 51)),
                "date": ["2024-01-01"] * 25 + ["2024-01-02"] * 25,
                "gender": ["Male", "Female", "M", "F", "male", "female"] * 8 + ["Male", "Female"],
                "age": [25, 30, 35, 40, 45] * 10,
                "height_cm": [175, 165, 180, 160, 185] * 10,
                "weight_kg": [70.5, 65.2, 80.0, 55.4, 90.1] * 10,
                "activity_type": ["Running", "Yoga", "Cycling", "Swimming", "Walking"] * 10,
                "duration_minutes": [30, 45, 60, 30, 45] * 10,
                "intensity": ["High", "Medium", "Low", "L", "M"] * 10,
                "calories_burned": [300, 200, 400, 250, 150] * 10,
                "avg_heart_rate": [130, 110, 140, 125, 105] * 10,
                "resting_heart_rate": [65, 60, 70, 58, 62] * 10,
                "blood_pressure_systolic": [120, 115, 130, 110, 125] * 10,
                "blood_pressure_diastolic": [80, 75, 85, 70, 80] * 10,
                "hydration_level": [3, 4, 2, 3, 4] * 10,
                "hours_sleep": [7.5, 8.0, 6.5, 7.0, 8.5] * 10,
                "stress_level": [3, 2, 4, 1, 3] * 10,
                "daily_steps": [8000, 5000, 10000, 6000, 7500] * 10,
                "bmi": [23.0, 23.9, 24.7, 21.6, 26.3] * 10,
                "health_condition": ["None", "Asthma", None, "", "N/A"] * 10,
                "smoking_status": ["Never", "Former smoker", "Current smoker", "Non-smoker", "Never"] * 10,
            }
        )
        sample_data.to_csv(target_file, index=False)
        print("Sample raw dataset successfully created.")
    else:
        print(f"Raw dataset already exists at: {target_file}")

    return target_file


if __name__ == "__main__":
    generate_sample_dataset()
