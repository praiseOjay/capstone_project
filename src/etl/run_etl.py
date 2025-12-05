import os
import sys
from pathlib import Path
from config.env_config import setup_env
from src.etl.extract.extract import extract_data
from src.etl.transform.transform import transform_data
from src.etl.load.load import load_data
from src.utils.logging_utils import setup_logger


def main():
    # Get the argument from the run_etl command and set up the environment
    setup_env(sys.argv)

    # Set up logger
    logger = setup_logger("etl_pipeline", "etl_pipeline.log")
    try:
        logger.info("Starting ETL pipeline...")
        # Extract unclean health and fitness statistics CSV data
        extracted_data = extract_data()
        # Transform and Enrich health and fitness statistics
        transformed_data = transform_data(extracted_data)
        # Load the transformed data into a CSV file
        load_data(transformed_data)
        logger.info("ETL pipeline completed successfully.")
        print(
            f"ETL pipeline run successfully in "
            f"{os.getenv('ENV', 'error')} environment!"
        )
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
