import os
import sys
import subprocess
from pathlib import Path
from config.env_config import setup_env
from src.etl.run_etl import main as run_etl
from src.utils.logging_utils import setup_logger

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def main():
    # Get the argument from the run_app command and set up the environment
    setup_env(sys.argv)

    # Set up logger
    logger = setup_logger("app", "app.log")

    try:
        logger.info("Starting ETL pipeline...")
        # Run the ETL pipeline
        run_etl()
        logger.info("ETL pipeline completed successfully.")

        # Start the Streamlit app using subprocess
        logger.info("Starting Streamlit app...")
        streamlit_script = project_root / "src" / "streamlit" / "app.py"

        # Run streamlit as a subprocess
        cmd = [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(streamlit_script),
            "--server.headless",
            "false",
            "--server.port",
            "8501",
            "--server.address",
            "localhost",
        ]

        logger.info(f"Running command: {' '.join(cmd)}")

        # This will start the Streamlit server
        result = subprocess.run(cmd, cwd=project_root)

        if result.returncode == 0:
            logger.info("Streamlit app started successfully.")
        else:
            logger.error(
                f"Streamlit app failed with return code: {result.returncode}"
            )

    except Exception as e:
        logger.error(f"App run failed: {e}")
        sys.exit(1)


def run_etl_only():
    """Run only the ETL pipeline without Streamlit."""
    setup_env(sys.argv)
    logger = setup_logger("etl_only", "etl_only.log")

    try:
        logger.info("Starting ETL pipeline...")
        run_etl()
        logger.info("ETL pipeline completed successfully.")
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        sys.exit(1)


def run_streamlit_only():
    """Run only the Streamlit app."""
    setup_env(sys.argv)
    logger = setup_logger("streamlit_only", "streamlit_only.log")

    try:
        logger.info("Starting Streamlit app...")
        project_root = Path(__file__).parent.parent
        streamlit_script = project_root / "src" / "streamlit" / "app.py"

        cmd = [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(streamlit_script),
            "--server.headless",
            "false",
            "--server.port",
            "8501",
            "--server.address",
            "localhost",
        ]

        result = subprocess.run(cmd, cwd=project_root)

        if result.returncode == 0:
            logger.info("Streamlit app started successfully.")
        else:
            logger.error(
                f"Streamlit app failed with return code: {result.returncode}"
            )

    except Exception as e:
        logger.error(f"Streamlit app failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Check for command line arguments to determine what to run
    if len(sys.argv) > 1:
        if sys.argv[1] == "etl_only":
            run_etl_only()
        elif sys.argv[1] == "streamlit_only":
            run_streamlit_only()
        else:
            main()
    else:
        main()
