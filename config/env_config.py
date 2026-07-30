import os
from typing import List
from dotenv import load_dotenv

ENVS = ["dev", "test", "prod"]


def setup_env(argv: List[str]) -> None:
    """
    Set up the environment for the ETL process.
    Expects environment name ('dev', 'test', 'prod') as an argument.
    """
    if len(argv) == 3 and argv[1] in ["etl_only", "streamlit_only"] and argv[2] in ENVS:
        env = argv[2]
    elif len(argv) == 2 and argv[1] in ENVS:
        env = argv[1]
    else:
        raise ValueError(
            "Please provide an environment: " f"{ENVS}. E.g. run_etl dev"
        )

    cleanup_previous_env()
    os.environ["ENV"] = env

    env_file = ".env" if env == "prod" else f".env.{env}"

    if not os.path.exists(env_file):
        raise FileNotFoundError(f"Environment file '{env_file}' not found")

    print(f"Loading environment variables from: {env_file}")
    load_dotenv(env_file, override=True)


def cleanup_previous_env() -> None:
    """
    Clear relevant environment variables to avoid
    conflicts when switching environments.
    This is useful when running the ETL process
    multiple times with different configurations.
    This function should be called before loading
    new environment variables.
    It clears the environment variables related to
    database configurations to ensure that the
    new environment variables are loaded correctly.
    This is particularly important in a development
    or testing environment where the same script
    might be run multiple times with different
    configurations.
    This function is not necessary in a production
    environment where the script is run once
    with a specific configuration.
    It is a good practice to clear the environment
    variables to avoid any potential conflicts
    or confusion when switching between different
    environments.
    """
    keys_to_clear = [
        "SOURCE_DB_NAME",
        "SOURCE_DB_USER",
        "SOURCE_DB_PASSWORD",
        "SOURCE_DB_HOST",
        "SOURCE_DB_PORT",
        "TARGET_DB_NAME",
        "TARGET_DB_USER",
        "TARGET_DB_PASSWORD",
        "TARGET_DB_HOST",
        "TARGET_DB_PORT",
    ]
    for key in keys_to_clear:
        if key in os.environ:
            del os.environ[key]
