import psycopg2
from dotenv import dotenv_values
from pathlib import Path


def start_conn(env_path: Path, database_name: str) -> psycopg2.extensions.connection:
    """Initiates a connection to the specified database.

    Reads connection credentials (host, port, user, password) from the
    environment variables file at `env_path`.

    Args:
        env_path: Path to the environment variables file.
        database_name: Name of the database to connect to.

    Returns:
        The connection object for the specified database.
    """
    config = dotenv_values(env_path)
    conn = psycopg2.connect(
        host=config["DB_HOST"],
        port=config["DB_PORT"],
        user=config["DB_USER"],
        password=config["DB_PASSWORD"],
        dbname=database_name,
    )
    return conn