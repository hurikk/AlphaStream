import psycopg2
from dotenv import dotenv_values
from pathlib import Path

def start_conn(env_path: Path, database_name: str) -> psycopg2.extensions.connection:
    """
        
    Used to initiate a connection to the specified database
    
    Args:
        env_path: path of environmental variables
        database_name: the name of the database to connect to
        
    Returns:
        The connection refenrence to the specified database
    
    """
    config = dotenv_values(env_path)
    conn = psycopg2.connect(
        host=config["DB_HOST"],
        port=config["DB_PORT"],
        user=config["DB_USER"],
        password=config["DB_PASSWORD"],
        dbname=database_name
    )
    return conn
    