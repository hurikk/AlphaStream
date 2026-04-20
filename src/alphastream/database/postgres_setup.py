from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path

from src.alphastream.queries.postgres_queries import PostgresQuery
from src.alphastream.database.helpers import start_conn

class PostgresSetup():
    """

    Used to create a new database
    
    Attributes:
        conn: stores a reference to the object of the connection established with the specified database
        db_exists_or_no: Checks whether the database you want to create already exists or not

    """
    
    def __init__(self, env_path: Path, new_db_name: str) -> None:
        """
        
        Args:
            env_path: path of environmental variables
            new_db_name: the name of the database you want to create
        
        """
        self.conn = start_conn(env_path, database_name="postgres")
        self.db_exists_or_no = PostgresQuery.db_exists_or_no(env_path, new_db_name)
 
   
    def create_new_db(self, new_db_name: str) -> None:
        """
        
        Create a new database if it doesn't already exist
        
        Args:
            new_db_name: the name of the database you want to create
        
        """
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = self.conn.cursor()
        if not self.db_exists_or_no:
            cursor.execute(f"CREATE DATABASE {new_db_name}")
        cursor.close()
        self.conn.close()
    
    
    def init_db(self, new_db_name: str) -> None:
        """
        
        Used to call the function that creates the new database
        
        Args:
            new_db_name: the name of the database you want to create
        
        """
        self.create_new_db(new_db_name)
    