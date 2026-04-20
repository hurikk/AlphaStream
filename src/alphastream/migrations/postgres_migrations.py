from pathlib import Path

from src.alphastream.queries.postgres_queries import PostgresQuery
from src.alphastream.database.helpers import start_conn

class PostgresMigration():
    """

    Used to create the desired structure within the database
    
    Attributes:
        conn: stores a reference to the object of the connection established with the specified database
        query: stores a reference to the object for performing queries in a specific database

    """
    
    def __init__(self, env_path: Path, db_name: str) -> None:
        """
        
        Args:
            env_path: path of environmental variables
            db_name: database name to connect to
        
        """
        self.conn = start_conn(env_path, db_name)
        self.query = PostgresQuery(env_path, db_name)
    
    
    def create_schema(self, schema_name: str) -> None:
        """
        
        Used to create a specified schema
        
        Args:
            schema_name: the name of a specific schema
        
        """
        cursor = self.conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        self.conn.commit()
        cursor.close()
    
    
    def create_table(self, schema_name: str, table_name: str) -> None:
        """
        
        Used to create a specified table into a specific schema
        
        Args:
            schema_name: the name of a specific schema
            table_name: the name of a specific table
        
        """
        if not self.query.schema_exists_or_no(schema_name):
            self.create_schema(schema_name)
        
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}(
                Date VARCHAR,
                Close FLOAT,
                High FLOAT,
                Low FLOAT,
                Open FLOAT,
                Volume FLOAT,
                Ticker VARCHAR
            )"""
        )
        self.conn.commit()
        cursor.close()
        