import polars as pl
import datetime
import io
from pathlib import Path

from src.alphastream.database.helpers import start_conn

class PostgresQuery:
    """

    Enables queries within the Postgres database inside the container
    
    Attributes:
        conn: stores a reference to the object of the connection established with the specified database

    """
    
    def __init__(self, env_path: Path, db_name: str) -> None:
        """
        
        Args:
            env_path: path of environmental variables
            db_name: database name to connect to
        
        """
        self.conn = start_conn(env_path, db_name)
        
    @staticmethod
    def db_exists_or_no(env_path, db_name: str) -> bool:
        """
        
        Checks whether or not a database exists
        
        Args:
            env_path: path of environmental variables
            db_name: database name to make the query
            
        Returns:
            A boolean value where True means the database exists and False means it dosn't exists
        
        """
        conn = start_conn(env_path, database_name="postgres")
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone() is not None
        cursor.close()
        conn.close()
        return exists
    
    
    def schema_exists_or_no(self, schema_name: str) -> bool:
        """
        
        Checks whether or not a schema exists
        
        Args:
            schema_name: the schema name to make the query
            
        Returns:
            A boolean value where True means the schema exists and False means it dosn't exists
        
        """
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists
    
    
    def table_exists_or_no(self, schema_name:str, table_name: str) -> bool:
        """
        
        Checks whether or not a table exists
        
        Args:
            schema_name: the schema name to make the query
            table_name: the table name to make the query
            
        Returns:
            A boolean value where True means the table exists and False means it dosn't exists
        
        """
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            AND table_name = '{table_name}'
        """)
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists
    
    
    def get_most_recent_day(self, schema_name: str, table_name: str) -> datetime.date:
        """
        
        Retrieves the most recent day from the table records
        
        Args:
            schema_name: the schema name to make the query
            table_name: the table name to make the query
            
        Returns:
            The most recent day from the table records
        
        """
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT MAX(CAST(date AS DATE)) FROM {schema_name}.{table_name}")
        start_date = cursor.fetchone()[0]
        cursor.close()
        return start_date
    
    
    def insert_data(self, actions_data_df: pl.DataFrame, schema_name: str, table_name: str) -> None:
        """
        
        Inserts data into the specified table of the specified schema
        
        Args:
            actions_data_df: the polars dataframe from which you want to insert records into the specified table
            schema_name: the schema name to speficy where to insert the data
            table_name: the table name to speficy where to insert the data
        
        """
        buffer_mem = io.StringIO()
        actions_data_df.write_csv(buffer_mem)
        buffer_mem.seek(0)
        
        cursor = self.conn.cursor()
        cursor.copy_expert(f"COPY {schema_name}.{table_name} FROM STDIN WITH CSV HEADER", buffer_mem)
        self.conn.commit()
        cursor.close()
        