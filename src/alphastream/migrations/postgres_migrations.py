from pathlib import Path

from src.alphastream.queries.postgres_queries import PostgresQuery
from src.alphastream.database.helpers import start_conn


def get_table_schema(schema_name: str) -> str:
    """Returns the column definitions (DDL) for a given layer's table.

    Args:
        schema_name: The name of the layer to look up ("landing",
            "bronze", "silver", or "gold").

    Returns:
        A string with the comma-separated column definitions to be
        used inside a `CREATE TABLE` statement, or None if
        `schema_name` is not recognized.
    """
    schemas_dict = {
        "landing": """ticker VARCHAR NOT NULL,
                      reference_date DATE NOT NULL,
                      ingested_at TIMESTAMP NOT NULL,
                      payload JSONB NOT NULL""",

        "bronze": """low NUMERIC,
                     high NUMERIC,
                     open NUMERIC,
                     close NUMERIC,
                     volume BIGINT,
                     ticker VARCHAR NOT NULL,
                     reference_date DATE NOT NULL,
                     ingested_at TIMESTAMP NOT NULL""",

        "silver": """low NUMERIC,
                     high NUMERIC,
                     open NUMERIC,
                     close NUMERIC,
                     volume BIGINT,
                     ticker VARCHAR NOT NULL,
                     reference_date DATE NOT NULL,
                     ingested_at TIMESTAMP NOT NULL""",

        "gold": """low NUMERIC,
                   high NUMERIC,
                   open NUMERIC,
                   close NUMERIC,
                   volume BIGINT,
                   ticker VARCHAR NOT NULL,
                   reference_date DATE NOT NULL,
                   ingested_at TIMESTAMP NOT NULL,
                   return_21d NUMERIC,
                   close_to_ma_21 NUMERIC,
                   rsi_14 NUMERIC,
                   volatility_21d NUMERIC,
                   relative_volume NUMERIC""",
    }

    return schemas_dict.get(schema_name)


class PostgresMigration:
    """Used to create the desired structure within the database.

    Attributes:
        conn: A reference to the connection object established with
            the specified database.
        query: A `PostgresQuery` instance used to check whether
            schemas already exist before creating them.
    """

    def __init__(self, env_path: Path, db_name: str) -> None:
        """Initializes the migration helper with a database connection.

        Args:
            env_path: Path to the environment variables file.
            db_name: Name of the database to connect to.
        """
        self.conn = start_conn(env_path, db_name)
        self.query = PostgresQuery(env_path, db_name)

    def create_schema(self, schema_name: str) -> None:
        """Creates the specified schema if it does not already exist.

        Args:
            schema_name: Name of the schema to create.

        Returns:
            None
        """
        cursor = self.conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        self.conn.commit()
        cursor.close()

    def create_table(self, schema_name: str, table_name: str) -> None:
        """Creates the specified table within the given schema.

        If the schema does not yet exist, it is created first via
        `create_schema`. The table's column definitions are resolved
        by `get_table_schema` based on `schema_name`.

        Args:
            schema_name: Name of the schema to create the table in.
            table_name: Name of the table to create.

        Returns:
            None
        """
        if not self.query.schema_exists_or_no(schema_name):
            self.create_schema(schema_name)

        cursor = self.conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({get_table_schema(schema_name)})")
        self.conn.commit()
        cursor.close()
