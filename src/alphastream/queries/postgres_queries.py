import pandas as pd
import datetime
import io
from pathlib import Path

from src.alphastream.database.helpers import start_conn


def get_fields_based_on_layer(layer_name: str) -> str:
    """Returns the comma-separated column list expected for a given layer.

    Args:
        layer_name: The name of the layer to look up ("landing",
            "bronze", or "silver").

    Returns:
        A comma-separated string of column names for the given layer,
        or None if `layer_name` is not recognized.
    """
    fields_dict = {
        "landing": "ticker, reference_date, ingested_at, payload",
        "bronze": "low, high, open, close, volume, ticker, reference_date, ingested_at",
        "silver": "low, high, open, close, volume, ticker, reference_date, ingested_at",
    }
    return fields_dict.get(layer_name)


class PostgresQuery:
    """Enables queries within the Postgres database inside the container.

    Attributes:
        conn: A reference to the connection object established with
            the specified database.
    """

    def __init__(self, env_path: Path, db_name: str) -> None:
        """Initializes the query helper with a database connection.

        Args:
            env_path: Path to the environment variables file.
            db_name: Name of the database to connect to.
        """
        self.conn = start_conn(env_path, db_name)

    @staticmethod
    def db_exists_or_no(env_path: Path, db_name: str) -> bool:
        """Checks whether a database exists.

        Args:
            env_path: Path to the environment variables file.
            db_name: Name of the database to check.

        Returns:
            True if the database exists, False otherwise.
        """
        conn = start_conn(env_path, database_name="postgres")
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone() is not None
        cursor.close()
        conn.close()
        return exists

    def schema_exists_or_no(self, schema_name: str) -> bool:
        """Checks whether a schema exists.

        Args:
            schema_name: Name of the schema to check.

        Returns:
            True if the schema exists, False otherwise.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists

    def table_exists_or_no(self, schema_name: str, table_name: str) -> bool:
        """Checks whether a table exists.

        Args:
            schema_name: Name of the schema containing the table.
            table_name: Name of the table to check.

        Returns:
            True if the table exists, False otherwise.
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
        """Retrieves the most recent day present in a table's records.

        Args:
            schema_name: Name of the schema containing the table.
            table_name: Name of the table to query.

        Returns:
            The most recent date found in the table's "date" column.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT MAX(CAST(date AS DATE)) FROM {schema_name}.{table_name}")
        start_date = cursor.fetchone()[0]
        cursor.close()
        return start_date

    def insert_data(self, actions_data_df: pd.DataFrame, schema_name: str, table_name: str) -> None:
        """Inserts data into the specified table via a CSV COPY operation.

        Args:
            actions_data_df: The DataFrame containing the records to
                insert into the specified table.
            schema_name: Name of the schema containing the target
                table.
            table_name: Name of the table to insert data into.

        Returns:
            None
        """
        buffer_mem = io.StringIO()
        actions_data_df.to_csv(buffer_mem, index=False)
        buffer_mem.seek(0)

        cursor = self.conn.cursor()
        cursor.copy_expert(f"COPY {schema_name}.{table_name} FROM STDIN WITH CSV HEADER", buffer_mem)
        self.conn.commit()
        cursor.close()

    def get_last_ingested_date(
        self,
        schema_name: str,
        table_name: str,
        ticker: str | None = None,
        column: str = "reference_date",
    ) -> datetime.date | datetime.datetime | None:
        """Retrieves the most recent value of a given column.

        Optionally filters by ticker.

        Args:
            schema_name: Name of the schema containing the table.
            table_name: Name of the table to query.
            ticker: Optional ticker to filter the query by. If None,
                considers all tickers.
            column: The column to aggregate with MAX (e.g.
                "reference_date", "ingested_at"). Defaults to
                "reference_date".

        Returns:
            The most recent value for the given column and filter, or
            None if there is no matching record yet.
        """
        query = f"SELECT MAX({column}) FROM {schema_name}.{table_name}"
        params = []

        if ticker is not None:
            query += " WHERE ticker = %s"
            params.append(ticker)

        cursor = self.conn.cursor()
        cursor.execute(query, params or None)
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None

    def get_records(
        self,
        schema_name: str,
        table_name: str,
        ticker: str | None = None,
        since: datetime.datetime | None = None,
    ) -> pd.DataFrame:
        """Retrieves records from the given layer's table.

        The set of columns returned depends on `schema_name`, as
        resolved by `get_fields_based_on_layer`.

        Args:
            schema_name: Name of the schema (layer) to query, e.g.
                "landing", "bronze", or "silver".
            table_name: Name of the table to query.
            ticker: Optional ticker to filter the query by.
            since: Optional lower, exclusive bound on "ingested_at",
                used for incremental loads.

        Returns:
            A DataFrame with the columns corresponding to
            `schema_name`, as defined in `get_fields_based_on_layer`.
        """
        query = f"SELECT {get_fields_based_on_layer(schema_name)} FROM {schema_name}.{table_name}"
        conditions = []
        params = []

        if ticker is not None:
            conditions.append("ticker = %s")
            params.append(ticker)

        if since is not None:
            conditions.append("ingested_at > %s")
            params.append(since)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        return pd.read_sql(query, self.conn, params=params or None)
