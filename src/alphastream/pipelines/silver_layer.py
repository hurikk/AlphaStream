import logging
import pandas as pd
from pathlib import Path

from src.alphastream.database.postgres_setup import PostgresSetup
from src.alphastream.migrations.postgres_migrations import PostgresMigration
from src.alphastream.queries.postgres_queries import PostgresQuery
from src.alphastream.transformations import silver

logger = logging.getLogger(__name__)


def _run_silver_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """Applies the full Silver transformation pipeline to a DataFrame.

    Parses dates, standardizes ticker symbols, validates that there are
    no future dates or out-of-range prices, and removes duplicate rows.

    Args:
        df: The raw Bronze DataFrame to be cleaned and validated.

    Returns:
        A cleaned and validated DataFrame ready for insertion into the
        Silver layer.
    """
    df = silver.parse_dates(df)
    df = silver.standardize_tickers(df)
    df = silver.validate_no_future_dates(df)
    df = silver.validate_price_ranges(df)
    df = silver.drop_duplicates(df)
    return df


def insert_into_silver_layer(db_name: str, schema_name: str, table_name: str) -> None:
    """Inserts or updates cleaned stock data in the Silver layer.

    Reads records from the Bronze layer that are newer than the last
    ingested timestamp in Silver, applies cleaning, validation, and
    standardization, and writes the resulting data into the specified
    Postgres table.

    Args:
        db_name: Name of the target Postgres database.
        schema_name: Name of the schema containing the Silver table.
        table_name: Name of the Silver table to create (if needed) and
            insert records into.

    Returns:
        None
    """
    env_path = Path(".env")

    PostgresSetup(env_path, db_name).init_db(db_name)
    query = PostgresQuery(env_path, db_name)

    if not query.table_exists_or_no(schema_name, table_name):
        logger.info("Silver table does not exist. Creating structure.")
        PostgresMigration(env_path, db_name).create_table(schema_name, table_name)

    last_silver_ts = query.get_last_ingested_date(schema_name, table_name, column="ingested_at")
    bronze_df = query.get_records("bronze", "main_bronze", since=last_silver_ts)

    if bronze_df.empty:
        logger.info("No new data in Bronze to process.")
        return

    clean_bronze_df = _run_silver_pipeline(bronze_df)

    query.insert_data(clean_bronze_df, schema_name, table_name)
    logger.info(f"{len(clean_bronze_df)} rows inserted into Silver.")
