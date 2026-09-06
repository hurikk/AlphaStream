import logging
import pandas as pd
from pathlib import Path

from src.alphastream.database.postgres_setup import PostgresSetup
from src.alphastream.migrations.postgres_migrations import PostgresMigration
from src.alphastream.queries.postgres_queries import PostgresQuery
from src.alphastream.transformations import gold

logger = logging.getLogger(__name__)


def _run_gold_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """Applies the full Gold transformation pipeline to a DataFrame.

    Builds engineered features, validates the resulting output, fills
    missing values using an expanding median, and profiles zero and
    flatline patterns in the data for monitoring purposes.

    Args:
        df: The cleaned Silver DataFrame to be transformed.

    Returns:
        A DataFrame with engineered features, validated and with
        missing values filled, ready for insertion into the Gold
        layer.
    """
    df = gold.build_features(df)
    df = gold.validate_gold_output(df)
    df = gold.fill_missing_with_expanding_median(df)
    gold.profile_zeros_and_flatline(df)
    return df


def insert_into_gold_layer(db_name: str, schema_name: str, table_name: str) -> None:
    """Inserts or updates feature-engineered stock data in the Gold layer.

    Reads records from the Silver layer that are newer than the last
    ingested timestamp in Gold, applies feature engineering and
    validation, and writes the resulting data into the specified
    Postgres table. If validation discards all rows, no data is
    inserted.

    Args:
        db_name: Name of the target Postgres database.
        schema_name: Name of the schema containing the Gold table.
        table_name: Name of the Gold table to create (if needed) and
            insert records into.

    Returns:
        None
    """
    env_path = Path(".env")

    PostgresSetup(env_path, db_name).init_db(db_name)
    query = PostgresQuery(env_path, db_name)

    if not query.table_exists_or_no(schema_name, table_name):
        logger.info("Gold table does not exist. Creating structure.")
        PostgresMigration(env_path, db_name).create_table(schema_name, table_name)

    last_gold_ts = query.get_last_ingested_date(schema_name, table_name, column="ingested_at")
    silver_df = query.get_records("silver", "main_silver", since=last_gold_ts)

    if silver_df.empty:
        logger.info("No new data in Silver to process.")
        return

    gold_df = _run_gold_pipeline(silver_df)

    if gold_df.empty:
        logger.warning("All rows were discarded during validation. Nothing to insert.")
        return

    query.insert_data(gold_df, schema_name, table_name)
    logger.info(f"{len(gold_df)} rows inserted into Gold.")
