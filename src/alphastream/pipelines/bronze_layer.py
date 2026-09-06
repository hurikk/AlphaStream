import logging
import pandas as pd
from pathlib import Path
from datetime import date
from typing import Optional

from src.alphastream.utils.utils import get_tickers
from src.alphastream.transformations.bronze import expand_payload_df
from src.alphastream.database.postgres_setup import PostgresSetup
from src.alphastream.migrations.postgres_migrations import PostgresMigration
from src.alphastream.queries.postgres_queries import PostgresQuery

logger = logging.getLogger(__name__)


def _parse_landing_infos(
    ticker: str,
    query: PostgresQuery,
    last_date: Optional[date],
    schema_name: str = "landing",
    table_name: str = "main_landing",
) -> pd.DataFrame:
    """Fetches and expands Landing records for a single ticker.

    Retrieves the raw Landing records newer than `last_date` for the
    given ticker and expands the JSON payload into individual columns
    suitable for the Bronze layer.

    Args:
        ticker: The stock ticker symbol to fetch records for.
        query: A `PostgresQuery` instance used to run the retrieval.
        last_date: The most recent date already present in the Bronze
            layer for this ticker, or None if no history exists yet.
        schema_name: Name of the schema containing the Landing table.
            Defaults to "landing".
        table_name: Name of the Landing table to read from. Defaults
            to "main_landing".

    Returns:
        A DataFrame with the expanded payload columns, or an empty
        DataFrame if no Landing records are found.
    """
    landing_df = query.get_records(schema_name, table_name, ticker, last_date)

    if landing_df.empty:
        return pd.DataFrame()

    return expand_payload_df(landing_df)


def insert_into_bronze_layer(db_name: str, schema_name: str, table_name: str) -> None:
    """Inserts or updates expanded stock data in the Bronze layer.

    For each ticker returned by `get_tickers`, fetches the Landing
    records that are newer than the last ingested date in Bronze,
    expands their JSON payload into structured columns, and inserts
    the resulting records into the specified Postgres table. Tickers
    that fail to process are logged and skipped, without interrupting
    the rest of the pipeline.

    Args:
        db_name: Name of the target Postgres database.
        schema_name: Name of the schema containing the Bronze table.
        table_name: Name of the Bronze table to create (if needed) and
            insert records into.

    Returns:
        None
    """
    env_path = Path(".env")
    tickers = [f"{ticker}.SA" for ticker in get_tickers()]

    PostgresSetup(env_path, db_name).init_db(db_name)
    query = PostgresQuery(env_path, db_name)

    if not query.table_exists_or_no(schema_name, table_name):
        logger.info("Bronze table does not exist. Creating structure.")
        PostgresMigration(env_path, db_name).create_table(schema_name, table_name)

    all_records = []

    for ticker in tickers:
        try:
            last_date = query.get_last_ingested_date(schema_name, table_name, ticker)

            (logger.info(f"No history for {ticker}. Transforming all Landing data.") if last_date is None
             else logger.info(f"History found for {ticker} up to {last_date}. Fetching new data."))

            ticker_bronze_df = _parse_landing_infos(ticker, query, last_date)

            if ticker_bronze_df.empty:
                logger.info(f"No new data for {ticker}. Skipping.")
                continue

            all_records.append(ticker_bronze_df)

        except Exception:
            logger.exception(f"Error processing {ticker} in the Bronze Layer.")

    if not all_records:
        logger.info("No new records to insert into Bronze Layer.")
        return

    bronze_df = pd.concat(all_records, ignore_index=True)

    query.insert_data(bronze_df, schema_name, table_name)
    logger.info(f"Total records prepared for Bronze: {len(bronze_df)}")
