import math
import json
import logging
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from src.alphastream.utils.utils import get_tickers
from src.alphastream.database.postgres_setup import PostgresSetup
from src.alphastream.migrations.postgres_migrations import PostgresMigration
from src.alphastream.queries.postgres_queries import PostgresQuery

logger = logging.getLogger(__name__)


def _download_raw(ticker: str, **yf_kwargs: Any) -> pd.DataFrame:
    """Downloads raw stock data exactly as returned by yfinance.

    No schema transformation, renaming, or formatting is applied here —
    this function only fetches and resets the index of the raw data.

    Args:
        ticker: The stock ticker symbol to download (e.g., "PETR4.SA").
        **yf_kwargs: Additional keyword arguments forwarded to
            `yfinance.download` (e.g., `period`, `start`, `end`).

    Returns:
        A DataFrame with the raw data from yfinance, indexed reset to a
        column. Returns an empty DataFrame if no data is available.
    """
    raw = yf.download(ticker, progress=False, threads=False, **yf_kwargs)
    if raw.empty:
        return raw
    return raw.reset_index()


def _clean_value(value: Any) -> Optional[Any]:
    """Converts NaN float values to None.

    This is necessary so that JSON serialization produces `null` instead
    of the invalid `NaN` token, which Postgres would otherwise reject.

    Args:
        value: The value to clean, of any type.

    Returns:
        None if the value is a NaN float, otherwise the original value
        unchanged.
    """
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _to_raw_records(raw_df: pd.DataFrame, ticker: str) -> list[dict]:
    """Serializes each row of the raw DataFrame into a Landing record.

    The original payload is preserved as JSON, while the reference date
    is extracted separately as control metadata — needed to track what
    has already been downloaded for incremental updates.

    Args:
        raw_df: The raw DataFrame returned by `_download_raw`.
        ticker: The stock ticker symbol associated with the data.

    Returns:
        A list of dictionaries, each representing one record with keys
        "ticker", "reference_date", "ingested_at", and "payload".
    """
    records = []
    for row in raw_df.to_dict(orient="records"):
        # The first column, whatever its exact name returned by yfinance
        # (e.g., "Date" or a MultiIndex tuple), is always the date
        date_key = raw_df.columns[0]
        raw_date = row[date_key]
        parsed_date = pd.to_datetime(raw_date).date()

        raw_payload = {
            str(key): _clean_value(value)
            for key, value in row.items()
        }

        records.append({
            "ticker": ticker,
            "reference_date": parsed_date.isoformat(),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload": json.dumps(raw_payload, default=str),
        })
    return records


def insert_into_landing_layer(db_name: str, schema_name: str, table_name: str) -> None:
    """Inserts or updates raw stock data in the Landing layer.

    For each ticker returned by `get_tickers`, downloads either the full
    20-year history (if no prior data exists) or only the missing days
    since the last ingested date, then inserts the resulting raw records
    into the specified Postgres table. Tickers that fail to download are
    logged and skipped, without interrupting the rest of the pipeline.

    Args:
        db_name: Name of the target Postgres database.
        schema_name: Name of the schema containing the Landing table.
        table_name: Name of the Landing table to create (if needed) and
            insert records into.

    Returns:
        None
    """
    env_path = Path(".env")
    tickers = [f"{ticker}.SA" for ticker in get_tickers()]

    PostgresSetup(env_path, db_name).init_db(db_name)
    query = PostgresQuery(env_path, db_name)

    if not query.table_exists_or_no(schema_name, table_name):
        logger.info("Landing table does not exist. Creating structure.")
        PostgresMigration(env_path, db_name).create_table(schema_name, table_name)

    all_records = []

    for ticker in tickers:
        try:
            last_date = query.get_last_ingested_date(schema_name, table_name, ticker)

            if last_date is None:
                logger.info(f"No history for {ticker}. Downloading 20 years.")
                raw_df = _download_raw(ticker, period="20y")
            else:
                start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                end_date = datetime.today().strftime("%Y-%m-%d")
                if start_date >= end_date:
                    logger.info("%s is already up to date.", ticker)
                    continue
                logger.info("Updating %s from %s to %s.", ticker, start_date, end_date)
                raw_df = _download_raw(ticker, start=start_date, end=end_date)

            if raw_df.empty:
                logger.info("No new data for %s.", ticker)
                continue

            all_records.extend(_to_raw_records(raw_df, ticker))

        except Exception:
            logger.warning("Failed to process %s.", ticker, exc_info=True)
            continue

    if not all_records:
        logger.info("No new data to insert into Landing.")
        return

    all_records = pd.DataFrame(all_records)

    query.insert_data(all_records, schema_name, table_name)
    logger.info(
        "Inserted %d raw records into %s.%s.",
        len(all_records), schema_name, table_name
    )
