import logging
import pandas as pd

logger = logging.getLogger(__name__)


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Converts date/time columns to the correct dtypes.

    Args:
        df: The DataFrame containing a "reference_date" column with
            date values in a format parseable by `pd.to_datetime`.

    Returns:
        A copy of the DataFrame with "reference_date" converted to
        datetime dtype.
    """
    df = df.copy()
    df["reference_date"] = pd.to_datetime(df["reference_date"])
    return df


def standardize_tickers(df: pd.DataFrame) -> pd.DataFrame:
    """Ensures a consistent format for ticker symbols.

    Strips leading/trailing whitespace and converts all ticker symbols
    to uppercase.

    Args:
        df: The DataFrame containing a "ticker" column.

    Returns:
        A copy of the DataFrame with "ticker" values stripped and
        uppercased.
    """
    df = df.copy()
    df["ticker"] = df["ticker"].str.strip().str.upper()
    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Removes duplicate rows for the same ticker and reference date.

    When multiple records exist for the same (ticker, reference_date)
    pair, only the most recently ingested one (by "ingested_at") is
    kept.

    Args:
        df: The DataFrame containing "ticker", "reference_date", and
            "ingested_at" columns.

    Returns:
        A DataFrame with duplicate (ticker, reference_date) rows
        removed, keeping the most recent record for each pair.
    """
    before = len(df)
    df = df.sort_values("ingested_at").drop_duplicates(
        subset=["ticker", "reference_date"], keep="last"
    )
    dropped = before - len(df)
    if dropped:
        logger.info(f"{dropped} duplicate rows removed.")
    return df


def validate_price_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """Removes rows with implausible price or volume values.

    A row is considered valid when all of "open", "high", "low", and
    "close" are strictly positive and "volume" is non-negative. Rows
    that fail this check are dropped and logged.

    Args:
        df: The DataFrame containing "open", "high", "low", "close",
            and "volume" columns.

    Returns:
        A DataFrame containing only the rows with valid price and
        volume values.
    """
    price_cols = ["open", "high", "low", "close"]
    before = len(df)

    mask_valid = (df[price_cols] > 0).all(axis=1) & (df["volume"] >= 0)
    invalid_count = (~mask_valid).sum()
    if invalid_count:
        logger.warning(f"{invalid_count} rows with invalid price/volume discarded.")

    df = df[mask_valid]
    logger.info(f"Price validation: {before} -> {len(df)} rows.")
    return df


def validate_no_future_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Removes records with a reference_date in the future.

    Future dates typically indicate a source or ingestion error, since
    stock data should never be dated ahead of the current day.

    Args:
        df: The DataFrame containing a "reference_date" column with
            datetime values.

    Returns:
        A DataFrame containing only the rows whose "reference_date" is
        on or before today.
    """
    today = pd.Timestamp.now().normalize()
    mask_valid = df["reference_date"] <= today

    invalid_count = (~mask_valid).sum()
    if invalid_count:
        logger.warning(f"{invalid_count} rows with future date discarded.")

    return df[mask_valid]
