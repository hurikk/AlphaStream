import ast
import pandas as pd


def parse_payload_key(key: str) -> str:
    """Decodes a serialized tuple-like key into a normalized field name.

    Converts keys such as "('Close', 'MGLU3.SA')" into "Close" and
    "('Date', '')" into "Date". Keys that are not tuple-like strings
    are returned unchanged.

    Args:
        key: The raw payload key, potentially a string representation
            of a tuple in the form "('field', 'ticker')".

    Returns:
        The normalized field name extracted from the tuple, or the
        original key if it could not be parsed as a tuple.
    """
    try:
        field, _ = ast.literal_eval(key)
        return f"{field}"
    except (ValueError, SyntaxError):
        # Key is already in "normal" form, not a tuple
        return key


def parse_payload(payload: dict) -> dict:
    """Normalizes all keys in a raw payload dict.

    Applies `parse_payload_key` to every key in the payload and removes
    the "Date" key, since the date is already tracked separately as
    control metadata (`reference_date`).

    Args:
        payload: The raw payload dict, as deserialized from the
            Landing layer's JSON column, with tuple-like string keys.

    Returns:
        A dict with normalized keys and the "Date" key removed.
    """
    expanded_payload = {parse_payload_key(k): v for k, v in payload.items()}
    del expanded_payload["Date"]
    return expanded_payload


def expand_row(row: dict) -> dict:
    """Expands a single Landing row into a flattened Bronze row.

    Uses `parse_payload` to normalize the payload keys and re-attaches
    the control columns (`ticker`, `reference_date`, `ingested_at`)
    that came from the Landing layer.

    Args:
        row: A dict representing one row from the Landing DataFrame,
            containing "payload", "ticker", "reference_date", and
            "ingested_at" keys.

    Returns:
        A dict with the expanded payload fields plus the original
        control columns, ready to become one row of the Bronze
        DataFrame.
    """
    expanded = parse_payload(row["payload"])
    expanded["ticker"] = row["ticker"]
    expanded["reference_date"] = row["reference_date"]
    expanded["ingested_at"] = row["ingested_at"]
    return expanded


def expand_payload_df(df_landing: pd.DataFrame) -> pd.DataFrame:
    """Expands an entire Landing DataFrame into Bronze format.

    Applies `expand_row` to every row of the Landing DataFrame,
    converting the JSON payload into individual columns suitable for
    the Bronze layer.

    Args:
        df_landing: The raw Landing DataFrame, with one "payload"
            column containing JSON-like data per row.

    Returns:
        A DataFrame with the payload expanded into columns, or an
        empty DataFrame if `df_landing` is empty.
    """
    if df_landing.empty:
        return pd.DataFrame()

    bronze_rows = df_landing.apply(expand_row, axis=1).tolist()
    return pd.DataFrame(bronze_rows)
