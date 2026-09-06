import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates daily return and log-return per ticker.

    Args:
        df: The DataFrame containing "ticker", "reference_date", and
            "close" columns.

    Returns:
        A copy of the DataFrame, sorted by ticker and reference_date,
        with "daily_return" and "log_return" columns added.
    """
    df = df.copy()
    df = df.sort_values(["ticker", "reference_date"])
    close_prev = df.groupby("ticker")["close"].shift(1)
    df["daily_return"] = df["close"] / close_prev - 1
    df["log_return"] = np.log(df["close"] / close_prev)
    return df


def add_multi_horizon_returns(df: pd.DataFrame, horizons: list[int] = [5, 21]) -> pd.DataFrame:
    """Adds cumulative returns over multiple horizons.

    Useful for capturing short- and medium-term momentum signals.

    Args:
        df: The DataFrame containing "ticker" and "close" columns.
        horizons: List of lookback periods (in rows) to compute
            cumulative returns for. Defaults to [5, 21].

    Returns:
        A copy of the DataFrame with one "return_{h}d" column added
        per horizon in `horizons`.
    """
    df = df.copy()
    for h in horizons:
        df[f"return_{h}d"] = df.groupby("ticker")["close"].transform(
            lambda s: s / s.shift(h) - 1
        )
    return df


def add_moving_averages(df: pd.DataFrame, windows: list[int] = [7, 21, 50]) -> pd.DataFrame:
    """Adds moving averages of the close price and their normalized ratio.

    For each window, adds both the raw moving average and the ratio of
    close price to that moving average (the ratio, not the raw MA, is
    the feature intended for modeling, since it is scale-independent).

    Args:
        df: The DataFrame containing "ticker" and "close" columns.
        windows: List of window sizes (in rows) to compute moving
            averages for. Defaults to [7, 21, 50].

    Returns:
        A copy of the DataFrame with "ma_{w}" and "close_to_ma_{w}"
        columns added per window in `windows`.
    """
    df = df.copy()
    for w in windows:
        ma = df.groupby("ticker")["close"].transform(lambda s: s.rolling(w).mean())
        df[f"ma_{w}"] = ma
        df[f"close_to_ma_{w}"] = df["close"] / ma
    return df


def add_rsi(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    """Adds the Relative Strength Index (RSI) over a rolling window, per ticker.

    Args:
        df: The DataFrame containing "ticker" and "close" columns.
        window: The rolling window size (in rows) used to compute
            average gains and losses. Defaults to 14.

    Returns:
        A copy of the DataFrame with an "rsi_{window}" column added.
    """
    df = df.copy()

    def _rsi(close: pd.Series) -> pd.Series:
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window).mean()
        avg_loss = loss.rolling(window).mean()
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    df[f"rsi_{window}"] = df.groupby("ticker")["close"].transform(_rsi)
    return df


def add_volatility(df: pd.DataFrame, window: int = 21) -> pd.DataFrame:
    """Adds rolling volatility (standard deviation of daily return).

    Args:
        df: The DataFrame containing "ticker" and "daily_return"
            columns. The latter must be computed beforehand, e.g. by
            calling `add_returns`.
        window: The rolling window size (in rows) used to compute the
            standard deviation. Defaults to 21.

    Returns:
        A copy of the DataFrame with a "volatility_{window}d" column
        added.

    Raises:
        ValueError: If the "daily_return" column is not present in
            `df`.
    """
    df = df.copy()
    if "daily_return" not in df.columns:
        raise ValueError("add_volatility requires 'daily_return' — run add_returns first.")
    df[f"volatility_{window}d"] = df.groupby("ticker")["daily_return"].transform(
        lambda s: s.rolling(window).std()
    )
    return df


def add_atr(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    """Adds the Average True Range (ATR), accounting for high/low/close gaps.

    Args:
        df: The DataFrame containing "ticker", "high", "low", and
            "close" columns.
        window: The rolling window size (in rows) used to average the
            true range. Defaults to 14.

    Returns:
        A copy of the DataFrame with an "atr_{window}" column added.
    """
    df = df.copy()

    def _atr(g: pd.DataFrame) -> pd.Series:
        prev_close = g["close"].shift(1)
        tr = pd.concat(
            [
                g["high"] - g["low"],
                (g["high"] - prev_close).abs(),
                (g["low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return tr.rolling(window).mean()

    df[f"atr_{window}"] = df.groupby("ticker", group_keys=False).apply(_atr)
    return df


def add_relative_volume(df: pd.DataFrame, window: int = 21) -> pd.DataFrame:
    """Adds volume relative to its rolling average, to flag abnormal spikes.

    Args:
        df: The DataFrame containing "ticker" and "volume" columns.
        window: The rolling window size (in rows) used to compute the
            average volume. Defaults to 21.

    Returns:
        A copy of the DataFrame with a "relative_volume" column added.
    """
    df = df.copy()
    avg_volume = df.groupby("ticker")["volume"].transform(
        lambda s: s.rolling(window).mean()
    )
    df["relative_volume"] = df["volume"] / avg_volume
    return df


def add_lags(
    df: pd.DataFrame,
    columns: list[str] = ["close", "daily_return"],
    lags: list[int] = [1, 2, 3],
) -> pd.DataFrame:
    """Adds lagged columns to give the model explicit short-term memory.

    Args:
        df: The DataFrame containing "ticker" and the columns listed
            in `columns`.
        columns: The columns to generate lags for. Defaults to
            ["close", "daily_return"].
        lags: The lag offsets (in rows) to generate for each column.
            Defaults to [1, 2, 3].

    Returns:
        A copy of the DataFrame with one "{col}_lag{lag}" column added
        per combination of `columns` and `lags`.
    """
    df = df.copy()
    for col in columns:
        for lag in lags:
            df[f"{col}_lag{lag}"] = df.groupby("ticker")[col].shift(lag)
    return df


def add_quality_flags(df: pd.DataFrame, flatline_window: int = 5) -> pd.DataFrame:
    """Flags suspicious rows (flatline price, zero volume) without removing them.

    The decision to discard or keep these rows is left to the final
    consumer of the data (model training, dashboard, etc.), not to the
    Gold layer itself.

    Args:
        df: The DataFrame containing "ticker", "close", and "volume"
            columns.
        flatline_window: The number of consecutive unchanged closing
            prices required to flag a row as "flatline". Defaults to
            5.

    Returns:
        A copy of the DataFrame with "is_flatline" and
        "is_zero_volume" boolean columns added.
    """
    df = df.copy()

    same_as_prev = df.groupby("ticker")["close"].transform(lambda s: s == s.shift(1))
    df["is_flatline"] = (
        same_as_prev.groupby(df["ticker"]).transform(
            lambda s: s.rolling(flatline_window).sum()
        )
        >= flatline_window
    )

    df["is_zero_volume"] = df["volume"] == 0

    return df


def validate_gold_output(
    df: pd.DataFrame,
    critical_cols: list[str] = ["ticker", "reference_date", "close"],
) -> pd.DataFrame:
    """Validates data integrity before writing to the Gold layer.

    Removes rows with nulls in critical columns (indicating an
    ingestion failure) and rows with price or moving-average values
    <= 0 (physically invalid). Does not remove NaNs from derived
    features, since these are expected as a side effect of rolling
    windows, nor does it automatically remove flatline or zero-volume
    rows (which can reflect real market conditions) — those are only
    flagged via `add_quality_flags`.

    Args:
        df: The DataFrame to validate, containing at least the columns
            listed in `critical_cols`, plus "close" and any "ma_*"
            columns.
        critical_cols: The columns that must not contain nulls.
            Defaults to ["ticker", "reference_date", "close"].

    Returns:
        A DataFrame with invalid rows removed.
    """
    n_before = len(df)

    df = df.dropna(subset=critical_cols)
    n_after_critical = len(df)
    if n_before - n_after_critical > 0:
        logger.warning(
            f"{n_before - n_after_critical} rows removed due to nulls in {critical_cols}."
        )

    price_cols = [c for c in df.columns if c == "close" or c.startswith("ma_")]
    invalid_price_mask = (df[price_cols] <= 0).any(axis=1)
    n_invalid_price = invalid_price_mask.sum()
    if n_invalid_price > 0:
        logger.warning(f"{n_invalid_price} rows with price/MA <= 0 removed.")
        df = df[~invalid_price_mask]

    n_dropped_total = n_before - len(df)
    logger.info(f"Gold validation: {n_dropped_total} rows removed out of {n_before}.")

    return df


def profile_zeros_and_flatline(df: pd.DataFrame) -> None:
    """Logs diagnostics on zero rates and flatline concentration per ticker.

    This function performs no data transformation — it only logs
    monitoring information and returns nothing.

    Args:
        df: The DataFrame to profile. May optionally contain
            "daily_return", "log_return", "relative_volume",
            "is_flatline", and "is_zero_volume" columns; any that are
            missing are simply skipped.

    Returns:
        None
    """
    zero_cols = [c for c in ["daily_return", "log_return", "relative_volume"] if c in df.columns]
    if zero_cols:
        zero_rate = (df[zero_cols] == 0).mean().round(4)
        logger.info(f"Zero rate:\n{zero_rate}")

    if "is_flatline" in df.columns:
        flatline_by_ticker = (
            df.groupby("ticker")["is_flatline"].mean().sort_values(ascending=False)
        )
        top_flatline = flatline_by_ticker[flatline_by_ticker > 0].head(15)
        if not top_flatline.empty:
            logger.warning(f"Tickers with the highest % of flatline rows:\n{top_flatline}")

    if "is_zero_volume" in df.columns:
        n_zero_vol = df["is_zero_volume"].sum()
        if n_zero_vol > 0:
            logger.info(f"{n_zero_vol} rows with volume == 0.")


def fill_missing_with_expanding_median(
    df: pd.DataFrame,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Fills NaNs using the expanding median up to each date.

    Only uses information available up to (and including) each row's
    own date, never looking into the future — this makes it safe to
    use in predictive models with time-based validation splits.

    Args:
        df: The DataFrame containing "ticker" and "reference_date"
            columns, plus the numeric columns to be filled.
        columns: The columns to fill. If None, defaults to all numeric
            columns except "ticker", "reference_date", "close",
            "high", "low", "volume", "is_flatline", "is_zero_volume",
            and "ingested_at".

    Returns:
        A copy of the DataFrame, sorted by ticker and reference_date,
        with NaNs in the selected columns filled using the expanding
        median per ticker. Some NaNs may remain for the first row(s)
        of tickers with no prior history.
    """
    df = df.copy()
    df = df.sort_values(["ticker", "reference_date"])

    if columns is None:
        exclude = {"ticker", "reference_date", "close", "high", "low", "volume",
                   "is_flatline", "is_zero_volume", "ingested_at"}
        numeric_cols = df.select_dtypes(include="number").columns
        columns = [c for c in numeric_cols if c not in exclude]

    for col in columns:
        expanding_median = df.groupby("ticker")[col].transform(
            lambda s: s.expanding(min_periods=1).median()
        )
        df[col] = df[col].fillna(expanding_median)

    n_remaining = df[columns].isna().sum().sum()
    if n_remaining > 0:
        logger.warning(f"{n_remaining} values still null (first row of new tickers).")

    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Computes the final set of features selected for the Gold layer.

    Runs the returns, momentum, moving-average, RSI, volatility, and
    relative-volume feature functions in sequence, then narrows the
    result down to the fixed set of columns expected downstream.

    Args:
        df: The cleaned Silver DataFrame containing at least "ticker",
            "reference_date", "open", "high", "low", "close",
            "volume", and "ingested_at" columns.

    Returns:
        A DataFrame containing only `selected_columns`: the original
        OHLCV and control columns plus "return_21d", "close_to_ma_21",
        "rsi_14", "volatility_21d", and "relative_volume".

    Raises:
        ValueError: If any of the expected columns are missing after
            feature engineering, which would indicate a mismatch
            between this function and the underlying feature
            functions.
    """
    df = add_returns(df)
    df = add_multi_horizon_returns(df, horizons=[21])
    df = add_moving_averages(df, windows=[21])
    df = add_rsi(df, window=14)
    df = add_volatility(df, window=21)
    df = add_relative_volume(df, window=21)

    selected_columns = [
        "low",
        "high",
        "open",
        "close",
        "volume",
        "ticker",
        "reference_date",
        "ingested_at",
        "return_21d",
        "close_to_ma_21",
        "rsi_14",
        "volatility_21d",
        "relative_volume",
    ]

    missing = [col for col in selected_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"Expected columns not found after feature engineering: {missing}"
        )

    df = df[selected_columns]

    logger.info(
        f"Feature engineering complete: "
        f"{len(df)} rows, {len(df.columns)} columns."
    )

    return df
