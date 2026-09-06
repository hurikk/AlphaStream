import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula retorno diário e log-retorno por ticker."""
    df = df.copy()
    df = df.sort_values(["ticker", "reference_date"])
    close_prev = df.groupby("ticker")["close"].shift(1)
    df["daily_return"] = df["close"] / close_prev - 1
    df["log_return"] = np.log(df["close"] / close_prev)
    return df


def add_multi_horizon_returns(df: pd.DataFrame, horizons: list[int] = [5, 21]) -> pd.DataFrame:
    """Adiciona retornos acumulados em múltiplos horizontes (momentum de curto/médio prazo)."""
    df = df.copy()
    for h in horizons:
        df[f"return_{h}d"] = df.groupby("ticker")["close"].transform(
            lambda s: s / s.shift(h) - 1
        )
    return df


def add_moving_averages(df: pd.DataFrame, windows: list[int] = [7, 21, 50]) -> pd.DataFrame:
    """Adiciona médias móveis de fechamento e a razão close/MA (normalizada, não a MA bruta)."""
    df = df.copy()
    for w in windows:
        ma = df.groupby("ticker")["close"].transform(lambda s: s.rolling(w).mean())
        df[f"ma_{w}"] = ma
        df[f"close_to_ma_{w}"] = df["close"] / ma
    return df


def add_rsi(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    """RSI (Relative Strength Index) em janela móvel, por ticker."""
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
    """Volatilidade (desvio padrão do retorno diário) em janela móvel."""
    df = df.copy()
    if "daily_return" not in df.columns:
        raise ValueError("add_volatility requer 'daily_return' — rode add_returns antes.")
    df[f"volatility_{window}d"] = df.groupby("ticker")["daily_return"].transform(
        lambda s: s.rolling(window).std()
    )
    return df


def add_atr(df: pd.DataFrame, window: int = 14) -> pd.DataFrame:
    """Average True Range: volatilidade considerando gaps entre high/low/close."""
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
    """Volume relativo à média móvel de volume — sinaliza spikes anormais."""
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
    """Adiciona colunas defasadas (lags) para dar ao modelo memória explícita de curto prazo."""
    df = df.copy()
    for col in columns:
        for lag in lags:
            df[f"{col}_lag{lag}"] = df.groupby("ticker")[col].shift(lag)
    return df


def add_quality_flags(df: pd.DataFrame, flatline_window: int = 5) -> pd.DataFrame:
    """Marca linhas suspeitas (preço 'flatline', volume zerado) sem removê-las.

    A decisão de descartar ou não fica com o consumidor final (treino de
    modelo, dashboard, etc.), não com a camada Gold.
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
    """Valida integridade dos dados antes de gravar na Gold.

    - Remove linhas com nulos em colunas críticas (falha de ingestão).
    - Remove linhas com preço/MA <= 0 (fisicamente inválido).
    - NÃO remove NaNs de features derivadas (esperados por janelas móveis).
    - NÃO remove flatline/volume zero automaticamente (pode ser mercado real);
      apenas marca via add_quality_flags.
    """
    n_before = len(df)

    df = df.dropna(subset=critical_cols)
    n_after_critical = len(df)
    if n_before - n_after_critical > 0:
        logger.warning(
            f"{n_before - n_after_critical} linhas removidas por nulos em {critical_cols}."
        )

    price_cols = [c for c in df.columns if c == "close" or c.startswith("ma_")]
    invalid_price_mask = (df[price_cols] <= 0).any(axis=1)
    n_invalid_price = invalid_price_mask.sum()
    if n_invalid_price > 0:
        logger.warning(f"{n_invalid_price} linhas com preço/MA <= 0 removidas.")
        df = df[~invalid_price_mask]

    n_dropped_total = n_before - len(df)
    logger.info(f"Validação Gold: {n_dropped_total} linhas removidas de {n_before}.")

    return df


def profile_zeros_and_flatline(df: pd.DataFrame) -> None:
    """Diagnóstico (apenas log): taxa de zeros e concentração de flatline por ticker."""
    zero_cols = [c for c in ["daily_return", "log_return", "relative_volume"] if c in df.columns]
    if zero_cols:
        zero_rate = (df[zero_cols] == 0).mean().round(4)
        logger.info(f"Taxa de zeros:\n{zero_rate}")

    if "is_flatline" in df.columns:
        flatline_por_ticker = (
            df.groupby("ticker")["is_flatline"].mean().sort_values(ascending=False)
        )
        top_flatline = flatline_por_ticker[flatline_por_ticker > 0].head(15)
        if not top_flatline.empty:
            logger.warning(f"Tickers com maior % de linhas flatline:\n{top_flatline}")

    if "is_zero_volume" in df.columns:
        n_zero_vol = df["is_zero_volume"].sum()
        if n_zero_vol > 0:
            logger.info(f"{n_zero_vol} linhas com volume == 0.")


def fill_missing_with_expanding_median(
    df: pd.DataFrame,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Preenche NaN usando a mediana acumulada até aquela data (sem olhar o futuro).

    Seguro para uso em modelos preditivos com validação temporal.
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
        logger.warning(f"{n_remaining} valores ainda nulos (primeira linha de tickers novos).")

    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula apenas as features selecionadas para a camada Gold."""
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
            f"Colunas esperadas não encontradas após feature engineering: {missing}"
        )

    df = df[selected_columns]

    logger.info(
        f"Feature engineering concluído: "
        f"{len(df)} linhas, {len(df.columns)} colunas."
    )

    return df
