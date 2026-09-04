import logging
import pandas as pd

logger = logging.getLogger(__name__)


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas de data/hora para os dtypes corretos."""
    df = df.copy()
    df["reference_date"] = pd.to_datetime(df["reference_date"])
    return df
 
 
def standardize_tickers(df: pd.DataFrame) -> pd.DataFrame:
    """Garante formato consistente do ticker (ex: sempre maiúsculo, sem espaços)."""
    df = df.copy()
    df["ticker"] = df["ticker"].str.strip().str.upper()
    return df
 
 
def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicatas por (ticker, reference_date), mantendo o registro mais recente."""
    before = len(df)
    df = df.sort_values("ingested_at").drop_duplicates(
        subset=["ticker", "reference_date"], keep="last"
    )
    dropped = before - len(df)
    if dropped:
        logger.info(f"{dropped} linhas duplicadas removidas.")
    return df

 
def validate_price_ranges(df: pd.DataFrame) -> pd.DataFrame:
    """Remove linhas com preços ou volume implausíveis (negativos ou nulos)."""
    price_cols = ["open", "high", "low", "close"]
    before = len(df)
 
    mask_valid = (df[price_cols] > 0).all(axis=1) & (df["volume"] >= 0)
    invalid_count = (~mask_valid).sum()
    if invalid_count:
        logger.warning(f"{invalid_count} linhas com preços/volume inválidos descartadas.")
 
    df = df[mask_valid]
    logger.info(f"Validação de preços: {before} -> {len(df)} linhas.")
    return df
 
 
def validate_no_future_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove registros com reference_date no futuro (erro de fonte/ingestão)."""
    today = pd.Timestamp.now().normalize()
    mask_valid = df["reference_date"] <= today
 
    invalid_count = (~mask_valid).sum()
    if invalid_count:
        logger.warning(f"{invalid_count} linhas com data futura descartadas.")
 
    return df[mask_valid]
