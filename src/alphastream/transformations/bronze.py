import ast
import json
import pandas as pd

def parse_payload_key(key: str) -> str:
    """
    Parser: decodifica a chave serializada em tupla.
    Converte chave tipo "('Close', 'MGLU3.SA')" -> "Close_MGLU3.SA"
    e "('Date', '')" -> "Date"
    """
    try:
        field, _ = ast.literal_eval(key)
        return f"{field}"
    except (ValueError, SyntaxError):
        # chave já vem "normal", sem ser tupla
        return key


def parse_payload(payload: str) -> dict:
    """
    Parser: decodifica o JSON bruto do payload em dict com chaves já normalizadas.
    """
    expanded_payload = {parse_payload_key(k): v for k, v in payload.items()}
    del expanded_payload["Date"]
    return expanded_payload


def expand_row(row: dict) -> dict:
    """
    Transformação: usa o parser e monta a linha final da Bronze,
    preservando as colunas de controle vindas da Landing.
    """
    expanded = parse_payload(row["payload"])
    expanded["ticker"] = row["ticker"]
    expanded["reference_date"] = row["reference_date"]
    expanded["ingested_at"] = row["ingested_at"]
    return expanded


def expand_payload_df(df_landing: pd.DataFrame) -> pd.DataFrame:
    """
    Transformação: aplica a expansão em todo o DataFrame de Landing,
    convertendo o payload JSON em colunas para a Bronze.
    """
    if df_landing.empty:
        return pd.DataFrame()

    bronze_rows = df_landing.apply(expand_row, axis=1).tolist()
    return pd.DataFrame(bronze_rows)
