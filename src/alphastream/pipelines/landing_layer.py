import math
import json
import logging
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta, timezone

from src.alphastream.utils.utils import get_tickers
from src.alphastream.database.postgres_setup import PostgresSetup
from src.alphastream.migrations.postgres_migrations import PostgresMigration
from src.alphastream.queries.postgres_queries import PostgresQuery

logger = logging.getLogger(__name__)


def _download_raw(ticker: str, **yf_kwargs) -> pd.DataFrame:
    """
    Baixa os dados exatamente como o yfinance retorna, sem nenhuma
    transformação de schema, renomeação ou formatação.
    """
    raw = yf.download(ticker, progress=False, threads=False, **yf_kwargs)
    if raw.empty:
        return raw
    return raw.reset_index()


def _clean_value(value):
    """
    Converte NaN (float) em None, para que a serialização JSON gere
    'null' em vez do token inválido 'NaN', que o Postgres rejeita.
    """
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _to_raw_records(raw_df: pd.DataFrame, ticker: str) -> list[dict]:
    """
    Serializa cada linha do DataFrame bruto em um registro para a Landing,
    preservando o payload original como JSON e extraindo apenas a data
    como metadado de controle (necessária para saber o que já foi baixado).
    """
    records = []
    for row in raw_df.to_dict(orient="records"):
        # A primeira coluna, seja qual for o nome exato retornado pelo
        # yfinance (ex: "Date" ou tupla de MultiIndex), é sempre a data
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
    
    env_path = Path(".env")
    tickers = [f"{ticker}.SA" for ticker in get_tickers()]
    
    PostgresSetup(env_path, db_name).init_db(db_name)
    query = PostgresQuery(env_path, db_name)

    if not query.table_exists_or_no(schema_name, table_name):
        logger.info("Tabela Landing não existe. Criando estrutura.")
        PostgresMigration(env_path, db_name).create_table(schema_name, table_name)

    all_records = []
    
    for ticker in tickers:
        try:
            last_date = query.get_last_ingested_date(schema_name, table_name, ticker)
            
            if last_date is None:
                logger.info(f"Sem histórico para {ticker}. Baixando 20 anos.")
                raw_df = _download_raw(ticker, period="20y")
            else:
                start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                end_date = datetime.today().strftime("%Y-%m-%d")
                if start_date >= end_date:
                    logger.info("%s já está atualizado.", ticker)
                    continue
                logger.info("Atualizando %s de %s até %s.", ticker, start_date, end_date)
                raw_df = _download_raw(ticker, start=start_date, end=end_date)
                
            if raw_df.empty:
                logger.info("Nenhum dado novo para %s.", ticker)
                continue
            
            all_records.extend(_to_raw_records(raw_df, ticker))
            
        except Exception:
            logger.warning("Falha ao processar %s.", ticker, exc_info=True)
            continue
    
    if not all_records:
        logger.info("Nenhum dado novo para inserir na Landing.")
        return
    
    all_records = pd.DataFrame(all_records)

    query.insert_data(all_records, schema_name, table_name)
    logger.info(f"Inseridos {len(all_records)} registros brutos em {schema_name}.{table_name}.", len(all_records), schema_name, table_name)
