import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from src.alphastream.utils.utils import get_tickers
from src.alphastream.transformations.bronze import expand_payload_df
from src.alphastream.database.postgres_setup import PostgresSetup
from src.alphastream.migrations.postgres_migrations import PostgresMigration
from src.alphastream.queries.postgres_queries import PostgresQuery

logger = logging.getLogger(__name__)


def _parse_landing_infos(ticker: str, 
                         query: PostgresQuery,
                         last_date,
                         schema_name: str = "landing", 
                         table_name: str = "main_landing"):
    
    landing_df = query.get_records(schema_name, table_name, ticker, last_date)
    
    if landing_df.empty:
        return pd.DataFrame()

    return expand_payload_df(landing_df)


def insert_into_bronze_layer(db_name: str, schema_name: str, table_name: str) -> None:
    """
    
    It generates and updates a database of all stocks listed on the B3
    
    """
    
    env_path = Path(".env")
    tickers = [f"{ticker}.SA" for ticker in get_tickers()]
    
    PostgresSetup(env_path, db_name).init_db(db_name)
    query = PostgresQuery(env_path, db_name)
    
    if not query.table_exists_or_no(schema_name, table_name):
        logger.info("Tabela Bronze não existe. Criando estrutura.")
        PostgresMigration(env_path, db_name).create_table(schema_name, table_name)
    
    all_records = []
    
    for ticker in tickers:
        try:
            last_date = query.get_last_ingested_date(schema_name, table_name, ticker)
            
            (logger.info(f"Sem histórico para {ticker}. Transformando todos os dados da Landing.") if last_date is None
             else logger.info(f"Histórico encontrado para {ticker} até {last_date}. Buscando dados novos."))
                
            ticker_bronze_df = _parse_landing_infos(ticker, query, last_date)  
            
            if ticker_bronze_df.empty:
                logger.info(f"Nenhum dado novo para {ticker}. Pulando.")
                continue     
            
            all_records.append(ticker_bronze_df)
        
        except Exception:
            logger.exception(f"Erro ao processar {ticker} na Bronze Layer.")
        
    if not all_records:
        logger.info("Nenhum novo registro para inserir na Bronze Layer.")
        return
    
    bronze_df = pd.concat(all_records, ignore_index=True)
    
    query.insert_data(bronze_df, schema_name, table_name)
    logger.info(f"Total de registros preparados para Bronze: {len(bronze_df)}")
