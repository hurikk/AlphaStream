import logging
import pandas as pd
from pathlib import Path

from src.alphastream.database.postgres_setup import PostgresSetup
from src.alphastream.migrations.postgres_migrations import PostgresMigration
from src.alphastream.queries.postgres_queries import PostgresQuery
from src.alphastream.transformations import silver

logger = logging.getLogger(__name__)


def _run_silver_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = silver.parse_dates(df)
    df = silver.standardize_tickers(df)
    df = silver.validate_no_future_dates(df)
    df = silver.validate_price_ranges(df)
    df = silver.drop_duplicates(df)
    return df


def insert_into_silver_layer(db_name: str, schema_name: str, table_name: str) -> None:
    """
    Lê os dados da camada Bronze, aplica limpeza/validação/padronização
    e grava o resultado na camada Silver.
    """
    env_path = Path(".env")
 
    PostgresSetup(env_path, db_name).init_db(db_name)
    query = PostgresQuery(env_path, db_name)
    
    if not query.table_exists_or_no(schema_name, table_name):
        logger.info("Tabela Silver não existe. Criando estrutura.")
        PostgresMigration(env_path, db_name).create_table(schema_name, table_name)
        
    last_silver_ts = query.get_last_ingested_date(schema_name, table_name, column="ingested_at")
    bronze_df = query.get_records("bronze", "main_bronze", since=last_silver_ts)
    
    if bronze_df.empty:
        logger.info("Nenhum dado novo na Bronze para processar.")
        return

    clean_bronze_df = _run_silver_pipeline(bronze_df)
    
    query.insert_data(clean_bronze_df, schema_name, table_name)
    logger.info(f"{len(clean_bronze_df)} linhas inseridas na Silver.")
