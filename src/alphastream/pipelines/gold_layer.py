import logging
import pandas as pd
from pathlib import Path

from src.alphastream.database.postgres_setup import PostgresSetup
from src.alphastream.migrations.postgres_migrations import PostgresMigration
from src.alphastream.queries.postgres_queries import PostgresQuery
from src.alphastream.transformations import gold

logger = logging.getLogger(__name__)


def _run_gold_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = gold.build_features(df)
    df = gold.validate_gold_output(df)
    df = gold.fill_missing_with_expanding_median(df)
    gold.profile_zeros_and_flatline(df)
    return df


def insert_into_gold_layer(db_name: str, schema_name: str, table_name: str) -> None:
    """
    Lê os dados da camada Silver e realiza feature engineering
    e grava o resultado na camada Gold.
    """
    
    env_path = Path(".env")
 
    PostgresSetup(env_path, db_name).init_db(db_name)
    query = PostgresQuery(env_path, db_name)
    
    if not query.table_exists_or_no(schema_name, table_name):
        logger.info("Tabela Silver não existe. Criando estrutura.")
        PostgresMigration(env_path, db_name).create_table(schema_name, table_name)
        
    last_gold_ts = query.get_last_ingested_date(schema_name, table_name, column="ingested_at")
    silver_df = query.get_records("silver", "main_silver", since=last_gold_ts)
    
    if silver_df.empty:
        logger.info("Nenhum dado novo na Bronze para processar.")
        return

    gold_df = _run_gold_pipeline(silver_df)
    
    if gold_df.empty:
        logger.warning("Todas as linhas foram descartadas na validação. Nada a inserir.")
        return
    
    query.insert_data(gold_df, schema_name, table_name)
    logger.info(f"{len(gold_df)} linhas inseridas na Silver.")
