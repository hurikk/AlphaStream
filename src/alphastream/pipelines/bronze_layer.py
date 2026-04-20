import polars as pl
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta

from src.alphastream.utils.utils import get_tickers
from src.alphastream.database.postgres_setup import PostgresSetup
from src.alphastream.migrations.postgres_migrations import PostgresMigration
from src.alphastream.queries.postgres_queries import PostgresQuery

def insert_into_bronze_layer(db_name: str, schema_name: str, table_name: str) -> None:
    """
    
    It generates and updates a database of all stocks listed on the B3
    
    """
    
    env_path = Path(".env")
    
    tickers = get_tickers()
    tickers = [ticker + ".SA" for ticker in tickers]
    
    db_initialization = PostgresSetup(env_path, db_name)
    db_initialization.init_db(db_name)    
    
    query = PostgresQuery(env_path, db_name)
    actions_data_df = pl.DataFrame()
    
    if not query.table_exists_or_no(schema_name, table_name):
        
        migration = PostgresMigration(env_path, db_name)
        migration.create_table(schema_name, table_name)
        
        for ticker in tickers:
            action_data = yf.download(ticker, period="20y")
            action_data = action_data.reset_index()
            action_data = pl.from_pandas(action_data)
            action_data = action_data.with_columns(
                pl.lit(ticker).alias("ticker")
            ).rename({"('Date', '')": "Date",
                      f"('Close', '{ticker}')": "Close",
                      f"('High', '{ticker}')": "High",
                      f"('Low', '{ticker}')": "Low",
                      f"('Open', '{ticker}')": "Open", 
                      f"('Volume', '{ticker}')": "Volume"})
            action_data = action_data.with_columns(
                pl.col("Date").dt.strftime("%Y-%m-%d")
            )
            if not action_data.is_empty():
                if actions_data_df.is_empty():
                    actions_data_df = action_data
                else:
                    actions_data_df = pl.concat([actions_data_df, action_data])
        query.insert_data(actions_data_df, schema_name, table_name)
        
    else:
        actions_data_df_temp = pl.DataFrame()
        start_date = (query.get_most_recent_day(schema_name, table_name) + timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = (datetime.today()).strftime("%Y-%m-%d")
        for ticker in tickers:
            
            df_with_new_records = yf.download(ticker, start=start_date, end=end_date)
            df_with_new_records = df_with_new_records.reset_index()
            df_with_new_records = pl.from_pandas(df_with_new_records)
            df_with_new_records = df_with_new_records.with_columns(
                pl.lit(ticker).alias("ticker")
            ).rename({"('Date', '')": "Date",
                        f"('Close', '{ticker}')": "Close",
                        f"('High', '{ticker}')": "High",
                        f"('Low', '{ticker}')": "Low",
                        f"('Open', '{ticker}')": "Open", 
                        f"('Volume', '{ticker}')": "Volume"})
            df_with_new_records = df_with_new_records.with_columns(
                pl.col("Date").dt.strftime("%Y-%m-%d")
            )
            if not df_with_new_records.is_empty():
                if actions_data_df_temp.is_empty():
                    actions_data_df_temp = df_with_new_records
                else:
                    actions_data_df_temp = pl.concat([actions_data_df_temp, df_with_new_records])
                
        actions_data_df = pl.concat([actions_data_df, actions_data_df_temp])
        query.insert_data(actions_data_df, schema_name, table_name)
