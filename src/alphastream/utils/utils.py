import requests
import polars as pl
import yfinance as yf

from pathlib import Path
from datetime import datetime, timedelta

def get_tickers() -> list:
    """
    
    Get the tickers for all stocks listed on the B3
    
    Returns: A list of all the stock tickers listed on B3
    
    """
    endpoint = "https://brapi.dev/api/available"
    response = requests.get(endpoint)
    data = response.json()
    tickers = data['stocks']
    return tickers


def gen_actions_dataset() -> None:
    """
    
    It generates and updates a database of all stocks listed on the B3
    
    """
    tickers = get_tickers()
    tickers = [ticker + ".SA" for ticker in tickers]
    
    path = Path("./dataset/actions_dataset.parquet")
    
    if not path.exists():
        actions_data_df = pl.DataFrame()
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
        actions_data_df.write_parquet("./dataset/actions_dataset.parquet")
        
    else:
        actions_data_df = pl.read_parquet(path)
        actions_data_df_temp = pl.DataFrame()
        start_date = datetime.strftime(datetime.strptime(actions_data_df.select(pl.col("Date").max()).item(), "%Y-%m-%d") + timedelta(days=1), "%Y-%m-%d")
        end_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
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
        actions_data_df.write_parquet("./dataset/actions_dataset.parquet")
