import requests

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
