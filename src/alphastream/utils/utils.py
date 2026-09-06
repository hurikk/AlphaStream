import requests


def get_tickers() -> list[str]:
    """Gets the tickers for all stocks listed on B3.

    Fetches the list of available tickers from the Brapi public API.

    Returns:
        A list of stock ticker strings listed on B3.
    """
    endpoint = "https://brapi.dev/api/available"
    response = requests.get(endpoint)
    data = response.json()
    tickers = data["stocks"]
    return tickers
