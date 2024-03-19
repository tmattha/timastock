import json
import pandas as pd
from urllib.request import urlopen 
from .global_vars import api_key

def historical_prices(symbol, start: str = None, end: str = None):
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={start}&to={end}&apikey={api_key()}"
    with urlopen(url) as response:
        df = pd.DataFrame(json.load(response)['historical'])
        df = df.set_index('date')
        df.index = pd.to_datetime(df.index)
        return df

def market_cap(symbol, start: str = None, end: str = None):
    url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{symbol}?from={start}&to={end}&apikey={api_key()}"
    df = pd.read_json(url).set_index("date")
    df.index = pd.to_datetime(df.index)
    return df
