import pandas as pd
from .global_vars import api_key

def company_profile(symbol: str) -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}?apikey={api_key()}"
    return pd.read_json(url)

def executives(symbol: str) -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/key-executives/{symbol}?apikey={api_key()}"
    return pd.read_json(url)

def shares_float(symbol: str) -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v4/historical/shares_float?symbol={symbol}&apikey={api_key()}"
    return pd.read_json(url).set_index("date")
