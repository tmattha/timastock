import pandas as pd
from .global_vars import api_key

def search_by_name(query):
    url = "https://financialmodelingprep.com/api/v3/search-name?query={}&limit=10&apikey={}".format(query, api_key())
    return pd.read_json(url)

def get_symbols_for_exchange(exchange: str) -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/symbol/{exchange}?apikey={api_key()}"
    return pd.read_json(url)

def get_symbols_with_finstatement() -> list:
    url = f"https://financialmodelingprep.com/api/v3/financial-statement-symbol-lists?apikey={api_key()}"
    return pd.read_json(url)[0]

def get_stock_symbols() -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={api_key()}"
    return pd.read_json(url)

def get_tradable_symbols() -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={api_key()}"
    return pd.read_json(url).set_index("symbol")

def get_screener_symbols(query: dict) -> pd.DataFrame:
    request = ""
    for (property, value) in query.items():
        if len(request) > 0:
            request += "&"
        request += f"{property}={value}"
    url = f"https://financialmodelingprep.com/api/v3/stock-screener?{request}&apikey={api_key()}"
    return pd.read_json(url).set_index("symbol")
