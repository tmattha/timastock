import pandas as pd
from .global_vars import api_key

def income_statement(symbol: str, period: str = 'annual', limit: int = 30) -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}?period={period}&limit={limit}&apikey={api_key()}"
    return pd.read_json(url).set_index('calendarYear')

def balance_sheet(symbol: str, period: str = 'annual', limit: int = 30) -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?period={period}&limit={limit}&apikey={api_key()}"
    return pd.read_json(url).set_index('calendarYear')

def cashflow_statement(symbol: str, period: str = 'annual', limit: int = 30) -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{symbol}?period={period}&limit={limit}&apikey={api_key()}"
    return pd.read_json(url).set_index('calendarYear')

def key_metrics(symbol: str, period: str = 'annual', limit: int = 30) -> pd.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/key-metrics/{symbol}?period={period}&limit={limit}&apikey={api_key()}"
    return pd.read_json(url).set_index('calendarYear')
