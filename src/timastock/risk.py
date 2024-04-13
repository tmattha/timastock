import numpy as np
import pandas as pd

def moving_average(prices: pd.DataFrame, window: int = 30) -> pd.Series:
    return prices["adjClose"].rolling(window=window).mean()

def max_drawdown(prices: pd.DataFrame) -> float:
    mov_avg = moving_average(prices)
    drawdowns = (prices["adjClose"] - mov_avg) / mov_avg
    return drawdowns.dropna().min()

def volatility(prices: pd.DataFrame):
    price_percentages = prices['adjClose'].pct_change()
    timediff: pd.Timedelta = prices.index.max() - prices.index.min()
    measurements_per_year = len(prices) / (timediff.days / 365)
    return price_percentages.std() * (measurements_per_year ** 0.5)

def rolling_volatility(prices: pd.DataFrame, interval: int = 30):
    price_percentages = prices['adjClose'].pct_change()
    right_volatilities = price_percentages.rolling(interval).std() * (252 ** 0.5)
    left_volatilities = right_volatilities.shift(-interval + 1)
    return left_volatilities

def ebit_volatility(income_statement: pd.DataFrame):
    ebit_percentages = income_statement['operatingIncome'].pct_change()
    return ebit_percentages.std()

def debt_to_equity(balance_sheet: pd.DataFrame):
    mry = balance_sheet.index.max()
    return balance_sheet.loc[mry, "totalDebt"] / balance_sheet.loc[mry, "totalEquity"]
