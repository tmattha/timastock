import numpy as np
import pandas as pd

def volatility(prices: pd.DataFrame):
    price_percentages = prices['adjClose'].pct_change()
    timediff: pd.Timedelta = price_percentages.index[0] - price_percentages.index[-1]
    return price_percentages.std() * (252 ** 0.5)

def rolling_volatility(prices: pd.DataFrame, interval: int = 30):
    price_percentages = prices['adjClose'].pct_change()
    right_volatilities = price_percentages.rolling(interval).std() * (252 ** 0.5)
    left_volatilities = right_volatilities.shift(-interval + 1)
    return left_volatilities
