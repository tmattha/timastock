import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from dataclasses import dataclass

@dataclass
class AlphaBeta:
    alpha: float
    beta: float

def period_avg(series: pd.Series, period: str="Y") -> pd.Series:
    return series.groupby(series.index.to_period(period)).mean()

def period_low(series: pd.Series, period: str="Y") -> pd.Series:
    return series.groupby(series.index.to_period(period)).min()

def alpha_beta(prices: pd.DataFrame, reference_prices: pd.DataFrame) -> AlphaBeta:
    regr = LinearRegression()
    intersection = prices.index.intersection(reference_prices.index) 
    # ensure values are within same index
    x = reference_prices.loc[intersection]["adjClose"].pct_change().values[1:].reshape(-1,1)
    y =           prices.loc[intersection]["adjClose"].pct_change().values[1:].reshape(-1,1)
    regr.fit(x, y)
    return AlphaBeta(regr.intercept_[0], regr.coef_[0][0])

# def market_cap(prices: pd.DataFrame, balance_sheet: pd.DataFrame) -> pd.Series:
#     closest_filing = prices.index.to_series().apply(lambda d: balance_sheet["date"].loc[balance_sheet["date"] <= d].max()).dropna()
#     common_stock = closest_filing.apply(lambda d: balance_sheet.loc[balance_sheet["date"] == d, "commonStock"].iloc[0])
#     return prices["close"] * common_stock
