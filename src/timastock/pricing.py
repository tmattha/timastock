import numpy as np
import pandas as pd

def period_avg(series: pd.Series, period: str="Y") -> pd.Series:
    return series.groupby(series.index.to_period(period)).mean()

def period_low(series: pd.Series, period: str="Y") -> pd.Series:
    return series.groupby(series.index.to_period(period)).min()

# def market_cap(prices: pd.DataFrame, balance_sheet: pd.DataFrame) -> pd.Series:
#     closest_filing = prices.index.to_series().apply(lambda d: balance_sheet["date"].loc[balance_sheet["date"] <= d].max()).dropna()
#     common_stock = closest_filing.apply(lambda d: balance_sheet.loc[balance_sheet["date"] == d, "commonStock"].iloc[0])
#     return prices["close"] * common_stock
