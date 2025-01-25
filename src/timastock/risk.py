import numpy as np
import pandas as pd
import polars as pl
from .misc import AnyPolarsFrame

def moving_average(prices: pd.DataFrame, window: int = 30) -> pd.Series:
    return prices["adjClose"].rolling(window=window).mean()

def max_drawdown(prices: pd.DataFrame) -> float:
    mov_avg = moving_average(prices)
    drawdowns = (prices["adjClose"] - mov_avg) / mov_avg
    return drawdowns.dropna().min()

def drawdown(prices: AnyPolarsFrame, period: str = "1mo"):
    prices = prices.sort("date")
    adj_close_means = prices.rolling("date", group_by="symbol", period=period, closed="left").agg(
        pl.col("adjClose").mean().alias("adjCloseMean")
    )
    return prices.join(adj_close_means, on=["symbol", "date"]).select(
        pl.col("symbol"),
        pl.col("date"),
        ((pl.col("adjClose") - pl.col("adjCloseMean")) / pl.col("adjCloseMean")).alias("drawdown")
    )


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

def ebit_volatility(income_statement: AnyPolarsFrame, interval: int = 2):
    income_statement = income_statement.sort("calendarYear")
    result = income_statement.with_columns(pl.col("operatingIncome").pct_change().over("symbol").alias("pct_change"))
    result = result.rolling("calendarYear", group_by="symbol", period=f"{interval}i").agg(
        pl.col("pct_change").drop_nulls().std().alias("ebitVolatility") # / pl.col("operatingIncome").mean() # .std()
    )
    return result.select(
        pl.col("symbol"),
        pl.col("calendarYear"),
        pl.col("ebitVolatility")
    )



def debt_to_equity(balance_sheet: pd.DataFrame):
    mry = balance_sheet.index.max()
    return balance_sheet.loc[mry, "totalDebt"] / balance_sheet.loc[mry, "totalEquity"]
