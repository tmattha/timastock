import numpy as np
import polars as pl
from .misc import AnyPolarsFrame

def annual_return(prices: AnyPolarsFrame) -> AnyPolarsFrame:
    hist = raw_return(prices)

    result = hist.select(
        pl.col('symbol'),
        ((pl.col('totalReturn').pow(365 / pl.col('totalDays'))) - 1).alias('annualReturn')
    )

    return result

def raw_return(prices: AnyPolarsFrame) -> AnyPolarsFrame:
    hist = prices.group_by('symbol').agg(
        pl.col('adjClose').last().alias('lastClose'),
        pl.col('date').last().alias('lastDate'),
        pl.col('adjClose').first().alias('firstClose'),
        pl.col('date').first().alias('firstDate'),
    )
    hist = hist.with_columns(
        (pl.col('lastClose') / pl.col('firstClose')).alias('totalReturn'),
        (pl.col('lastDate') - pl.col('firstDate')).dt.total_days().alias('totalDays'))

    return hist.select("symbol", "totalReturn", "totalDays")

# discounted by (unsustainable) pbRatio growth
# def real_annual_return(prices: pd.DataFrame, market_cap: pd.DataFrame, balance_sheet: pd.DataFrame) -> float:
#     ar = annual_return(prices)
#     pb_growth = valuation.annual_pb_ratio_growth(market_cap, balance_sheet)
#     return ar - pb_growth
