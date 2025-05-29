import urllib.request
import json

import polars as pl
from fmp.global_vars import api_key
from fmp.common import ignore_rate_limit, convert_exceptions_to_none, multi_dataframe

@ignore_rate_limit
def rating(symbol: str, limit: int | None = 10000) -> pl.DataFrame:
    url = f"https://financialmodelingprep.com/stable/ratings-historical?symbol={symbol}&limit={limit}&apikey={api_key()}"
    with urllib.request.urlopen(url) as response:
        df = pl.read_json(response)
    return df.select(
        pl.col("symbol").cast(pl.String),
        pl.col("date").str.to_date(),
        # actually interesting
        pl.col("rating").cast(pl.String),
        pl.col("overallScore").cast(pl.Int8),
        pl.col("discountedCashFlowScore").cast(pl.Int8),
        pl.col("returnOnEquityScore").cast(pl.Int8),
        pl.col("returnOnAssetsScore").cast(pl.Int8),
        pl.col("debtToEquityScore").cast(pl.Int8),
        pl.col("priceToEarningsScore").cast(pl.Int8),
        pl.col("priceToBookScore").cast(pl.Int8)
    )

multi_ratings = multi_dataframe(convert_exceptions_to_none(rating))

@ignore_rate_limit
def price_target(symbol: str) -> pl.DataFrame:
    url = f"https://financialmodelingprep.com/api/v4/price-target/?symbol={symbol}&apikey={api_key()}"
    print(url)
    with urllib.request.urlopen(url) as response:
        df = pl.read_json(response)
    return df.select(
        pl.col("symbol").cast(pl.String),
        pl.col("publishedDate").str.to_datetime("%+"),
        # actually interesting
        pl.col("newsURL").cast(pl.String),
        pl.col("newsTitle").cast(pl.String),
        pl.col("analystName").cast(pl.String),
        pl.col("priceTarget").cast(pl.Float64),
        pl.col("adjPriceTarget").cast(pl.Float64),
        pl.col("priceWhenPosted").cast(pl.Float64),
        pl.col("newsPublisher").cast(pl.String),
        pl.col("newsBaseURL").cast(pl.String),
        pl.col("analystCompany").cast(pl.String),
    )

multi_price_targets = multi_dataframe(convert_exceptions_to_none(price_target))
