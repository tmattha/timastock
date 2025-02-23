import json
import urllib.request
import polars as pl
from fmp.global_vars import api_key
from fmp.common import ignore_rate_limit, multi_dataframe, convert_exceptions_to_none
from datetime import datetime

HISORICAL_PRICES_CURRENCY_FIELDS = ["open", "high", "low", "close", "adjClose"]
HISORICAL_PRICES_VALIDATED_FIELDS = ["symbol", "date", "volume"] + HISORICAL_PRICES_CURRENCY_FIELDS

@ignore_rate_limit
def historical_prices(symbol, start: str = None, end: str = datetime.today().strftime("%Y-%m-%d")) -> pl.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={start}&to={end}&apikey={api_key()}"
    with urllib.request.urlopen(url) as response:
        raw = json.load(response)
        # Add symbol from top level to every entry.
        for entry in raw["historical"]:
            entry["symbol"] = raw["symbol"]
        df = pl.DataFrame(raw["historical"])
    return df.select(
        pl.col("symbol"),
        pl.col("date").str.to_date(),
        # actually interesting
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("adjClose").cast(pl.Float64),
        pl.col("volume").cast(pl.Int64),
        pl.col("unadjustedVolume").cast(pl.Int64),
        pl.col("change").cast(pl.Float64),
        pl.col("changePercent").cast(pl.Float64),
        pl.col("vwap").cast(pl.Float64),
        pl.col("changeOverTime").cast(pl.Float64),
    )

multi_historical_prices = multi_dataframe(convert_exceptions_to_none(historical_prices))


@ignore_rate_limit
def market_cap(symbol, start: str = None, end: str = datetime.today().strftime("%Y-%m-%d")) -> pl.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{symbol}?from={start}&to={end}&apikey={api_key()}"
    with urllib.request.urlopen(url) as response:
        df = pl.read_json(response)
    return df.select(
        pl.col("symbol"),
        pl.col("date").str.to_date(),
        pl.col("marketCap").cast(pl.Float64)
    )

multi_market_caps = multi_dataframe(convert_exceptions_to_none(market_cap))
