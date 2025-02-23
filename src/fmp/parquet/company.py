import urllib.request

import polars as pl
from fmp.global_vars import api_key
from fmp.common import ignore_rate_limit, convert_exceptions_to_none, multi_dataframe

COMPANY_PROFILE_VALIDATED_FIELDS = ["symbol", "companyName", "currency", "isin", "exchangeShortName", "industry", "sector", "country", "isEtf"]

@ignore_rate_limit
def company_profile(symbol: str) -> pl.DataFrame | None:
    url = (
        f"https://financialmodelingprep.com/api/v3/profile/{symbol}?apikey={api_key()}"
    )
    with urllib.request.urlopen(url) as response:
        df = pl.read_json(response)
    return df.select(
        pl.col("symbol").cast(pl.String),
        # actually interesting
        pl.col("price").cast(pl.Float64),
        pl.col("beta").cast(pl.Float64),
        pl.col("volAvg").cast(pl.Float64),
        pl.col("mktCap").cast(pl.Float64),
        pl.col("lastDiv").cast(pl.Float64),
        pl.col("changes").cast(pl.Float64),
        pl.col("companyName").cast(pl.String),
        pl.col("currency").cast(pl.String),
        pl.col("isin").cast(pl.String),
        pl.col("exchange").cast(pl.String),
        pl.col("exchangeShortName").cast(pl.String),
        pl.col("industry").cast(pl.String),
        pl.col("website").cast(pl.String),
        pl.col("description").cast(pl.String),
        pl.col("ceo").cast(pl.String),
        pl.col("sector").cast(pl.String),
        pl.col("country").cast(pl.String),
        pl.col("fullTimeEmployees").cast(pl.Int64),
        pl.col("address").cast(pl.String),
        pl.col("city").cast(pl.String),
        pl.col("state").cast(pl.String),
        pl.col("zip").cast(pl.String),
        pl.col("ipoDate").str.to_date("%Y-%m-%d", strict=False),
        pl.col("isEtf").cast(pl.Boolean),
        pl.col("isActivelyTrading").cast(pl.Boolean),
        pl.col("isAdr").cast(pl.Boolean),
        pl.col("isFund").cast(pl.Boolean),
    )

multi_company_profiles = multi_dataframe(convert_exceptions_to_none(company_profile))
