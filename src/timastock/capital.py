import polars as pl

def capital_employed(balance_sheets: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    # Use Int32 for sorting and iteration
    balance_sheets = balance_sheets.with_columns(pl.col("calendarYear").cast(pl.Int32))
    balance_sheets = balance_sheets.sort("calendarYear")
    means = balance_sheets.rolling("calendarYear", group_by="symbol", period="2i").agg(
        pl.col("totalAssets").mean(),
        pl.col("totalCurrentLiabilities").mean()
    )
    return means.select(
        pl.col("symbol"),
        pl.col("calendarYear"),
        (pl.col("totalAssets") - pl.col("totalCurrentLiabilities")).alias("capitalEmployed")
    )
