import polars as pl

from .misc import AnyPolarsFrame

def capital_employed(balance_sheets: AnyPolarsFrame) -> AnyPolarsFrame:
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

def return_on_capital_employed(income_stmt: AnyPolarsFrame, balance_sheet: AnyPolarsFrame):
    all_capital_employed = capital_employed(balance_sheet)
    joined = balance_sheet.join(all_capital_employed, on=["symbol", "calendarYear"], how="inner")
    joined = joined.join(income_stmt, on=["symbol", "calendarYear"], how="inner")
    return joined.select(
        pl.col("symbol"),
        pl.col("calendarYear"),
        (pl.col("netIncome") / pl.col("capitalEmployed")).alias("returnOnCapitalEmployed")
    )

def gross_profitability(income_stmt: AnyPolarsFrame, balance_sheet: AnyPolarsFrame):
    joined = balance_sheet.join(income_stmt, on=["symbol", "calendarYear"], how="inner")
    return joined.select(
        pl.col("symbol"),
        pl.col("calendarYear"),
        (pl.col("grossProfit") / pl.col("totalAssets").rolling_mean(2, min_samples=1)).alias("grossProfitability")
    )
