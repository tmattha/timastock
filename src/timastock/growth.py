import pandas as pd

def annual_revenue_growth(income_statement: pd.DataFrame) -> float:
    revenue =  income_statement["revenue"]
    latest_statement = revenue.index.max()
    oldest_statement = revenue.index.min()
    latest_revenue = revenue[latest_statement]
    oldest_revenue = revenue[oldest_statement]
    pct_change = latest_revenue / oldest_revenue
    pct_change_annual = pct_change ** (1 / (latest_statement - oldest_statement))
    return pct_change_annual - 1
