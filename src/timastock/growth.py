import pandas as pd

def annual_revenue_growth(income_statement: pd.DataFrame) -> float:
    revenue =  income_statement["revenue"]
    latest_statement = revenue.index.max()
    oldest_statement = revenue.index.min()
    for year in range(oldest_statement, latest_statement):
        if revenue.loc[oldest_statement] == 0:
            oldest_statement = oldest_statement + 1
    if latest_statement - oldest_statement < 1:
        return 0
    latest_revenue = revenue[latest_statement]
    oldest_revenue = revenue[oldest_statement]
    change = latest_revenue / oldest_revenue
    change_annual = change ** (1 / (latest_statement - oldest_statement))
    return change_annual - 1

def annual_capital_employed_growth(balance_sheet: pd.DataFrame) -> float:
    latest_statement = balance_sheet.index.max()
    oldest_statement = balance_sheet.index.min()
    capital_employed = balance_sheet["totalAssets"] - balance_sheet["totalCurrentLiabilities"]
    change = capital_employed.loc[latest_statement] / capital_employed.loc[oldest_statement]
    change_annual = change ** (1 / (latest_statement - oldest_statement))
    return change_annual - 1

def investment_ratio(income_statement: pd.DataFrame, cashflow_statement: pd.DataFrame) -> float:
    return -cashflow_statement["netCashUsedForInvestingActivites"].sum() / income_statement["revenue"].sum()
