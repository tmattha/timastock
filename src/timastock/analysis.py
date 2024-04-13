import numpy as np
import pandas as pd
from . import weighting

def return_on_capital_employed(income_stmt: pd.DataFrame, balance_sheet: pd.DataFrame) -> dict:
    # drop nun-numeric rows
    income_stmt = income_stmt.select_dtypes([np.number])
    balance_sheet = balance_sheet.select_dtypes([np.number])
    # shift balance sheet to middle of year
    # approximates average available capital
    balance_sheet_avg = (
        balance_sheet.shift(-1).iloc[:-1, :] +
        balance_sheet.iloc[:-1, :]
        )/ 2
    # shorten income statement by one year
    # we have no balance sheet data of the previous year
    income_stmt_current = income_stmt.iloc[:-1, :]
    assets = balance_sheet_avg["totalAssets"] - balance_sheet_avg["totalCurrentLiabilities"]
    if assets.sum == 0:
        return np.nan
    ebit = income_stmt_current["operatingIncome"]
    result = (ebit.sum() / assets.sum())
    return result

def return_on_equity(income_stmt: pd.DataFrame, balance_sheet: pd.DataFrame):
    # drop nun-numeric rows
    income_stmt = income_stmt.select_dtypes([np.number])
    balance_sheet = balance_sheet.select_dtypes([np.number])
    # shift balance sheet to middle of year
    # approximates average available capital
    balance_sheet_avg = (
        balance_sheet.shift(-1).iloc[:-1, :] +
        balance_sheet.iloc[:-1, :]
        )/ 2
    # shorten income statement by one year
    # we have no balance sheet data of the previous year
    income_stmt_current = income_stmt.iloc[:-1, :]
    equity = balance_sheet_avg["totalEquity"]
    net_income = income_stmt_current["netIncome"]
    result = (net_income.sum() / equity.sum())
    return result
