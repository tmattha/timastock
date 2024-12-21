import numpy as np
import pandas as pd

def pb_ratio(market_cap: pd.DataFrame, balance_sheet: pd.DataFrame) -> float:
    latest_cap_date = market_cap.index.max()
    latest_sheet_year = balance_sheet.index.max()
    relevant_year = min(latest_cap_date.year - 1, latest_sheet_year)
    latest_cap = market_cap.loc[latest_cap_date, "marketCap"]
    equity = balance_sheet.loc[relevant_year, "totalEquity"]
    return latest_cap / equity

def annual_pb_ratio_growth(market_cap: pd.DataFrame, balance_sheet: pd.DataFrame) -> float:
    if len(market_cap) == 0 or len(balance_sheet) == 0:
        return np.nan
    oldest_market_cap = market_cap.loc[[market_cap.index.min()]]
    latest_pb_ratio = pb_ratio(market_cap, balance_sheet)
    oldest_pb_ratio = pb_ratio(oldest_market_cap, balance_sheet)
    ratio = latest_pb_ratio / oldest_pb_ratio
    timediff: pd.Timedelta = market_cap.index.max() - market_cap.index.min()
    return (ratio ** (365 / timediff.days)) - 1
