import numpy as np
import pandas as pd
from . import valuation

def annual_return(prices: pd.DataFrame) -> float:
    hist = prices['adjClose']
    ratio = hist.iloc[0] / hist.iloc[-1]
    timediff: pd.Timedelta = hist.index[0] - hist.index[-1]
    return (ratio ** (365 / timediff.days)) - 1

# discounted by (unsustainable) pbRatio growth
def real_annual_return(prices: pd.DataFrame, market_cap: pd.DataFrame, balance_sheet: pd.DataFrame) -> float:
    ar = annual_return(prices)
    pb_growth = valuation.annual_pb_ratio_growth(market_cap, balance_sheet)
    return ar - pb_growth
