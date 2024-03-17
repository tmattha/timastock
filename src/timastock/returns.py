import numpy as np
import pandas as pd

def annual_return(prices: pd.DataFrame):
    hist = prices['adjClose']
    ratio = hist.iloc[0] / hist.iloc[-1]
    timediff: pd.Timedelta = hist.index[0] - hist.index[-1]
    return (ratio ** (365 / timediff.days)) - 1
