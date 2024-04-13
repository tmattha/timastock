import numpy as np
import pandas as pd

def period_avg(series: pd.Series, period: str="Y") -> pd.Series:
    return series.groupby(series.index.to_period(period)).mean()

def period_low(series: pd.Series, period: str="Y") -> pd.Series:
    return series.groupby(series.index.to_period(period)).min()
