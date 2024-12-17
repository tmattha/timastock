import numpy as np
import pandas as pd

def normalize(portfolio_absolute: dict[str, float]) ->  dict[str, float]:
    """Normalizes portfolio to percentages."""
    total_value = sum(portfolio_absolute.values())
    return {symbol: value / total_value for symbol, value in portfolio_absolute.items()}

def apply_weighting_to_series(portfolio_relative: dict[str, float], series: dict[str, pd.Series]) -> pd.Series:
    return sum([series[symbol] * percentage for symbol, percentage in portfolio_relative.items()])
