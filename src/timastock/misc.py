import pandas as pd

def series_common_index(series_a: pd.Series, series_b: pd.Series) -> tuple[pd.Series, pd.Series]:
    intersection = series_a.index.intersection(series_b.index)
    return series_a.loc[intersection], series_b.loc[intersection]
