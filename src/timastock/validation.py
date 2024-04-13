import matplotlib.pyplot as plt
import pandas as pd
import scipy.stats as sps
import seaborn as sns
from collections import namedtuple

IndicatorTtestResult = namedtuple("IndicatorTtestResult", ["result", "median", "high_mean", "low_mean"])
IndicatorBandTtestResult = namedtuple("IndicatorTtestResult", ["result", "lower_boundary", "upper_boundary", "outside_mean", "inside_mean"])

def indicator_ttest(data: pd.DataFrame, indicator: str, target: str, alternative: str = "two-sided", with_plot: bool = True) -> IndicatorTtestResult:
    median = data[indicator].median()
    data_sorted = data.sort_values(indicator)
    high_indicator = data_sorted.iloc[(len(data_sorted)//2):][target]
    low_indicator = data_sorted.iloc[:(len(data_sorted)//2)][target]
    high_mean = high_indicator.mean()
    low_mean = low_indicator.mean()
    if with_plot:
        sns.histplot(
            {f"High {indicator}: {high_mean:0.3f}": high_indicator, f"Low {indicator} {low_mean:0.3f}": low_indicator},
            kde=tuple, element="step", bins=30)
    result = sps.ttest_ind(high_indicator, low_indicator, equal_var=False, alternative=alternative)
    return IndicatorTtestResult(result, median, high_mean, low_mean)

def indicator_band_ttest(data: pd.DataFrame, indicator: str, target: str, alternative: str = "two-sided", with_plot: bool = True, quantiles: float = 0.25):
    lower_boundary = data[indicator].quantile(quantiles)
    upper_boundary = data[indicator].quantile(1 - quantiles)
    outside = data[(data[indicator] < lower_boundary) | (data[indicator] > upper_boundary)][target]
    inside = data[(data[indicator] > lower_boundary) & (data[indicator] < upper_boundary)][target]
    outside_mean = outside.mean()
    inside_mean = inside.mean()
    if with_plot:
        sns.histplot(
            {f"Outside Band {indicator}: {outside_mean:0.3f}": outside, f"Inside Band {indicator} {inside_mean:0.3f}": inside},
            kde=tuple, element="step", bins=30)
    result = sps.ttest_ind(outside, inside, equal_var=False, alternative=alternative)
    return IndicatorBandTtestResult(result, lower_boundary, upper_boundary, outside_mean, inside_mean)

def quantile_elimination(data: pd.DataFrame, column: str, quantile: float):
    upper_boundary = data[column].quantile(1 - quantile)
    lower_boundary = data[column].quantile(quantile)
    return data[(data[column] < upper_boundary) & (data[column] > lower_boundary)]

def indicator_overlap(data: pd.DataFrame, indicator1: str, indicator2: str):
    both_high = data.loc[(data[indicator1] > data[indicator1].median()) & (data[indicator2] > data[indicator2].median())]
    return (2 * len(both_high)) / len(data)