import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import scipy.stats as sps
import seaborn as sns
from collections import namedtuple
from .misc import AnyPolarsFrame

import typing as t

IndicatorBrunnerMunzelResult = namedtuple("IndicatorBrunnerMunzelResult", ["result", "median", "high_median", "low_median"])
IndicatorLeveneResult = namedtuple("IndicatorLeveneResult", ["result", "variance", "high_variance", "low_variance"])

def indicator_brunnermunzel(data: AnyPolarsFrame, indicator: str, target: str, split_on: t.Iterable[str] | str | None, alternative: str = "two-sided", with_plot: bool = True) -> IndicatorBrunnerMunzelResult:
    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    median = data.select(pl.median(target)).item()
    if split_on is None:
        low_indicator = data.filter(pl.col(indicator) < pl.median(indicator))
        high_indicator = data.filter(pl.col(indicator) > pl.median(indicator))
    else:
        low_indicator = data.filter(pl.col(indicator) < pl.median(indicator).over(split_on))
        high_indicator = data.filter(pl.col(indicator) > pl.median(indicator).over(split_on))
    low_median = low_indicator.select(pl.median(target)).item()
    high_median = high_indicator.select(pl.median(target)).item()
    bin_edges = data.select(pl.col(target).hist(bin_count=60, include_breakpoint=True).struct.field("breakpoint")).get_column("breakpoint")
    
    if with_plot:
        low_histogramm = low_indicator.select(
            bin_edges.rolling_mean(2).drop_nulls().alias(target),
            pl.col(target).hist(bin_edges).alias("low"),
        )
        high_histogramm = high_indicator.select(
            bin_edges.rolling_mean(2).drop_nulls().alias(target),
            pl.col(target).hist(bin_edges).alias("high"),
        )
        overall_histogramm = low_histogramm.join(high_histogramm, on=target)
        overall_histogramm = overall_histogramm.with_columns(
            (pl.col("high").cast(pl.Int64) - pl.col("low").cast(pl.Int64)).alias("diff")
        )
        sns.lineplot(
            overall_histogramm.unpivot(["low", "high", "diff"], variable_name=indicator, value_name="count", index=target),
            x=target, y="count", hue=indicator, drawstyle="steps-mid")
        plt.axhline(color="black",linewidth=0.5)
    result = sps.brunnermunzel(low_indicator.get_column(target), high_indicator.get_column(target), alternative=alternative)
    return IndicatorBrunnerMunzelResult(result, median, high_median, low_median)

def indicator_levene(data: AnyPolarsFrame, indicator: str, target: str, split_on: t.Iterable[str] | str | None,with_plot: bool = True) -> IndicatorLeveneResult:
    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    variance = data.select(pl.var(indicator)).item()
    if split_on is None:
        low_indicator = data.filter(pl.col(indicator) < pl.median(indicator))
        high_indicator = data.filter(pl.col(indicator) > pl.median(indicator))
    else:
        low_indicator = data.filter(pl.col(indicator) < pl.median(indicator).over(split_on))
        high_indicator = data.filter(pl.col(indicator) > pl.median(indicator).over(split_on))
    low_variance = low_indicator.select(pl.var(target)).item()
    high_variance = high_indicator.select(pl.var(target)).item()

    binned = data.with_columns(pl.col(indicator).qcut(5, allow_duplicates=True, labels=["veryLow", "low", "medium", "high", "veryHigh"]).over(split_on))
    binned = binned.select(indicator, pl.col(target).var().alias(f"{target}Variance").over(indicator))
    if with_plot:
        sns.lineplot(binned, x=indicator, y=f"{target}Variance")
    result = sps.levene(low_indicator.get_column(target), high_indicator.get_column(target))
    return IndicatorLeveneResult(result, variance, high_variance, low_variance)

def quantile_elimination(data: AnyPolarsFrame, column: str, quantile: float):
    return data.filter(
        pl.col(column) > pl.col(column).quantile(quantile),
        pl.col(column) < pl.col(column).quantile(1 - quantile))


def indicator_overlap(data: pl.DataFrame, indicator1: str, indicator2: str):
    both_high = data.filter(
        pl.col(indicator1) > pl.median(indicator1),
        pl.col(indicator2) > pl.median(indicator2))
    return (2 * len(both_high)) / len(data)