import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import scipy.stats as sps
import seaborn as sns
from collections import namedtuple
from .misc import AnyPolarsFrame

import typing as t

IndicatorBrunnerMunzelResult = namedtuple("IndicatorBrunnerMunzelResult", ["results", "medians"])
IndicatorLeveneResult = namedtuple("IndicatorLeveneResult", ["results", "variances"])
EnumQuantiles = pl.Enum(["veryLow", "low", "medium", "high", "veryHigh"])


def indicator_brunnermunzel(data: AnyPolarsFrame, indicator: str, target: str, split_on: t.Iterable[str] | str | None, alternative: str = "two-sided", with_plot: bool = True) -> IndicatorBrunnerMunzelResult:
    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    binned = data.with_columns(pl.col(indicator).qcut(EnumQuantiles.categories.len(), allow_duplicates=True, labels=EnumQuantiles.categories.to_list()).over(split_on).alias("quantile"))

    medians = dict({q: binned.filter(pl.col("quantile") == q).select(pl.median(target)).item() for q in EnumQuantiles.categories})
    counts = dict({q: binned.filter(pl.col("quantile") == q).select(pl.len()).item() for q in EnumQuantiles.categories})

    results = dict({
        q: sps.brunnermunzel(
            binned.filter(pl.col("quantile") == q).get_column(target),
            binned.filter(pl.col("quantile") != q).get_column(target),
            alternative=alternative)
        for q in EnumQuantiles.categories})
    
    if with_plot:
        plt.figure()
        sns.kdeplot(binned, x=target, hue="quantile", fill=False)
        plt.title(indicator)
        plt.show()

        for q in EnumQuantiles.categories:
            print(f"Median if indicator in {q:8s} quantile: {medians[q]:.5g} {results[q].pvalue * 100:.3f} % over {counts[q]} samples.")

    return IndicatorBrunnerMunzelResult(results, medians)

def indicator_levene(data: AnyPolarsFrame, indicator: str, target: str, split_on: t.Iterable[str] | str | None,with_plot: bool = True) -> IndicatorLeveneResult:
    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    binned = data.with_columns(pl.col(indicator).qcut(EnumQuantiles.categories.len(), allow_duplicates=True, labels=EnumQuantiles.categories.to_list()).over(split_on).alias("quantile"))

    
    variances = binned.group_by("quantile").agg(pl.col(target).var().alias(f"{target}Variance"))
    variances_tuple = dict({q: variances.filter(pl.col("quantile") == q).get_column(f"{target}Variance").item() for q in EnumQuantiles.categories})

    counts = dict({q: binned.filter(pl.col("quantile") == q).select(pl.len()).item() for q in EnumQuantiles.categories})

    results = dict({
        q: sps.levene(
            binned.filter(pl.col("quantile") == q).get_column(target),
            binned.filter(pl.col("quantile") != q).get_column(target))
        for q in EnumQuantiles.categories})
    
    if with_plot:
        plt.figure()
        sns.lineplot(variances, x="quantile", y=f"{target}Variance")
        plt.title(f"Variances in {target} over quantiles of {indicator}")
        plt.show()

        for q in EnumQuantiles.categories:
            print(f"Variance if indicator in {q:8s} quantile: {variances_tuple[q]:.5g} {results[q].pvalue * 100:.3f} % over {counts[q]} samples.")

    
    return IndicatorLeveneResult(results, variances_tuple)

def quantile_elimination(data: AnyPolarsFrame, column: str, quantile: float):
    return data.filter(
        pl.col(column) > pl.col(column).quantile(quantile),
        pl.col(column) < pl.col(column).quantile(1 - quantile))


def indicator_overlap(data: pl.DataFrame, indicator1: str, indicator2: str):
    both_high = data.filter(
        pl.col(indicator1) > pl.median(indicator1),
        pl.col(indicator2) > pl.median(indicator2))
    return (2 * len(both_high)) / len(data)