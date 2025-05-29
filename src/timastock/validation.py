import plotly.express as px
import polars as pl
import scipy.stats as sps
from collections import namedtuple
from .misc import AnyPolarsFrame

import typing as t

IndicatorBrunnerMunzelResult = namedtuple("IndicatorBrunnerMunzelResult", ["results", "medians"])
IndicatorLeveneResult = namedtuple("IndicatorLeveneResult", ["results", "variances"])
EnumQuantiles = pl.Enum(["veryLow", "low", "medium", "high", "veryHigh"])


def indicator_brunnermunzel(data: AnyPolarsFrame, indicator: str, target: str, split_on: t.Iterable[str] | str | None = None, alternative: str = "two-sided", with_plot: bool = True) -> IndicatorBrunnerMunzelResult:
    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    data = data.select(
        indicator, target,
        (pl.col(indicator).rank() / (pl.col(indicator).len() + 1)).over(split_on).alias("relativeRank"))
    binned = data.with_columns(pl.col("relativeRank").qcut(EnumQuantiles.categories.len(), allow_duplicates=True, labels=EnumQuantiles.categories.to_list()).alias("quantile"))

    medians = binned.group_by("quantile").agg(
        pl.col(indicator).quantile(0.05).alias(f"{indicator}Low"),
        pl.col(indicator).median().alias(f"{indicator}Median"),
        pl.col(indicator).quantile(0.95).alias(f"{indicator}High"),
        pl.col(target).median().alias(f"{target}Median"))
    medians_tuple = dict({q: medians.filter(pl.col("quantile") == q).get_column(f"{target}Median").item() for q in EnumQuantiles.categories})
    counts = dict({q: binned.filter(pl.col("quantile") == q).select(pl.len()).item() for q in EnumQuantiles.categories})

    results = dict({
        q: sps.brunnermunzel(
            binned.filter(pl.col("quantile") == q).get_column(target),
            binned.filter(pl.col("quantile") != q).get_column(target),
            alternative=alternative)
        for q in EnumQuantiles.categories})
    
    if with_plot:
        preprocessed = medians.unpivot(
            [f"{indicator}Low", f"{indicator}Median", f"{indicator}High"],
            index=["quantile", f"{target}Median"], value_name=indicator)
        fig = px.line(preprocessed, x=indicator, y=f"{target}Median", color="quantile",
            title=f"Median of {target} over quantiles of {indicator}", markers=True,
            template="plotly_dark")
        fig.update_traces(marker={"symbol": "diamond-tall", "size": 10, "line": {"color": fig.layout["plot_bgcolor"], "width": 1}})
        fig.show()

        for q in EnumQuantiles.categories:
            print(f"Median if indicator in {q:8s} quantile: {medians_tuple[q]:.5g} {results[q].pvalue * 100:.3f} % over {counts[q]} samples.")

    return IndicatorBrunnerMunzelResult(results, medians_tuple)

def indicator_levene(data: AnyPolarsFrame, indicator: str, target: str, split_on: t.Iterable[str] | str | None, with_plot: bool = True) -> IndicatorLeveneResult:
    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    data = data.select(
        indicator, target,
        (pl.col(indicator).rank() / (pl.col(indicator).len() + 1)).over(split_on).alias("relativeRank"))

    binned = data.with_columns(pl.col("relativeRank").qcut(EnumQuantiles.categories.len(), allow_duplicates=True, labels=EnumQuantiles.categories.to_list()).alias("quantile"))

    
    variances = binned.group_by("quantile").agg(
        pl.col(indicator).quantile(0.05).alias(f"{indicator}Low"),
        pl.col(indicator).median().alias(f"{indicator}Median"),
        pl.col(indicator).quantile(0.95).alias(f"{indicator}High"),
        pl.col(target).var().alias(f"{target}Variance"))
    variances_tuple = dict({q: variances.filter(pl.col("quantile") == q).get_column(f"{target}Variance").item() for q in EnumQuantiles.categories})

    counts = dict({q: binned.filter(pl.col("quantile") == q).select(pl.len()).item() for q in EnumQuantiles.categories})

    results = dict({
        q: sps.levene(
            binned.filter(pl.col("quantile") == q).get_column(target),
            binned.filter(pl.col("quantile") != q).get_column(target))
        for q in EnumQuantiles.categories})
    
    if with_plot:
        preprocessed = variances.unpivot(
            [f"{indicator}Low", f"{indicator}Median", f"{indicator}High"],
            index=["quantile", f"{target}Variance"], value_name=indicator)
        # plt.figure()
        # sns.lineplot(preprocessed, x=indicator, y=f"{target}Variance", hue="quantile", marker="d")
        # plt.title(f"Variances in {target} over quantiles of {indicator}")
        # plt.show()
        fig = px.line(preprocessed, x=indicator, y=f"{target}Variance", color="quantile",
            title=f"Variance of {target} over quantiles of {indicator}", markers=True,
            template="plotly_dark")
        fig.update_traces(marker={"symbol": "diamond-tall", "size": 10, "line": {"color": fig.layout["plot_bgcolor"], "width": 1}})
        fig.show()

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