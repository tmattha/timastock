import matplotlib.pyplot as plt
import pandas as pd
import scipy.stats as sps

def indicator_ttest(data: pd.DataFrame, indicator: str, target: str, alternative: str = "two-sided", with_plot: bool = True):
    median = data[indicator].median()
    high_indicator = data.loc[data[indicator] <= median][target]
    low_indicator = data.loc[data[indicator] >= median][target]
    high_mean = high_indicator.mean()
    low_mean = low_indicator.mean()
    if with_plot:
        plt.hist(high_indicator, bins=30, label=f"High {indicator}: {high_indicator.mean():0.3f}%")
        plt.hist(low_indicator, bins=30, label=f"Low {indicator} {low_indicator.mean():0.3f}%")
        plt.legend()
    result = sps.ttest_ind(high_indicator, low_indicator, equal_var=False, alternative=alternative)
    return (result, median, high_mean, low_mean)
