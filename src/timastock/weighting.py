import pandas as pd

def linear_weights(n: int, index: pd.Series):
    raw_weights = [float(x + 1) for x in range(n)]
    raw_weights.reverse()
    array = pd.Series(raw_weights)
    # sum should be 1
    array /= (n * (n + 1)) / 2
    array.index = index
    return array
