import numpy as np
import pandas as pd

def executives_pay(executives: pd.DataFrame, currency: str = "USD"):
    if (executives["currencyPay"] != currency).any(): return np.nan
    return executives["pay"].sum()

def doctor_ratio(executives: pd.DataFrame):
    doctors = executives.loc[executives["name"].str.startswith("Dr.")]
    return len(doctors) / len(executives)

def women_ratio(executives: pd.DataFrame):
    women = executives.loc[executives["gender"] == "female"]
    return len(women) / len(executives)
