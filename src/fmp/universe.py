import pandas as pd
from . import financials, pricing, company

def get_universe(symbols: list) -> list[dict]:
    universe = {}
    n = len(symbols)
    for (idx, symbol) in enumerate(symbols):
        if idx % 30 == 0 or idx == len(symbols) - 1:
            print(f"Constructing Universe ::: {idx:4d} / {n:4d} :::")
        try:
            symbol_info = {}
            symbol_info["incomeStatement"] = financials.income_statement(symbol)
            symbol_info["balanceSheet"] = financials.balance_sheet(symbol)
            symbol_info["cashflowStatement"] = financials.cashflow_statement(symbol)
            symbol_info["metrics"] = financials.key_metrics(symbol)
            symbol_info["prices"] = pricing.historical_prices(symbol, start="2000-01-01")
            symbol_info["profile"] = company.company_profile(symbol)
            universe[symbol] = symbol_info
        except Exception as err:
            print(f"Error on symbol {symbol}: {err}")
    return universe
