import pandas as pd
from . import financials, pricing, company

def get_universe(symbols: list, start: str=None, end: str=None) -> list[dict]:
    universe = {}
    n = len(symbols)
    for (idx, symbol) in enumerate(symbols):
        if idx % 20 == 0 or idx == len(symbols) - 1:
            print(f"Constructing Universe ::: {idx:4d} / {n:4d} :::")
        try:
            symbol_info = {}
            symbol_info["incomeStatement"] = financials.income_statement(symbol)
            symbol_info["balanceSheet"] = financials.balance_sheet(symbol)
            symbol_info["cashflowStatement"] = financials.cashflow_statement(symbol)
            symbol_info["metrics"] = financials.key_metrics(symbol)
            symbol_info["prices"] = pricing.historical_prices(symbol, start=start, end=end)
            symbol_info["marketCap"] = pricing.market_cap(symbol, start=start, end=end)
            symbol_info["profile"] = company.company_profile(symbol)
            symbol_info["executives"] = company.executives(symbol)
            universe[symbol] = symbol_info
        except Exception as err:
            print(f"Error on symbol {symbol}: {err}")
    print(f"Successfully exported {len(universe)} of {len(symbols)} data sets.")
    return universe
