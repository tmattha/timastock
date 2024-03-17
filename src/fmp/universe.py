import pandas as pd
from . import financials, pricing, company

def get_universe(symbols: list) -> list[dict]:
    universe = {}
    for symbol in symbols:
        symbol_info = {}
        symbol_info["incomeStatement"] = financials.income_statement(symbol)
        symbol_info["balanceSheet"] = financials.balance_sheet(symbol)
        symbol_info["cashflowStatement"] = financials.cashflow_statement(symbol)
        symbol_info["prices"] = pricing.historical_prices(symbol, start="2000-01-01")
        symbol_info["profile"] = company.company_profile(symbol)
        universe[symbol] = symbol_info
    return universe
