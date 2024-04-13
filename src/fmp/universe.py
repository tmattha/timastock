from concurrent import futures
import random
import time
from urllib.error import HTTPError
import pandas as pd
import tqdm
from . import financials, pricing, company, global_vars

def _get_universe_entry(symbol: str) -> dict:
    symbol_info = {}
    retry = True
    while retry:
        try:
            if "incomeStatement"    not in symbol_info:
                symbol_info["incomeStatement"] = financials.income_statement(symbol)
            if "balanceSheet"       not in symbol_info:
                symbol_info["balanceSheet"] = financials.balance_sheet(symbol)
            if "cashflowStatement"  not in symbol_info:
                symbol_info["cashflowStatement"] = financials.cashflow_statement(symbol)
            if "metrics"            not in symbol_info:
                symbol_info["metrics"] = financials.key_metrics(symbol)
            if "prices"             not in symbol_info:
                symbol_info["prices"] = pricing.full_historical_prices(symbol)
            if "marketCap"          not in symbol_info:
                symbol_info["marketCap"] = pricing.market_cap(symbol)
            if "profile"            not in symbol_info:
                symbol_info["profile"] = company.company_profile(symbol)
            if "executives"         not in symbol_info:
                symbol_info["executives"] = company.executives(symbol)
            return {symbol: symbol_info}
        except HTTPError as err:
            time.sleep(random.randrange(60))
            retry = True
        except Exception as err:
            print(f"Error on symbol {symbol}: {err}")
            return None

def get_universe(symbols: list) -> list[dict]:
    universe = {}
    with futures.ThreadPoolExecutor(8) as executor:
        runner = executor.map(lambda s: _get_universe_entry(s), symbols)
        for stat in tqdm.tqdm(runner, "Constructing Universe", len(symbols)):
            if stat is not None: universe.update(stat)
    print(f"Successfully exported {len(universe)} of {len(symbols)} data sets.")
    return universe
