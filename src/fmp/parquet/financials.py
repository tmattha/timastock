import urllib.request
import time
import inspect

import polars as pl
from fmp.global_vars import api_key

import queue
import tqdm
import typing as t
from concurrent import futures
from urllib.error import HTTPError
from functools import partial, wraps

def _multithread_concat(worker_funcs: t.Iterable[t.Callable[[], pl.DataFrame]]) -> pl.DataFrame:
    caller_name = inspect.currentframe().f_back.f_code.co_name
    frame = None
    with futures.ThreadPoolExecutor(8) as executor:
        workers = [executor.submit(worker_func) for worker_func in worker_funcs]
        for worker in tqdm.tqdm(futures.as_completed(workers), caller_name, len(worker_funcs)):
            result = worker.result()
            
            if frame is None:
                frame = result
            else:
                frame = pl.concat([frame, result])

    return frame

def ignore_rate_limit(func: t.Callable) -> t.Callable:
    @wraps(func)
    def __func_ignoring_rate_limit(*args, **kwargs) -> pl.DataFrame:
        while True:
            try:
                result = func(*args, **kwargs)
                return result
            except HTTPError as err:
                if err.code == 429:
                    time.sleep(10)
                else:
                    raise err
            except Exception as err:
                raise err
    return __func_ignoring_rate_limit

@ignore_rate_limit
def income_statement(symbol: str, period: str = 'annual', limit: int = 30) -> pl.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}?period={period}&limit={limit}&apikey={api_key()}"
    with urllib.request.urlopen(url) as response:
        df = pl.read_json(response)
    return df.select(
        pl.col("symbol"),
        pl.col("date").str.to_date(),
        pl.col("fillingDate").str.to_date(),
        pl.col("calendarYear").cast(pl.Int16),
        pl.col("reportedCurrency"),
        pl.col("period"),
        pl.col("link"),
        # actually interesting
        pl.col("revenue").cast(pl.Float64),
        pl.col("costOfRevenue").cast(pl.Float64),
        pl.col("grossProfit").cast(pl.Float64),
        pl.col("grossProfitRatio").cast(pl.Float64),
        pl.col("researchAndDevelopmentExpenses").cast(pl.Float64),
        pl.col("generalAndAdministrativeExpenses").cast(pl.Float64),
        pl.col("sellingAndMarketingExpenses").cast(pl.Float64),
        pl.col("sellingGeneralAndAdministrativeExpenses").cast(pl.Float64),
        pl.col("otherExpenses").cast(pl.Float64),
        pl.col("operatingExpenses").cast(pl.Float64),
        pl.col("costAndExpenses").cast(pl.Float64),
        pl.col("interestIncome").cast(pl.Float64),
        pl.col("interestExpense").cast(pl.Float64),
        pl.col("depreciationAndAmortization").cast(pl.Float64),
        pl.col("ebitda").cast(pl.Float64),
        pl.col("ebitdaratio").cast(pl.Float64),
        pl.col("operatingIncome").cast(pl.Float64),
        pl.col("operatingIncomeRatio").cast(pl.Float64),
        pl.col("totalOtherIncomeExpensesNet").cast(pl.Float64),
        pl.col("incomeBeforeTax").cast(pl.Float64),
        pl.col("incomeBeforeTaxRatio").cast(pl.Float64),
        pl.col("incomeTaxExpense").cast(pl.Float64),
        pl.col("netIncome").cast(pl.Float64),
        pl.col("netIncomeRatio").cast(pl.Float64),
        pl.col("eps").cast(pl.Float64),
        pl.col("epsdiluted").cast(pl.Float64),
        pl.col("weightedAverageShsOut").cast(pl.Float64),
        pl.col("weightedAverageShsOutDil").cast(pl.Float64)
        )

def multi_income_statements(symbols: list[str], period: str = 'annual', limit: int = 30) -> pl.DataFrame:
    return _multithread_concat([
        partial(income_statement, symbol, period, limit)
        for symbol in symbols])

@ignore_rate_limit
def balance_sheet(symbol: str, period: str = 'annual', limit: int = 30) -> pl.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?period={period}&limit={limit}&apikey={api_key()}"
    with urllib.request.urlopen(url) as response:
        df = pl.read_json(response)
    return df.select(
        pl.col("symbol"),
        pl.col("date").str.to_date(),
        pl.col("fillingDate").str.to_date(),
        pl.col("calendarYear").cast(pl.Int16),
        pl.col("reportedCurrency"),
        pl.col("period"),
        pl.col("link"),
        # actually interesting
        pl.col("cashAndCashEquivalents").cast(pl.Float64),
        pl.col("shortTermInvestments").cast(pl.Float64),
        pl.col("cashAndShortTermInvestments").cast(pl.Float64),
        pl.col("netReceivables").cast(pl.Float64),
        pl.col("inventory").cast(pl.Float64),
        pl.col("otherCurrentAssets").cast(pl.Float64),
        pl.col("totalCurrentAssets").cast(pl.Float64),
        pl.col("propertyPlantEquipmentNet").cast(pl.Float64),
        pl.col("goodwill").cast(pl.Float64),
        pl.col("intangibleAssets").cast(pl.Float64),
        pl.col("goodwillAndIntangibleAssets").cast(pl.Float64),
        pl.col("longTermInvestments").cast(pl.Float64),
        pl.col("taxAssets").cast(pl.Float64),
        pl.col("otherNonCurrentAssets").cast(pl.Float64),
        pl.col("totalNonCurrentAssets").cast(pl.Float64),
        pl.col("otherAssets").cast(pl.Float64),
        pl.col("totalAssets").cast(pl.Float64),
        pl.col("accountPayables").cast(pl.Float64),
        pl.col("shortTermDebt").cast(pl.Float64),
        pl.col("taxPayables").cast(pl.Float64),
        pl.col("deferredRevenue").cast(pl.Float64),
        pl.col("otherCurrentLiabilities").cast(pl.Float64),
        pl.col("totalCurrentLiabilities").cast(pl.Float64),
        pl.col("longTermDebt").cast(pl.Float64),
        pl.col("deferredRevenueNonCurrent").cast(pl.Float64),
        pl.col("deferredTaxLiabilitiesNonCurrent").cast(pl.Float64),
        pl.col("otherNonCurrentLiabilities").cast(pl.Float64),
        pl.col("totalNonCurrentLiabilities").cast(pl.Float64),
        pl.col("otherLiabilities").cast(pl.Float64),
        pl.col("capitalLeaseObligations").cast(pl.Float64),
        pl.col("totalLiabilities").cast(pl.Float64),
        pl.col("preferredStock").cast(pl.Float64),
        pl.col("commonStock").cast(pl.Float64),
        pl.col("retainedEarnings").cast(pl.Float64),
        pl.col("accumulatedOtherComprehensiveIncomeLoss").cast(pl.Float64),
        pl.col("othertotalStockholdersEquity").cast(pl.Float64),
        pl.col("totalStockholdersEquity").cast(pl.Float64),
        pl.col("totalEquity").cast(pl.Float64),
        pl.col("totalLiabilitiesAndStockholdersEquity").cast(pl.Float64),
        pl.col("minorityInterest").cast(pl.Float64),
        pl.col("totalLiabilitiesAndTotalEquity").cast(pl.Float64),
        pl.col("totalInvestments").cast(pl.Float64),
        pl.col("totalDebt").cast(pl.Float64),
        pl.col("netDebt").cast(pl.Float64)
        )

def multi_balance_sheets(symbols: list[str], period: str = 'annual', limit: int = 30) -> pl.DataFrame:
    return _multithread_concat([
        partial(balance_sheet, symbol, period, limit)
        for symbol in symbols])

@ignore_rate_limit
def cashflow_statement(symbol: str, period: str = 'annual', limit: int = 30) -> pl.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{symbol}?period={period}&limit={limit}&apikey={api_key()}"
    with urllib.request.urlopen(url) as response:
        df = pl.read_json(response)
    return df.select(
        pl.col("symbol"),
        pl.col("date").str.to_date(),
        pl.col("fillingDate").str.to_date(),
        pl.col("calendarYear").cast(pl.Int16),
        pl.col("reportedCurrency"),
        pl.col("period"),
        pl.col("link"),
        # actually interesting
        pl.col("netIncome").cast(pl.Float64),
        pl.col("depreciationAndAmortization").cast(pl.Float64),
        pl.col("deferredIncomeTax").cast(pl.Float64),
        pl.col("stockBasedCompensation").cast(pl.Float64),
        pl.col("changeInWorkingCapital").cast(pl.Float64),
        pl.col("accountsReceivables").cast(pl.Float64),
        pl.col("inventory").cast(pl.Float64),
        pl.col("accountsPayables").cast(pl.Float64),
        pl.col("otherWorkingCapital").cast(pl.Float64),
        pl.col("otherNonCashItems").cast(pl.Float64),
        pl.col("netCashProvidedByOperatingActivities").cast(pl.Float64),
        pl.col("investmentsInPropertyPlantAndEquipment").cast(pl.Float64),
        pl.col("acquisitionsNet").cast(pl.Float64),
        pl.col("purchasesOfInvestments").cast(pl.Float64),
        pl.col("salesMaturitiesOfInvestments").cast(pl.Float64),
        pl.col("otherInvestingActivites").cast(pl.Float64),
        pl.col("netCashUsedForInvestingActivites").cast(pl.Float64),
        pl.col("debtRepayment").cast(pl.Float64),
        pl.col("commonStockIssued").cast(pl.Float64),
        pl.col("commonStockRepurchased").cast(pl.Float64),
        pl.col("dividendsPaid").cast(pl.Float64),
        pl.col("otherFinancingActivites").cast(pl.Float64),
        pl.col("netCashUsedProvidedByFinancingActivities").cast(pl.Float64),
        pl.col("effectOfForexChangesOnCash").cast(pl.Float64),
        pl.col("netChangeInCash").cast(pl.Float64),
        pl.col("cashAtEndOfPeriod").cast(pl.Float64),
        pl.col("cashAtBeginningOfPeriod").cast(pl.Float64),
        pl.col("operatingCashFlow").cast(pl.Float64),
        pl.col("capitalExpenditure").cast(pl.Float64),
        pl.col("freeCashFlow").cast(pl.Float64),
        )

def multi_cashflow_statement(symbols: list[str], period: str = 'annual', limit: int = 30) -> pl.DataFrame:
    return _multithread_concat([
        partial(cashflow_statement, symbol, period, limit)
        for symbol in symbols])

@ignore_rate_limit
def key_metrics(symbol: str, period: str = 'annual', limit: int = 30) -> pl.DataFrame:
    url = f"https://financialmodelingprep.com/api/v3/key-metrics/{symbol}?period={period}&limit={limit}&apikey={api_key()}"
    with urllib.request.urlopen(url) as response:
        df = pl.read_json(response)
    return df.select(
        pl.col("symbol"),
        pl.col("date").str.to_date(),
        pl.col("calendarYear").cast(pl.Int16),
        pl.col("period"),
        # actually interesting
        pl.col("revenuePerShare").cast(pl.Float64),
        pl.col("netIncomePerShare").cast(pl.Float64),
        pl.col("operatingCashFlowPerShare").cast(pl.Float64),
        pl.col("freeCashFlowPerShare").cast(pl.Float64),
        pl.col("cashPerShare").cast(pl.Float64),
        pl.col("bookValuePerShare").cast(pl.Float64),
        pl.col("tangibleBookValuePerShare").cast(pl.Float64),
        pl.col("shareholdersEquityPerShare").cast(pl.Float64),
        pl.col("interestDebtPerShare").cast(pl.Float64),
        pl.col("marketCap").cast(pl.Float64),
        pl.col("enterpriseValue").cast(pl.Float64),
        pl.col("peRatio").cast(pl.Float64),
        pl.col("priceToSalesRatio").cast(pl.Float64),
        pl.col("pocfratio").cast(pl.Float64),
        pl.col("pfcfRatio").cast(pl.Float64),
        pl.col("pbRatio").cast(pl.Float64),
        pl.col("ptbRatio").cast(pl.Float64),
        pl.col("evToSales").cast(pl.Float64),
        pl.col("enterpriseValueOverEBITDA").cast(pl.Float64),
        pl.col("evToOperatingCashFlow").cast(pl.Float64),
        pl.col("evToFreeCashFlow").cast(pl.Float64),
        pl.col("earningsYield").cast(pl.Float64),
        pl.col("freeCashFlowYield").cast(pl.Float64),
        pl.col("debtToEquity").cast(pl.Float64),
        pl.col("debtToAssets").cast(pl.Float64),
        pl.col("netDebtToEBITDA").cast(pl.Float64),
        pl.col("currentRatio").cast(pl.Float64),
        pl.col("interestCoverage").cast(pl.Float64),
        pl.col("incomeQuality").cast(pl.Float64),
        pl.col("dividendYield").cast(pl.Float64),
        pl.col("payoutRatio").cast(pl.Float64),
        pl.col("salesGeneralAndAdministrativeToRevenue").cast(pl.Float64),
        pl.col("researchAndDdevelopementToRevenue").cast(pl.Float64),
        pl.col("intangiblesToTotalAssets").cast(pl.Float64),
        pl.col("capexToOperatingCashFlow").cast(pl.Float64),
        pl.col("capexToRevenue").cast(pl.Float64),
        pl.col("capexToDepreciation").cast(pl.Float64),
        pl.col("stockBasedCompensationToRevenue").cast(pl.Float64),
        pl.col("grahamNumber").cast(pl.Float64),
        pl.col("roic").cast(pl.Float64),
        pl.col("returnOnTangibleAssets").cast(pl.Float64),
        pl.col("grahamNetNet").cast(pl.Float64),
        pl.col("workingCapital").cast(pl.Float64),
        pl.col("tangibleAssetValue").cast(pl.Float64),
        pl.col("netCurrentAssetValue").cast(pl.Float64),
        pl.col("investedCapital").cast(pl.Float64),
        pl.col("averageReceivables").cast(pl.Float64),
        pl.col("averagePayables").cast(pl.Float64),
        pl.col("averageInventory").cast(pl.Float64),
        pl.col("daysSalesOutstanding").cast(pl.Float64),
        pl.col("daysPayablesOutstanding").cast(pl.Float64),
        pl.col("daysOfInventoryOnHand").cast(pl.Float64),
        pl.col("receivablesTurnover").cast(pl.Float64),
        pl.col("payablesTurnover").cast(pl.Float64),
        pl.col("inventoryTurnover").cast(pl.Float64),
        pl.col("roe").cast(pl.Float64),
        pl.col("capexPerShare").cast(pl.Float64)
        )

def multi_key_metrics(symbols: list[str], period: str = 'annual', limit: int = 30) -> pl.DataFrame:
    return _multithread_concat([
        partial(key_metrics, symbol, period, limit)
        for symbol in symbols])
