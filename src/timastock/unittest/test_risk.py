import unittest

import polars as pl
import polars.testing as polt
import timastock as tm

class RiskTest(unittest.TestCase):

    def test_ebit_volatility(self):
        years = pl.Series("calendarYear", [2016, 2017, 2018], dtype=pl.Int32)
        income_statement = pl.DataFrame({
            "symbol": ["BLUB", "BLUB", "BLUB"],
            "calendarYear": years,
            "operatingIncome": [50.0, 150.0, 150.0]})
        expected = pl.DataFrame({
            "symbol": ["BLUB", "BLUB", "BLUB"],
            "calendarYear": years,
            "ebitVolatility": [None, None, 2 ** (1/2)]})
        result = tm.risk.ebit_volatility(income_statement, interval=2)
        print(result)
        
        polt.assert_frame_equal(result, expected)

    def test_drawdown(self):
        dates = pl.Series("date", ["2024-01-01", "2024-01-02", "2024-01-03"])
        dates = dates.str.to_date()
        print(dates)
        prices = pl.DataFrame({
            "symbol": ["BLUB", "BLUB", "BLUB"],
            "date": dates,
            "adjClose": [20.0, 20.0, 10.0]
        })
        expected = pl.DataFrame({
            "symbol": ["BLUB", "BLUB", "BLUB"],
            "date": dates,
            "drawdown": [None, 0.0, -0.5]})
        result = tm.risk.drawdown(prices, period="1d")
        print(result)

        polt.assert_frame_equal(result, expected)
