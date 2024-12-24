import unittest

import polars as pl
import polars.testing as polt
import timastock as tm

class CapitalTest(unittest.TestCase):
    def test_capital_employed(self):
        years = pl.Series("calendarYear", [2016, 2017, 2018], dtype=pl.Int32)
        balance_sheet = pl.DataFrame({
            "symbol": ["AAPL", "AAPL", "AAPL"],
            "calendarYear": years,
            "totalAssets": [17.5, 20.0, 23.5],
            "totalCurrentLiabilities": [3.0, 3.5, 1.0]})
        expected = pl.DataFrame({
            "symbol": ["AAPL", "AAPL", "AAPL"],
            "calendarYear": years,
            # First interval does not have left value.
            "capitalEmployed": [14.5, 15.5, 19.5]})
        result = tm.capital.capital_employed(balance_sheet)
        print(result)
        
        polt.assert_frame_equal(result, expected)
        # Should not be subject to order.
        result = tm.capital.capital_employed(balance_sheet.sort("calendarYear", descending=True))
        print(result)
        polt.assert_frame_equal(result, expected)
