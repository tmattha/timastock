import unittest

import polars as pl
import polars.testing as polt
import timastock as tm

class ReturnsTest(unittest.TestCase):
    def test_annual_return(self):

        dates = pl.Series("date", ["2023-01-01", "2024-01-01", "2022-01-01", "2024-01-01", "2023-01-01", "2023-03-15"])
        dates = dates.str.to_date()
        prices = pl.DataFrame({
            "symbol": ["BLUB", "BLUB", "APPL", "APPL", "MSFT", "MSFT"],
            "date": dates,
            "adjClose": [20.0, 25.0, 1.0, 4.0, 2.0, 1.0]
        })
        # 25 % in one year
        # 300 % in two years
        # -50% in 1/5 year
        expected = pl.DataFrame({
            "symbol": ["BLUB", "APPL", "MSFT"],
            "annualReturn": [0.25, 1.0, -0.96875]})
        result = tm.returns.annual_return(prices)
        print(result)

        polt.assert_frame_equal(result, expected, check_row_order=False)
