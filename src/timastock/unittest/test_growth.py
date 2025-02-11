import unittest

import polars as pl
import polars.testing as polt
import timastock as tm

class GrowthTest(unittest.TestCase):
    def test_yoy_revenue_growth(self):

        years = pl.Series("calendarYear", [2016, 2017, 2018, 2019, 2020], dtype=pl.Int32)
        income_statement = pl.DataFrame({
            "symbol": ["BLUB"] * 5,
            "calendarYear": years,
            "revenue": [1.0, 2.0, 2.0, 1.0, 0.0]})
        expected = pl.DataFrame({
            "symbol": ["BLUB"] * 4,
            "calendarYear": pl.Series("calendarYear", [2017, 2018, 2019, 2020], dtype=pl.Int32),
            "revenueGrowth": [1.0, 0.0, -0.5, -1.0]})
        result = tm.growth.yoy_growth("revenue", income_statement)
        print(result)

        polt.assert_frame_equal(result, expected, check_row_order=False)
