import unittest

import polars as pl
import polars.testing as polt
import timastock as tm


class MiscTest(unittest.TestCase):
    def test_pairwise_avg(self):
        lf = pl.LazyFrame({"year": [2011, 2012, 2013], "value": [3.0, 5.0, 7.0]})
        result =  tm.misc.pairwise_avg(lf, "value")
        expected = pl.LazyFrame({"year": [2011, 2012, 2013], "value": [4.0, 6.0, None]})
        print(result.collect())
        polt.assert_frame_equal(result, expected)

    def test_yearpair_avg(self):
        years = pl.Series(["2011-01-01", "2012-01-01", "2013-01-01"])
        years = years.str.to_date()
        lf = pl.LazyFrame({"year": years, "value": [3.0, 5.0, 7.0]})
        result =  tm.misc.yearpair_avg(lf, "year", "value")
        expected = pl.LazyFrame({"year": years, "value": [4.0, 6.0, None]})
        print(result.collect())
        polt.assert_frame_equal(result, expected)
        