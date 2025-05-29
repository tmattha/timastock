
import polars as pl

def weight_by_inverse_frequency(column: str | pl.Expr, weight: str | pl.Expr):
    if not isinstance(column, pl.Expr):
        column = pl.col(column)
    if not isinstance(weight, pl.Expr):
        weight = pl.col(weight)
    return (column * pl.len() / pl.len().over(weight) / weight.n_unique())
