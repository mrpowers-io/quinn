"""Math routines for PySpark."""
from __future__ import annotations

from typing import Optional, Union

from pyspark.sql import Column
from pyspark.sql import functions as F  # noqa: N812


def rand_laplace(
    mu: Union[float, Column],
    beta: Union[float, Column],
    seed: Optional[int] = None,
) -> Column:
    """Generate random numbers from Laplace(mu, beta).

    :param mu: mu parameter of Laplace distribution
    :param beta: beta parameter of Laplace distribution
    :param seed: random seed value (optional, default None)
    :returns: column with random numbers
    """
    if not isinstance(mu, Column):
        mu = F.lit(mu)

    if not isinstance(beta, Column):
        beta = F.lit(beta)

    u = F.rand(seed)

    return (
        F.when(u < F.lit(0.5), mu + beta * F.log(2 * u))
        .otherwise(mu - beta * F.log(2 * (1 - u)))
        .alias("laplace_random")
    )


def div_or_else(
    cola: Column,
    colb: Column,
    default: Union[float, Column] = 0.0,
) -> Column:
    """Return result of division of cola by colb or default if colb is zero.

    :param cola: dividend
    :param colb: divisor
    :param default: default value
    :returns: result of division or zero
    """
    if not isinstance(default, Column):
        default = F.lit(default)

    return F.when(colb == F.lit(0.0), default).otherwise(cola / colb)
