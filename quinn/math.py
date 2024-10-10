# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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


def rand_range(
    minimum: Union[int, Column],
    maximum: Union[int, Column],
    seed: Optional[int] = None,
) -> Column:
    """Generate random numbers uniformly distributed in [`minimum`, `maximum`).

    :param minimum: minimum value of the random numbers
    :param maximum: maximum value of the random numbers
    :param seed: random seed value (optional, default None)
    :returns: column with random numbers
    """
    if not isinstance(minimum, Column):
        minimum = F.lit(minimum)

    if not isinstance(maximum, Column):
        maximum = F.lit(maximum)

    u = F.rand(seed)

    return minimum + (maximum - minimum) * u


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
