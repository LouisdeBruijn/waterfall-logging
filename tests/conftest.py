import string
import warnings
from datetime import datetime, timedelta
from typing import Callable

import numpy as np
import pandas as pd
import pyspark
import pytest
from pandas._testing import assert_series_equal

from waterfall_logging.log import Waterfall


@pytest.fixture
def dataframe(request):
    frame_type = request.param
    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore", category=FutureWarning)
        n = 30
        if frame_type == "missing":
            df = pd.util.testing.makeMissingDataframe()
        elif frame_type == "time":
            df = pd.util.testing.makeTimeDataFrame()
        elif frame_type == "nan":
            df = pd.DataFrame({letter: [np.nan] * n for letter in string.ascii_uppercase[:5]})
        elif frame_type == "None":
            df = pd.DataFrame({letter: ["char"] + [None] * (n - 1) for letter in string.ascii_uppercase[:5]})
        elif frame_type == "markdown":
            return pd.DataFrame(
                data={
                    "Table": ["table1", "table1", "table1"],
                    "col1": [50, 150, 250],
                    "Δ col1": [0, 100, 100],
                    "Rows": [2727, 2827, 2927],
                    "Δ Rows": [0, 100, 100],
                    "Reason": ["initial", "add-on", "extra"],
                    "Configurations flag": ["read_markdown", "read_markdown", "read_markdown"],
                },
            )
        elif frame_type == "large":
            return pd.DataFrame(data=np.random.randn(100000, 5), columns=[s for s in string.ascii_uppercase[:5]])
        else:
            df = pd.util.testing.makeDataFrame()

        df["user_id"] = [None] * 3 + [f"user_{i}" for i in range(2, 11)] * int(len(df) / 10)

        return df


def assert_waterfall_log(w: Waterfall, dataframe: pd.DataFrame):
    """Asserts whether the count of the dataframe is equal to the last addition to the logging DataFrame."""
    unique_table_names = w._log.iloc[:, 0].unique()
    for table_name in unique_table_names:
        waterfall_specific_table = w._log.loc[w._log.iloc[:, 0] == table_name]
        delta_column_names = [col for col in waterfall_specific_table if col.startswith(w.delta_prefix)]

        initial_count = pd.Series(
            [len(dataframe)] * (len(dataframe.columns) + 1),
            index=dataframe.columns.to_list() + ["Rows"],
        )

        sum_delta_columns = waterfall_specific_table[delta_column_names].sum().set_axis(initial_count.index)
        dataframe_count = initial_count.add(sum_delta_columns)
        last_waterfall_addition = waterfall_specific_table[dataframe_count.index].iloc[-1]

        assert_series_equal(dataframe_count, last_waterfall_addition, check_dtype=False, check_names=False)


@pytest.fixture(scope="session")
def spark_session():
    spark = pyspark.sql.SparkSession.builder.enableHiveSupport().appName(str(__file__)).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def time_tracker():
    """"""
    tick = datetime.now()
    yield
    tock = datetime.now()
    runtime = tock - tick
    print(f"\n runtime: {runtime.total_seconds()}")


def track_aggregation_time(sdf, expressions):
    """"""
    tick = datetime.now()
    sdf.agg(*expressions)
    tock = datetime.now()
    runtime = tock - tick
    print(f"\n aggregation time: {runtime.total_seconds()}")


class PerformanceException(Exception):
    """"""

    def __init__(self, runtime: timedelta, limit: timedelta):
        self.runtime = runtime
        self.limit = limit

    def __str__(self):
        return f"Performance test failed, runtime: {self.runtime.total_seconds()}, limit: {self.limit.total_seconds()}"


def track_performance(method: Callable, runtime_limit=timedelta(seconds=2)):
    """"""

    def run_function_and_validate_runtime(*args, **kwargs):
        """"""
        tick = datetime.now()
        result = method(*args, **kwargs)
        tock = datetime.now()
        runtime = tock - tick
        print(f"\n runtime: {runtime.total_seconds()}")

        if runtime > runtime_limit:
            raise PerformanceException(runtime=runtime, limit=runtime_limit)

        return result

    return run_function_and_validate_runtime()
