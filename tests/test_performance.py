import pyspark
import pyspark.sql.functions as F
import pytest

from tests.conftest import track_aggregation_time


@pytest.mark.performance
@pytest.mark.filterwarnings("ignore::FutureWarning")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("dataframe", ["large"], indirect=True)
def test__spark_count_distinct(time_tracker, spark_session: pyspark.sql.SparkSession, dataframe) -> None:
    """"""
    sdf = spark_session.createDataFrame(dataframe)

    expression = [F.count_distinct(F.col(col)).alias(col) for col in dataframe.columns]
    track_aggregation_time(sdf, expression)

    expression = [F.expr(f"count(distinct {col})").alias(col) for col in dataframe.columns]
    track_aggregation_time(sdf, expression)


@pytest.mark.performance
@pytest.mark.filterwarnings("ignore::FutureWarning")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("dataframe", ["large"], indirect=True)
def test__spark_count_distinct_drop_na(time_tracker, spark_session: pyspark.sql.SparkSession, dataframe) -> None:
    """"""
    sdf = spark_session.createDataFrame(dataframe)
    expression = [F.count(F.when(~F.isnan(F.col(col)), F.col(col))).alias(col) for col in dataframe.columns]
    track_aggregation_time(sdf, expression)

    expression = [
        F.sum((~F.isnan(F.col(col)) & ~F.isnull(col)).cast("integer")).alias(col) for col in dataframe.columns
    ]
    track_aggregation_time(sdf, expression)
