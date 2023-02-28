import pandas as pd
import plotly.graph_objects as go
import pyspark
import pytest
from pandas._testing import assert_frame_equal, assert_series_equal

from tests.conftest import assert_waterfall_log
from waterfall_logging.log import PandasWaterfall, SparkWaterfall

# pytestmark =


@pytest.mark.filterwarnings("ignore::FutureWarning")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__spark_count_distinct(spark_session: pyspark.sql.SparkSession, dataframe: pd.DataFrame) -> None:
    """"""
    w = SparkWaterfall(columns=dataframe.columns[0], distinct_columns=dataframe.columns[1:], dropna=False)

    sdf = spark_session.createDataFrame(dataframe)
    w.log(sdf)

    values = [len(dataframe)] * len(w.columns)
    for col in w.distinct_columns:
        if str(dataframe[col].dtype) == "object":
            # Pyspark `F.count_distinct()` does not include None in a column of `StringType()` (`object` in Pandas)
            # but does include NaNs in numeric columns
            # for performance reasons, keeping this behaviour in the SparkWaterfall implementation
            values.append(dataframe[col].nunique(dropna=True))
        else:
            values.append(dataframe[col].nunique(dropna=False))
    values.append(len(dataframe))

    columns = dataframe.columns.to_list() + ["Rows"]
    dataframe_count = pd.Series(values, columns)

    last_waterfall_addition = w._log[columns].iloc[-1]

    assert_series_equal(last_waterfall_addition, dataframe_count, check_dtype=False, check_names=False)


@pytest.mark.filterwarnings("ignore::FutureWarning")
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__spark_count_distinct_dropna(spark_session: pyspark.sql.SparkSession, dataframe: pd.DataFrame) -> None:
    """"""
    w = SparkWaterfall(columns=dataframe.columns[0], distinct_columns=dataframe.columns[1:], dropna=True)

    sdf = spark_session.createDataFrame(dataframe)
    w.log(sdf)

    values = (
        dataframe[w.columns].count().to_list()
        + dataframe[w.distinct_columns].nunique(dropna=True).to_list()
        + [len(dataframe)]
    )
    columns = dataframe.columns.to_list() + ["Rows"]
    dataframe_count = pd.Series(values, columns)

    last_waterfall_addition = w._log[columns].iloc[-1]

    assert_series_equal(last_waterfall_addition, dataframe_count, check_dtype=False, check_names=False)


@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__pandas_count_distinct(spark_session: pyspark.sql.SparkSession, dataframe: pd.DataFrame) -> None:
    """"""
    w = PandasWaterfall(columns=dataframe.columns[:-1], distinct_columns=dataframe.columns[-1])
    w.log(dataframe)

    values = (
        [len(dataframe)] * len(w.columns)
        + dataframe[w.distinct_columns].nunique(dropna=False).to_list()
        + [len(dataframe)]
    )
    columns = dataframe.columns.to_list() + ["Rows"]
    dataframe_count = pd.Series(values, columns)

    last_waterfall_addition = w._log[columns].iloc[-1]

    assert_series_equal(last_waterfall_addition, dataframe_count, check_dtype=False, check_names=False)


@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__pandas_count_distinct_dropna(spark_session: pyspark.sql.SparkSession, dataframe: pd.DataFrame) -> None:
    """"""
    w = PandasWaterfall(columns=dataframe.columns[0], distinct_columns=dataframe.columns[1:], dropna=True)
    w.log(dataframe)

    values = (
        dataframe[w.columns].count().to_list()
        + dataframe[w.distinct_columns].nunique(dropna=True).to_list()
        + [len(dataframe)]
    )
    columns = dataframe.columns.to_list() + ["Rows"]

    dataframe_count = pd.Series(values, columns)
    last_waterfall_addition = w._log[columns].iloc[-1]

    assert_series_equal(last_waterfall_addition, dataframe_count, check_dtype=False, check_names=False)


@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__dropna(dataframe: pd.DataFrame) -> None:
    """"""
    w = PandasWaterfall(columns=dataframe.columns, dropna=False)
    w.log(table=dataframe)

    dataframe_count = pd.Series([len(dataframe)] * len(dataframe.columns), index=dataframe.columns.to_list())
    last_waterfall_addition = w._log[dataframe.columns].iloc[-1]

    w = PandasWaterfall(columns=dataframe.columns, dropna=True)
    w.log(table=dataframe)

    dataframe_count_dropna = dataframe.count()
    last_waterfall_addition_dropna = w._log[dataframe.columns].iloc[-1]

    assert_series_equal(dataframe_count, last_waterfall_addition, check_dtype=False, check_names=False)
    assert_series_equal(dataframe_count_dropna, last_waterfall_addition_dropna, check_dtype=False, check_names=False)


@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__row_count_column(dataframe: pd.DataFrame) -> None:
    """"""
    w = PandasWaterfall(columns=dataframe.columns, row_count_column="row_count")
    w.log(table=dataframe)

    assert w._log["row_count"].sum() == len(dataframe)

    w_no_row_count_column = PandasWaterfall(columns=dataframe.columns, row_count_column=None)
    w_no_row_count_column.log(table=dataframe)

    assert "Rows" not in w_no_row_count_column._log.columns
    assert len(w_no_row_count_column._log.columns) == len(dataframe.columns) * 2 + 3


@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__count_is_different_when_table_name_is_different(dataframe: pd.DataFrame) -> None:
    """"""
    first_table_name, second_table_name = "first_table_name", "second_table_name"
    w = PandasWaterfall(table_name=first_table_name, columns=dataframe.columns, dropna=False, row_count_column="Rows")
    second_dataframe = dataframe.copy()

    w.log(table=dataframe, reason="first table count")
    assert_waterfall_log(w, dataframe)
    w.log(table=dataframe.sample(frac=0.5), reason="halved first table")
    assert_waterfall_log(w, dataframe)
    w.log(table=second_dataframe, table_name=second_table_name, reason="loaded second table")
    assert_waterfall_log(w, dataframe)
    w.log(table=pd.concat([dataframe, second_dataframe]), table_name=first_table_name, reason="join tables")
    assert_waterfall_log(w, dataframe)
    w.log(table=pd.concat([dataframe, second_dataframe]), table_name=second_table_name, reason="join tables")
    assert_waterfall_log(w, dataframe)


@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__delta_prefix_changed(dataframe: pd.DataFrame) -> None:
    """"""
    delta_prefix = "delta "
    w = PandasWaterfall(columns=dataframe.columns, delta_prefix=delta_prefix)
    w.log(table=dataframe)

    assert len([col for col in w._log.columns if col.startswith(delta_prefix)]) == len(dataframe.columns) + 1


@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__create_log(dataframe: pd.DataFrame) -> None:
    """"""
    w = PandasWaterfall(columns=dataframe.columns)
    columns = w._create_log().columns.to_list()

    for col in dataframe.columns:
        assert col in columns
        assert f"{w.delta_prefix}{col}" in columns
    assert columns[0] == "Table"
    assert columns[-2:] == ["Reason", "Configurations flag"]


@pytest.mark.filterwarnings("ignore:test__distinct_columns_raises_warning")
@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__distinct_columns_raises_warning(dataframe: pd.DataFrame) -> None:
    """"""
    with pytest.warns(
        expected_warning=UserWarning,
        match="Column names in `distinct_columns` overwrite names in `columns` with distinct counts:",
    ):
        PandasWaterfall(columns=dataframe.columns, distinct_columns=dataframe.columns)


@pytest.mark.parametrize("dataframe", ["", "missing", "nan", "None"], indirect=True)
def test__log(dataframe: pd.DataFrame) -> None:
    """"""
    w = PandasWaterfall(columns=dataframe.columns)
    w.log(dataframe)

    assert len(w._log.columns) == len(dataframe.columns) * 2 + 5
    assert w._log.shape[0] == 1


@pytest.mark.parametrize("dataframe", ["markdown"], indirect=True)
def test__read_markdown(dataframe: pd.DataFrame) -> None:
    """"""
    w = PandasWaterfall()
    w.read_markdown(
        filepath_or_buffer="output/tests/read_markdown_table.md",
        sep="|",
        header=0,
        index_col=False,
        skiprows=[1],
        skipinitialspace=True,
    )

    assert_frame_equal(w._log, dataframe)


@pytest.mark.parametrize("dataframe", ["markdown"], indirect=True)
def test__to_markdown(dataframe: pd.DataFrame) -> None:
    """"""
    w = PandasWaterfall()
    w._log = dataframe

    assert w._log.to_markdown() == dataframe.to_markdown()


@pytest.mark.parametrize("dataframe", ["markdown"], indirect=True)
def test__plot(dataframe: pd.DataFrame) -> None:
    """"""
    w = PandasWaterfall()
    w._log = dataframe
    plot = w.plot()

    assert isinstance(plot, go.Figure)
    assert plot.data[0]["x"] == ("initial", "add-on", "extra", "Total")
    assert plot.data[0]["y"] == (2727, 100, 100, 2927)
