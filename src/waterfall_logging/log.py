from __future__ import annotations

import abc
import warnings
from typing import List

import pandas as pd
import plotly.graph_objects as go
import pyspark
import pyspark.sql.functions as F
from pandas._typing import FilePath, ReadCsvBuffer

from waterfall_logging.utils import iterable_to_list


class Waterfall(abc.ABC):
    """Logs a table on each filtering step in waterfall fashion."""

    def __init__(
        self,
        table_name: str | None = None,
        columns: List[str] | None = None,
        distinct_columns: List[str] | None = None,
        dropna: bool = False,
        delta_prefix: str = "Δ ",
        row_count_column: str = "Rows",
    ):
        """

        Args:
            table_name (str): Specifies the name of the table to log
            columns (Lists[str]): Specifies which columns to log
            distinct_columns (List[str]): Specifies which distinct column values to log
            delta_prefix (str): Prefix for column names with discrete difference (delta) with previous row
            dropna (bool): Whether to exclude NaN in the row counts
            row_count_column (str): Column name for an added column that counts rows in table

        """
        self.table_name = table_name
        self.columns = iterable_to_list(columns)
        self.distinct_columns = iterable_to_list(distinct_columns)
        self.delta_prefix = delta_prefix
        self.dropna = dropna
        self.row_count_column = row_count_column

        self._input_columns = self.columns + self.distinct_columns
        self._all_columns = self._input_columns.copy()
        if self.row_count_column:
            self._all_columns.append(self.row_count_column)
        self._static_columns = ["Table", "Reason", "Configurations flag"]
        self._log = None

        distinct_overwrite = set(self.columns).intersection(set(self.distinct_columns))
        if distinct_overwrite:
            warnings.warn(
                f"Column names in `distinct_columns` overwrite names in `columns` with distinct counts: "
                f"{distinct_overwrite}.",
            )

    def _create_log(self) -> pd.DataFrame:
        """"""
        columns_with_deltas = []
        for column in self._all_columns:
            columns_with_deltas += [column, f"{self.delta_prefix}{column}"]

        column_names = [self._static_columns[0]] + columns_with_deltas + self._static_columns[1:]
        return pd.DataFrame(columns=column_names)

    def _count_entries(self, table) -> List[int]:
        """"""
        if self.dropna:
            return self._count_or_distinct_dropna(table)
        else:
            return self._count_or_distinct(table)

    @abc.abstractmethod
    def _count_or_distinct(self, table) -> List[int]:
        """Column count for `self.columns` and distinct column count for `self.distinct_columns` including NaNs."""

    @abc.abstractmethod
    def _count_or_distinct_dropna(self, table) -> List[int]:
        """Column count for `self.columns` and distinct column count for `self.distinct_columns` excluding NaNs."""

    def log(
        self,
        table: pd.DataFrame | pyspark.sql.DataFrame,
        reason: str | None = None,
        configuration_flag: str | None = None,
        table_name: str | None = None,
    ) -> None:
        """Logs table (distinct) counts to logging DataFrame.

        Args:
            table (pd.DataFrame): DataFrame that the filtering is applied to
            reason (str): Specifies reasoning for DataFrame filtering step
            configuration_flag (str): Specifies configurations flag used for DataFrame filtering step
            table_name (str): First column in table is the `table id` column that should contain the table name value

        Examples:
            >>> waterfall = PandasWaterfall()
            >>> waterfall.log(table, reason='Filtered in-scope bicycles', configuration_flag='inscope=True',
            ... table_name='sample_table')

        Returns:
            None

        """
        table_name = table_name or self.table_name

        if self._log is None:
            self._log = self._create_log()

        current_table = self._log.loc[self._log.iloc[:, 0] == table_name]
        entries = self._count_entries(table)

        prior_entries = (
            entries.copy() if current_table.empty else current_table[self._all_columns].iloc[-1, :].to_list()
        )

        column_entries = []
        for entry, prior_entry in zip(entries, prior_entries):
            column_entries += [entry, entry - prior_entry]

        calculated_columns = [table_name] + column_entries + [reason, configuration_flag]

        self._log.loc[len(self._log)] = calculated_columns

    def read_markdown(
        self,
        filepath_or_buffer: FilePath | ReadCsvBuffer[bytes] | ReadCsvBuffer[str],
        columns: List[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        """Reads table from Markdown file.

        Args:
            filepath_or_buffer (str, path object or file-like object):
                Any valid string path is acceptable. The string could be a URL. Valid
                URL schemes include http, ftp, s3, gs, and file. For file URLs, a host is
                expected. A local file could be: file://localhost/path/to/table.csv.
                If you want to pass in a path object, pandas accepts any ``os.PathLike``.
                By file-like object, we refer to objects with a ``read()`` method, such as
                a file handle (e.g. via builtin ``open`` function) or ``StringIO``.
            columns (Lists[str]): Specifies which column to read in and log

        Examples:
            >>> f = open('output/tests/read_markdown_table.md', 'r')
            >>> print(f.read())
            | Table  |   col1 |   Δ col1 |   Rows |   Δ Rows | Reason  | Configurations flag   |
            |:-------|-------:|---------:|-------:|---------:|:--------|:----------------------|
            | table1 |     50 |        0 |   2727 |        0 | initial | read_markdown         |
            | table1 |    150 |      100 |   2827 |        0 | add-on  | read_markdown         |
            | table1 |    250 |      100 |   2927 |      100 | extra   | read_markdown         |
            >>> waterfall = PandasWaterfall()
            >>> waterfall.read_markdown(
            ...     filepath_or_buffer='output/tests/read_markdown_table.md',
            ....    sep='|', header=0, index_col=False, skiprows=[1], skipinitialspace=True
            ... )
            >>> print(waterfall._log)
                Table  col1  Δ col1  Rows  Δ Rows   Reason Configurations flag
            0  table1    50       0  2727       0  example       read_markdown
            1  table1   150     100  2827       0  add-on        read_markdown
            1  table1   250     100  2927       0  extra         read_markdown
            >>> print(type(waterfall._log))
            <class 'pandas.core.frame.DataFrame'>

        Returns:
            None

        """
        self._log = (
            pd.read_table(filepath_or_buffer, *args, **kwargs)
            # strips trailing whitespaces in column names
            # drops columns with all NA values
            .rename(str.rstrip, axis="columns").dropna(axis=1, how="all")
            # strips trailing whitespaces in column values
            .apply(lambda row: row.str.rstrip() if row.dtype == object else row)
        )
        self._log = self._log[columns] if columns else self._log

    def to_markdown(self, *args, index=False, **kwargs):
        """Print DataFrame in Markdown-friendly format.

        Args:
            index (bool): Add index (row) labels

        Examples:
            >>> waterfall = PandasWaterfall()
            >>> print(waterfall.to_markdown(index=True))
            |    | Table  |   col1 |   Δ col1 |   Rows |   Δ Rows | Reason  | Configurations flag |
            |---:|:-------|-------:|---------:|-------:|---------:|:--------|:--------------------|
            |  0 | table1 |     50 |        0 |   2727 |        0 | example | to_markdown         |
            |  1 | table1 |    150 |      100 |   2827 |        0 | example | to_markdown         |
            >>> print(waterfall.to_markdown(index=False))
            | Table  |   col1 |   Δ col1 |   Rows |   Δ Rows | Reason  | Configurations flag |
            |:-------|-------:|---------:|-------:|---------:|:--------|:--------------------|
            | table1 |     50 |        0 |   2727 |        0 | example | to_markdown         |
            | table1 |    150 |      100 |   2827 |        0 | example | to_markdown         |

        Returns:
            (str): DataFrame in Markdown-friendly format

        """
        return self._log.to_markdown(*args, index=index, **kwargs)

    def plot(
        self,
        *args,
        y_col: str = "Rows",
        y_col_delta: str = "Δ Rows",
        x_col: str = "Reason",
        drop_zero_delta: bool = False,
        **kwargs,
    ) -> go.Figure:
        """Plots a logging DataFrame column in a waterfall chart.

        Args:
            y_col (str): Specifies column that contains absolute value counts for y-axis of plot
            y_col_delta (str): Specifies column that contains delta value counts for y-axis of plot
            x_col (str): Specifies column that contains the filtering explanation for x-axis of plot
            drop_zero_delta (bool): Whether to remove rows for `y_col_delta` that contain zeros

        Examples:
            >>> waterfall = PandasWaterfall()
            >>> fig = waterfall.plot(y_col='Rows', y_col_delta='Δ Rows', x_col='Reason',
            ... textfont=dict(family='sans-serif', size=11),
            ... connector={'line': {'color': 'rgba(0,0,0,0)'}},
            ... totals={'marker': {'color': '#dee2e6', 'line': {'color': '#dee2e6', 'width': 1}}}
            ... )

        Returns:
            go.Figure: waterfall chart

        """
        df = self._log.copy()
        if drop_zero_delta:
            df = df.iloc[1:]
            indices = df[(df[y_col_delta] == 0)].index
            df = df.drop(indices, inplace=False)

        measure = ["absolute"] + ["relative"] * (df.shape[0] - 1) + ["total"]
        x = df[x_col].to_list() + ["Total"]
        y = [df.loc[df.index[0], y_col]] + [x for x in df[y_col_delta][1:]] + [df.loc[df.index[-1], y_col]]

        return go.Figure(
            go.Waterfall(
                *args,
                measure=measure,
                x=x,
                y=y,
                text=y,
                **kwargs,
            ),
        )


class PandasWaterfall(Waterfall):
    """Logs a Pandas DataFrame on each filtering step in waterfall fashion."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _count_or_distinct(self, table: pd.DataFrame) -> List[int]:
        """Column count for `self.columns` and distinct column count for `self.distinct_columns` including NaNs."""
        table_length = len(table)

        input_columns = [
            table[c].nunique(dropna=False) if c in self.distinct_columns else table_length for c in self._input_columns
        ]

        if self.row_count_column:
            input_columns.append(table_length)

        return input_columns

    def _count_or_distinct_dropna(self, table: pd.DataFrame) -> List[int]:
        """Column count for `self.columns` and distinct column count for `self.distinct_columns` excluding NaNs."""
        input_columns = [
            table[c].nunique(dropna=True) if c in self.distinct_columns else table[c].count()
            for c in self._input_columns
        ]

        if self.row_count_column:
            input_columns.append(len(table))

        return input_columns


class SparkWaterfall(Waterfall):
    """Logs a PySpark DataFrame on each filtering step in waterfall fashion."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _count_or_distinct(self, table: pyspark.sql.DataFrame) -> List[int]:
        """Column count for `self.columns` and distinct column count for `self.distinct_columns` including NaNs."""
        expressions = [F.count_distinct(F.col(col)).alias(col) for col in self.distinct_columns]
        # `F.count_distinct` excludes None values in columns of StringType()
        expressions.append(F.count(F.lit(1)).alias("row_count"))

        df = table.agg(*expressions).toPandas()
        row_count = df.loc[0, "row_count"]

        entries = [row_count] * len(self.columns) + [df.loc[0, col] for col in self.distinct_columns]

        if self.row_count_column:
            entries.append(row_count)

        return entries

    def _count_or_distinct_dropna(self, table: pyspark.sql.DataFrame) -> List[int]:
        """Column count for `self.columns` and distinct column count for `self.distinct_columns` excluding NaNs."""
        count_expressions = [
            F.count(
                F.when(
                    ~F.isnan(
                        F.col(col),
                    ),
                    F.col(col),
                ),
            ).alias(col)
            for col in self.columns
        ]

        distinct_expressions = [
            F.count_distinct(
                F.when(
                    ~F.isnan(
                        F.col(col),
                    ),
                    F.col(col),
                ),
            ).alias(col)
            for col in self.distinct_columns
        ]

        expressions = count_expressions + distinct_expressions

        if self.row_count_column:
            expressions.append(F.count(F.lit(1)).alias("row_count"))
            df = table.agg(*expressions).toPandas()

            return [df.loc[0, col] for col in self._input_columns] + [df.loc[0, "row_count"]]
        else:
            df = table.agg(*expressions).toPandas()
            return [df.loc[0, col] for col in self._input_columns]
