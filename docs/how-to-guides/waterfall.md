
!!! note
    A how-to guide serves the needs of the user who is at work. Its obligation is to help the user accomplish a task.

### Count

Count columns via the `columns` parameter in the `__init__`
of the `PandasWaterfall` or `SparkWaterfall` class. An example is provided below.

```python
from waterfall_logging.log import PandasWaterfall, SparkWaterfall
pandas_w = PandasWaterfall(columns=['user_id'])

spark_w = SparkWaterfall(columns=['user_id'])
```

::: waterfall_logging.log.Waterfall.__init__
    options:
      show_root_toc_entry: False
      show_root_heading: False
      show_root_full_path: False
      show_signature_annotations: True


### Count distinct

Count distinct columns via the `distinct_columns` parameter in the `__init__`
of the `PandasWaterfall` or `SparkWaterfall` class. An example is provided below.

```python
from waterfall_logging.log import PandasWaterfall, SparkWaterfall
pandas_w = PandasWaterfall(distinct_columns=['user_id'])

spark_w = SparkWaterfall(distinct_columns=['user_id'])
```

!!! note
    Column names in `distinct_columns` overwrite names in `columns` with distinct counts!

::: waterfall_logging.log.Waterfall.__init__
    options:
      show_root_toc_entry: False
      show_root_heading: False
      show_root_full_path: False
      show_signature_annotations: True

### Drop NaN

Drop NaN values in the (distinct) counts of your Pandas or Spark DataFrame.

```python
from waterfall_logging.log import PandasWaterfall, SparkWaterfall
pandas_w = PandasWaterfall(columns=['A'], dropna=True)

spark_w = SparkWaterfall(columns=['A'], dropna=True)
```

::: waterfall_logging.log.Waterfall.__init__
    options:
      show_root_toc_entry: False
      show_root_heading: False
      show_root_full_path: False
      show_signature_annotations: True

### Log Waterfall step

Logs a Pandas or Spark DataFrame.

```python
import pandas as pd
import pyspark

from waterfall_logging.log import PandasWaterfall, SparkWaterfall

df = pd.DataFrame(data={'A': [0,1,2,3,4,5], 'B': ['id1', 'id2', 'id3', 'id4', 'id4']})

pandas_w = PandasWaterfall(columns=['A'], distinct_columns=['B'])
pandas_w.log(table=df, reason='example', configuration_flag='initial')

spark = pyspark.sql.SparkSession.builder.enableHiveSupport().appName(str(__file__)).getOrCreate()
sdf = spark.createDataFrame(df)

spark_w = SparkWaterfall(columns=['A'], distinct_columns=['B'])
spark_w.log(table=sdf, reason='example', configuration_flag='initial')
```

::: waterfall_logging.log.Waterfall.log
    options:
      show_root_toc_entry: False
      show_root_heading: False
      show_root_full_path: False
      show_signature_annotations: True



### Write to Markdown file

Write Waterfall logging tables to Markdown files.

Under the hood uses [pandas.to_markdown](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_markdown.html).
All arguments that can be provided for `pandas.to_markdown()` function, can also be used in this function.

::: waterfall_logging.log.Waterfall.to_markdown
    options:
      show_root_toc_entry: False
      show_root_heading: False
      show_root_full_path: False
      show_signature_annotations: True


### Read from Markdown file

Read Waterfall logging tables from Markdown files.

Under the hood uses [pandas.read_table](https://pandas.pydata.org/docs/reference/api/pandas.read_table.html).
All arguments that can be provided for `pandas.read_table()`, can also be used in this function.

Some useful arguments:

- The seperator in our markdown file is a pipe: `sep=|`.
- The header with column names start at row index: `header=0`.
- The markdown file does not contain an index column: `index_col=False`.
- The row that separates the headers from the values is on the first row: `skiprows=[1]`.
- Markdowns column names are preceded with initial spaces, to be removed when reading: `skipinitialspace=True`.


::: waterfall_logging.log.Waterfall.read_markdown
    options:
      show_root_toc_entry: False
      show_root_heading: False
      show_root_full_path: False
      show_signature_annotations: True


### Plot Waterfall

Plots Waterfall logging tables in a chart.

Under the hood plots uses a Plotly Waterfall chart [plotly.graph_objects.Waterfall](https://plotly.com/python/waterfall-charts/).

!!! note
    Make sure to use an unique `reason` arguments for each logging step, otherwise your plot will look strange!

::: waterfall_logging.log.Waterfall.plot
    options:
      show_root_toc_entry: False
      show_root_heading: False
      show_root_full_path: False
      show_signature_annotations: True

See below for a more in-depth example, including `fig.update_layout` and `fig.update_traces`.

```python
from waterfall_logging.log import PandasWaterfall

waterfall_log = PandasWaterfall()
waterfall_log.read_markdown(filepath_or_buffer='output/tests/read_markdown_table.md',
    sep='|', header=0, index_col=False, skiprows=[1], skipinitialspace=True
)

fig = waterfall_log.plot(y_col='Rows',
    textfont=dict(family='sans-serif', size=11),
    connector={'line': {'color': 'rgba(0,0,0,0)'}},
    totals={'marker': {'color': '#dee2e6', 'line': {'color': '#dee2e6', 'width': 1}}}
)

fig.update_layout(
    autosize=True,
    width=1000,
    height=1000,
    title=f'Data filtering steps',
    xaxis=dict(title='Filtering steps'),
    yaxis=dict(title='# of entries'),
    showlegend=False,
    waterfallgroupgap=0.1,
)

fig.update_traces(
    textposition='outside',
)

fig.write_image('output/tests/read_markdown_table.png')
```
![image](/images/read_markdown_table.png)
