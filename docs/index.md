
# Home

Waterfall-logging is a Python package to log (distinct) column counts in a DataFrame, export it as a Markdown table and plot a Waterfall statistics figure.

It provides an implementation in Pandas `PandasWaterfall` and PySpark `SparkWaterfall`.

## Project overview

The documentation consists of four separate parts

1. [Tutorials](tutorials/pandas.md) are learning-oriented
2. [How-To Guides](how-to-guides/waterfall.md) are task-oriented
3. [Reference](reference/log.md) is the technical documentation
4. [Explanation](explanation.md) is understanding-oriented

Quickly find what you're looking for depending on
your use case by looking at the different pages.

## How to install

Install Waterfall-logging from PyPi

```commandline
pip install waterfall-logging
```

## Usage

```python
import pandas as pd
from waterfall_logging.log import PandasWaterfall

bicycle_rides = pd.DataFrame(data=[
    ['Shimano', 'race', 28, '2023-02-13', 1],
    ['Gazelle', 'comfort', 31, '2023-02-15', 1],
    ['Shimano', 'race', 31, '2023-02-16', 2],
    ['Batavia', 'comfort', 30, '2023-02-17', 3],
], columns=['brand', 'ride_type', 'wheel_size', 'date', 'bike_id'])

bicycle_rides_log = PandasWaterfall(table_name='rides', columns=['brand', 'ride_type', 'wheel_size'],
    distinct_columns=['bike_id'])
bicycle_rides_log.log(table=bicycle_rides, reason='Logging initial column values', configuration_flag='')

bicycle_rides = bicycle_rides.loc[lambda row: row['wheel_size'] > 30]
bicycle_rides_log.log(table=bicycle_rides, reason='Remove small wheels',
    configuration_flag='small_wheel=False')

print(bicycle_rides_log.to_markdown())

| Table   |   brand |   Δ brand |   ride_type |   Δ ride_type |   wheel_size |   Δ wheel_size |   bike_id |   Δ bike_id |   Rows |   Δ Rows | Reason                        | Configurations flag   |
|:--------|--------:|----------:|------------:|--------------:|-------------:|---------------:|----------:|------------:|-------:|---------:|:------------------------------|:----------------------|
| rides   |       4 |         0 |           4 |             0 |            4 |              0 |         3 |           0 |      4 |        0 | Logging initial column values |                       |
| rides   |       2 |        -2 |           2 |            -2 |            2 |             -2 |         2 |          -1 |      2 |       -2 | Remove small wheels           | small_wheel=False     |
```
