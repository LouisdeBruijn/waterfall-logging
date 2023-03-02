[![Version](https://img.shields.io/pypi/v/waterfall-logging)](https://pypi.org/project/waterfall-logging/)
[![](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![Downloads](https://pepy.tech/badge/waterfall-logging)](https://pepy.tech/project/waterfall-logging)
[!](https://img.shields.io/github/license/LouisdeBruijn/waterfall-logging)
[![Docs - GitHub.io](https://img.shields.io/static/v1?logo=readthdocs&style=flat&color=purple&label=docs&message=waterfall-statistics)][#docs-package]

[#docs-package]: https://LouisdeBruijn.github.io/waterfall-logging/

# Waterfall-logging

Waterfall-logging is a Python package to log (distinct) column counts in a DataFrame, export it as a Markdown table and plot a Waterfall statistics figure.

It provides an implementation in Pandas `PandasWaterfall` and PySpark `SparkWaterfall`.

Documentation with examples can be found [here](https://LouisdeBruijn.github.io/waterfall-logging).

Developed by Louis de Bruijn, https://louisdebruijn.com.


## Installation

### Install to use
Install Waterfall-logging using PyPi:

```commandline
pip install waterfall-logging
```

### Install to contribute

```commandline
git clone https://github.com/LouisdeBruijn/waterfall-logging
python -m pip install -e .

pre-commit install --hook-type pre-commit --hook-type pre-push
```

## Documentation

Documentation can be created via

```commandline
mkdocs serve
```

## Usage

Instructions are provided in the documentation's [how-to-guides](https://LouisdeBruijn.github.io/waterfall-logging//how-to-guides/).

```python
import pandas as pd
from waterfall_logging.log import PandasWaterfall

bicycle_rides = pd.DataFrame(data=[
    ['Shimano', 'race', 28, '2023-02-13', 1],
    ['Gazelle', 'comfort', 31, '2023-02-15', 1],
    ['Shimano', 'race', 31, '2023-02-16', 2],
    ['Batavia', 'comfort', 30, '2023-02-17', 3],
], columns=['brand', 'ride_type', 'wheel_size', 'date', 'bike_id']
)

bicycle_rides_log = PandasWaterfall(table_name='rides', columns=['brand', 'ride_type', 'wheel_size'],
    distinct_columns=['bike_id'])
bicycle_rides_log.log(table=bicycle_rides, reason='Logging initial column values', configuration_flag='')

bicycle_rides = bicycle_rides.loc[lambda row: row['wheel_size'] > 30]
bicycle_rides_log.log(table=bicycle_rides, reason="Remove small wheels",
    configuration_flag='small_wheel=False')

print(bicycle_rides_log.to_markdown())

| Table   |   brand |   Δ brand |   ride_type |   Δ ride_type |   wheel_size |   Δ wheel_size |   bike_id |   Δ bike_id |   Rows |   Δ Rows | Reason                        | Configurations flag   |
|:--------|--------:|----------:|------------:|--------------:|-------------:|---------------:|----------:|------------:|-------:|---------:|:------------------------------|:----------------------|
| rides   |       4 |         0 |           4 |             0 |            4 |              0 |         3 |           0 |      4 |        0 | Logging initial column values |                       |
| rides   |       2 |        -2 |           2 |            -2 |            2 |             -2 |         2 |          -1 |      2 |       -2 | Remove small wheels           | small_wheel=False     |
```
