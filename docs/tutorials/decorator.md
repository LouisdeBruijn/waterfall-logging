
This part of the project documentation focuses on a *learning-oriented* approach.
Use the tutorial below to learn how the `waterfall` function decorator can be useful.

```python
import pandas as pd
from waterfall_logging.log import PandasWaterfall
from waterfall_logging.decorator import waterfall


waterfall_log = PandasWaterfall(table_name="decorator_markdown", columns=]"size"])


@waterfall(log=waterfall_log, reason="filter brands", configuration_flag=f'{["Gazelle", "Batavia"]}')
def filter_brands(bicycle_rides: pd.DataFrame) -> pd.DataFrame:
    """"""
    return bicycle_rides[bicycle_rides["brand"].isin(["Gazelle", "Batavia"])]


@waterfall(log=waterfall_log, reason="filter small wheels", configuration_flag=">= 31")
def filter_small_wheels(bicycle_rides: pd.DataFrame) -> pd.DataFrame:
    """"""
    return bicycle_rides[bicycle_rides["wheel_size"] >= 31]


def main():
    """"""
    bicycle_rides = pd.DataFrame(data=[
        ['Shimano', 'race', 28, '2023-02-13', 1],
        ['Gazelle', 'comfort', 31, '2023-02-15', 1],
        ['Shimano', 'race', 31, '2023-02-16', 2],
        ['Batavia', 'comfort', 30, '2023-02-17', 3],
    ], columns=['brand', 'ride_type', 'wheel_size', 'date', 'bike_id'])

    bicycle_rides = filter_brands(bicycle_rides)

    bicycle_rides = filter_small_wheels(bicycle_rides)

    print(waterfall_log.to_markdown())
    '''
    | Table              |   size |   Δ size |   Rows |   Δ Rows | Reason              | Configurations flag    |
    |:-------------------|-------:|---------:|-------:|---------:|:--------------------|:-----------------------|
    | decorator_markdown |      2 |        0 |      2 |        0 | filter brands       | ['Gazelle', 'Batavia'] |
    | decorator_markdown |      1 |       -1 |      1 |       -1 | filter small wheels | >= 31                  |
    '''


if __name__ == "__main__":
    main()
```
