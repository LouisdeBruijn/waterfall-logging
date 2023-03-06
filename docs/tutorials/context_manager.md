
This part of the project documentation focuses on a *learning-oriented* approach.
Use the tutorial below to learn how the `waterfall` context manager as a function decorator can be useful.

```python
import pandas as pd
from waterfall_logging.log import PandasWaterfall
from waterfall_logging.context_manager import waterfall


waterfall_log = PandasWaterfall(table_name="context_manager_markdown", columns=["size"])


def filter_brands(bicycle_rides: pd.DataFrame) -> pd.DataFrame:
    """"""
    return bicycle_rides[bicycle_rides["brand"].isin(["Gazelle", "Batavia"])]


def filter_small_wheels(bicycle_rides: pd.DataFrame) -> pd.DataFrame:
    """"""
    return bicycle_rides[bicycle_rides["wheel_size"] >= 31]


@waterfall(log=waterfall_log, variable_names=["bicycle_rides"], markdown_kwargs={'buf': 'path_to_save_markdown.md'})
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
    """
    | Table                    |   size |   Δ size |   Rows |   Δ Rows | Reason   | Configurations flag   |
    |:-------------------------|-------:|---------:|-------:|---------:|:---------|:----------------------|
    | context_manager_markdown |      4 |        0 |      4 |        0 |          |                       |
    | context_manager_markdown |      2 |       -2 |      2 |       -2 |          |                       |
    | context_manager_markdown |      1 |       -1 |      1 |       -1 |          |                       |
    """

    print("The `waterfall_log` is also saved in the file `path_to_save_markdown.md`!")


if __name__ == "__main__":
    main()
```
