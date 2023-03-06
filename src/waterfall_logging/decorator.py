import functools
from typing import Callable, Optional

from waterfall_logging.log import Waterfall


def waterfall(
    log: Waterfall,
    reason: Optional[str] = None,
    configuration_flag: Optional[str] = None,
    table_name: Optional[str] = None,
) -> Callable:
    """Waterfall function decorator.

    Args:
        log (Waterfall): Waterfall object
        reason (str): Specifies reasoning for DataFrame filtering step
        configuration_flag (str): Specifies configurations flag used for DataFrame filtering step
        table_name (str): First column in table is the `table id` column that should contain the table name value

    Examples:
        >>> import pandas as pd
        >>> import numpy as np
        >>> from waterfall_logging.log import PandasWaterfall
        >>> from waterfall_logging.decorator import waterfall
        >>> waterfall_log = PandasWaterfall(table_name='decorator_example', columns=['col1'])
        >>> @waterfall(log=waterfall_log, reason='dropping NaN')
        ... def drop_na(table: pd.DataFrame):
        ...     return table.dropna(axis=0)
        ...
        >>> df = drop_na(table=pd.DataFrame({'col1': [0, np.nan, 1, 2, 3], 'col2': [3, 4, np.nan, 5, 6]}))
        >>> print(waterfall_log.to_markdown())
        | Table             |   col1 |   Δ col1 |   Rows |   Δ Rows | Reason       | Configurations flag   |
        |:------------------|-------:|---------:|-------:|---------:|:-------------|:----------------------|
        | decorator_example |      3 |        0 |      3 |        0 | dropping NaN |                       |

    Returns:
        decorator (Callable): decorator object

    """

    def decorator_waterfall(method: Callable):
        """"""

        @functools.wraps(method)
        def wrapper_waterfall(*args, **kwargs):
            """"""
            table = method(*args, **kwargs)
            log.log(table, reason, configuration_flag, table_name)
            return table

        return wrapper_waterfall

    return decorator_waterfall
