import functools
from typing import Callable, Optional

from waterfall_logging.log import Waterfall


def waterfall(
    log: Waterfall,
    reason: Optional[str] = None,
    configuration_flag: Optional[str] = None,
    table_name: Optional[str] = None,
):
    """"""

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
