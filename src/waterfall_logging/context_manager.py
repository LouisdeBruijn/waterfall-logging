import functools
import sys
import types
from typing import Any, Callable, Dict, List, Optional

from waterfall_logging.log import Waterfall


def waterfall(log: Waterfall, variable_names: List[str], markdown_kwargs: Optional[Dict] = None):
    """Waterfall context manager decorator.

    Args:
        log (Waterfall): Waterfall object
        variable_names (List): array of variables names to log waterfall
        markdown_kwargs (dict): keyword arguments to save the markdown with to_markdown(**markdown_kwargs)

    Examples:
        >>> import pandas as pd
        >>> import numpy as np
        >>> from waterfall_logging.log import PandasWaterfall
        >>> from waterfall_logging.context_manager import waterfall
        >>> waterfall_log = PandasWaterfall(table_name='context_manager_example', columns=['col1'])
        >>> @waterfall(log=waterfall_log, variable_names=['table'], markdown_kwargs={'buf': 'ctx_manager_example.md'})
        ... def main():
        ...     table = pd.DataFrame({'col1': [0, np.nan, 1, 2, 3], 'col2': [3, 4, np.nan, 5, 6]})
        ...     table.dropna(axis=0)
        ...
        >>> main()
        >>> print(waterfall_log.to_markdown())
        | Table                   |   col1 |   Δ col1 |   Rows |   Δ Rows | Reason   | Configurations flag   |
        |:------------------------|-------:|---------:|-------:|---------:|:---------|:----------------------|
        | context_manager_example |      5 |        0 |      5 |        0 |          |                       |
        >>> print("The `waterfall_log` is also saved in the file `ctx_manager_example.md`!")


    Returns:
        decorator (Callable): decorator object

    """
    markdown_kwargs = {} if not markdown_kwargs else markdown_kwargs

    def decorator_waterfall(method: Callable):
        """"""

        @functools.wraps(method)
        def wrapper_waterfall(*args, **kwargs):
            """"""
            with WaterfallContext(log, method.__name__, variable_names, markdown_kwargs):
                return_value = method(*args, **kwargs)
            return return_value

        return wrapper_waterfall

    return decorator_waterfall


class WaterfallContext:
    """Waterfall context to trace any function calls inside the context."""

    def __init__(self, log: Waterfall, name: str, variable_names: List[str], markdown_kwargs: Optional[Dict] = None):
        """

        Args:
            log (Waterfall): waterfall logging object
            name (str): name of the function that is to de debugged
            variable_names (List): array of variables names to log waterfall
            markdown_kwargs (dict): keyword arguments to save the markdown with to_markdown(**markdown_kwargs)

        """
        self.name = name
        self.variable_names = variable_names
        self.log = log
        self.markdown_kwargs = {} if not markdown_kwargs else markdown_kwargs

        self._df = None

    def __enter__(self):
        """Enter the trace."""
        sys.settrace(self.trace_calls)

    def __exit__(self, *args, **kwargs):
        """Exit the trace and export Waterfall object to Markdown."""
        self.log.to_markdown(**self.markdown_kwargs)
        sys.settrace = None

    def trace_calls(self, frame: types.FrameType, event, arg: Any):
        """

        Args:
            frame (types.FrameType): a tracing frame
            event (str): a tracing event
            arg (Any): a tracing argument

        Returns:
            traced_lines (types.MethodType): traced lines

        """
        if event != "call":
            # only trace our call to the decorated function
            return
        elif frame.f_code.co_name != self.name:
            # return the trace function to use when you go into that function call
            return

        traced_lines = self.trace_lines
        return traced_lines

    def trace_lines(self, frame: types.FrameType, event: str, arg: Any) -> None:
        """

        Args:
            frame (types.FrameType): a tracing frame
            event (str): a tracing event
            arg (Any): a tracing argument

        Returns:
            None

        """
        if event not in ["line", "return"]:
            return

        local_vars = frame.f_locals

        for var_name, var_value in local_vars.items():
            if var_name in self.variable_names:
                df = local_vars[var_name]
                if df is not self._df:
                    self.log.log(df)
                    self._df = df
