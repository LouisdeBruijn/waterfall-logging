import functools
import sys
import types
from typing import Any, Callable, Dict, List, Optional

from waterfall_logging.log import Waterfall


def waterfall(log: Waterfall, variable_names: List[str], markdown_kwargs: Optional[Dict] = None):
    """"""
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
            markdown_kwargs (dict): Location to save the markdown file

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
