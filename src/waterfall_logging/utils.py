import collections.abc
from typing import Any, List

from pandas.core.base import PandasObject


def iterable_to_list(iterable: Any) -> List[str]:
    """Takes an iterable and returns that iterable as a List."""
    if isinstance(iterable, PandasObject):
        # pd.Series or pd.DataFrame
        return iterable.to_list() if not iterable.empty else []
    elif isinstance(iterable, collections.abc.Sequence) and not isinstance(iterable, str):
        # List, Tuple
        return list(iterable)
    elif isinstance(iterable, str):
        # string
        return [iterable]
    elif not iterable:
        return []
    else:
        return list(iterable)
