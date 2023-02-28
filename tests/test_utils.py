from typing import Any

import pandas as pd
import pytest

from waterfall_logging.log import PandasWaterfall


@pytest.mark.parametrize(
    "columns",
    [
        "A",
        ("A", "B", "C", "D"),
        ["A", "B", "C", "D"],
        {"A", "B", "C", "D"},
        {"A": "A", "B": "B", "C": "C", "D": "D"}.keys(),
        {"A": "A", "B": "B", "C": "C", "D": "D"}.values(),
        pd.Series(["A", "B", "C", "D"]),
        pd.DataFrame(data=[[1, 1, 1, 1]], columns=["A", "B", "C", "D"]).columns,
    ],
    ids=["string", "tuple", "list", "set", "dict_keys", "dict_values", "pd.Series", "pd.DataFrame.columns"],
)
def test__iterable_to_list(columns: Any) -> None:
    """"""
    w = PandasWaterfall(columns=columns)
    if isinstance(columns, str):
        assert [columns] == w.columns
    else:
        assert {"A", "B", "C", "D"}.issubset(set(w.columns))
