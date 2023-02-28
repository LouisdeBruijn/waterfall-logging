import pandas as pd
import pytest

from waterfall_logging.context_manager import waterfall
from waterfall_logging.log import PandasWaterfall


@pytest.mark.parametrize("dataframe", [""], indirect=True)
def test__context_manager_decorator(dataframe) -> None:
    """"""
    w = PandasWaterfall(columns=dataframe.columns)

    @waterfall(log=w, variable_names=["variable_to_log"])
    def dummy(df: pd.DataFrame) -> None:
        """"""
        variable_to_log = dataframe
        variable_to_log = variable_to_log.dropna(axis=0)

    dummy(dataframe)

    assert isinstance(w._log, pd.DataFrame)
    assert len(w._log) == 2
    assert not w._log[dataframe.columns].isnull().any().any()
