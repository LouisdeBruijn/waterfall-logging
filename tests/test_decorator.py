import pandas as pd
import pytest
from pandas._testing import assert_series_equal

from waterfall_logging.decorator import waterfall
from waterfall_logging.log import PandasWaterfall


@pytest.mark.parametrize("dataframe", [""], indirect=True)
def test__waterfall_decorator(dataframe) -> None:
    """"""
    w = PandasWaterfall(columns=dataframe.columns, dropna=True)

    @waterfall(log=w)
    def return_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """"""
        return dataframe

    dataframe = return_dataframe(dataframe)

    assert isinstance(w._log, pd.DataFrame)
    assert_series_equal(w._log[dataframe.columns].iloc[0], dataframe.count(), check_dtype=False, check_names=False)
