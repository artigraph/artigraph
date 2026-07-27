from typing import Annotated

import pandas as pd
import pytest

import arti.views.pandas as pandas_views
from arti import Artifact, Type, View
from arti.types import Collection, Date, Int64, String, Struct
from arti.views.pandas import DataFrame


class Transactions(Artifact):
    type: Type = Collection(
        element=Struct(
            fields={
                "transaction_date": Date(),
                "transaction_id": String(),
                "amount_cents": Int64(),
            }
        )
    )


def test_pandas_dataframe_view() -> None:
    view = View.from_annotation(
        Annotated[pd.DataFrame, Transactions],
        mode="READWRITE",
    )

    assert isinstance(view, DataFrame)
    assert view.artifact_class is Transactions
    assert view.type == Transactions.model_fields["type"].default


def test_pandas_view_unknown_attribute() -> None:
    with pytest.raises(AttributeError, match="unknown"):
        pandas_views.unknown
