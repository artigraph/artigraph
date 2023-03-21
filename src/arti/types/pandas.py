from __future__ import annotations

from typing import Any, cast

import numpy as np
import pandas as pd

from arti.types import List, String, Struct, Type, TypeAdapter, TypeSystem
from arti.types.numpy import numpy_type_system

# TODO: How should (multi)indexes be handled; perhaps as a "hint"?

pandas_type_system = TypeSystem(key="pandas", extends=(numpy_type_system,))


@pandas_type_system.register_adapter
class SeriesAdapter(TypeAdapter):
    artigraph = List
    system = pd.Series

    @classmethod
    def matches_artigraph(cls, type_: Type, *, hints: dict[str, Any]) -> bool:
        return (
            isinstance(type_, cls.artigraph)
            # List(element=Struct(...)) are handled by the DataFrameAdapter.
            and not isinstance(type_.element, Struct)
        )

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any], type_system: TypeSystem) -> Type:
        dtype = type_.dtype
        if dtype == np.dtype("O"):
            # TODO: Should we handle empty series by defaulting to "String", but issuing
            # a warning?
            example_value = type_.iloc[0]
            if isinstance(example_value, str):
                return List(element=String())
            # TODO: Handle dicts, lists, etc.
            raise NotImplementedError(
                f"Non-string {dtype} is not supported yet, got values of: {example_value}"
            )
        return List(element=type_system.to_artigraph(dtype, hints=hints))

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return isinstance(type_, cls.system)

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem) -> Any:
        assert isinstance(type_, cls.artigraph)
        dtype = type_system.to_system(type_.element, hints=hints)
        return pd.Series([dtype()], dtype=dtype)


@pandas_type_system.register_adapter
class DataFrameAdapter(TypeAdapter):
    """Convert between a List of Structs and a pd.DataFrame.

    Expects a List type like:
    >>> from arti.types import Float64, Int8, List, Struct
    >>> from arti.types.pandas import pandas_type_system
    >>>
    >>> arti_type = List(element=Struct(fields={"col1": Int8(), "col2": Float64()}))
    >>> pandas_type_system.to_system(arti_type, hints={})
       col1  col2
    0     0   0.0
    """

    artigraph = List
    system = pd.DataFrame

    @classmethod
    def matches_artigraph(cls, type_: Type, *, hints: dict[str, Any]) -> bool:
        # Match Lists of Structs, but not sub-fields (eg: a column containing lists). We may need to
        # pass a `hint` to identify when we're not at the root to distinguish the main dataframe
        # from columns containing list[dict[...]] values.
        return isinstance(type_, cls.artigraph) and isinstance(type_.element, Struct)

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any], type_system: TypeSystem) -> Type:
        assert isinstance(type_, cls.system)
        return List(
            element=Struct(
                fields={
                    name: cast(List, type_system.to_artigraph(type_[name], hints=hints)).element
                    for name in type_.columns
                }
            )
        )

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return isinstance(type_, cls.system)

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem) -> Any:
        assert isinstance(type_, cls.artigraph)
        assert isinstance(type_.element, Struct)
        # NOTE: We automatically wrap the sub-types as List(...) to match the SeriesAdapter.
        return pd.DataFrame(
            {
                name: type_system.to_system(List(element=subtype), hints=hints)
                for name, subtype in type_.element.fields.items()
            }
        )
