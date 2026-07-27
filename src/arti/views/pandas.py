from __future__ import annotations

from typing import Any, ClassVar

from arti.views import View

try:
    import pandas as _pandas

    from arti.types.pandas import pandas_type_system
except ModuleNotFoundError as error:  # pragma: no cover - dependency-free subprocess
    if (error.name or "").split(".", 1)[0] not in {"numpy", "pandas"}:
        raise
    # View discovery imports every view module. Keep discovery safe for minimal installations;
    # there cannot be a pd.DataFrame annotation to register when Pandas itself is unavailable.
else:

    class DataFrame(View):
        python_type: ClassVar[type[_pandas.DataFrame]] = _pandas.DataFrame
        type_system = pandas_type_system


def __getattr__(name: str) -> Any:
    if name == "DataFrame":  # pragma: no cover - only reachable without Pandas installed
        raise ModuleNotFoundError("Pandas views require Pandas: `pip install pandas`")
    raise AttributeError(name)


__all__ = ["DataFrame"]
