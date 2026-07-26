from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager


@contextmanager
def wrap_exc(error_type: type[Exception], *, prefix: str) -> Generator[None]:
    """
    Wrap exceptions of `error_type` and add a message prefix.

    `error_type` must be initializable with a single string message argument.

    NOTE: When used inside a generator, any exceptions raised by the *caller of the generator* will **not** be wrapped.
    """
    try:
        yield
    except error_type as e:
        msg = str(e)
        if getattr(e, "wrapped", False) and e.__cause__ is not None:
            src = e.__cause__  # Shorten exception chains to the root and last wrapped only
        else:
            msg = f" - {msg}"
            src = e
        error = error_type(f"{prefix}{msg}")
        object.__setattr__(error, "wrapped", True)
        raise error from src
