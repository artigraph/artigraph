# Only the parts of distributed's API that Artigraph uses, since its own hints are incomplete.
from collections.abc import Callable, Iterable
from types import TracebackType
from typing import Any, Self

class Future[T]:
    def result(self, timeout: float | None = None) -> T: ...

class LocalCluster:
    scheduler_address: str
    def __init__(
        self,
        *,
        n_workers: int | None = None,
        threads_per_worker: int | None = None,
        processes: bool | None = None,
        dashboard_address: str | None = ":8787",
    ) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

class Client:
    def __init__(self, address: str | LocalCluster | None = None) -> None: ...
    def submit[T](
        self, func: Callable[..., T], *args: Any, key: str | None = None, pure: bool = True
    ) -> Future[T]: ...
    def gather[T](self, futures: Iterable[Future[T]]) -> list[T]: ...
    def cancel(self, futures: Iterable[Future[Any]]) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
