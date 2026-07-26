from types import TracebackType
from typing import Self

class Server:
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

def create_server(
    host: str,
    port: int,
    in_memory: bool = False,
    default_bucket: str | None = None,
    data_dir: str | None = None,
    project_number: str | None = None,
) -> Server: ...
