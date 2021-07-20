from typing import Optional

from arti.storage.core import Storage


class LocalFile(Storage):
    path: Optional[str] = None
