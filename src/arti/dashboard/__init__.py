"""
Browse the Graphs, snapshots, tags, and partitions recorded in a metadata Backend.

The dashboard is a read-only page bundled with Artigraph and served with the standard library, so it
needs no extra dependencies. Start it with `arti dashboard`, or from Python:

```python
from arti.dashboard import DashboardData, make_server

server = make_server(DashboardData(backend), port=8000)
server.serve_forever()
```
"""

from __future__ import annotations

from arti.dashboard._data import DashboardData, NotFoundError
from arti.dashboard._server import make_server

__all__ = ["DashboardData", "NotFoundError", "make_server"]
