from __future__ import annotations

import json
import threading
from collections.abc import Generator
from http.server import HTTPServer
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

from arti import GraphSnapshot
from arti.backends.memory import MemoryBackend
from arti.backends.sql.codec import MetadataCodecError
from arti.dashboard import DashboardData, make_server
from tests.arti.dashboard.example import build_graph, write_transactions


@pytest.fixture
def snapshot(tmp_path: Path) -> GraphSnapshot:
    write_transactions(tmp_path)
    snapshot = build_graph(tmp_path, MemoryBackend()).build()
    snapshot.tag("prod")
    return snapshot


@pytest.fixture
def server(snapshot: GraphSnapshot) -> Generator[HTTPServer]:
    with make_server(DashboardData(snapshot.backend), port=0) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join()


def get(server: HTTPServer, path: str) -> tuple[int, str, bytes]:
    host, port = server.server_address[:2]
    try:
        with urlopen(f"http://{host!s}:{port}{path}") as response:
            return response.status, response.headers["Content-Type"], response.read()
    except HTTPError as error:
        with error:
            return error.code, error.headers["Content-Type"], error.read()


def get_json(server: HTTPServer, path: str) -> tuple[int, Any]:
    status, content_type, body = get(server, path)
    assert content_type == "application/json"
    return status, json.loads(body)


def test_server_serves_the_page(server: HTTPServer) -> None:
    status, content_type, body = get(server, "/")
    assert (status, content_type) == (200, "text/html")
    assert b"assets/dashboard.js" in body

    status, content_type, _ = get(server, "/assets/dashboard.js")
    assert status == 200
    assert content_type in {"application/javascript", "text/javascript"}

    for path in ("/missing.js", "/assets/../index.html", "/assets"):
        assert get(server, path)[:2] == (404, "text/plain")


def test_server_serves_the_api(server: HTTPServer, snapshot: GraphSnapshot) -> None:
    data = DashboardData(snapshot.backend)

    assert get_json(server, "/api/graphs") == (200, data.graphs())
    assert get_json(server, "/api/graphs/spend") == (200, data.graph("spend"))
    version = str(snapshot.graph.fingerprint)
    assert get_json(server, f"/api/graphs/spend/versions/{version}") == (
        200,
        data.graph_version("spend", version),
    )
    fingerprint = str(snapshot.fingerprint)
    assert get_json(server, f"/api/graphs/spend/snapshots/{fingerprint}") == (
        200,
        data.snapshot("spend", fingerprint),
    )


def test_server_decodes_graph_names(snapshot: GraphSnapshot, tmp_path: Path) -> None:
    graph = build_graph(tmp_path, snapshot.backend, name="team/spend")
    with make_server(DashboardData(snapshot.backend, definitions=[graph]), port=0) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            status, document = get_json(server, "/api/graphs/team%2Fspend")
        finally:
            server.shutdown()
            thread.join()
    assert status == 200
    assert document["name"] == "team/spend"


def test_server_reports_errors(server: HTTPServer) -> None:
    assert get_json(server, "/api/unknown") == (404, {"error": "Unknown API path: /api/unknown"})
    assert get_json(server, "/api") == (404, {"error": "Unknown API path: /api"})
    assert get_json(server, "/api/graphs/missing") == (
        404,
        {"error": "Unknown Graph: 'missing'"},
    )

    with mock.patch.object(DashboardData, "graphs", side_effect=RuntimeError("boom")):
        assert get_json(server, "/api/graphs") == (500, {"error": "RuntimeError: boom"})

    error = MetadataCodecError("Unable to import persisted type 'model:Thing'")
    error.__cause__ = ImportError("No module named 'model'")
    with mock.patch.object(DashboardData, "graphs", side_effect=error):
        status, document = get_json(server, "/api/graphs")
    assert status == 500
    assert document["error"].startswith(
        "MetadataCodecError: Unable to import persisted type 'model:Thing' (Restoring metadata"
    )
