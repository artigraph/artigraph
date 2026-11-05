from __future__ import annotations

import json
import logging
import mimetypes
from collections.abc import Callable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from importlib.resources import files
from importlib.resources.abc import Traversable
from typing import Any
from urllib.parse import unquote, urlsplit

from arti.dashboard._data import JSON, DashboardData, NotFoundError

logger = logging.getLogger(__name__)

STATIC = files("arti.dashboard") / "static"

_IMPORT_HINT = " ".join(
    [
        "Restoring metadata imports the modules that defined it, so run the dashboard where those",
        "modules are importable, eg: from the directory the Graph was built in",
    ]
)


def _route(data: DashboardData, segments: list[str]) -> Callable[[], JSON] | None:
    match segments:
        case ["graphs"]:
            return data.graphs
        case ["graphs", name]:
            return lambda: data.graph(name)
        case ["graphs", name, "versions", fingerprint]:
            return lambda: data.graph_version(name, fingerprint)
        case ["graphs", name, "snapshots", fingerprint]:
            return lambda: data.snapshot(name, fingerprint)
        case _:
            return None


def _static_file(path: str) -> Traversable | None:
    segments = [segment for segment in path.split("/") if segment]
    if any(segment in {".", ".."} for segment in segments):
        return None
    resource = STATIC
    for segment in segments or ["index.html"]:
        resource = resource / segment
    return resource if resource.is_file() else None


class DashboardRequestHandler(BaseHTTPRequestHandler):
    """Serve the bundled dashboard page and its read-only JSON API."""

    data: DashboardData  # Set on subclasses by `make_server`
    server_version = "ArtigraphDashboard"

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), format % args)

    def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: HTTPStatus, document: JSON) -> None:
        self._send(status, json.dumps(document).encode(), "application/json")

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path == "/api" or path.startswith("/api/"):
            self._serve_api(path)
        else:
            self._serve_static(path)

    def _serve_api(self, path: str) -> None:
        segments = [unquote(segment) for segment in path.split("/")[2:]]
        if (handler := _route(self.data, segments)) is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": f"Unknown API path: {path}"})
            return
        try:
            document = handler()
        except NotFoundError as error:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": str(error)})
        except Exception as error:
            # Report the failure to the page (and log the traceback) rather than hiding it.
            logger.exception("Failed to serve %s", path)
            message = f"{type(error).__name__}: {error}"
            if isinstance(error.__cause__, ImportError):
                message += f" ({_IMPORT_HINT})"
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": message})
        else:
            self._send_json(HTTPStatus.OK, document)

    def _serve_static(self, path: str) -> None:
        if (resource := _static_file(path)) is None:
            self._send(HTTPStatus.NOT_FOUND, b"Not Found", "text/plain")
            return
        content_type, _ = mimetypes.guess_type(resource.name)
        self._send(HTTPStatus.OK, resource.read_bytes(), content_type or "application/octet-stream")


def make_server(data: DashboardData, *, host: str = "127.0.0.1", port: int = 8000) -> HTTPServer:
    """
    Create (but don't start) a dashboard server.

    The server handles one request at a time, so Backends don't need to support concurrent use.
    Pass `port=0` to pick a free port, then read it from `server.server_address`.
    """
    if not (STATIC / "index.html").is_file():  # coverage: ignore
        raise FileNotFoundError(
            f"The dashboard page is missing from {STATIC}. Rebuild it with `mise run dashboard`."
        )
    handler = type("DashboardRequestHandler", (DashboardRequestHandler,), {"data": data})
    return HTTPServer((host, port), handler)
