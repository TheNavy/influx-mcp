#!/usr/bin/env python3
"""influx-mcp: a small, read-oriented MCP server for InfluxDB 2.x.

The server speaks the Model Context Protocol over Streamable HTTP and talks to
InfluxDB through its HTTP API. It uses the Python standard library only.

Layout of this file, top to bottom:

    Config        settings read from environment variables
    InfluxClient  the few InfluxDB API calls that are needed
    Tools         the MCP tools and their input validation
    McpServer     JSON-RPC dispatch (initialize, ping, tools/list, tools/call)
    HTTP handler  Streamable HTTP transport, stateless, JSON responses
"""

from __future__ import annotations

import csv
import hmac
import io
import json
import logging
import os
import re
import signal
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Mapping

__version__ = "0.1.0"

SERVER_NAME = "influx-mcp"

# Protocol revisions this server answers to, newest first.
PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")

MAX_REQUEST_BYTES = 1_000_000  # largest accepted MCP request body
MAX_OUTPUT_CHARS = 250_000  # largest query result returned to the client
BUCKET_PAGE_SIZE = 100  # InfluxDB's maximum page size for /api/v2/buckets
SCHEMA_ROW_LIMIT = 5_000  # cap for measurement, field and tag listings

log = logging.getLogger(SERVER_NAME)


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


class ConfigError(Exception):
    """A required setting is missing or a setting has an invalid value."""


@dataclass(frozen=True)
class Config:
    influx_url: str
    influx_token: str
    influx_org: str
    host: str = "0.0.0.0"
    port: int = 5001
    auth_token: str = ""
    allowed_origins: frozenset[str] = field(default_factory=frozenset)
    max_rows: int = 1000
    default_rows: int = 200
    timeout_s: float = 30.0

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "Config":
        def required(name: str) -> str:
            value = env.get(name, "").strip()
            if not value:
                raise ConfigError(f"{name} is not set")
            return value

        def number(name: str, default: str, kind: Callable[[str], Any], minimum: float) -> Any:
            raw = env.get(name, "").strip() or default
            try:
                value = kind(raw)
            except ValueError:
                raise ConfigError(f"{name} must be a number, got {raw!r}") from None
            if value < minimum:
                raise ConfigError(f"{name} must be at least {minimum}, got {raw!r}")
            return value

        url = required("INFLUXDB_URL").rstrip("/")
        if urllib.parse.urlsplit(url).scheme not in ("http", "https"):
            raise ConfigError("INFLUXDB_URL must start with http:// or https://")

        max_rows = number("MCP_MAX_ROWS", "1000", int, 1)
        origins = frozenset(
            item.strip().rstrip("/").lower()
            for item in env.get("MCP_ALLOWED_ORIGINS", "").split(",")
            if item.strip()
        )
        return cls(
            influx_url=url,
            influx_token=required("INFLUXDB_TOKEN"),
            influx_org=required("INFLUXDB_ORG"),
            host=env.get("MCP_HOST", "").strip() or "0.0.0.0",
            port=number("MCP_PORT", "5001", int, 1),
            auth_token=env.get("MCP_AUTH_TOKEN", "").strip(),
            allowed_origins=origins,
            max_rows=max_rows,
            default_rows=min(200, max_rows),
            timeout_s=number("INFLUXDB_TIMEOUT", "30", float, 0.1),
        )


# --------------------------------------------------------------------------
# InfluxDB client
# --------------------------------------------------------------------------


class InfluxError(Exception):
    """InfluxDB rejected a request or could not be reached."""


@dataclass
class Table:
    """One block of an InfluxDB CSV response: a header and its rows."""

    columns: list[str]
    rows: list[list[str]] = field(default_factory=list)


@dataclass
class QueryResult:
    tables: list[Table]
    row_count: int
    truncated: bool

    def values(self, column: str = "_value") -> list[str]:
        """All values of one column across all tables, in order."""
        out: list[str] = []
        for table in self.tables:
            if column in table.columns:
                index = table.columns.index(column)
                out.extend(row[index] for row in table.rows if index < len(row))
        return out

    def to_csv(self) -> str:
        """Render the tables as CSV, one block per table, blank line between."""
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        for number, table in enumerate(self.tables):
            if number:
                buffer.write("\n")
            writer.writerow(table.columns)
            writer.writerows(table.rows)
        return buffer.getvalue()


class InfluxClient:
    """The subset of the InfluxDB 2.x HTTP API this server needs."""

    def __init__(self, config: Config) -> None:
        self._base = config.influx_url
        self._org = config.influx_org
        self._timeout = config.timeout_s
        self._headers = {
            "Authorization": f"Token {config.influx_token}",
            "User-Agent": f"{SERVER_NAME}/{__version__}",
        }

    def _open(self, method: str, path: str, params: dict[str, Any], body: bytes | None = None,
              headers: dict[str, str] | None = None):
        url = f"{self._base}{path}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(
            url, data=body, method=method, headers={**self._headers, **(headers or {})}
        )
        try:
            return urllib.request.urlopen(request, timeout=self._timeout)
        except urllib.error.HTTPError as error:
            raise InfluxError(self._describe(error)) from None
        except (urllib.error.URLError, OSError) as error:
            reason = getattr(error, "reason", error)
            raise InfluxError(f"cannot reach InfluxDB: {reason}") from None

    @staticmethod
    def _describe(error: urllib.error.HTTPError) -> str:
        """Turn an InfluxDB error response into a short message."""
        detail = ""
        try:
            raw = error.read(20_000).decode("utf-8", "replace")
            try:
                detail = str(json.loads(raw).get("message", "")).strip()
            except (ValueError, AttributeError):
                detail = raw.strip()[:500]
        except OSError:
            pass
        return f"InfluxDB answered HTTP {error.code}" + (f": {detail}" if detail else "")

    def buckets(self) -> list[dict[str, Any]]:
        """All buckets the token can see, with their retention period."""
        found: list[dict[str, Any]] = []
        offset = 0
        while True:
            params = {"org": self._org, "limit": BUCKET_PAGE_SIZE, "offset": offset}
            try:
                with self._open("GET", "/api/v2/buckets", params) as response:
                    page = json.load(response).get("buckets", [])
            except (ValueError, AttributeError, OSError) as error:
                raise InfluxError(f"unexpected answer from InfluxDB: {error}") from None
            for bucket in page:
                seconds = sum(
                    rule.get("everySeconds", 0) for rule in bucket.get("retentionRules") or []
                )
                found.append(
                    {
                        "name": bucket.get("name"),
                        "type": bucket.get("type", "user"),
                        "retention_seconds": seconds or None,
                        "description": bucket.get("description") or None,
                    }
                )
            if len(page) < BUCKET_PAGE_SIZE:
                return found
            offset += BUCKET_PAGE_SIZE

    def query(self, flux: str, max_rows: int) -> QueryResult:
        """Run a Flux query and read at most ``max_rows`` rows of the answer.

        Reading stops as soon as the row or size limit is reached, so a query
        that returns far too much does not have to be downloaded completely.
        """
        body = json.dumps(
            {"query": flux, "type": "flux", "dialect": {"header": True, "annotations": []}}
        ).encode("utf-8")
        headers = {"Content-Type": "application/json", "Accept": "application/csv"}
        tables: list[Table] = []
        current: Table | None = None
        row_count = 0
        chars = 0
        truncated = False
        with self._open("POST", "/api/v2/query", {"org": self._org}, body, headers) as response:
            text = io.TextIOWrapper(response, encoding="utf-8", newline="")
            try:
                for record in csv.reader(text):
                    if not record or not any(record):
                        current = None  # a blank line ends the current table
                        continue
                    cells = record[1:]  # the first column is reserved for annotations
                    if current is None:
                        current = Table(columns=cells)
                        tables.append(current)
                        continue
                    chars += sum(len(cell) + 1 for cell in cells)
                    if row_count >= max_rows or chars > MAX_OUTPUT_CHARS:
                        truncated = True
                        break
                    current.rows.append(cells)
                    row_count += 1
            except (OSError, csv.Error, UnicodeDecodeError) as error:
                raise InfluxError(f"reading the query result failed: {error}") from None
        return QueryResult(tables=tables, row_count=row_count, truncated=truncated)


# --------------------------------------------------------------------------
# Tools
# --------------------------------------------------------------------------


class ToolInputError(Exception):
    """A tool was called with arguments that cannot be used."""


_START_PATTERN = re.compile(
    r"-\d+(ns|us|ms|s|m|h|d|w|mo|y)"  # relative duration such as -30d
    r"|\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2}))?"  # date or RFC 3339 time
)


def flux_string(value: str) -> str:
    """Quote a value as a Flux string literal.

    Escapes the backslash, the double quote and ``${``, which would otherwise
    start a string interpolation.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("${", "\\${")
    return f'"{escaped}"'


def _text_arg(arguments: Mapping[str, Any], name: str) -> str:
    value = arguments.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ToolInputError(f"'{name}' is required and must be a non-empty string")
    return value


def _start_arg(arguments: Mapping[str, Any]) -> str | None:
    value = arguments.get("start")
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not _START_PATTERN.fullmatch(value):
        raise ToolInputError(
            "'start' must be a relative duration such as -30d or -12h, "
            "or a time such as 2025-01-31 or 2025-01-31T00:00:00Z"
        )
    return value


_START_PROPERTY = {
    "type": "string",
    "description": (
        "How far back to look, as a relative duration (-7d, -12h) or a time "
        "(2025-01-31T00:00:00Z). Defaults to InfluxDB's own default of the last 30 days."
    ),
}


class Tools:
    """The MCP tools. Each tool method returns the text sent to the client."""

    def __init__(self, config: Config, influx: InfluxClient) -> None:
        self._config = config
        self._influx = influx
        self._handlers: dict[str, Callable[[Mapping[str, Any]], str]] = {
            "list_buckets": self._list_buckets,
            "list_measurements": self._list_measurements,
            "describe_measurement": self._describe_measurement,
            "query": self._query,
        }

    def definitions(self) -> list[dict[str, Any]]:
        read_only = {"readOnlyHint": True, "openWorldHint": False}
        return [
            {
                "name": "list_buckets",
                "title": "List buckets",
                "description": (
                    "List the InfluxDB buckets the server can read, with their "
                    "retention period. Start here to find out where data is stored."
                ),
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
                "annotations": read_only,
            },
            {
                "name": "list_measurements",
                "title": "List measurements",
                "description": (
                    "List the measurements in a bucket that received data in the "
                    "chosen period."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bucket": {"type": "string", "description": "Bucket name."},
                        "start": _START_PROPERTY,
                    },
                    "required": ["bucket"],
                    "additionalProperties": False,
                },
                "annotations": read_only,
            },
            {
                "name": "describe_measurement",
                "title": "Describe a measurement",
                "description": (
                    "List the field keys and tag keys of one measurement. Use this "
                    "before writing a query to learn which fields and tags exist."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "bucket": {"type": "string", "description": "Bucket name."},
                        "measurement": {"type": "string", "description": "Measurement name."},
                        "start": _START_PROPERTY,
                    },
                    "required": ["bucket", "measurement"],
                    "additionalProperties": False,
                },
                "annotations": read_only,
            },
            {
                "name": "query",
                "title": "Run a Flux query",
                "description": (
                    "Run a Flux query and return the result as CSV, one block per "
                    "result table. The result is cut off after max_rows rows, so "
                    "keep it small: always set a time range, aggregate with "
                    "aggregateWindow() for longer periods, and drop unneeded columns "
                    'with keep(columns: ["_time", "_value"]). Intended for reading '
                    "data only."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "flux": {"type": "string", "description": "The Flux query to run."},
                        "max_rows": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": self._config.max_rows,
                            "description": (
                                "Maximum number of rows to return. Defaults to "
                                f"{self._config.default_rows}, at most {self._config.max_rows}."
                            ),
                        },
                    },
                    "required": ["flux"],
                    "additionalProperties": False,
                },
                "annotations": read_only,
            },
        ]

    def has(self, name: str) -> bool:
        return name in self._handlers

    def call(self, name: str, arguments: Mapping[str, Any]) -> str:
        return self._handlers[name](arguments)

    def _list_buckets(self, arguments: Mapping[str, Any]) -> str:
        return json.dumps({"buckets": self._influx.buckets()}, indent=2)

    def _schema_values(self, function: str, parameters: list[str]) -> list[str]:
        flux = f'import "influxdata/influxdb/schema"\nschema.{function}({", ".join(parameters)})'
        return sorted(set(self._influx.query(flux, SCHEMA_ROW_LIMIT).values()))

    def _list_measurements(self, arguments: Mapping[str, Any]) -> str:
        bucket = _text_arg(arguments, "bucket")
        start = _start_arg(arguments)
        parameters = [f"bucket: {flux_string(bucket)}"]
        if start:
            parameters.append(f"start: {start}")
        names = self._schema_values("measurements", parameters)
        return json.dumps({"bucket": bucket, "count": len(names), "measurements": names}, indent=2)

    def _describe_measurement(self, arguments: Mapping[str, Any]) -> str:
        bucket = _text_arg(arguments, "bucket")
        measurement = _text_arg(arguments, "measurement")
        start = _start_arg(arguments)
        parameters = [
            f"bucket: {flux_string(bucket)}",
            f"measurement: {flux_string(measurement)}",
        ]
        if start:
            parameters.append(f"start: {start}")
        fields = self._schema_values("measurementFieldKeys", parameters)
        tags = [
            key
            for key in self._schema_values("measurementTagKeys", parameters)
            if not key.startswith("_")  # _start, _stop, _field, _measurement are not real tags
        ]
        return json.dumps(
            {"bucket": bucket, "measurement": measurement, "fields": fields, "tags": tags},
            indent=2,
        )

    def _query(self, arguments: Mapping[str, Any]) -> str:
        flux = _text_arg(arguments, "flux")
        max_rows = arguments.get("max_rows", self._config.default_rows)
        if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 1:
            raise ToolInputError("'max_rows' must be a positive integer")
        max_rows = min(max_rows, self._config.max_rows)
        result = self._influx.query(flux, max_rows)
        if not result.row_count:
            return "The query returned no rows."
        text = result.to_csv()
        if result.truncated:
            text += (
                f"\n[Result cut off after {result.row_count} rows. Narrow the time "
                "range, aggregate, or filter to get a complete result.]"
            )
        return text


# --------------------------------------------------------------------------
# MCP: JSON-RPC dispatch
# --------------------------------------------------------------------------

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

INSTRUCTIONS = (
    "Read access to an InfluxDB 2.x time series database. Typical order: "
    "list_buckets, then list_measurements, then describe_measurement, then query "
    "with Flux. Keep query results small by setting a time range and aggregating."
)


class RpcError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


class McpServer:
    """Handles decoded JSON-RPC messages. Knows nothing about HTTP."""

    def __init__(self, tools: Tools) -> None:
        self._tools = tools
        self._methods: dict[str, Callable[[Mapping[str, Any]], dict[str, Any]]] = {
            "initialize": self._initialize,
            "ping": lambda params: {},
            "tools/list": lambda params: {"tools": self._tools.definitions()},
            "tools/call": self._call_tool,
        }

    def handle(self, message: Any) -> dict[str, Any] | None:
        """Process one message. Returns the response, or None if none is due."""
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            return _error(None, INVALID_REQUEST, "not a JSON-RPC 2.0 message")
        method = message.get("method")
        if method is None:
            return None  # a response from the client; nothing to answer
        if not isinstance(method, str):
            return _error(message.get("id"), INVALID_REQUEST, "'method' must be a string")
        if "id" not in message:
            return None  # a notification, for example notifications/initialized
        request_id = message["id"]
        params = message.get("params", {})
        if not isinstance(params, dict):
            return _error(request_id, INVALID_PARAMS, "'params' must be an object")
        handler = self._methods.get(method)
        if handler is None:
            return _error(request_id, METHOD_NOT_FOUND, f"method not found: {method}")
        try:
            result = handler(params)
        except RpcError as error:
            return _error(request_id, error.code, error.message)
        except Exception:  # keep the server alive, whatever a handler does
            log.exception("unhandled error in %s", method)
            return _error(request_id, INTERNAL_ERROR, "internal error")
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def _initialize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        requested = params.get("protocolVersion")
        version = requested if requested in PROTOCOL_VERSIONS else PROTOCOL_VERSIONS[0]
        return {
            "protocolVersion": version,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": SERVER_NAME, "version": __version__},
            "instructions": INSTRUCTIONS,
        }

    def _call_tool(self, params: Mapping[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        if not isinstance(name, str) or not self._tools.has(name):
            raise RpcError(INVALID_PARAMS, f"unknown tool: {name}")
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, dict):
            raise RpcError(INVALID_PARAMS, "'arguments' must be an object")
        try:
            text = self._tools.call(name, arguments)
        except (ToolInputError, InfluxError) as error:
            # Reported as a tool result so the model can read it and correct itself.
            return {"content": [{"type": "text", "text": str(error)}], "isError": True}
        return {"content": [{"type": "text", "text": text}], "isError": False}


# --------------------------------------------------------------------------
# HTTP transport (Streamable HTTP, stateless)
# --------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    """One MCP endpoint at /mcp and a health check at /health.

    The server keeps no session state and never opens a server-to-client
    stream: every POST is answered with a single JSON body.
    """

    protocol_version = "HTTP/1.1"
    server_version = f"{SERVER_NAME}/{__version__}"
    sys_version = ""

    # Set by make_server().
    config: Config
    mcp: McpServer

    def log_message(self, format: str, *args: Any) -> None:
        log.info("%s %s", self.address_string(), format % args)

    def _send(self, status: int, payload: Any = None, headers: dict[str, str] | None = None) -> None:
        body = b"" if payload is None else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        if body:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _reject(self, status: int, message: str, headers: dict[str, str] | None = None) -> None:
        self.close_connection = True  # the request body may not have been read
        self._send(status, {"error": message}, headers)

    @property
    def _route(self) -> str:
        return urllib.parse.urlsplit(self.path).path.rstrip("/") or "/"

    def _origin_allowed(self) -> bool:
        """Reject browser requests from unknown sites (DNS rebinding protection).

        Programs such as MCP clients send no Origin header and pass. A browser
        always sends one on a POST, and it must be listed explicitly.
        """
        origin = self.headers.get("Origin")
        return origin is None or origin.rstrip("/").lower() in self.config.allowed_origins

    def _authorized(self) -> bool:
        expected = self.config.auth_token
        if not expected:
            return True
        scheme, _, given = self.headers.get("Authorization", "").partition(" ")
        return scheme.lower() == "bearer" and hmac.compare_digest(
            given.strip().encode("utf-8"), expected.encode("utf-8")
        )

    def do_GET(self) -> None:
        if self._route == "/health":
            self._send(200, {"status": "ok", "server": SERVER_NAME, "version": __version__})
        elif self._route == "/mcp":
            # No server-initiated stream is offered; the specification asks for 405 then.
            self._reject(405, "use POST", {"Allow": "POST"})
        else:
            self._reject(404, "not found")

    def do_DELETE(self) -> None:
        if self._route == "/mcp":
            self._reject(405, "sessions are not used", {"Allow": "POST"})
        else:
            self._reject(404, "not found")

    def do_POST(self) -> None:
        if self._route != "/mcp":
            return self._reject(404, "not found")
        if not self._origin_allowed():
            return self._reject(403, "origin not allowed")
        if not self._authorized():
            return self._reject(401, "missing or wrong bearer token", {"WWW-Authenticate": "Bearer"})
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            return self._reject(411, "Content-Length is required")
        if length < 0 or length > MAX_REQUEST_BYTES:
            return self._reject(413, "request too large")
        try:
            message = json.loads(self.rfile.read(length))
        except (ValueError, UnicodeDecodeError):
            return self._send(400, _error(None, PARSE_ERROR, "request body is not valid JSON"))

        if isinstance(message, list):  # JSON-RPC batch, part of protocol revision 2025-03-26
            if not message:
                return self._send(400, _error(None, INVALID_REQUEST, "empty batch"))
            responses = [r for r in map(self.mcp.handle, message) if r is not None]
            return self._send(200, responses) if responses else self._send(202)
        response = self.mcp.handle(message)
        if response is None:
            return self._send(202)  # notification or client response: accepted, no body
        self._send(200, response)


def make_server(config: Config, influx: InfluxClient | None = None) -> ThreadingHTTPServer:
    """Build the HTTP server. ``influx`` can be replaced in tests."""
    tools = Tools(config, influx or InfluxClient(config))
    handler = type("BoundHandler", (Handler,), {"config": config, "mcp": McpServer(tools)})
    server = ThreadingHTTPServer((config.host, config.port), handler)
    server.daemon_threads = True
    return server


def main() -> int:
    level = os.environ.get("LOG_LEVEL", "").strip().upper()
    logging.basicConfig(
        level=level if level in ("DEBUG", "INFO", "WARNING", "ERROR") else "INFO",
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    try:
        config = Config.from_env(os.environ)
    except ConfigError as error:
        log.error("configuration error: %s", error)
        return 2

    def stop(signum: int, frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    server = make_server(config)
    log.info(
        "%s %s listening on http://%s:%d/mcp, InfluxDB at %s, org %s, bearer token %s",
        SERVER_NAME, __version__, config.host, config.port, config.influx_url,
        config.influx_org, "required" if config.auth_token else "not required",
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("stopping")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
