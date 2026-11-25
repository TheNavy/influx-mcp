"""Tests for influx_mcp. Run with: python -m unittest -v

The tests start the real HTTP server on a free local port and point it at a
small stand-in for InfluxDB, so the whole path from MCP request to InfluxDB
API call is exercised without a database.
"""

from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import influx_mcp
from influx_mcp import Config, ConfigError, flux_string

INFLUX_TOKEN = "influx-secret"

CSV_TWO_TABLES = (
    ",result,table,_time,_value\r\n"
    ",_result,0,2025-01-01T00:00:00Z,1.5\r\n"
    ",_result,0,2025-01-01T01:00:00Z,2.5\r\n"
    "\r\n"
    ",result,table,_time,_value,room\r\n"
    ',_result,1,2025-01-01T00:00:00Z,7,"kitchen, north"\r\n'
)


def csv_values(*values: str) -> str:
    return ",result,table,_value\r\n" + "".join(f",_result,0,{v}\r\n" for v in values)


class FakeInflux(BaseHTTPRequestHandler):
    """Answers like InfluxDB 2.x for the two endpoints the server uses."""

    requests: list[dict] = []
    query_answer: str = CSV_TWO_TABLES
    fail_with: tuple[int, dict] | None = None

    def log_message(self, *args) -> None:
        pass

    def _reply(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _record(self, body: bytes = b"") -> dict:
        parts = urlsplit(self.path)
        entry = {
            "method": self.command,
            "path": parts.path,
            "params": {k: v[0] for k, v in parse_qs(parts.query).items()},
            "auth": self.headers.get("Authorization"),
            "body": json.loads(body) if body else None,
        }
        type(self).requests.append(entry)
        return entry

    def _failed(self) -> bool:
        if type(self).fail_with is None:
            return False
        status, payload = type(self).fail_with
        self._reply(status, json.dumps(payload).encode(), "application/json")
        return True

    def do_GET(self) -> None:
        entry = self._record()
        if self._failed():
            return
        offset = int(entry["params"].get("offset", "0"))
        page = [
            {"name": "home", "type": "user", "retentionRules": [{"everySeconds": 86400}]},
            {"name": "_monitoring", "type": "system", "retentionRules": []},
        ]
        body = json.dumps({"buckets": page if offset == 0 else []}).encode()
        self._reply(200, body, "application/json")

    def do_POST(self) -> None:
        entry = self._record(self.rfile.read(int(self.headers["Content-Length"])))
        if self._failed():
            return
        flux = entry["body"]["query"]
        if "schema.measurements(" in flux:
            answer = csv_values("power", "temperature")
        elif "schema.measurementFieldKeys(" in flux:
            answer = csv_values("value")
        elif "schema.measurementTagKeys(" in flux:
            answer = csv_values("_start", "_stop", "_field", "_measurement", "room")
        else:
            answer = type(self).query_answer
        self._reply(200, answer.encode(), "text/csv; charset=utf-8")


def serve(server: ThreadingHTTPServer) -> str:
    threading.Thread(target=server.serve_forever, args=(0.01,), daemon=True).start()
    return f"http://127.0.0.1:{server.server_address[1]}"


class ServerTestCase(unittest.TestCase):
    """Starts a stand-in InfluxDB and the MCP server for each test."""

    extra_env: dict[str, str] = {}

    def setUp(self) -> None:
        FakeInflux.requests = []
        FakeInflux.query_answer = CSV_TWO_TABLES
        FakeInflux.fail_with = None
        self.influx = ThreadingHTTPServer(("127.0.0.1", 0), FakeInflux)
        influx_url = serve(self.influx)
        env = {
            "INFLUXDB_URL": influx_url,
            "INFLUXDB_TOKEN": INFLUX_TOKEN,
            "INFLUXDB_ORG": "my-org",
            "MCP_HOST": "127.0.0.1",
            "MCP_PORT": "1",  # replaced below; port 0 lets the system pick a free one
            **self.extra_env,
        }
        config = Config.from_env(env)
        config = influx_mcp.Config(**{**config.__dict__, "port": 0})
        self.server = influx_mcp.make_server(config)
        self.url = serve(self.server)
        self.addCleanup(self._stop)

    def _stop(self) -> None:
        for server in (self.server, self.influx):
            server.shutdown()
            server.server_close()

    def http(self, method: str, path: str, body=None, headers=None, raw: bytes | None = None):
        """Send a request; returns (status, headers, decoded JSON or None)."""
        data = raw if raw is not None else (None if body is None else json.dumps(body).encode())
        request = urllib.request.Request(
            self.url + path, data=data, method=method,
            headers={"Content-Type": "application/json",
                     "Accept": "application/json, text/event-stream", **(headers or {})},
        )
        try:
            response = urllib.request.urlopen(request, timeout=5)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            text = response.read()
            return response.status, response.headers, (json.loads(text) if text else None)

    def rpc(self, method: str, params=None, headers=None):
        message = {"jsonrpc": "2.0", "id": 1, "method": method}
        if params is not None:
            message["params"] = params
        status, _, answer = self.http("POST", "/mcp", message, headers)
        self.assertEqual(status, 200)
        return answer

    def call(self, name: str, arguments=None):
        answer = self.rpc("tools/call", {"name": name, "arguments": arguments or {}})
        result = answer["result"]
        return result["content"][0]["text"], result["isError"]


class ProtocolTests(ServerTestCase):
    def test_initialize_echoes_supported_version(self) -> None:
        answer = self.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                         "clientInfo": {"name": "t", "version": "0"}})
        result = answer["result"]
        self.assertEqual(result["protocolVersion"], "2025-06-18")
        self.assertEqual(result["serverInfo"]["name"], "influx-mcp")
        self.assertIn("tools", result["capabilities"])

    def test_initialize_offers_latest_for_unknown_version(self) -> None:
        answer = self.rpc("initialize", {"protocolVersion": "1999-01-01"})
        self.assertEqual(answer["result"]["protocolVersion"], influx_mcp.PROTOCOL_VERSIONS[0])

    def test_no_session_header_is_issued(self) -> None:
        message = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        _, headers, _ = self.http("POST", "/mcp", message)
        self.assertIsNone(headers.get("Mcp-Session-Id"))

    def test_ping(self) -> None:
        self.assertEqual(self.rpc("ping")["result"], {})

    def test_notification_is_accepted_without_body(self) -> None:
        status, _, body = self.http(
            "POST", "/mcp", {"jsonrpc": "2.0", "method": "notifications/initialized"})
        self.assertEqual((status, body), (202, None))

    def test_tools_list(self) -> None:
        tools = self.rpc("tools/list")["result"]["tools"]
        self.assertEqual(
            [tool["name"] for tool in tools],
            ["list_buckets", "list_measurements", "describe_measurement", "query"],
        )
        for tool in tools:
            self.assertEqual(tool["inputSchema"]["type"], "object")
            self.assertTrue(tool["annotations"]["readOnlyHint"])

    def test_unknown_method(self) -> None:
        self.assertEqual(self.rpc("resources/list")["error"]["code"], influx_mcp.METHOD_NOT_FOUND)

    def test_unknown_tool(self) -> None:
        answer = self.rpc("tools/call", {"name": "drop_everything"})
        self.assertEqual(answer["error"]["code"], influx_mcp.INVALID_PARAMS)

    def test_invalid_json(self) -> None:
        status, _, body = self.http("POST", "/mcp", raw=b"{not json")
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], influx_mcp.PARSE_ERROR)

    def test_message_without_jsonrpc_version(self) -> None:
        status, _, body = self.http("POST", "/mcp", {"id": 1, "method": "ping"})
        self.assertEqual(status, 200)
        self.assertEqual(body["error"]["code"], influx_mcp.INVALID_REQUEST)

    def test_batch(self) -> None:
        batch = [
            {"jsonrpc": "2.0", "id": 1, "method": "ping"},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "id": 2, "method": "ping"},
        ]
        status, _, body = self.http("POST", "/mcp", batch)
        self.assertEqual(status, 200)
        self.assertEqual([item["id"] for item in body], [1, 2])


class TransportTests(ServerTestCase):
    def test_health(self) -> None:
        status, _, body = self.http("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

    def test_get_on_endpoint_is_not_allowed(self) -> None:
        status, headers, _ = self.http("GET", "/mcp")
        self.assertEqual(status, 405)
        self.assertEqual(headers["Allow"], "POST")

    def test_delete_on_endpoint_is_not_allowed(self) -> None:
        self.assertEqual(self.http("DELETE", "/mcp")[0], 405)

    def test_unknown_path(self) -> None:
        self.assertEqual(self.http("POST", "/elsewhere", {})[0], 404)

    def test_trailing_slash(self) -> None:
        status, _, _ = self.http("POST", "/mcp/", {"jsonrpc": "2.0", "id": 1, "method": "ping"})
        self.assertEqual(status, 200)

    def test_browser_origin_is_rejected_by_default(self) -> None:
        status, _, _ = self.http("POST", "/mcp", {"jsonrpc": "2.0", "id": 1, "method": "ping"},
                                 {"Origin": "https://evil.example"})
        self.assertEqual(status, 403)

    def test_oversized_request(self) -> None:
        status, _, _ = self.http("POST", "/mcp", raw=b" " * (influx_mcp.MAX_REQUEST_BYTES + 1))
        self.assertEqual(status, 413)

    def test_no_auth_needed_without_token(self) -> None:
        self.assertEqual(self.rpc("ping")["result"], {})


class AuthTests(ServerTestCase):
    extra_env = {"MCP_AUTH_TOKEN": "let-me-in", "MCP_ALLOWED_ORIGINS": "https://ok.example/"}
    ping = {"jsonrpc": "2.0", "id": 1, "method": "ping"}

    def test_missing_token(self) -> None:
        status, headers, _ = self.http("POST", "/mcp", self.ping)
        self.assertEqual(status, 401)
        self.assertEqual(headers["WWW-Authenticate"], "Bearer")

    def test_wrong_token(self) -> None:
        status, _, _ = self.http("POST", "/mcp", self.ping, {"Authorization": "Bearer nope"})
        self.assertEqual(status, 401)

    def test_correct_token(self) -> None:
        status, _, _ = self.http("POST", "/mcp", self.ping, {"Authorization": "Bearer let-me-in"})
        self.assertEqual(status, 200)

    def test_health_needs_no_token(self) -> None:
        self.assertEqual(self.http("GET", "/health")[0], 200)

    def test_listed_origin_is_allowed(self) -> None:
        status, _, _ = self.http(
            "POST", "/mcp", self.ping,
            {"Authorization": "Bearer let-me-in", "Origin": "https://ok.example"})
        self.assertEqual(status, 200)


class ToolTests(ServerTestCase):
    def test_list_buckets(self) -> None:
        text, is_error = self.call("list_buckets")
        self.assertFalse(is_error)
        buckets = json.loads(text)["buckets"]
        self.assertEqual([b["name"] for b in buckets], ["home", "_monitoring"])
        self.assertEqual(buckets[0]["retention_seconds"], 86400)
        self.assertIsNone(buckets[1]["retention_seconds"])
        sent = FakeInflux.requests[0]
        self.assertEqual(sent["path"], "/api/v2/buckets")
        self.assertEqual(sent["params"]["org"], "my-org")
        self.assertEqual(sent["auth"], f"Token {INFLUX_TOKEN}")

    def test_list_measurements(self) -> None:
        text, is_error = self.call("list_measurements", {"bucket": "home", "start": "-7d"})
        self.assertFalse(is_error)
        self.assertEqual(json.loads(text)["measurements"], ["power", "temperature"])
        flux = FakeInflux.requests[0]["body"]["query"]
        self.assertIn('schema.measurements(bucket: "home", start: -7d)', flux)

    def test_list_measurements_without_start_uses_influx_default(self) -> None:
        self.call("list_measurements", {"bucket": "home"})
        self.assertIn('schema.measurements(bucket: "home")', FakeInflux.requests[0]["body"]["query"])

    def test_bucket_name_is_escaped(self) -> None:
        self.call("list_measurements", {"bucket": 'a"b\\c${d}'})
        self.assertIn('bucket: "a\\"b\\\\c\\${d}"', FakeInflux.requests[0]["body"]["query"])

    def test_describe_measurement(self) -> None:
        text, is_error = self.call("describe_measurement", {"bucket": "home", "measurement": "power"})
        self.assertFalse(is_error)
        described = json.loads(text)
        self.assertEqual(described["fields"], ["value"])
        self.assertEqual(described["tags"], ["room"])

    def test_query_returns_csv_blocks(self) -> None:
        text, is_error = self.call("query", {"flux": 'from(bucket: "home")'})
        self.assertFalse(is_error)
        self.assertEqual(
            text,
            "result,table,_time,_value\n"
            "_result,0,2025-01-01T00:00:00Z,1.5\n"
            "_result,0,2025-01-01T01:00:00Z,2.5\n"
            "\n"
            "result,table,_time,_value,room\n"
            '_result,1,2025-01-01T00:00:00Z,7,"kitchen, north"\n',
        )
        sent = FakeInflux.requests[0]
        self.assertEqual(sent["path"], "/api/v2/query")
        self.assertEqual(sent["body"]["query"], 'from(bucket: "home")')
        self.assertEqual(sent["body"]["dialect"], {"header": True, "annotations": []})

    def test_query_is_cut_off_at_max_rows(self) -> None:
        text, is_error = self.call("query", {"flux": "x", "max_rows": 2})
        self.assertFalse(is_error)
        self.assertIn("2025-01-01T01:00:00Z,2.5", text)
        self.assertNotIn("kitchen", text)
        self.assertIn("cut off after 2 rows", text)

    def test_query_exactly_at_limit_is_not_marked_as_cut_off(self) -> None:
        text, _ = self.call("query", {"flux": "x", "max_rows": 3})
        self.assertIn("kitchen", text)
        self.assertNotIn("cut off", text)

    def test_query_without_rows(self) -> None:
        FakeInflux.query_answer = "\r\n"
        text, is_error = self.call("query", {"flux": "x"})
        self.assertFalse(is_error)
        self.assertEqual(text, "The query returned no rows.")

    def test_query_requires_flux(self) -> None:
        text, is_error = self.call("query", {})
        self.assertTrue(is_error)
        self.assertIn("'flux'", text)
        self.assertEqual(FakeInflux.requests, [])

    def test_query_rejects_bad_max_rows(self) -> None:
        for bad in (0, -1, "10", True, 1.5):
            _, is_error = self.call("query", {"flux": "x", "max_rows": bad})
            self.assertTrue(is_error, bad)

    def test_bad_start_is_rejected_before_reaching_influx(self) -> None:
        for bad in ("7d", "-7 days", '-7d) |> to(bucket: "x"', 7):
            _, is_error = self.call("list_measurements", {"bucket": "home", "start": bad})
            self.assertTrue(is_error, bad)
        self.assertEqual(FakeInflux.requests, [])

    def test_influx_error_is_reported_without_token(self) -> None:
        FakeInflux.fail_with = (400, {"code": "invalid", "message": "compilation failed: oops"})
        text, is_error = self.call("query", {"flux": "nonsense"})
        self.assertTrue(is_error)
        self.assertIn("HTTP 400", text)
        self.assertIn("compilation failed: oops", text)
        self.assertNotIn(INFLUX_TOKEN, text)

    def test_error_during_execution_is_reported(self) -> None:
        FakeInflux.query_answer = ",error,reference\r\n,runtime error: division by zero,\r\n"
        text, is_error = self.call("query", {"flux": "x"})
        self.assertTrue(is_error)
        self.assertIn("division by zero", text)

    def test_unreachable_influx_is_reported(self) -> None:
        self.influx.shutdown()
        self.influx.server_close()
        text, is_error = self.call("list_buckets")
        self.assertTrue(is_error)
        self.assertIn("cannot reach InfluxDB", text)
        self.assertNotIn(INFLUX_TOKEN, text)
        self.influx = ThreadingHTTPServer(("127.0.0.1", 0), FakeInflux)  # for cleanup
        serve(self.influx)


class RowLimitTests(ServerTestCase):
    extra_env = {"MCP_MAX_ROWS": "1"}

    def test_max_rows_cannot_exceed_configured_cap(self) -> None:
        text, _ = self.call("query", {"flux": "x", "max_rows": 500})
        self.assertIn("cut off after 1 rows", text)


class ConfigTests(unittest.TestCase):
    base = {"INFLUXDB_URL": "http://db:8086/", "INFLUXDB_TOKEN": "t", "INFLUXDB_ORG": "o"}

    def test_defaults(self) -> None:
        config = Config.from_env(self.base)
        self.assertEqual(config.influx_url, "http://db:8086")
        self.assertEqual((config.host, config.port), ("0.0.0.0", 5001))
        self.assertEqual((config.max_rows, config.default_rows), (1000, 200))
        self.assertEqual(config.auth_token, "")

    def test_required_settings(self) -> None:
        for name in self.base:
            env = {**self.base, name: " "}
            with self.assertRaisesRegex(ConfigError, name):
                Config.from_env(env)

    def test_invalid_values(self) -> None:
        for name, value in (("INFLUXDB_URL", "db:8086"), ("MCP_PORT", "http"),
                            ("MCP_PORT", "70000"),
                            ("MCP_MAX_ROWS", "0"), ("INFLUXDB_TIMEOUT", "-1")):
            with self.assertRaises(ConfigError, msg=name):
                Config.from_env({**self.base, name: value})

    def test_origins_are_normalised(self) -> None:
        env = {**self.base, "MCP_ALLOWED_ORIGINS": " https://A.example/ , http://b.example ,"}
        self.assertEqual(Config.from_env(env).allowed_origins,
                         frozenset({"https://a.example", "http://b.example"}))


class FluxStringTests(unittest.TestCase):
    def test_plain(self) -> None:
        self.assertEqual(flux_string("home"), '"home"')

    def test_escapes(self) -> None:
        self.assertEqual(flux_string('a"b'), '"a\\"b"')
        self.assertEqual(flux_string("a\\b"), '"a\\\\b"')
        self.assertEqual(flux_string("${x}"), '"\\${x}"')


if __name__ == "__main__":
    unittest.main()
