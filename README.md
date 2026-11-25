# influx-mcp

A small [Model Context Protocol](https://modelcontextprotocol.io) server that lets an AI assistant
read data from **InfluxDB 2.x**.

- One Python file, standard library only. No packages to install, nothing to patch.
- Speaks MCP over Streamable HTTP, so it runs as a container next to your database.
- Four tools: list buckets, list measurements, describe a measurement, run a Flux query.
- Query results are capped, so a careless query cannot flood the assistant's context.

## Contents

- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Connect a client](#connect-a-client)
- [Tools](#tools)
- [Configuration](#configuration)
- [Security](#security)
- [HTTP interface](#http-interface)
- [Development](#development)
- [Limitations](#limitations)
- [License](#license)

## How it works

```
MCP client ──HTTP POST /mcp──► influx-mcp ──HTTP──► InfluxDB 2.x
(assistant)   JSON-RPC 2.0                 /api/v2/buckets
                                           /api/v2/query (Flux)
```

1. The MCP client sends a JSON-RPC message to `/mcp`, for example `tools/call` with the tool
   `query` and a Flux query.
2. influx-mcp checks the request (origin, optional bearer token, size), validates the tool
   arguments and calls the InfluxDB HTTP API with the configured token.
3. InfluxDB answers with CSV. influx-mcp reads it row by row, stops at the row limit, and returns
   the result as text in a single JSON response.

The server keeps no state between requests: there are no sessions and no server-to-client streams.
A restart is invisible to clients.

The code in [`influx_mcp.py`](influx_mcp.py) follows the same order:

| Part | Job |
|---|---|
| `Config` | Reads and checks the environment variables. |
| `InfluxClient` | The two InfluxDB API calls: list buckets, run a Flux query. |
| `Tools` | Tool definitions, argument validation, result formatting. |
| `McpServer` | JSON-RPC dispatch: `initialize`, `ping`, `tools/list`, `tools/call`. |
| `Handler` | The HTTP layer: routing, origin check, bearer token, size limit. |

## Quick start

You need a running InfluxDB 2.x, its organization name and an API token.

### Docker Compose

```bash
cp docker-compose.example.yml docker-compose.yml
```

```bash
cp .env.example .env
```

Edit `.env` and set `INFLUXDB_URL`, `INFLUXDB_TOKEN` and `INFLUXDB_ORG`, then start the container:

```bash
docker compose up -d
```

Check that it is running:

```bash
curl http://localhost:5001/health
```

### Docker

```bash
docker run -d --name influx-mcp -p 5001:5001 \
  -e INFLUXDB_URL=http://influxdb:8086 \
  -e INFLUXDB_TOKEN=YOUR_READ_ONLY_TOKEN \
  -e INFLUXDB_ORG=YOUR_ORG \
  ghcr.io/thenavy/influx-mcp:latest
```

### Without Docker

Python 3.10 or newer is enough:

```bash
INFLUXDB_URL=http://localhost:8086 INFLUXDB_TOKEN=YOUR_READ_ONLY_TOKEN INFLUXDB_ORG=YOUR_ORG \
  python influx_mcp.py
```

## Connect a client

The endpoint is `http://HOST:5001/mcp`, transport "Streamable HTTP" (often just called "HTTP").

Claude Code:

```bash
claude mcp add --transport http influxdb http://HOST:5001/mcp
```

With a bearer token (see `MCP_AUTH_TOKEN`):

```bash
claude mcp add --transport http influxdb http://HOST:5001/mcp \
  --header "Authorization: Bearer YOUR_MCP_TOKEN"
```

Clients that are configured through a JSON file usually take an entry like this:

```json
{
  "mcpServers": {
    "influxdb": {
      "type": "http",
      "url": "http://HOST:5001/mcp",
      "headers": { "Authorization": "Bearer YOUR_MCP_TOKEN" }
    }
  }
}
```

## Tools

| Tool | Arguments | Returns |
|---|---|---|
| `list_buckets` | none | Buckets with type and retention period, as JSON. |
| `list_measurements` | `bucket`, optional `start` | Measurement names in the bucket, as JSON. |
| `describe_measurement` | `bucket`, `measurement`, optional `start` | Field keys and tag keys, as JSON. |
| `query` | `flux`, optional `max_rows` | Query result as CSV, one block per result table. |

`start` says how far back to look: a relative duration such as `-7d`, or a time such as
`2025-01-31T00:00:00Z`. Without it, InfluxDB's default of the last 30 days applies. A measurement
that received no data in that period is not listed.

`query` returns at most `max_rows` rows (default 200, upper limit `MCP_MAX_ROWS`). If the result is
cut off, a note at the end says so. Example of a query and its result:

```flux
from(bucket: "home")
  |> range(start: -6h)
  |> filter(fn: (r) => r._measurement == "temperature")
  |> aggregateWindow(every: 1h, fn: mean)
  |> keep(columns: ["_time", "_value"])
```

```csv
result,table,_time,_value
_result,0,2025-01-01T01:00:00Z,20.4
_result,0,2025-01-01T02:00:00Z,20.1
```

Errors from InfluxDB, such as a Flux syntax error, are returned as the tool result with
`isError: true`, so the assistant can read the message and correct its query.

## Configuration

All settings are environment variables.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `INFLUXDB_URL` | yes | | Base URL of InfluxDB, for example `http://influxdb:8086`. |
| `INFLUXDB_TOKEN` | yes | | InfluxDB API token. Use a read-only token. |
| `INFLUXDB_ORG` | yes | | Organization name or ID. |
| `INFLUXDB_TIMEOUT` | no | `30` | Seconds to wait for InfluxDB. |
| `MCP_HOST` | no | `0.0.0.0` | Address to listen on. |
| `MCP_PORT` | no | `5001` | Port to listen on. |
| `MCP_AUTH_TOKEN` | no | empty | If set, clients must send `Authorization: Bearer <value>`. |
| `MCP_ALLOWED_ORIGINS` | no | empty | Browser origins allowed to call the server, comma separated. |
| `MCP_MAX_ROWS` | no | `1000` | Upper limit for rows returned by one query. |
| `LOG_LEVEL` | no | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR`. |

The server refuses to start if a required variable is missing or a value is invalid, and logs
which one.

## Security

**Use a read-only InfluxDB token.** This is what actually makes the server read-only. The server
itself only lists buckets and runs queries, but Flux is a full language: a query can contain
`to()` and write data if the token permits it. The server does not try to filter Flux. Create a
token that can only read:

```bash
influx auth create --org YOUR_ORG --read-buckets --description "influx-mcp"
```

To limit it to a single bucket, use `--read-bucket BUCKET_ID` instead of `--read-buckets`.

**Set `MCP_AUTH_TOKEN` unless the port is reachable only by you.** Without it, anyone who can
reach the port can read everything the InfluxDB token can read.

**The server speaks plain HTTP.** Tokens and data are not encrypted in transit. Keep it on a
trusted network, or put a reverse proxy with TLS in front of it.

**Browser requests are rejected by default.** A request that carries an `Origin` header is
answered with `403` unless the origin is listed in `MCP_ALLOWED_ORIGINS`. This protects against
web pages that try to reach the server through your browser (DNS rebinding). Programs such as MCP
clients send no `Origin` header and are not affected.

Keep `.env` out of version control; it is listed in `.gitignore`.

## HTTP interface

| Request | Answer |
|---|---|
| `POST /mcp` with a JSON-RPC request | `200` and a JSON-RPC response as `application/json`. |
| `POST /mcp` with a notification | `202`, no body. |
| `GET /mcp`, `DELETE /mcp` | `405`: no streams and no sessions are offered. |
| `GET /health` | `200` and `{"status": "ok", ...}`. Needs no token. Does not contact InfluxDB. |

Other status codes: `400` invalid JSON, `401` missing or wrong bearer token, `403` origin not
allowed, `404` unknown path, `413` request larger than 1 MB.

Supported protocol revisions: `2025-11-25`, `2025-06-18`, `2025-03-26` and `2024-11-05`. The
server implements the tools part of the protocol; it offers no resources and no prompts.

## Development

Run the tests:

```bash
python -m unittest -v
```

The tests start the real server on a free local port and point it at a small stand-in for
InfluxDB, so no database is needed.

Build the image locally:

```bash
docker build -t influx-mcp .
```

GitHub Actions runs the tests on every push and pull request. A push to `main` publishes
`ghcr.io/thenavy/influx-mcp:latest`; a tag such as `v1.2.3` publishes `1.2.3` and `1.2`.

## Limitations

- InfluxDB 2.x with Flux only. InfluxDB 1.x (InfluxQL) and 3.x (SQL) are not supported.
- Protocol revisions newer than `2025-11-25` are not implemented. Clients are expected to fall
  back to the `initialize` handshake.
- No TLS and no OAuth; see [Security](#security).
- A query result is limited to `MCP_MAX_ROWS` rows and roughly 250,000 characters.

## License

[GNU General Public License v3.0](LICENSE)
