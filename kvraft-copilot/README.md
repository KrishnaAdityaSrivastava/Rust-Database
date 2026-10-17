# KVRaft Copilot — AI-Native MCP Server

An **MCP (Model Context Protocol) server** for the [KVRaft](https://github.com/KrishnaAdityaSrivastava/Rust-Database) distributed key-value database. Built with [NitroStack](https://nitrostack.ai) and TypeScript, it exposes the live cluster as structured tools to any MCP-compatible AI agent (Cursor, Claude Desktop, GitHub Copilot, etc.).

## Architecture

```
AI Agent (MCP client)
      │
      ▼
KVRaft Copilot  ←─  NitroStack MCP server (this repo)
      │  HTTP fetch (6 s timeout)
      ▼
Go REST Gateway  (:8080 by default)
      │  gRPC / Raft
      ▼
KVRaft Cluster  (Raft nodes: node1:7001, node2:7002, node3:7003)
```

The Copilot connects to the **Go REST gateway** (`KVRAFT_GATEWAY_URL`) and exposes five MCP tools that give AI agents first-class, type-safe access to the cluster.

---

## Quick Start

```bash
cp .env.example .env   # set KVRAFT_GATEWAY_URL (and any optional vars)
npm install
npm run dev            # STDIO transport — development / hot-reload
npm start              # build → HTTP/SSE transport — production
```

Or with Docker (from the repo root):

```bash
docker compose up --build
# Copilot available at http://localhost:3001
```

---

## Configuration

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KVRAFT_GATEWAY_URL` | `http://127.0.0.1:8080` | KVRaft Go REST gateway URL |
| `NODE_ENV` | `development` | `production` enables dual STDIO + HTTP/SSE transport |
| `MCP_TRANSPORT_TYPE` | `stdio` (dev) / `dual` (prod) | Force a transport: `stdio` \| `http` \| `dual` |
| `PORT` | `3000` | HTTP bind port (production only) |
| `HOST` | `localhost` | HTTP bind address (production only) |
| `ENABLE_CORS` | — | Set to `true` to enable CORS headers |
| `NITRO_LOG_LEVEL` | `info` | Log verbosity |
| `NITRO_MCP_PROTOCOL_VERSION` | `auto` | MCP wire version — see [MCP Protocol](#mcp-protocol) |
| `NITROSTACK_APP_MODE` | `universal` | NitroStack app mode |

---

## MCP Tools

All five tools are registered via the `@Tool` decorator in [`src/modules/kvraft/kvraft.tools.ts`](src/modules/kvraft/kvraft.tools.ts).

---

### `get_cluster_status`

> Get KVRaft cluster status, including the reported leader ID, Raft term, and status message.

**Parameters:** none

**Returns:**

| Field | Type | Description |
| :--- | :--- | :--- |
| `gateway` | `string` | Gateway URL that was queried |
| `success` | `boolean` | Whether the request succeeded |
| `message` | `string` | Human-readable status |
| `leader_id` | `number \| null` | Current Raft leader node ID |
| `term` | `number \| null` | Current Raft term |

**Example invocation:**

```json
{
  "tool": "get_cluster_status",
  "input": {}
}
```

**Example response:**

```json
{
  "gateway": "http://127.0.0.1:8080",
  "success": true,
  "message": "Leader is node 1",
  "leader_id": 1,
  "term": 4
}
```

---

### `diagnose_nodes`

> Check every configured KVRaft node independently. Reports reachability, health, response latency, errors, leader ID, and Raft term. Use this to investigate suspected node crashes or cluster availability problems. This is a **read-only** diagnostic — it does not restart nodes.

**Parameters:** none

**Returns:**

| Field | Type | Description |
| :--- | :--- | :--- |
| `gateway` | `string` | Gateway URL queried |
| `success` | `boolean` | `true` if all nodes responded healthily |
| `message` | `string` | e.g. `"3/3 nodes responding healthily"` |
| `cluster_size` | `number` | Total configured node count |
| `healthy_nodes` | `number` | Count of reachable, healthy nodes |
| `nodes` | `NodeHealth[]` | Per-node diagnostic details (see below) |

**`NodeHealth` object:**

| Field | Type | Description |
| :--- | :--- | :--- |
| `node_id` | `number` | Node identifier |
| `address` | `string` | e.g. `"node1:7001"` |
| `reachable` | `boolean` | Whether the node responded |
| `healthy` | `boolean` | Whether the node passed health check |
| `latency_ms` | `number` | Round-trip latency in milliseconds |
| `message` | `string?` | Role description, e.g. `"Leader"` / `"Follower"` |
| `error` | `string?` | Error message if unreachable |
| `leader_id` | `number \| null` | Leader as seen by this node |
| `term` | `number?` | Raft term as seen by this node |

**Example invocation:**

```json
{
  "tool": "diagnose_nodes",
  "input": {}
}
```

**Example response:**

```json
{
  "gateway": "http://127.0.0.1:8080",
  "success": true,
  "message": "3/3 nodes responding healthily",
  "cluster_size": 3,
  "healthy_nodes": 3,
  "nodes": [
    { "node_id": 1, "address": "node1:7001", "reachable": true, "healthy": true, "latency_ms": 2, "message": "Leader",   "leader_id": 1, "term": 1 },
    { "node_id": 2, "address": "node2:7002", "reachable": true, "healthy": true, "latency_ms": 3, "message": "Follower", "leader_id": 1, "term": 1 },
    { "node_id": 3, "address": "node3:7003", "reachable": true, "healthy": true, "latency_ms": 2, "message": "Follower", "leader_id": 1, "term": 1 }
  ]
}
```

> **Note:** This tool is decorated with `@Widget('cluster-health')`, so MCP clients that support NitroStack widgets will render a rich cluster-health panel automatically.

---

### `get_value`

> Read a key and its typed value from KVRaft.

**Parameters:**

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `key` | `string` (min length 1) | ✅ | Key to retrieve |

**Returns:**

| Field | Type | Description |
| :--- | :--- | :--- |
| `key` | `string` | The key that was read |
| `success` | `boolean` | Whether the key was found |
| `message` | `string` | Status detail |
| `value` | `{ type: "string" \| "int" \| "float", value: string \| number } \| null` | Typed value, or `null` if not found |
| `leader_id` | `number \| null` | Raft leader at read time |
| `term` | `number \| null` | Raft term at read time |

**Example invocation:**

```json
{
  "tool": "get_value",
  "input": { "key": "app:config:timeout" }
}
```

**Example response:**

```json
{
  "key": "app:config:timeout",
  "success": true,
  "message": "OK",
  "value": { "type": "int", "value": "30" },
  "leader_id": 1,
  "term": 4
}
```

---

### `set_value`

> Write a string, integer, or floating-point value to KVRaft through the Go gateway.

**Parameters:**

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `key` | `string` (min length 1) | ✅ | Key to write |
| `value_type` | `"string" \| "int" \| "float"` | ✅ | Declares the storage type |
| `value` | `string` | ✅ | Value to store (always passed as a string — see type rules below) |

**Type rules for `value`:**

| `value_type` | `value` format | Validation |
| :--- | :--- | :--- |
| `string` | Any UTF-8 string | None |
| `int` | Integer string, e.g. `"-42"` | Must match `/^-?\d+$/`; must fit in signed 64-bit (`i64`) range |
| `float` | Numeric string, e.g. `"3.14"` | Must parse as a finite `Number` |

**Returns:**

| Field | Type | Description |
| :--- | :--- | :--- |
| `key` | `string` | The key that was written |
| `value_type` | `string` | The type used |
| `success` | `boolean` | Whether the write succeeded |
| `message` | `string` | Status detail |
| `leader_id` | `number \| null` | Raft leader at write time |
| `term` | `number \| null` | Raft term at write time |

**Example invocations:**

```json
// Write a string
{ "tool": "set_value", "input": { "key": "env", "value_type": "string", "value": "production" } }

// Write an integer
{ "tool": "set_value", "input": { "key": "retries", "value_type": "int", "value": "5" } }

// Write a float
{ "tool": "set_value", "input": { "key": "threshold", "value_type": "float", "value": "0.95" } }
```

**Example response:**

```json
{
  "key": "retries",
  "value_type": "int",
  "success": true,
  "message": "OK",
  "leader_id": 1,
  "term": 4
}
```

---

### `delete_key`

> Delete a key from KVRaft through the Go HTTP gateway.

**Parameters:**

| Parameter | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `key` | `string` (min length 1) | ✅ | Key to delete |

**Returns:**

| Field | Type | Description |
| :--- | :--- | :--- |
| `key` | `string` | The key that was deleted |
| `success` | `boolean` | Whether the delete succeeded |
| `message` | `string` | Status detail |
| `leader_id` | `number \| null` | Raft leader at delete time |
| `term` | `number \| null` | Raft term at delete time |

**Example invocation:**

```json
{
  "tool": "delete_key",
  "input": { "key": "app:config:timeout" }
}
```

**Example response:**

```json
{
  "key": "app:config:timeout",
  "success": true,
  "message": "Key deleted",
  "leader_id": 1,
  "term": 4
}
```

---

## MCP Protocol

The server auto-negotiates between MCP wire revisions (controlled by `NITRO_MCP_PROTOCOL_VERSION`):

| Value | Spec | Notes |
| :--- | :--- | :--- |
| `auto` *(default)* | Both | Serves `2026-07-28` stateless spec with automatic fallback to the `2025-06-18` sessionful spec |
| `2026-07-28` | Stateless | `server/discover`, per-request `_meta`, `Mcp-Method`/`Mcp-Name` headers, cache hints, MRTR, extensions |
| `2025-06-18` | Sessionful (legacy) | Classic session-based SSE transport |

Your `@Tool` / `@Resource` / `@Prompt` / `@Widget` code is identical regardless of which version is active.

---

## Common Commands

```bash
npm run dev      # Development: STDIO transport with hot-reload
npm run build    # Compile TypeScript → dist/
npm start        # Production: build + HTTP/SSE transport
npm run upgrade  # Upgrade NitroStack dependencies
```
