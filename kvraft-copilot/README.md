# KVRaft Copilot — AI‑Native MCP Server

**An MCP (Model Context Protocol) server** for the [KVRaft](https://github.com/KrishnaAdityaSrivastava/Rust-Database) distributed key‑value database. Built with [NitroStack](https://nitrostack.ai) and TypeScript, it exposes the live cluster as structured tools to any MCP‑compatible AI agent (Cursor, Claude Desktop, GitHub Copilot, etc.).

---

## Architecture

```mermaid
flowchart TD
    A[AI Agent (MCP client)] --> B[KVRaft Copilot – NitroStack MCP server]
    B -->|HTTP fetch (6 s timeout)| C[Go REST Gateway (⟨KVRAFT_GATEWAY_URL⟩)]
    C -->|gRPC / Raft| D[KVRaft Cluster (node1:7001, node2:7002, node3:7003)]
```

The Copilot connects to the **Go REST gateway** (`KVRAFT_GATEWAY_URL`) and offers a suite of tightly‑typed tools that give AI agents first‑class access to the cluster.

---

## Quick Start

```bash
# Configure gateway URL and optional flags
cp .env.example .env
# Edit .env to point KVRAFT_GATEWAY_URL at your cluster
npm install
npm run dev            # STDIO transport – hot‑reload (dev)
npm start              # Build → HTTP/SSE transport (prod)
```

Or with Docker (run from the repo root):

```bash
docker compose up --build
# Copilot reachable at http://localhost:3001
```

---

## Configuration

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KVRAFT_GATEWAY_URL` | `http://127.0.0.1:8080` | URL of the KVRaft Go REST gateway |
| `NODE_ENV` | `development` | `production` enables dual STDIO + HTTP/SSE transport |
| `MCP_TRANSPORT_TYPE` | `stdio` (dev) / `dual` (prod) | Force transport: `stdio` \| `http` \| `dual` |
| `PORT` | `3000` | HTTP bind port (prod only) |
| `HOST` | `localhost` | HTTP bind address (prod only) |
| `ENABLE_CORS` | — | Set to `true` to enable CORS headers |
| `NITRO_LOG_LEVEL` | `info` | Log verbosity |
| `NITRO_MCP_PROTOCOL_VERSION` | `auto` | MCP wire version – see [MCP Protocol](#mcp-protocol) |
| `NITROSTACK_APP_MODE` | `universal` | NitroStack app mode |

---

## MCP Tools

All tools are registered via the `@Tool` decorator in [`src/modules/kvraft/kvraft.tools.ts`](src/modules/kvraft/kvraft.tools.ts). The server now provides **10** tools.

---

### `get_cluster_status`
> Get KVRaft cluster status, including the reported leader ID, Raft term, and status message.

**Parameters:** none

**Returns:**
| Field | Type | Description |
| :--- | :--- | :--- |
| `gateway` | `string` | Gateway URL queried |
| `success` | `boolean` | Request success |
| `message` | `string` | Human‑readable status |
| `leader_id` | `number \| null` | Current Raft leader |
| `term` | `number \| null` | Current Raft term |

**Example:**
```json
{ "tool": "get_cluster_status", "input": {} }
```
---

### `diagnose_nodes`
> Check every configured KVRaft node independently. Reports reachability, health, latency, errors, leader ID and Raft term. Read‑only.

**Parameters:** none

**Returns:**
| Field | Type | Description |
| :--- | :--- | :--- |
| `gateway` | `string` | Gateway URL |
| `success` | `boolean` | All nodes healthy |
| `message` | `string` | Summary message |
| `cluster_size` | `number` | Total nodes |
| `healthy_nodes` | `number` | Nodes that responded healthy |
| `nodes` | `NodeHealth[]` | Per‑node details |

`NodeHealth` fields are documented in the original README.
---

### `get_value`
> Read a key and its typed value.

**Parameters:** `key: string`

**Returns:** `key`, `success`, `message`, `value` (typed), `leader_id`, `term`.
---

### `set_value`
> Write a typed value (`string`, `int`, `float`).

**Parameters:** `key`, `value_type`, `value` (string).

**Returns:** `key`, `value_type`, `success`, `message`, `leader_id`, `term`.
---

### `delete_key`
> Delete a key.

**Parameters:** `key`

**Returns:** `key`, `success`, `message`, `leader_id`, `term`.
---

### `get_node_details`
> Inspect a particular node’s Raft state and log information.

**Parameters:**
| Parameter | Type | Description |
| :--- | :--- | :--- |
| `node_id` | `number` (≥ 1) | Identifier of the node to inspect |

**Returns:** raw response from the gateway endpoint `/health/node/:node_id`. Typical fields include `node_id`, `address`, `leader_id`, `term`, `log_last_index`, `log_last_term`, `state`, and any recent errors.
---

### `get_cluster_metrics`
> Query Prometheus for request rates, latency, error counts, and other cluster‑wide metrics.

**Parameters:** none

**Returns:** an object mirroring Prometheus’ JSON output, e.g. `{ request_rate: number, avg_latency_ms: number, error_rate: number, ... }`.
---

### `get_recent_errors`
> Retrieve and summarize recent node errors or structured logs.

**Parameters:** optional `limit: number` (1‑100, default 20).

**Returns:** `{ errors: [{ node_id, timestamp, level, message, stack? }], total: number }`.
---

### `diagnose_cluster`
> Run a full diagnostic sweep, aggregating node health, metrics, and recent errors into a structured report.

**Parameters:** none

**Returns:** a comprehensive object containing `cluster_status`, `node_health`, `metrics`, `errors`, and a high‑level `summary` string.
---

### `recommend_recovery`
> Suggest a recovery plan based on a supplied failure summary.

**Parameters:** `failure_summary: string`

**Returns:** `{ plan: string, steps: string[], risk: "low" | "medium" | "high", estimated_time: string }`.
---

## MCP Protocol

The server auto‑negotiates between MCP wire revisions (controlled by `NITRO_MCP_PROTOCOL_VERSION`):
| Value | Spec | Notes |
| :--- | :--- | :--- |
| `auto` *(default)* | Both | Serves the `2026‑07‑28` stateless spec with fallback to the legacy `2025‑06‑18` sessionful spec |
| `2026‑07‑28` | Stateless | `server/discover`, per‑request `_meta`, `Mcp‑Method`/`Mcp‑Name` headers, cache hints, MRTR, extensions |
| `2025‑06‑18` | Sessionful | Classic session‑based SSE transport |

Your `@Tool` / `@Resource` / `@Prompt` / `@Widget` code works unchanged across versions.
---

## Common Commands

```bash
npm run dev      # Development: STDIO transport with hot‑reload
npm run build    # Compile TypeScript → dist/
npm start        # Production: build + HTTP/SSE transport
npm run upgrade  # Upgrade NitroStack dependencies
```

---

## Visual Enhancements

All tool responses include a `gateway` field for traceability. Widgets such as `@Widget('cluster-health')` automatically render rich UI panels in MCP‑aware clients.

---

*For a deeper dive into each tool’s JSON schema, see the corresponding `*.json` files under `~/.gemini/antigravity/mcp/kvraft/`.*
