import { ToolDecorator as Tool, Widget, ExecutionContext, z } from '@nitrostack/core';

type WireValue = {
  type: 'string' | 'int' | 'float';
  value: string | number;
};

type NodeHealth = {
  node_id: number;
  address: string;
  reachable: boolean;
  healthy: boolean;
  latency_ms: number;
  message?: string;
  error?: string;
  leader_id?: number | null;
  term?: number;
};

type WireResponse = {
  success: boolean;
  value?: WireValue | null;
  message: string;
  leader_id?: number | null;
  term?: number;
  cluster_size?: number;
  healthy_nodes?: number;
  nodes?: NodeHealth[];
  // Additional fields for new endpoints
  [key: string]: any;
};

export class KVRaftTools {
  private readonly baseUrl = (process.env.KVRAFT_GATEWAY_URL ?? 'http://127.0.0.1:8080').replace(/\\+$/,'' );

  private async request(path: string, init: RequestInit = {}): Promise<WireResponse> {
    let response: Response;
    try {
      response = await fetch(`${this.baseUrl}${path}`, { ...init, signal: AbortSignal.timeout(6000) });
    } catch (error) {
      throw new Error(`Cannot reach KVRaft gateway at ${this.baseUrl}: ${error instanceof Error ? error.message : String(error)}`);
    }
    const body = await response.text();
    let result: WireResponse;
    try { result = JSON.parse(body) as WireResponse; } catch { throw new Error(`Invalid response from KVRaft gateway (HTTP ${response.status}): ${body}`); }
    if (!response.ok) { throw new Error(`KVRaft gateway returned HTTP ${response.status}: ${result.message ?? body}`); }
    return result;
  }

  /*** Existing tool definitions ***/

  @Tool({
    name: 'get_cluster_status',
    description: 'Get KVRaft cluster status, including the reported leader ID, Raft term, and status message.',
    inputSchema: z.object({}),
  })
  async getClusterStatus(_input: any, ctx: ExecutionContext) {
    const result = await this.request('/status');
    ctx.logger.info('Retrieved KVRaft cluster status');
    return {
      gateway: this.baseUrl,
      success: result.success,
      message: result.message,
      leader_id: result.leader_id ?? null,
      term: result.term ?? null,
    };
  }

  @Tool({
    name: 'diagnose_nodes',
    description: 'Check every configured KVRaft node independently. Reports reachability, health, response latency, errors, leader ID, and Raft term. Use this to investigate suspected node crashes or cluster availability problems. This is a read-only diagnostic and does not restart nodes.',
    inputSchema: z.object({}),
    invocation: { invoking: 'Checking KVRaft nodes...', invoked: 'Node diagnostics complete' },
    examples: {
      request: {},
      response: {
        gateway: 'http://127.0.0.1:8080',
        success: true,
        message: '3/3 nodes responding healthily',
        cluster_size: 3,
        healthy_nodes: 3,
        nodes: [
          { node_id: 1, address: 'node1:7001', reachable: true, healthy: true, latency_ms: 2, message: 'Leader', leader_id: 1, term: 1 },
          { node_id: 2, address: 'node2:7002', reachable: true, healthy: true, latency_ms: 3, message: 'Follower', leader_id: 1, term: 1 },
          { node_id: 3, address: 'node3:7003', reachable: true, healthy: true, latency_ms: 2, message: 'Follower', leader_id: 1, term: 1 },
        ],
      },
    },
  })
  @Widget('cluster-health')
  async diagnoseNodes(_input: any, ctx: ExecutionContext) {
    const result = await this.request('/health/nodes');
    ctx.logger.info('Checked health of KVRaft nodes');
    return {
      gateway: this.baseUrl,
      success: result.success,
      message: result.message,
      cluster_size: result.cluster_size ?? 0,
      healthy_nodes: result.healthy_nodes ?? 0,
      nodes: result.nodes ?? [],
    };
  }

  @Tool({
    name: 'get_value',
    description: 'Read a key and its typed value from KVRaft.',
    inputSchema: z.object({
      key: z.string().min(1).describe('Key to retrieve'),
    }),
  })
  async getValue(input: { key: string }, ctx: ExecutionContext) {
    const result = await this.request(`/kv/${encodeURIComponent(input.key)}`);
    ctx.logger.info(`Read KVRaft key: ${input.key}`);
    return {
      key: input.key,
      success: result.success,
      message: result.message,
      value: result.value ?? null,
      leader_id: result.leader_id ?? null,
      term: result.term ?? null,
    };
  }

  @Tool({
    name: 'set_value',
    description: 'Write a string, integer, or floating-point value to KVRaft through the Go gateway.',
    inputSchema: z.object({
      key: z.string().min(1).describe('Key to write'),
      value_type: z.enum(['string', 'int', 'float']),
      value: z.string().describe('Value to store. Use an integer string for int or a numeric string for float.'),
    }),
  })
  async setValue(input: { key: string; value_type: 'string' | 'int' | 'float'; value: string }, ctx: ExecutionContext) {
    let wireValue: WireValue;
    switch (input.value_type) {
      case 'string':
        wireValue = { type: 'string', value: input.value };
        break;
      case 'int': {
        if (!/^-?\d+$/.test(input.value)) { throw new Error('For type "int", value must be an integer string.'); }
        const parsed = BigInt(input.value);
        if (parsed < -(2n ** 63n) || parsed > 2n ** 63n - 1n) { throw new Error('Integer is outside the signed 64-bit range.'); }
        wireValue = { type: 'int', value: Number(input.value) };
        break;
      }
      case 'float': {
        const parsed = Number(input.value);
        if (!Number.isFinite(parsed)) { throw new Error('For type "float", value must be a finite number.'); }
        wireValue = { type: 'float', value: parsed };
        break;
      }
    }
    const result = await this.request(`/kv/${encodeURIComponent(input.key)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(wireValue),
    });
    ctx.logger.info(`Wrote KVRaft key: ${input.key}`);
    return {
      key: input.key,
      value_type: input.value_type,
      success: result.success,
      message: result.message,
      leader_id: result.leader_id ?? null,
      term: result.term ?? null,
    };
  }

  @Tool({
    name: 'delete_key',
    description: 'Delete a key from KVRaft through the Go HTTP gateway.',
    inputSchema: z.object({
      key: z.string().min(1).describe('Key to delete'),
    }),
  })
  async deleteKey(input: { key: string }, ctx: ExecutionContext) {
    const result = await this.request(`/kv/${encodeURIComponent(input.key)}`, { method: 'DELETE' });
    ctx.logger.info(`Deleted KVRaft key: ${input.key}`);
    return {
      key: input.key,
      success: result.success,
      message: result.message,
      leader_id: result.leader_id ?? null,
      term: result.term ?? null,
    };
  }

  @Tool({
    name: 'get_node_details',
    description: "Inspect a particular node's Raft state and log information.",
    inputSchema: z.object({
      node_id: z.number().int().min(1).describe('Identifier of the node to inspect'),
    }),
  })
  async getNodeDetails(input: { node_id: number }, ctx: ExecutionContext) {
    const result = await this.request(`/health/node/${input.node_id}`);
    ctx.logger.info(`Fetched details for node ${input.node_id}`);
    return result;
  }

  @Tool({
    name: 'get_cluster_metrics',
    description: 'Query Prometheus for request rates, latency, errors, and relevant cluster metrics.',
    inputSchema: z.object({}),
  })
  async getClusterMetrics(_input: any, ctx: ExecutionContext) {
    // /metrics returns Prometheus text exposition format, not JSON.
    // Fetch it directly and parse it into a structured object.
    let response: Response;
    try {
      response = await fetch(`${this.baseUrl}/metrics`, { signal: AbortSignal.timeout(6000) });
    } catch (error) {
      throw new Error(`Cannot reach KVRaft gateway at ${this.baseUrl}: ${error instanceof Error ? error.message : String(error)}`);
    }
    if (!response.ok) {
      throw new Error(`KVRaft gateway returned HTTP ${response.status} for /metrics`);
    }
    const text = await response.text();

    // Parse Prometheus text exposition format into structured JSON.
    type Sample = { labels: Record<string, string>; value: number };
    type MetricEntry = { help: string; type: string; samples: Sample[] };
    const metrics: Record<string, MetricEntry> = {};
    let currentName = '';

    for (const rawLine of text.split('\n')) {
      const line = rawLine.trim();
      if (!line) continue;

      if (line.startsWith('# HELP ')) {
        const rest = line.slice('# HELP '.length);
        const spaceIdx = rest.indexOf(' ');
        if (spaceIdx === -1) continue;
        const name = rest.slice(0, spaceIdx);
        const help = rest.slice(spaceIdx + 1);
        if (!metrics[name]) metrics[name] = { help: '', type: '', samples: [] };
        metrics[name].help = help;
        currentName = name;
        continue;
      }

      if (line.startsWith('# TYPE ')) {
        const rest = line.slice('# TYPE '.length);
        const parts = rest.split(' ');
        if (parts.length < 2) continue;
        const name = parts[0];
        const type = parts[1];
        if (!metrics[name]) metrics[name] = { help: '', type: '', samples: [] };
        metrics[name].type = type;
        currentName = name;
        continue;
      }

      if (line.startsWith('#')) continue;

      // Sample line: metric_name[{labels}] value [timestamp]
      const braceOpen = line.indexOf('{');
      const braceClose = line.indexOf('}');
      let metricName: string;
      let labelsStr: string;
      let rest: string;

      if (braceOpen !== -1 && braceClose !== -1) {
        metricName = line.slice(0, braceOpen);
        labelsStr = line.slice(braceOpen + 1, braceClose);
        rest = line.slice(braceClose + 1).trim();
      } else {
        const spaceIdx = line.indexOf(' ');
        if (spaceIdx === -1) continue;
        metricName = line.slice(0, spaceIdx);
        labelsStr = '';
        rest = line.slice(spaceIdx + 1).trim();
      }

      const valuePart = rest.split(' ')[0];
      const value = parseFloat(valuePart);
      if (Number.isNaN(value)) continue;

      const labels: Record<string, string> = {};
      if (labelsStr) {
        for (const pair of labelsStr.split(',')) {
          const eqIdx = pair.indexOf('=');
          if (eqIdx === -1) continue;
          const k = pair.slice(0, eqIdx).trim();
          const v = pair.slice(eqIdx + 1).trim().replace(/^"|"$/g, '');
          labels[k] = v;
        }
      }

      const name = metricName || currentName;
      if (!name) continue;
      if (!metrics[name]) metrics[name] = { help: '', type: '', samples: [] };
      metrics[name].samples.push({ labels, value });
    }

    ctx.logger.info('Retrieved cluster metrics');
    return { gateway: this.baseUrl, metrics };
  }

  @Tool({
    name: 'get_recent_errors',
    description: 'Retrieve and summarize recent node errors or structured logs.',
    inputSchema: z.object({
      limit: z.number().int().min(1).max(100).optional().describe('Maximum number of errors to return'),
    }),
  })
  async getRecentErrors(input: { limit?: number }, ctx: ExecutionContext) {
    const query = input.limit ? `?limit=${input.limit}` : '';
    const result = await this.request(`/errors/recent${query}`);
    ctx.logger.info('Fetched recent errors');
    return result;
  }

  @Tool({
    name: 'diagnose_cluster',
    description: 'Collect relevant evidence and return a structured diagnostic report.',
    inputSchema: z.object({}),
    invocation: {
      invoking: 'Running full cluster diagnostic...',
      invoked: 'Cluster diagnostic complete',
    },
  })
  async diagnoseCluster(_input: any, ctx: ExecutionContext) {
    const result = await this.request('/diagnose');
    ctx.logger.info('Performed full cluster diagnostic');
    return result;
  }

  @Tool({
    name: 'recommend_recovery',
    description: 'Suggest a recovery plan based on the observed failure and its risks.',
    inputSchema: z.object({
      failure_summary: z.string().describe('Brief description of the observed failure'),
    }),
  })
  async recommendRecovery(input: { failure_summary: string }, ctx: ExecutionContext) {
    const result = await this.request('/recovery/recommend', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ summary: input.failure_summary }),
    });
    ctx.logger.info('Generated recovery recommendation');
    return result;
  }
}
