
import {
  ToolDecorator as Tool,
  Widget,
  ExecutionContext,
  z,
} from '@nitrostack/core';

import Docker from 'dockerode';

const docker = new Docker({
  socketPath: '/var/run/docker.sock',
});

type WireValue = {
  type: 'string' | 'int' | 'float';
  value: string | number;
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
  [key: string]: any;
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


export class KVRaftTools {
  private readonly baseUrl = (
    process.env.KVRAFT_GATEWAY_URL ?? 'http://127.0.0.1:8080'
  ).replace(/\/+$/, '');

  private async addNodeToCluster(
    id: number,
    addr: string,
  ): Promise<WireResponse> {
    return this.request('/nodes/add', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        id,
        addr,
      }),
    });
  }

  private async removeNodeFromCluster(
    id: number,
  ): Promise<WireResponse> {
    return this.request(
      `/nodes/remove?id=${encodeURIComponent(id)}`,
      { method: 'DELETE' },
    );
  }

  private async request(
    path: string,
    init: RequestInit = {},
  ): Promise<WireResponse> {
    let response: Response;

    try {
      response = await fetch(`${this.baseUrl}${path}`, {
        ...init,
        signal: AbortSignal.timeout(6000),
      });
    } catch (error) {
      throw new Error(
        `Cannot reach KVRaft gateway at ${this.baseUrl}: ${error instanceof Error ? error.message : String(error)
        }`,
      );
    }

    const body = await response.text();
    let result: WireResponse;

    try {
      result = JSON.parse(body) as WireResponse;
    } catch {
      throw new Error(
        `Invalid response from KVRaft gateway (HTTP ${response.status}): ${body}`,
      );
    }

    if (!response.ok) {
      throw new Error(
        `KVRaft gateway returned HTTP ${response.status}: ${result.message ?? body
        }`,
      );
    }

    return result;
  }

  @Tool({
    name: 'get_cluster_status',
    description:
      'Get KVRaft cluster status, including the reported leader ID, Raft term, and status message.',
    inputSchema: z.object({}),
  })
  @Widget('ClusterStatus')
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
    description:
      'Check every configured KVRaft node independently. Reports reachability, health, response latency, errors, leader ID, and Raft term. Use this to investigate suspected node crashes or cluster availability problems. This is a read-only diagnostic and does not restart nodes.',
    inputSchema: z.object({}),
    invocation: {
      invoking: 'Checking KVRaft nodes...',
      invoked: 'Node diagnostics complete',
    },
    examples: {
      request: {},
      response: {
        gateway: 'http://127.0.0.1:8080',
        success: true,
        message: '3/3 nodes responding healthily',
        cluster_size: 3,
        healthy_nodes: 3,
        nodes: [
          {
            node_id: 1,
            address: 'node1:7001',
            reachable: true,
            healthy: true,
            latency_ms: 2,
            message: 'Leader',
            leader_id: 1,
            term: 1,
          },
          {
            node_id: 2,
            address: 'node2:7002',
            reachable: true,
            healthy: true,
            latency_ms: 3,
            message: 'Follower',
            leader_id: 1,
            term: 1,
          },
          {
            node_id: 3,
            address: 'node3:7003',
            reachable: true,
            healthy: true,
            latency_ms: 2,
            message: 'Follower',
            leader_id: 1,
            term: 1,
          },
        ],
      },
    },
  })
  @Widget('DiagnoseNodes')
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
  @Widget('GetValue')
  async getValue(
    input: { key: string },
    ctx: ExecutionContext,
  ) {
    const result = await this.request(
      `/kv/${encodeURIComponent(input.key)}`,
    );

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
    description:
      'Write a string, integer, or floating-point value to KVRaft through the Go gateway.',
    inputSchema: z.object({
      key: z.string().min(1).describe('Key to write'),
      value_type: z.enum(['string', 'int', 'float']),
      value: z
        .string()
        .describe(
          'Value to store. Use an integer string for int or a numeric string for float.',
        ),
    }),
  })
  @Widget('SetValue')
  async setValue(
    input: {
      key: string;
      value_type: 'string' | 'int' | 'float';
      value: string;
    },
    ctx: ExecutionContext,
  ) {
    let wireValue: WireValue;

    switch (input.value_type) {
      case 'string':
        wireValue = {
          type: 'string',
          value: input.value,
        };
        break;

      case 'int': {
        if (!/^-?\d+$/.test(input.value)) {
          throw new Error(
            'For type "int", value must be an integer string.',
          );
        }

        const parsed = BigInt(input.value);

        if (
          parsed < -(2n ** 63n) ||
          parsed > 2n ** 63n - 1n
        ) {
          throw new Error(
            'Integer is outside the signed 64-bit range.',
          );
        }

        wireValue = {
          type: 'int',
          value: Number(input.value),
        };
        break;
      }

      case 'float': {
        const parsed = Number(input.value);

        if (!Number.isFinite(parsed)) {
          throw new Error(
            'For type "float", value must be a finite number.',
          );
        }

        wireValue = {
          type: 'float',
          value: parsed,
        };
        break;
      }
    }

    const result = await this.request(
      `/kv/${encodeURIComponent(input.key)}`,
      {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(wireValue),
      },
    );

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
    description:
      'Delete a key from KVRaft through the Go HTTP gateway.',
    inputSchema: z.object({
      key: z.string().min(1).describe('Key to delete'),
    }),
  })
  @Widget('DeleteKey')
  async deleteKey(
    input: { key: string },
    ctx: ExecutionContext,
  ) {
    const result = await this.request(
      `/kv/${encodeURIComponent(input.key)}`,
      {
        method: 'DELETE',
      },
    );

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
    description:
      "Inspect a particular node's Raft state and log information.",
    inputSchema: z.object({
      node_id: z
        .number()
        .int()
        .min(1)
        .describe('Identifier of the node to inspect'),
    }),
  })
  @Widget('NodeDetails')
  async getNodeDetails(
    input: { node_id: number },
    ctx: ExecutionContext,
  ) {
    const result = await this.request(
      `/health/node/${input.node_id}`,
    );

    ctx.logger.info(
      `Fetched details for node ${input.node_id}`,
    );

    return result;
  }

  @Tool({
    name: 'get_cluster_metrics',
    description:
      'Query Prometheus for request rates, latency, errors, and relevant cluster metrics.',
    inputSchema: z.object({}),
  })
  @Widget('ClusterMetrics')
  async getClusterMetrics(
    _input: any,
    ctx: ExecutionContext,
  ) {
    let response: Response;

    try {
      response = await fetch(`${this.baseUrl}/metrics`, {
        signal: AbortSignal.timeout(6000),
      });
    } catch (error) {
      throw new Error(
        `Cannot reach KVRaft gateway at ${this.baseUrl}: ${error instanceof Error ? error.message : String(error)
        }`,
      );
    }

    if (!response.ok) {
      throw new Error(
        `KVRaft gateway returned HTTP ${response.status} for /metrics`,
      );
    }

    const text = await response.text();

    type Sample = {
      labels: Record<string, string>;
      value: number;
    };

    type MetricEntry = {
      help: string;
      type: string;
      samples: Sample[];
    };

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

        if (!metrics[name]) {
          metrics[name] = {
            help: '',
            type: '',
            samples: [],
          };
        }

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

        if (!metrics[name]) {
          metrics[name] = {
            help: '',
            type: '',
            samples: [],
          };
        }

        metrics[name].type = type;
        currentName = name;
        continue;
      }

      if (line.startsWith('#')) continue;

      const braceOpen = line.indexOf('{');
      const braceClose = line.indexOf('}');

      let metricName: string;
      let labelsStr: string;
      let rest: string;

      if (braceOpen !== -1 && braceClose !== -1) {
        metricName = line.slice(0, braceOpen);
        labelsStr = line.slice(
          braceOpen + 1,
          braceClose,
        );
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
          const v = pair
            .slice(eqIdx + 1)
            .trim()
            .replace(/^"|"$/g, '');

          labels[k] = v;
        }
      }

      const name = metricName || currentName;

      if (!name) continue;

      if (!metrics[name]) {
        metrics[name] = {
          help: '',
          type: '',
          samples: [],
        };
      }

      metrics[name].samples.push({
        labels,
        value,
      });
    }

    ctx.logger.info('Retrieved cluster metrics');

    return {
      gateway: this.baseUrl,
      metrics,
    };
  }

  @Tool({
    name: 'get_recent_errors',
    description:
      'Retrieve and summarize recent node errors or structured logs.',
    inputSchema: z.object({
      limit: z
        .number()
        .int()
        .min(1)
        .max(100)
        .optional()
        .describe('Maximum number of errors to return'),
    }),
  })
  @Widget('RecentErrors')
  async getRecentErrors(
    input: { limit?: number },
    ctx: ExecutionContext,
  ) {
    const query = input.limit
      ? `?limit=${input.limit}`
      : '';

    const result = await this.request(
      `/errors/recent${query}`,
    );

    ctx.logger.info('Fetched recent errors');

    return result;
  }

  @Tool({
    name: 'diagnose_cluster',
    description:
      'Collect relevant evidence and return a structured diagnostic report.',
    inputSchema: z.object({}),
    invocation: {
      invoking: 'Running full cluster diagnostic...',
      invoked: 'Cluster diagnostic complete',
    },
  })
  @Widget('DiagnoseCluster')
  async diagnoseCluster(
    _input: any,
    ctx: ExecutionContext,
  ) {
    const result = await this.request('/diagnose');

    ctx.logger.info('Performed full cluster diagnostic');

    return result;
  }

  @Tool({
    name: 'register_node',
    description:
      'Request adding a running KVRaft node to the cluster through a Raft membership change. The node must be reachable at its Raft address. This changes cluster membership.',
    inputSchema: z.object({
      node_id: z.number().int().min(1).max(999),
      addr: z.string().min(1).max(255).describe(
        'Raft address reachable by cluster peers, for example node4:6004',
      ),
    }),
  })
  @Widget('RegisterNode')
  async registerNode(
    input: { node_id: number; addr: string },
    ctx: ExecutionContext,
  ) {
    if (!/^[a-zA-Z0-9._-]+:\d{1,5}$/.test(input.addr)) {
      throw new Error(
        'addr must be a host:port address, such as node4:6004.',
      );
    }

    const port = Number(input.addr.slice(input.addr.lastIndexOf(':') + 1));
    if (port < 1 || port > 65535) {
      throw new Error('addr port must be between 1 and 65535.');
    }

    const result = await this.addNodeToCluster(
      input.node_id,
      input.addr,
    );

    ctx.logger.info(
      `Requested KVRaft membership addition for node ${input.node_id}`,
    );

    return {
      node_id: input.node_id,
      address: input.addr,
      success: result.success,
      message: result.message,
      leader_id: result.leader_id ?? null,
      term: result.term ?? null,
    };
  }

  @Tool({
    name: 'remove_node',
    description:
      'Request removing a node from KVRaft cluster membership through Raft. This changes cluster membership and may affect availability.',
    inputSchema: z.object({
      node_id: z.number().int().min(1).max(999),
    }),
  })
  @Widget('RemoveNode')
  async removeNode(
    input: { node_id: number },
    ctx: ExecutionContext,
  ) {
    const result = await this.removeNodeFromCluster(input.node_id);

    ctx.logger.info(
      `Requested KVRaft membership removal for node ${input.node_id}`,
    );

    return {
      node_id: input.node_id,
      success: result.success,
      message: result.message,
      leader_id: result.leader_id ?? null,
      term: result.term ?? null,
    };
  }

  @Tool({
    name: 'start_node',
    description:
      'Create and start a new KVRaft Docker node with a unique ID. ' +
      'Automatically discovers existing cluster peers so the new node can connect to them on boot. ' +
      'Does not change Raft membership on existing nodes — call register_node after this.',
    inputSchema: z.object({
      node_id: z.number().int().min(1).max(999),
      peer_ids: z.array(z.number().int().min(1).max(999))
        .max(20)
        .default([])
        .describe(
          'Additional peer IDs to include beyond those auto-discovered from the running cluster. ' +
          'Leave empty to rely entirely on auto-discovery.',
        ),
    }),
    invocation: {
      invoking: 'Starting KVRaft node...',
      invoked: 'KVRaft node started',
    },
  })
  async startNode(
    input: { node_id: number; peer_ids: number[] },
    ctx: ExecutionContext,
  ) {
    const id = input.node_id;
    const explicitPeers = input.peer_ids ?? [];
    const name = `node${id}`;
    const raft = 6000 + id;
    const api = 7000 + id;
    const grpc = 8000 + id;

    if (id < 1 || id > 999 || !Number.isInteger(id)) {
      throw new Error('node_id must be an integer between 1 and 999.');
    }

    if (
      explicitPeers.some(
        peer =>
          !Number.isInteger(peer) ||
          peer < 1 ||
          peer > 999 ||
          peer === id,
      ) ||
      new Set(explicitPeers).size !== explicitPeers.length
    ) {
      throw new Error(
        'Peer IDs must be unique integers between 1 and 999 and cannot include the node itself.',
      );
    }

    // ── Auto-discover existing cluster peers ────────────────────────────────
    // The new node must boot knowing about all current cluster members so its
    // reconnect loop can establish outbound TCP connections to them.  Without
    // this, existing nodes add the new node to their peer lists (via the Raft
    // AddNode log entry) but the new node itself has an empty peer list and
    // cannot reach anyone, resulting in one-sided connectivity.
    const discoveredPeerIds: number[] = [];
    try {
      const healthResult = await this.request('/health/nodes');
      if (Array.isArray(healthResult.nodes)) {
        for (const node of healthResult.nodes as Array<{ node_id: number; reachable?: boolean }>) {
          const peerId = node.node_id;
          if (
            Number.isInteger(peerId) &&
            peerId >= 1 &&
            peerId <= 999 &&
            peerId !== id
          ) {
            discoveredPeerIds.push(peerId);
          }
        }
      }
    } catch {
      // Cluster may not be up yet (bootstrapping first node); that is fine.
      ctx.logger.info(
        `Could not reach cluster to discover peers for node ${id}; starting with explicit peer list only.`,
      );
    }

    // Merge discovered peers with any explicitly provided ones, deduplicating.
    const allPeerIds = [
      ...new Set([...discoveredPeerIds, ...explicitPeers]),
    ].filter(p => p !== id);

    ctx.logger.info(
      `Node ${id}: discovered peers [${discoveredPeerIds.join(', ')}], ` +
      `explicit peers [${explicitPeers.join(', ')}], ` +
      `final peer list [${allPeerIds.join(', ')}]`,
    );
    // ────────────────────────────────────────────────────────────────────────

    const existing = docker.getContainer(name);
    let info;

    try {
      info = await existing.inspect();
    } catch (error: any) {
      if (error.statusCode !== 404) {
        throw error;
      }
    }

    // Reuse an existing container instead of creating a duplicate.
    if (info) {
      if (info.State.Running) {
        return {
          success: true,
          node_id: id,
          container: name,
          status: 'already_running',
          message:
            `${name} is already running. Cluster membership was not changed.`,
        };
      }

      await existing.start();

      ctx.logger.info(`Started existing KVRaft container ${name}`);

      return {
        success: true,
        node_id: id,
        container: name,
        status: 'started_existing',
        message:
          `${name} was started using its existing configuration and data. ` +
          'Cluster membership was not changed.',
      };
    }

    const cmd = [
      '--id',
      String(id),
      '--addr',
      `0.0.0.0:${raft}`,
      '--api-addr',
      `0.0.0.0:${api}`,
      '--grpc-addr',
      `0.0.0.0:${grpc}`,
    ];

    if (allPeerIds.length > 0) {
      cmd.push(
        '--peers',
        allPeerIds
          .map(peer => `${peer}:node${peer}:${6000 + peer}`)
          .join(','),
      );
    }

    cmd.push('--dir', '/data', '--log');

    const container = await docker.createContainer({
      Image: 'kvraft-node:local',
      name,
      Cmd: cmd,
      ExposedPorts: {
        [`${raft}/tcp`]: {},
        [`${api}/tcp`]: {},
        [`${grpc}/tcp`]: {},
      },
      HostConfig: {
        NetworkMode: 'kvnet',
        RestartPolicy: { Name: 'unless-stopped' },
        Binds: [`node${id}_data:/data`],
        PortBindings: {
          [`${raft}/tcp`]: [{ HostPort: String(raft) }],
          [`${api}/tcp`]: [{ HostPort: String(api) }],
          [`${grpc}/tcp`]: [{ HostPort: String(grpc) }],
        },
      },
    });

    try {
      await container.start();
    } catch (error) {
      await container.remove({ force: true }).catch(() => { });
      throw error;
    }

    ctx.logger.info(`Started new KVRaft node ${id}`);

    return {
      success: true,
      node_id: id,
      container: name,
      status: 'started',
      ports: { raft, api, grpc },
      peers: allPeerIds,
      discovered_peers: discoveredPeerIds,
      message:
        `Node ${id} started with peer list [${allPeerIds.join(', ')}]. ` +
        `Call register_node next to add it to the Raft cluster membership.`,
    };
  }

  @Tool({
    name: 'stop_node',
    description:
      'Stop a KVRaft node container without deleting the container or persistent data. This may affect cluster availability.',
    inputSchema: z.object({
      node_id: z.number().int().min(1).max(999),
    }),
    invocation: {
      invoking: 'Stopping KVRaft node...',
      invoked: 'KVRaft node stopped',
    },
  })
  async stopNode(
    input: { node_id: number },
    ctx: ExecutionContext,
  ) {
    const name = `node${input.node_id}`;
    const container = docker.getContainer(name);
    const info = await container.inspect();

    if (info.State.Running) {
      await container.stop({ t: 10 });
    }

    ctx.logger.info(`Stopped KVRaft node ${input.node_id}`);

    return {
      success: true,
      node_id: input.node_id,
      status: 'stopped',
      data_preserved: true,
    };
  }

  @Tool({
    name: 'restart_node',
    description:
      'Restart an existing KVRaft node container while preserving its configuration and data.',
    inputSchema: z.object({
      node_id: z.number().int().min(1).max(999),
    }),
  })
  async restartNode(
    input: { node_id: number },
    ctx: ExecutionContext,
  ) {
    const name = `node${input.node_id}`;
    const container = docker.getContainer(name);

    await container.inspect();
    await container.restart({ t: 10 });

    ctx.logger.info(`Restarted KVRaft node ${input.node_id}`);

    return {
      success: true,
      node_id: input.node_id,
      status: 'restarted',
    };
  }

  @Tool({
    name: 'list_node_containers',
    description:
      'List KVRaft node containers, including stopped containers, and report their Docker state.',
    inputSchema: z.object({}),
  })
  async listNodeContainers(_input: any, ctx: ExecutionContext) {
    const containers = await docker.listContainers({ all: true });

    const nodes = containers
      .filter(container =>
        container.Names.some(name => /^\/node[1-9]\d*$/.test(name)),
      )
      .map(container => ({
        name: container.Names.find(name =>
          /^\/node[1-9]\d*$/.test(name),
        )?.slice(1),
        status: container.State,
        state_description: container.Status,
      }));

    ctx.logger.info(`Listed ${nodes.length} KVRaft node containers`);

    return { success: true, count: nodes.length, nodes };
  }


  @Tool({
    name: 'recommend_recovery',
    description:
      'Inspect current KVRaft cluster health and recommend recovery steps based on live diagnostics. This tool does not perform recovery actions.',
    inputSchema: z.object({}),
  })
  @Widget('RecoveryAdvisor')
  async recommendRecovery(
    _input: Record<string, never>,
    ctx: ExecutionContext,
  ) {
    const result = await this.request('/diagnose');

    ctx.logger.info('Generated recovery recommendations from live diagnostics');

    if (!result.success || !Array.isArray(result.nodes)) {
      return {
        success: false,
        diagnosis: 'Cluster health unknown',
        message: result.message ?? 'Could not obtain reliable cluster diagnostics.',
        recovery_steps: [
          {
            order: 1,
            action: 'inspect_gateway_and_node_logs',
            description:
              'Check gateway connectivity and node logs before attempting recovery.',
            risk: 'low',
          },
        ],
      };
    }

    const unhealthy = result.nodes.filter(
      (node: NodeHealth) => !node.reachable || !node.healthy,
    );

    if (unhealthy.length === 0) {
      return {
        success: true,
        diagnosis: 'Cluster healthy',
        message: `${result.healthy_nodes}/${result.cluster_size} nodes healthy; no node-failure recovery is indicated.`,
        leader_id: result.leader_id ?? null,
        term: result.term ?? null,
        recovery_steps: [],
      };
    }

    return {
      success: true,
      diagnosis: 'Node health issue detected',
      message: `${unhealthy.length} node(s) are unreachable or unhealthy.`,
      leader_id: result.leader_id ?? null,
      term: result.term ?? null,
      unhealthy_nodes: unhealthy.map(node => ({
        node_id: node.node_id,
        address: node.address,
        reachable: node.reachable,
        healthy: node.healthy,
        message: node.message,
      })),
      recovery_steps: [
        {
          order: 1,
          action: 'diagnose_nodes',
          description: 'Inspect each node and verify its reported role and term.',
          risk: 'none',
        },
        {
          order: 2,
          action: 'get_recent_errors',
          description: 'Review recent errors to identify the cause.',
          risk: 'none',
        },
        {
          order: 3,
          action: 'inspect_node_logs',
          description:
            'Inspect logs for the unhealthy nodes before restarting or changing membership.',
          risk: 'low',
        },
      ],
    };
  }
}
