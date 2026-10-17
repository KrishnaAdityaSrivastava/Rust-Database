import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const transport = new StreamableHTTPClientTransport(
  new URL('http://127.0.0.1:3001/mcp')
);

const client = new Client({
  name: 'kvraft-local-tester',
  version: '1.0.0',
});

try {
  await client.connect(transport);

  const { tools } = await client.listTools();

  console.log('\nAvailable MCP tools:');
  console.log(tools.map((tool) => tool.name).join('\n'));

  for (const [name, args] of [
    ['get_cluster_status', {}],
    ['diagnose_nodes', {}],
    ['get_node_details', { node_id: 1 }],
    ['get_value', { key: 'name' }],
    ['get_cluster_metrics', {}],
    ['get_recent_errors', { limit: 10 }],
    ['diagnose_cluster', {}],
    [
      'recommend_recovery',
      {
        failure_summary:
          'One Raft node is unreachable while the remaining nodes are available.',
      },
    ],
  ]) {
    if (!tools.some((tool) => tool.name === name)) {
      console.log(`\n--- ${name}: tool not available ---`);
      continue;
    }

    console.log(`\n--- ${name} ---`);

    try {
      const result = await client.callTool({
        name,
        arguments: args,
      });

      console.log(JSON.stringify(result, null, 2));
    } catch (error) {
      console.error(`Tool failed: ${name}`, error);
    }
  }
} catch (error) {
  console.error('MCP test failed:', error);
  process.exitCode = 1;
} finally {
  await client.close().catch(() => {});
}
