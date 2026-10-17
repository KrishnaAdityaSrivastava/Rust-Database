
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

const transport = new StdioClientTransport({
  command: 'npm',
  args: ['run', 'start'],
  env: {
    ...process.env,
    KVRAFT_GRPC_ADDR: '127.0.0.1:8002',
  },
  stderr: 'inherit',
});

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
    ['get_value', { key: 'name' }],
  ]) {
    console.log(`\n--- ${name} ---`);
    const result = await client.callTool({ name, arguments: args });
    console.log(JSON.stringify(result, null, 2));
  }
} catch (error) {
  console.error('MCP test failed:', error);
  process.exitCode = 1;
} finally {
  await client.close();
}
