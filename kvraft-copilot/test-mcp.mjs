
const MCP_URL = process.env.MCP_URL ?? 'http://127.0.0.1:3001/mcp';
const RUN_DESTRUCTIVE_TESTS = process.env.RUN_DESTRUCTIVE_TESTS === '1';

const CLIENT_INFO = {
  name: 'kvraft-local-tester',
  version: '1.0.0',
};

const PROTOCOL_VERSION = '2025-03-26';
const TEST_KEY = 'mcp_test';

const readOnlyTests = [
  { name: 'get_cluster_status' },
  { name: 'diagnose_nodes' },
  { name: 'get_node_details', args: { node_id: 1 } },
  { name: 'get_cluster_metrics' },
  { name: 'get_recent_errors', args: { limit: 10 } },
  { name: 'diagnose_cluster' },
  {
    name: 'recommend_recovery',
    args: {
      failure_summary:
        'One Raft node is unreachable while the remaining nodes are available.',
    },
  },
  { name: 'get_value', args: { key: 'name' } },
];

const keyValueTests = [
  {
    name: 'set_value',
    args: { key: TEST_KEY, value: 'hello', value_type: 'string' },
  },
  { name: 'get_value', args: { key: TEST_KEY } },
  { name: 'delete_key', args: { key: TEST_KEY } },
];

const lifecycleTests = [
  { name: 'list_node_containers' },
  { name: 'start_node', args: { node_id: 4 } },
  { name: 'get_node_details', args: { node_id: 4 } },
  { name: 'stop_node', args: { node_id: 4 } },
  { name: 'start_node', args: { node_id: 4 } },
  { name: 'restart_node', args: { node_id: 4 } },
];

const membershipTests = [
  { name: 'register_node', args: { node_id: 4, addr: 'node4:6004' } },
  { name: 'diagnose_nodes' },
  { name: 'remove_node', args: { node_id: 4 } },
  { name: 'diagnose_nodes' },
];

let sessionId;
let nextRequestId = 0;

function getHeaders(extra = {}) {
  return {
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
    ...(sessionId ? { 'Mcp-Session-Id': sessionId } : {}),
    ...extra,
  };
}

async function post(body, headers = {}) {
  const response = await fetch(MCP_URL, {
    method: 'POST',
    headers: getHeaders(headers),
    body: JSON.stringify(body),
  });

  const newSessionId = response.headers.get('Mcp-Session-Id');
  if (newSessionId) sessionId = newSessionId;

  const text = await response.text();

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${text}`);
  }

  return { response, text };
}

function parseResponse(body, expectedId) {
  if (!body.trim()) return null;

  const candidates = [];

  // Streamable HTTP can return JSON or Server-Sent Events.
  if (body.includes('data:')) {
    for (const event of body.split(/\r?\n\r?\n/)) {
      const data = event
        .split(/\r?\n/)
        .filter((line) => line.startsWith('data:'))
        .map((line) => line.slice(5).trim())
        .join('\n');

      if (!data || data === '[DONE]') continue;

      try {
        candidates.push(JSON.parse(data));
      } catch {
        // Ignore non-JSON SSE events.
      }
    }
  } else {
    try {
      candidates.push(JSON.parse(body));
    } catch {
      throw new Error(`Unexpected response: ${body}`);
    }
  }

  const payload = candidates.find(
    (item) => item?.id === expectedId,
  );

  if (!payload) {
    const error = candidates.find((item) => item?.error);
    if (error) throw new Error(JSON.stringify(error.error));

    throw new Error(`No JSON-RPC response found for request ${expectedId}`);
  }

  if (payload.error) {
    throw new Error(JSON.stringify(payload.error));
  }

  return payload.result ?? payload;
}

async function rpc(method, params = {}) {
  const id = ++nextRequestId;

  const { text } = await post({
    jsonrpc: '2.0',
    id,
    method,
    params,
  });

  return parseResponse(text, id);
}

async function notifyInitialized() {
  const { response, text } = await post(
    {
      jsonrpc: '2.0',
      method: 'notifications/initialized',
    },
  );

  // Notifications do not have JSON-RPC response IDs.
  // A 202 Accepted or 200 OK is commonly used.
  if (response.status !== 202 && response.status !== 200) {
    console.warn('Initialization notification:', response.status, text);
  }
}

async function listTools() {
  const result = await rpc('tools/list');
  const tools = result?.tools ?? [];

  console.log('\nAvailable MCP tools:');
  console.log(tools.map((tool) => `- ${tool.name}`).join('\n') || '(none)');

  return new Map(tools.map((tool) => [tool.name, tool]));
}

async function runTest(test, availableTools) {
  const tool = availableTools.get(test.name);

  if (!tool) {
    console.log(`\n--- ${test.name}: unavailable ---`);
    return;
  }

  console.log(`\n--- ${test.name} ---`);

  try {
    const result = await rpc('tools/call', {
      name: test.name,
      arguments: test.args ?? {},
    });

    console.log(JSON.stringify(result, null, 2));

    if (result?.isError) {
      console.error('Tool reported an error.');
    }
  } catch (error) {
    console.error(
      'Tool failed:',
      error instanceof Error ? error.message : String(error),
    );
  }
}

async function runSuite(title, tests, availableTools) {
  console.log(`\n========== ${title} ==========`);

  for (const test of tests) {
    await runTest(test, availableTools);
  }
}


async function testDynamicMembership(availableTools) {
  const nodeId = 4;
  const address = 'node4:6004';

  async function callTool(name, args = {}) {
    if (!availableTools.has(name)) {
      throw new Error(`Required MCP tool is missing: ${name}`);
    }

    const result = await rpc('tools/call', {
      name,
      arguments: args,
    });

    console.log(`\n[${name}]`);
    console.log(JSON.stringify(result, null, 2));

    if (result?.isError) {
      throw new Error(`${name} returned an MCP tool error`);
    }

    return result;
  }

  console.log('\n========== DYNAMIC MEMBERSHIP TEST ==========');

  // 1. Capture the cluster before changing membership.
  console.log('\n--- BEFORE: cluster status ---');
  const before = await callTool('get_cluster_status');
  await callTool('diagnose_nodes');

  // 2. Ensure node 4 is running.
  await callTool('list_node_containers');
  await callTool('start_node', { node_id: nodeId });

  // 3. Inspect node 4 before registering it.
  try {
    await callTool("get_node_details", { node_id: 4 });
  } catch (error) {
    console.log(`Node 4 is not registered yet: ${error.message}`);
  }

  // Continue to registration instead of aborting.
  console.log("\n--- REGISTER: node 4 ---");
  const registration = await callTool("register_node", {
    node_id: 4,
    addr: "node4:6004",
  });

  console.log(JSON.stringify(registration, null, 2));

  // 4. Request a membership change.
  // const registration = await callTool('register_node', {
  //   node_id: nodeId,
  //   addr: address,
  // });

  console.log('\n--- REGISTRATION REQUEST COMPLETED ---');

  // 5. Query the cluster again; do not infer success from registration alone.
  const after = await callTool('get_cluster_status');
  const diagnostics = await callTool('diagnose_nodes');
  const nodeDetails = await callTool('get_node_details', {
    node_id: nodeId,
  });

  console.log('\n========== MEMBERSHIP TEST EVIDENCE ==========');
  console.log('Before:', JSON.stringify(before, null, 2));
  console.log('Registration:', JSON.stringify(registration, null, 2));
  console.log('After:', JSON.stringify(after, null, 2));
  console.log('Diagnostics:', JSON.stringify(diagnostics, null, 2));
  console.log('Node 4:', JSON.stringify(nodeDetails, null, 2));

  console.log(
    '\nInspect the responses for committed membership, node 4 in the ' +
    'peer/configuration list, and successful node communication. ' +
    'A successful tool response alone does not prove membership changed.',
  );
}


async function main() {
  console.log(`Connecting to ${MCP_URL}`);

  await rpc('initialize', {
    protocolVersion: PROTOCOL_VERSION,
    capabilities: {},
    clientInfo: CLIENT_INFO,
  });

  await notifyInitialized();

  const availableTools = await listTools();

  // await runSuite('Read-only tests', readOnlyTests, availableTools);
  // await runSuite('Key-value tests', keyValueTests, availableTools);

  if (RUN_DESTRUCTIVE_TESTS) {
    console.warn(
      '\nDestructive tests enabled: these may change cluster state.',
    );
    await testDynamicMembership(availableTools);
    await runSuite('Node lifecycle tests', lifecycleTests, availableTools);
    await runSuite('Membership tests', membershipTests, availableTools);
  } else {
    console.log(
      '\nSkipping node lifecycle and membership tests. ' +
      'Set RUN_DESTRUCTIVE_TESTS=1 to enable them.',
    );
  }
}

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

async function waitForNode(nodeId, availableTools, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;

  while (Date.now() < deadline) {
    try {
      const result = await rpc('tools/call', {
        name: 'get_node_details',
        arguments: { node_id: nodeId },
      });

      if (!result?.isError) {
        return result;
      }

      lastError = result;
    } catch (error) {
      lastError = error;
    }

    await sleep(500);
  }

  throw new Error(
    `Node ${nodeId} was not available after ${timeoutMs}ms. ` +
    `Last result: ${JSON.stringify(lastError)}`
  );
}

main().catch((error) => {
  console.error(
    '\nMCP test failed:',
    error instanceof Error ? error.message : String(error),
  );

  process.exitCode = 1;
});

