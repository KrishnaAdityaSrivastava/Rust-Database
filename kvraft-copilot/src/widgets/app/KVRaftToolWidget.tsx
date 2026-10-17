'use client';

import { useState, type CSSProperties } from 'react';
import { useWidgetSDK, useTheme } from '@nitrostack/widgets';

type ToolKind =
  | 'get_cluster_status'
  | 'get_value'
  | 'set_value'
  | 'delete_key'
  | 'get_node_details'
  | 'get_cluster_metrics'
  | 'get_recent_errors'
  | 'diagnose_cluster'
  | 'recommend_recovery';

type Field = { name: string; label: string; placeholder: string; type?: 'text' | 'number' | 'select'; options?: string[]; required?: boolean };

const CONFIG: Record<ToolKind, { title: string; subtitle: string; fields: Field[]; button: string }> = {
  get_cluster_status: { title: 'Cluster Status', subtitle: 'View the current leader and Raft term.', fields: [], button: 'Fetch status' },
  get_value: { title: 'Read Key', subtitle: 'Retrieve a typed value from the distributed store.', fields: [{ name: 'key', label: 'Key', placeholder: 'e.g. user:42', required: true }], button: 'Read value' },
  set_value: { title: 'Write Key', subtitle: 'Store a string, integer, or floating-point value.', fields: [{ name: 'key', label: 'Key', placeholder: 'e.g. user:42', required: true }, { name: 'value_type', label: 'Value type', placeholder: 'Select type', type: 'select', options: ['string', 'int', 'float'], required: true }, { name: 'value', label: 'Value', placeholder: 'Enter value', required: true }], button: 'Write value' },
  delete_key: { title: 'Delete Key', subtitle: 'Remove a key from KVRaft.', fields: [{ name: 'key', label: 'Key', placeholder: 'Key to delete', required: true }], button: 'Delete key' },
  get_node_details: { title: 'Node Details', subtitle: 'Inspect a node’s Raft state and log information.', fields: [{ name: 'node_id', label: 'Node ID', placeholder: '1', type: 'number', required: true }], button: 'Inspect node' },
  get_cluster_metrics: { title: 'Cluster Metrics', subtitle: 'Inspect Prometheus metrics collected by the gateway.', fields: [], button: 'Load metrics' },
  get_recent_errors: { title: 'Recent Errors', subtitle: 'Retrieve recent node errors and structured logs.', fields: [{ name: 'limit', label: 'Maximum errors', placeholder: '20', type: 'number' }], button: 'Fetch errors' },
  diagnose_cluster: { title: 'Cluster Diagnosis', subtitle: 'Collect evidence and generate a diagnostic report.', fields: [], button: 'Run diagnosis' },
  recommend_recovery: { title: 'Recovery Advisor', subtitle: 'Get a recovery plan based on an observed failure.', fields: [{ name: 'failure_summary', label: 'Observed failure', placeholder: 'Describe the node or cluster failure…', required: true }], button: 'Recommend recovery' },
};

function unwrap(value: any): any {
  if (value == null) return null;
  if (value.structuredContent != null) return value.structuredContent;
  if (value.data != null) return value.data;
  if (Array.isArray(value.content)) {
    const textPart = value.content.find((part: any) => part?.type === 'text' && typeof part.text === 'string');
    if (textPart) {
      try { return JSON.parse(textPart.text); } catch { return { message: textPart.text }; }
    }
  }
  return value;
}

export default function KVRaftToolWidget({ kind }: { kind: ToolKind }) {
  const { isReady, getToolOutput, callTool } = useWidgetSDK();
  const theme = useTheme();
  const dark = theme === 'dark';
  const config = CONFIG[kind];
  const [values, setValues] = useState<Record<string, string>>({ value_type: 'string', limit: '20', node_id: '1' });
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState('');
  const [running, setRunning] = useState(false);

  const c = {
    bg: dark ? '#15171b' : '#ffffff', panel: dark ? '#202329' : '#f7f8fa',
    text: dark ? '#f3f4f6' : '#17191d', muted: dark ? '#a1a7b3' : '#687080',
    border: dark ? '#363b45' : '#e1e5eb', accent: dark ? '#dbeafe' : '#202b3c',
    accentText: dark ? '#172033' : '#ffffff', green: dark ? '#86efac' : '#166534',
    red: dark ? '#fca5a5' : '#b91c1c', input: dark ? '#17191e' : '#ffffff',
  };

  const existing = unwrap(getToolOutput<any>());
  const output = result ?? existing;

  async function run() {
    setRunning(true); setError('');
    const args: Record<string, unknown> = {};
    for (const field of config.fields) {
      const raw = values[field.name] ?? '';
      if (raw.trim() !== '') args[field.name] = field.type === 'number' ? Number(raw) : raw;
    }
    try {
      const response = await callTool(kind, args);
      const parsed = unwrap(response);
      if (parsed == null) throw new Error('The tool returned no readable output.');
      setResult(parsed);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally { setRunning(false); }
  }

  const success = typeof output?.success === 'boolean' ? output.success : null;
  const leader = output?.leader_id;
  const term = output?.term;
  const nodes = Array.isArray(output?.nodes) ? output.nodes : null;

  return (
    <main style={{ background: c.bg, color: c.text, border: `1px solid ${c.border}`, borderRadius: 14, padding: 20, fontFamily: 'system-ui, sans-serif', maxWidth: 900, margin: '0 auto' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 14, flexWrap: 'wrap' }}>
        <div>
          <div style={{ fontSize: 11, letterSpacing: 1.2, textTransform: 'uppercase', color: c.muted, marginBottom: 6 }}>KVRAFT CONTROL PANEL</div>
          <h2 style={{ margin: 0, fontSize: 22, fontWeight: 700 }}>{config.title}</h2>
          <p style={{ margin: '6px 0 0', color: c.muted, fontSize: 13 }}>{config.subtitle}</p>
        </div>
        {success !== null && <span style={{ borderRadius: 999, padding: '5px 10px', fontSize: 12, fontWeight: 650, color: success ? c.green : c.red, background: success ? (dark ? '#163323' : '#eaf7ee') : (dark ? '#3a2023' : '#fff0f0') }}>{success ? 'SUCCESS' : 'FAILED'}</span>}
      </header>

      {!isReady ? (
        <div style={{ marginTop: 18, padding: 14, background: c.panel, borderRadius: 9, color: c.muted, fontSize: 13 }}>
          Waiting for NitroStack host initialization. Open this widget through NitroStack Studio to connect it to its tool.
        </div>
      ) : <>
        {config.fields.length > 0 && <section style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: 12, marginTop: 22 }}>
          {config.fields.map(field => <label key={field.name} style={{ display: 'grid', gap: 7, fontSize: 12, fontWeight: 650, color: c.muted }}>
            {field.label}
            {field.type === 'select' ? <select value={values[field.name] ?? ''} onChange={e => setValues(v => ({ ...v, [field.name]: e.target.value }))} style={inputStyle(c)}>{field.options?.map(option => <option key={option} value={option}>{option}</option>)}</select> :
              <input type={field.type ?? 'text'} required={field.required} value={values[field.name] ?? ''} placeholder={field.placeholder} onChange={e => setValues(v => ({ ...v, [field.name]: e.target.value }))} style={inputStyle(c)} />}
          </label>)}
        </section>}
        <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 16 }}>
          <button onClick={run} disabled={running || !isReady} style={{ border: 0, borderRadius: 8, padding: '10px 15px', fontWeight: 700, background: c.accent, color: c.accentText, cursor: running ? 'wait' : 'pointer', opacity: running ? 0.7 : 1 }}>{running ? 'Working…' : config.button}</button>
        </div>
        {error && <div role="alert" style={{ marginTop: 14, padding: 12, borderRadius: 8, color: c.red, background: dark ? '#321f23' : '#fff0f0', overflowWrap: 'anywhere', fontSize: 13 }}>{error}</div>}
        {output != null && <section style={{ marginTop: 22 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, marginBottom: 10 }}><h3 style={{ margin: 0, fontSize: 14 }}>Tool output</h3><span style={{ color: c.muted, fontSize: 11 }}>Latest response</span></div>
          {(leader != null || term != null) && <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: 10, marginBottom: 12 }}>
            {leader != null && <Stat label="Leader ID" value={String(leader)} c={c} />}
            {term != null && <Stat label="Raft term" value={String(term)} c={c} />}
            {output?.healthy_nodes != null && <Stat label="Healthy nodes" value={`${output.healthy_nodes}/${output.cluster_size ?? '?'}`} c={c} />}
          </div>}
          {nodes && <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: 10, marginBottom: 12 }}>{nodes.map((node: any) => <div key={node.node_id} style={{ border: `1px solid ${c.border}`, borderRadius: 9, padding: 12, background: c.panel }}><div style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}><strong>Node {node.node_id}</strong><strong style={{ color: node.healthy ? c.green : c.red, fontSize: 12 }}>{node.healthy ? 'Healthy' : 'Unhealthy'}</strong></div><div style={{ color: c.muted, fontSize: 12, marginTop: 5, overflowWrap: 'anywhere' }}>{node.address}</div><div style={{ fontSize: 12, marginTop: 8 }}>Latency: {node.latency_ms ?? '—'} ms · Term: {node.term ?? '—'}</div>{node.error && <div style={{ color: c.red, fontSize: 12, marginTop: 6 }}>{node.error}</div>}</div>)}</div>}
          {output?.message && <p style={{ margin: '0 0 10px', fontSize: 13 }}>{output.message}</p>}
          <pre style={{ margin: 0, padding: 14, borderRadius: 9, border: `1px solid ${c.border}`, background: c.panel, color: c.text, whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', maxHeight: 380, overflow: 'auto', fontSize: 12, lineHeight: 1.55 }}>{JSON.stringify(output, null, 2)}</pre>
        </section>}
      </>}
    </main>
  );
}

function inputStyle(c: Record<string, string>): CSSProperties {
  return { width: '100%', boxSizing: 'border-box', padding: '10px 11px', border: `1px solid ${c.border}`, borderRadius: 8, background: c.input, color: c.text, font: 'inherit', fontSize: 14, outlineOffset: 2 };
}

function Stat({ label, value, c }: { label: string; value: string; c: Record<string, string> }) {
  return <div style={{ border: `1px solid ${c.border}`, borderRadius: 9, background: c.panel, padding: 12 }}><div style={{ color: c.muted, fontSize: 11 }}>{label}</div><div style={{ fontSize: 22, fontWeight: 750, marginTop: 3 }}>{value}</div></div>;
}
