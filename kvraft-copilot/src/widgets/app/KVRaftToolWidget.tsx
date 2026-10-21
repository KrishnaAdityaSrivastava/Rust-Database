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
  | 'recommend_recovery'
  | 'start_node'
  | 'stop_node'
  | 'register_node'
  | 'remove_node';

type Field = {
  name: string;
  label: string;
  placeholder: string;
  type?: 'text' | 'number' | 'select';
  options?: string[];
  required?: boolean;
};

type ToolConfig = {
  title: string;
  subtitle: string;
  fields: Field[];
  button: string;
};

type ThemeColors = {
  bg: string;
  panel: string;
  text: string;
  muted: string;
  border: string;
  accent: string;
  accentText: string;
  green: string;
  red: string;
  input: string;
};

const CONFIG: Record<ToolKind, ToolConfig> = {
  get_cluster_status: {
    title: 'Cluster Status',
    subtitle: 'View the current leader and Raft term.',
    fields: [],
    button: 'Fetch status',
  },
  get_value: {
    title: 'Read Key',
    subtitle: 'Retrieve a typed value from the distributed store.',
    fields: [
      {
        name: 'key',
        label: 'Key',
        placeholder: 'e.g. user:42',
        required: true,
      },
    ],
    button: 'Read value',
  },
  set_value: {
    title: 'Write Key',
    subtitle: 'Store a string, integer, or floating-point value.',
    fields: [
      {
        name: 'key',
        label: 'Key',
        placeholder: 'e.g. user:42',
        required: true,
      },
      {
        name: 'value_type',
        label: 'Value type',
        placeholder: 'Select type',
        type: 'select',
        options: ['string', 'int', 'float'],
        required: true,
      },
      {
        name: 'value',
        label: 'Value',
        placeholder: 'Enter value',
        required: true,
      },
    ],
    button: 'Write value',
  },
  delete_key: {
    title: 'Delete Key',
    subtitle: 'Remove a key from KVRaft.',
    fields: [
      {
        name: 'key',
        label: 'Key',
        placeholder: 'Key to delete',
        required: true,
      },
    ],
    button: 'Delete key',
  },
  get_node_details: {
    title: 'Node Details',
    subtitle: 'Inspect a node’s Raft state and log information.',
    fields: [
      {
        name: 'node_id',
        label: 'Node ID',
        placeholder: '1',
        type: 'number',
        required: true,
      },
    ],
    button: 'Inspect node',
  },
  get_cluster_metrics: {
    title: 'Cluster Metrics',
    subtitle: 'Inspect Prometheus metrics collected by the gateway.',
    fields: [],
    button: 'Load metrics',
  },
  get_recent_errors: {
    title: 'Recent Errors',
    subtitle: 'Retrieve recent node errors and structured logs.',
    fields: [
      {
        name: 'limit',
        label: 'Maximum errors',
        placeholder: '20',
        type: 'number',
      },
    ],
    button: 'Fetch errors',
  },
  diagnose_cluster: {
    title: 'Cluster Diagnosis',
    subtitle: 'Collect evidence and generate a diagnostic report.',
    fields: [],
    button: 'Run diagnosis',
  },
  recommend_recovery: {
    title: 'Recovery Advisor',
    subtitle: 'Get a recovery plan based on an observed failure.',
    fields: [
      {
        name: 'failure_summary',
        label: 'Observed failure',
        placeholder: 'Describe the node or cluster failure…',
        required: true,
      },
    ],
    button: 'Recommend recovery',
  },
  start_node: {
    title: 'Start Raft Node',
    subtitle: 'Create and start a node container.',
    fields: [
      {
        name: 'node_id',
        label: 'Node ID',
        placeholder: '4',
        type: 'number',
        required: true,
      },
    ],
    button: 'Start node',
  },
  stop_node: {
    title: 'Stop Raft Node',
    subtitle: 'Stop a node container without deleting its persistent data.',
    fields: [
      {
        name: 'node_id',
        label: 'Node ID',
        placeholder: '4',
        type: 'number',
        required: true,
      },
    ],
    button: 'Stop node',
  },
  register_node: {
    title: 'Register Raft Node',
    subtitle: 'Ask the Go gateway to add a node to Raft membership.',
    fields: [
      {
        name: 'node_id',
        label: 'Node ID',
        placeholder: '4',
        type: 'number',
        required: true,
      },
      {
        name: 'addr',
        label: 'Raft address',
        placeholder: 'node4:6004',
        required: true,
      },
    ],
    button: 'Register node',
  },
  remove_node: {
    title: 'Remove Raft Node',
    subtitle: 'Remove a node from Raft membership. Its container will remain running.',
    fields: [
      {
        name: 'node_id',
        label: 'Node ID',
        placeholder: '4',
        type: 'number',
        required: true,
      },
    ],
    button: 'Remove node',
  },
};

function unwrap(value: any): any {
  if (value == null) {
    return null;
  }

  if (value.structuredContent != null) {
    return value.structuredContent;
  }

  if (value.data != null) {
    return value.data;
  }

  if (Array.isArray(value.content)) {
    const textPart = value.content.find(
      (part: any) =>
        part?.type === 'text' && typeof part.text === 'string',
    );

    if (textPart) {
      try {
        return JSON.parse(textPart.text);
      } catch {
        return { message: textPart.text };
      }
    }
  }

  return value;
}

function getColors(dark: boolean): ThemeColors {
  return {
    bg: dark ? '#15171b' : '#ffffff',
    panel: dark ? '#202329' : '#f7f8fa',
    text: dark ? '#f3f4f6' : '#17191d',
    muted: dark ? '#a1a7b3' : '#687080',
    border: dark ? '#363b45' : '#e1e5eb',
    accent: dark ? '#dbeafe' : '#202b3c',
    accentText: dark ? '#172033' : '#ffffff',
    green: dark ? '#86efac' : '#166534',
    red: dark ? '#fca5a5' : '#b91c1c',
    input: dark ? '#17191e' : '#ffffff',
  };
}

export default function KVRaftToolWidget({
  kind,
}: {
  kind: ToolKind;
}) {
  const { isReady, getToolOutput, callTool } = useWidgetSDK();
  const dark = useTheme() === 'dark';

  const config = CONFIG[kind];
  const colors = getColors(dark);

  const [values, setValues] = useState<Record<string, string>>({
    value_type: 'string',
    limit: '20',
  });
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState('');
  const [running, setRunning] = useState(false);

  const existingOutput = unwrap(getToolOutput<any>());
  const output = result ?? existingOutput;

  function updateField(name: string, value: string) {
    setValues((current) => ({
      ...current,
      [name]: value,
    }));
  }

  async function run() {
    if (running || !isReady) {
      return;
    }

    setError('');

    const args: Record<string, unknown> = {};

    for (const field of config.fields) {
      const raw = values[field.name] ?? '';

      if (field.required && raw.trim() === '') {
        setError(`${field.label} is required.`);
        return;
      }

      if (raw.trim() === '') {
        continue;
      }

      if (field.type === 'number') {
        const number = Number(raw);

        if (!Number.isFinite(number)) {
          setError(`${field.label} must be a valid number.`);
          return;
        }

        args[field.name] = number;
      } else {
        args[field.name] = raw;
      }
    }

    setRunning(true);

    try {
      const response = await callTool(kind, args);
      const parsed = unwrap(response);

      if (parsed == null) {
        throw new Error('The tool returned no readable output.');
      }

      setResult(parsed);
    } catch (err) {
      setError(
        err instanceof Error
          ? err.message
          : String(err),
      );
    } finally {
      setRunning(false);
    }
  }

  const success =
    typeof output?.success === 'boolean'
      ? output.success
      : null;

  const leader = output?.leader_id;
  const term = output?.term;

  const nodes = Array.isArray(output?.nodes)
    ? output.nodes
    : null;

  const panelStyle: CSSProperties = {
    background: colors.bg,
    color: colors.text,
    border: `1px solid ${colors.border}`,
    borderRadius: 14,
    padding: 20,
    fontFamily: 'system-ui, sans-serif',
    maxWidth: 900,
    margin: '0 auto',
    boxSizing: 'border-box',
  };

  return (
    <main style={panelStyle}>
      <header style={styles.header}>
        <div>
          <div style={styles.eyebrow(colors.muted)}>
            KVRaft Control Panel
          </div>

          <h2 style={styles.title}>
            {config.title}
          </h2>

          <p style={styles.subtitle(colors.muted)}>
            {config.subtitle}
          </p>
        </div>

        {success !== null && (
          <span
            style={styles.status(
              success,
              dark,
              colors,
            )}
          >
            {success ? 'SUCCESS' : 'FAILED'}
          </span>
        )}
      </header>

      {!isReady ? (
        <div
          style={styles.notice(colors)}
          role="status"
        >
          Waiting for NitroStack host initialization.
          Open this widget through NitroStack Studio to
          connect it to its tool.
        </div>
      ) : (
        <>
          {config.fields.length > 0 && (
            <section style={styles.fields}>
              {config.fields.map((field) => (
                <label
                  key={field.name}
                  style={styles.fieldLabel(colors.muted)}
                >
                  {field.label}

                  {field.type === 'select' ? (
                    <select
                      value={
                        values[field.name] ??
                        field.options?.[0] ??
                        ''
                      }
                      required={field.required}
                      onChange={(event) =>
                        updateField(
                          field.name,
                          event.target.value,
                        )
                      }
                      style={inputStyle(colors)}
                    >
                      {field.options?.map((option) => (
                        <option
                          key={option}
                          value={option}
                        >
                          {option}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <input
                      type={field.type ?? 'text'}
                      required={field.required}
                      value={values[field.name] ?? ''}
                      placeholder={field.placeholder}
                      onChange={(event) =>
                        updateField(
                          field.name,
                          event.target.value,
                        )
                      }
                      style={inputStyle(colors)}
                    />
                  )}
                </label>
              ))}
            </section>
          )}

          <div style={styles.actions}>
            <button
              type="button"
              onClick={run}
              disabled={running || !isReady}
              style={styles.button(
                running,
                colors,
              )}
            >
              {running ? 'Working…' : config.button}
            </button>
          </div>

          {error && (
            <div
              role="alert"
              style={styles.error(colors, dark)}
            >
              {error}
            </div>
          )}

          {output != null && (
            <section style={styles.outputSection}>
              <div style={styles.outputHeader}>
                <h3 style={styles.outputTitle}>
                  Tool output
                </h3>

                <span style={styles.caption(colors.muted)}>
                  Latest response
                </span>
              </div>

              {(leader != null ||
                term != null ||
                output?.healthy_nodes != null) && (
                  <div style={styles.stats}>
                    {leader != null && (
                      <Stat
                        label="Leader ID"
                        value={String(leader)}
                        colors={colors}
                      />
                    )}

                    {term != null && (
                      <Stat
                        label="Raft term"
                        value={String(term)}
                        colors={colors}
                      />
                    )}

                    {output?.healthy_nodes != null && (
                      <Stat
                        label="Healthy nodes"
                        value={`${output.healthy_nodes}/${output.cluster_size ?? '?'}`}
                        colors={colors}
                      />
                    )}
                  </div>
                )}

              {nodes && (
                <div style={styles.nodes}>
                  {nodes.map((node: any, index: number) => (
                    <div
                      key={node.node_id ?? index}
                      style={styles.nodeCard(colors)}
                    >
                      <div style={styles.nodeHeader}>
                        <strong>
                          Node {node.node_id ?? '—'}
                        </strong>

                        <strong
                          style={{
                            color: node.healthy
                              ? colors.green
                              : colors.red,
                            fontSize: 12,
                          }}
                        >
                          {node.healthy
                            ? 'Healthy'
                            : 'Unhealthy'}
                        </strong>
                      </div>

                      <div style={styles.nodeAddress(colors.muted)}>
                        {node.address ?? 'Address unavailable'}
                      </div>

                      <div style={styles.nodeMetadata}>
                        Latency: {node.latency_ms ?? '—'} ms
                        {' · '}
                        Term: {node.term ?? '—'}
                      </div>

                      {node.error && (
                        <div style={styles.nodeError(colors.red)}>
                          {node.error}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}

              {output?.message && (
                <p style={styles.message}>
                  {output.message}
                </p>
              )}

              <pre style={styles.json(colors)}>
                {JSON.stringify(output, null, 2)}
              </pre>
            </section>
          )}
        </>
      )}
    </main>
  );
}

function inputStyle(
  colors: ThemeColors,
): CSSProperties {
  return {
    width: '100%',
    boxSizing: 'border-box',
    padding: '10px 11px',
    border: `1px solid ${colors.border}`,
    borderRadius: 8,
    background: colors.input,
    color: colors.text,
    font: 'inherit',
    fontSize: 14,
    outlineOffset: 2,
  };
}

function Stat({
  label,
  value,
  colors,
}: {
  label: string;
  value: string;
  colors: ThemeColors;
}) {
  return (
    <div style={styles.statCard(colors)}>
      <div style={styles.statLabel(colors.muted)}>
        {label}
      </div>

      <div style={styles.statValue}>
        {value}
      </div>
    </div>
  );
}

const styles = {
  header: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    gap: 14,
    flexWrap: 'wrap',
  } satisfies CSSProperties,

  eyebrow: (color: string): CSSProperties => ({
    fontSize: 11,
    letterSpacing: 1.2,
    textTransform: 'uppercase',
    color,
    marginBottom: 6,
  }),

  title: {
    margin: 0,
    fontSize: 22,
    fontWeight: 700,
  } satisfies CSSProperties,

  subtitle: (color: string): CSSProperties => ({
    margin: '6px 0 0',
    color,
    fontSize: 13,
    lineHeight: 1.5,
  }),

  status: (
    success: boolean,
    dark: boolean,
    colors: ThemeColors,
  ): CSSProperties => ({
    borderRadius: 999,
    padding: '5px 10px',
    fontSize: 12,
    fontWeight: 650,
    color: success ? colors.green : colors.red,
    background: success
      ? dark
        ? '#163323'
        : '#eaf7ee'
      : dark
        ? '#3a2023'
        : '#fff0f0',
  }),

  notice: (colors: ThemeColors): CSSProperties => ({
    marginTop: 18,
    padding: 14,
    background: colors.panel,
    borderRadius: 9,
    color: colors.muted,
    fontSize: 13,
    lineHeight: 1.5,
  }),

  fields: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(min(190px, 100%), 1fr))',
    gap: 12,
    marginTop: 22,
  } satisfies CSSProperties,

  fieldLabel: (color: string): CSSProperties => ({
    display: 'grid',
    gap: 7,
    fontSize: 12,
    fontWeight: 650,
    color,
    minWidth: 0,
  }),

  actions: {
    display: 'flex',
    justifyContent: 'flex-end',
    marginTop: 16,
  } satisfies CSSProperties,

  button: (
    running: boolean,
    colors: ThemeColors,
  ): CSSProperties => ({
    border: 0,
    borderRadius: 8,
    padding: '10px 15px',
    fontWeight: 700,
    background: colors.accent,
    color: colors.accentText,
    cursor: running ? 'wait' : 'pointer',
    opacity: running ? 0.7 : 1,
  }),

  error: (
    colors: ThemeColors,
    dark: boolean,
  ): CSSProperties => ({
    marginTop: 14,
    padding: 12,
    borderRadius: 8,
    color: colors.red,
    background: dark ? '#321f23' : '#fff0f0',
    overflowWrap: 'anywhere',
    fontSize: 13,
  }),

  outputSection: {
    marginTop: 22,
  } satisfies CSSProperties,

  outputHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: 10,
    marginBottom: 10,
  } satisfies CSSProperties,

  outputTitle: {
    margin: 0,
    fontSize: 14,
  } satisfies CSSProperties,

  caption: (color: string): CSSProperties => ({
    color,
    fontSize: 11,
  }),

  stats: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(min(130px, 100%), 1fr))',
    gap: 10,
    marginBottom: 12,
  } satisfies CSSProperties,

  statCard: (colors: ThemeColors): CSSProperties => ({
    border: `1px solid ${colors.border}`,
    borderRadius: 9,
    background: colors.panel,
    padding: 12,
    minWidth: 0,
  }),

  statLabel: (color: string): CSSProperties => ({
    color,
    fontSize: 11,
  }),

  statValue: {
    fontSize: 22,
    fontWeight: 750,
    marginTop: 3,
    overflowWrap: 'anywhere',
  } satisfies CSSProperties,

  nodes: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(min(190px, 100%), 1fr))',
    gap: 10,
    marginBottom: 12,
  } satisfies CSSProperties,

  nodeCard: (colors: ThemeColors): CSSProperties => ({
    border: `1px solid ${colors.border}`,
    borderRadius: 9,
    padding: 12,
    background: colors.panel,
    minWidth: 0,
  }),

  nodeHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    gap: 8,
    flexWrap: 'wrap',
  } satisfies CSSProperties,

  nodeAddress: (color: string): CSSProperties => ({
    color,
    fontSize: 12,
    marginTop: 5,
    overflowWrap: 'anywhere',
  }),

  nodeMetadata: {
    fontSize: 12,
    marginTop: 8,
  } satisfies CSSProperties,

  nodeError: (color: string): CSSProperties => ({
    color,
    fontSize: 12,
    marginTop: 6,
    overflowWrap: 'anywhere',
  }),

  message: {
    margin: '0 0 10px',
    fontSize: 13,
    overflowWrap: 'anywhere',
  } satisfies CSSProperties,

  json: (colors: ThemeColors): CSSProperties => ({
    margin: 0,
    padding: 14,
    borderRadius: 9,
    border: `1px solid ${colors.border}`,
    background: colors.panel,
    color: colors.text,
    whiteSpace: 'pre-wrap',
    overflowWrap: 'anywhere',
    maxHeight: 380,
    overflow: 'auto',
    fontSize: 12,
    lineHeight: 1.55,
  }),
};