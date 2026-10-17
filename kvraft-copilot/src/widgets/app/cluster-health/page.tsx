
'use client';

import { useState } from 'react';
import { useWidgetSDK, useTheme } from '@nitrostack/widgets';

interface NodeHealth {
    node_id: number;
    address: string;
    reachable: boolean;
    healthy: boolean;
    latency_ms: number;
    message?: string;
    error?: string;
    leader_id?: number | null;
    term?: number;
}

interface ClusterHealth {
    gateway: string;
    success: boolean;
    message: string;
    cluster_size: number;
    healthy_nodes: number;
    nodes: NodeHealth[];
}

export default function ClusterHealthWidget() {
    const { isReady, getToolOutput, callTool } = useWidgetSDK();
    const theme = useTheme();

    // Hooks must be declared before conditional returns.
    const [refreshing, setRefreshing] = useState(false);
    const [refreshError, setRefreshError] = useState('');
    const [refreshData, setRefreshData] = useState<ClusterHealth | null>(null);

    async function refresh() {
        setRefreshing(true);
        setRefreshError('');

        try {
            const result = await callTool('diagnose_nodes', {});

            // Use returned data if callTool returns the tool response.
            if (result) {
                const candidate = result as any;
                const output = candidate.structuredContent ?? candidate;

                if (Array.isArray(output.nodes)) {
                    setRefreshData(output as ClusterHealth);
                } else if (Array.isArray(output?.data?.nodes)) {
                    setRefreshData(output.data as ClusterHealth);
                } else {
                    // Some SDK responses wrap structured output differently.
                    setRefreshData(null);
                }
            }
        } catch (error) {
            setRefreshError(
                error instanceof Error ? error.message : String(error),
            );
        } finally {
            setRefreshing(false);
        }
    }

    // A normal browser may not provide the NitroStack host context.
    if (!isReady) {
        return (
            <div
                style={{
                    padding: 24,
                    fontFamily: 'system-ui, sans-serif',
                    color: theme === 'dark' ? '#f5f5f5' : '#171717',
                }}
            >
                <h2 style={{ marginTop: 0 }}>KVRaft Cluster Diagnostics</h2>
                <p>Waiting for NitroStack host initialization...</p>
                <p style={{ fontSize: 13, opacity: 0.7 }}>
                    SDK ready: {String(isReady)}
                </p>
                <p style={{ fontSize: 13, opacity: 0.7 }}>
                    Open this widget through NitroStack Studio to receive actual tool
                    output.
                </p>
            </div>
        );
    }

    const toolData = getToolOutput<ClusterHealth>();
    const data =
        refreshData ??
        (toolData && Array.isArray(toolData.nodes) ? toolData : null);

    if (!data) {
        return (
            <div
                style={{
                    padding: 24,
                    fontFamily: 'system-ui, sans-serif',
                    color: theme === 'dark' ? '#f5f5f5' : '#171717',
                }}
            >
                <h2 style={{ marginTop: 0 }}>KVRaft Cluster Diagnostics</h2>
                <p>No cluster health data is available.</p>
                <p style={{ fontSize: 13, opacity: 0.7 }}>
                    Invoke the tool connected to this widget in NitroStack Studio.
                </p>

                {refreshError && (
                    <p style={{ color: '#dc2626', overflowWrap: 'anywhere' }}>
                        {refreshError}
                    </p>
                )}

                <button
                    onClick={refresh}
                    disabled={refreshing}
                    style={{
                        padding: '10px 16px',
                        borderRadius: 8,
                        border: 'none',
                        cursor: refreshing ? 'wait' : 'pointer',
                    }}
                >
                    {refreshing ? 'Checking...' : 'Run Diagnostics'}
                </button>
            </div>
        );
    }

    const dark = theme === 'dark';

    const colors = {
        background: dark ? '#171717' : '#ffffff',
        card: dark ? '#222222' : '#f7f7f8',
        text: dark ? '#f5f5f5' : '#171717',
        muted: dark ? '#a3a3a3' : '#666666',
        border: dark ? '#3f3f46' : '#e4e4e7',
        green: dark ? '#86efac' : '#166534',
        red: dark ? '#fca5a5' : '#b91c1c',
        button: dark ? '#e5e7eb' : '#18181b',
        buttonText: dark ? '#18181b' : '#ffffff',
    };

    return (
        <div
            style={{
                background: colors.background,
                color: colors.text,
                padding: 20,
                borderRadius: 12,
                fontFamily: 'system-ui, sans-serif',
                border: `1px solid ${colors.border}`,
            }}
        >
            <div
                style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    gap: 12,
                    flexWrap: 'wrap',
                }}
            >
                <div>
                    <h2 style={{ margin: 0, fontSize: 20 }}>
                        KVRaft Cluster
                    </h2>
                    <p style={{ color: colors.muted, marginBottom: 0 }}>
                        {data.message}
                    </p>
                    <p style={{ color: colors.muted, fontSize: 12 }}>
                        Gateway: {data.gateway}
                    </p>
                </div>

                <button
                    onClick={refresh}
                    disabled={refreshing}
                    style={{
                        padding: '9px 14px',
                        borderRadius: 8,
                        border: 'none',
                        background: colors.button,
                        color: colors.buttonText,
                        cursor: refreshing ? 'wait' : 'pointer',
                    }}
                >
                    {refreshing ? 'Checking...' : 'Refresh'}
                </button>
            </div>

            {refreshError && (
                <p
                    style={{
                        color: colors.red,
                        overflowWrap: 'anywhere',
                    }}
                >
                    Refresh failed: {refreshError}
                </p>
            )}

            <div
                style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))',
                    gap: 12,
                    marginTop: 20,
                    marginBottom: 20,
                }}
            >
                <div
                    style={{
                        background: colors.card,
                        padding: 14,
                        borderRadius: 8,
                    }}
                >
                    <div style={{ color: colors.muted, fontSize: 13 }}>
                        Healthy nodes
                    </div>
                    <strong style={{ fontSize: 25 }}>
                        {data.healthy_nodes}/{data.cluster_size}
                    </strong>
                </div>

                <div
                    style={{
                        background: colors.card,
                        padding: 14,
                        borderRadius: 8,
                    }}
                >
                    <div style={{ color: colors.muted, fontSize: 13 }}>
                        Cluster status
                    </div>
                    <strong
                        style={{
                            color: data.success ? colors.green : colors.red,
                        }}
                    >
                        {data.success ? 'Healthy' : 'Degraded'}
                    </strong>
                </div>
            </div>

            <div style={{ display: 'grid', gap: 12 }}>
                {data.nodes.map((node) => (
                    <div
                        key={node.node_id}
                        style={{
                            background: colors.card,
                            border: `1px solid ${colors.border}`,
                            borderRadius: 10,
                            padding: 16,
                        }}
                    >
                        <div
                            style={{
                                display: 'flex',
                                justifyContent: 'space-between',
                                alignItems: 'center',
                                gap: 10,
                                flexWrap: 'wrap',
                            }}
                        >
                            <strong>Node {node.node_id}</strong>

                            <span
                                style={{
                                    color: node.healthy ? colors.green : colors.red,
                                    fontWeight: 600,
                                }}
                            >
                                {node.healthy ? 'Healthy' : 'Unhealthy'}
                            </span>
                        </div>

                        <p
                            style={{
                                color: colors.muted,
                                margin: '8px 0',
                                overflowWrap: 'anywhere',
                            }}
                        >
                            {node.address}
                        </p>

                        <div
                            style={{
                                display: 'grid',
                                gridTemplateColumns:
                                    'repeat(auto-fit, minmax(100px, 1fr))',
                                gap: 12,
                            }}
                        >
                            <div>
                                <small style={{ color: colors.muted }}>
                                    Reachable
                                </small>
                                <div>{node.reachable ? 'Yes' : 'No'}</div>
                            </div>

                            <div>
                                <small style={{ color: colors.muted }}>
                                    Latency
                                </small>
                                <div>{node.latency_ms} ms</div>
                            </div>

                            <div>
                                <small style={{ color: colors.muted }}>
                                    Leader ID
                                </small>
                                <div>{node.leader_id ?? 'Unknown'}</div>
                            </div>

                            <div>
                                <small style={{ color: colors.muted }}>
                                    Raft term
                                </small>
                                <div>{node.term ?? 'Unknown'}</div>
                            </div>
                        </div>

                        {node.message && (
                            <p style={{ marginBottom: 0 }}>
                                {node.message}
                            </p>
                        )}

                        {node.error && (
                            <p
                                style={{
                                    color: colors.red,
                                    marginBottom: 0,
                                    overflowWrap: 'anywhere',
                                }}
                            >
                                Error: {node.error}
                            </p>
                        )}
                    </div>
                ))}
            </div>
        </div>
    );
}
