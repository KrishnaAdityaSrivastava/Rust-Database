'use client';
import KVRaftToolWidget from '../KVRaftToolWidget';

export default function ClusterStatusWidget() {
  return <KVRaftToolWidget kind='get_cluster_status' />;
}
