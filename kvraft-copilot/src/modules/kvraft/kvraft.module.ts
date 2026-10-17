import { Module } from '@nitrostack/core';
import { KVRaftTools } from './kvraft.tools.js';

@Module({
  name: 'kvraft',
  description: 'Tools for inspecting the KVRaft distributed database',
  controllers: [KVRaftTools],
})
export class KVRaftModule {}
