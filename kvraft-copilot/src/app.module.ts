import { McpApp, Module, ConfigModule } from '@nitrostack/core';
import { KVRaftModule } from './modules/kvraft/kvraft.module.js';
import { SystemHealthCheck } from './health/system.health.js';

@McpApp({
  module: AppModule,
  server: {
    name: 'kvraft-copilot',
    version: '1.0.0',
  },
  logging: {
    level: 'info',
  },
})
@Module({
  name: 'app',
  description: 'MCP tools for KVRaft cluster operations',
  imports: [
    ConfigModule.forRoot(),
    KVRaftModule,
  ],
  providers: [
    SystemHealthCheck,
  ],
})
export class AppModule {}
