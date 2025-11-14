/**
 * Configuration for Raft node and cache system
 */

import { NodeId } from './types';

export interface RaftConfig {
  // Node identification
  nodeId: NodeId;
  peers: NodeId[];

  // Timing configuration (in milliseconds)
  electionTimeoutMin: number; // Minimum election timeout
  electionTimeoutMax: number; // Maximum election timeout
  heartbeatInterval: number; // Leader heartbeat interval
  rpcTimeout: number; // RPC request timeout

  // Cache configuration
  maxMemory: number; // Maximum memory for cache in bytes
  ttlExpirationInterval: number; // Interval for TTL expiration check in ms
  lruEvictionEnabled: boolean; // Enable LRU eviction policy

  // Snapshot configuration
  snapshotThreshold: number; // Log size threshold to trigger snapshot
  snapshotChunkSize: number; // Chunk size for snapshot transfer in bytes

  // Persistence configuration
  dataDir: string; // Directory for persistent storage
}

export const DEFAULT_CONFIG: Partial<RaftConfig> = {
  electionTimeoutMin: 150,
  electionTimeoutMax: 300,
  heartbeatInterval: 50,
  rpcTimeout: 100,
  maxMemory: 100 * 1024 * 1024, // 100 MB
  ttlExpirationInterval: 1000, // 1 second
  lruEvictionEnabled: true,
  snapshotThreshold: 10000, // 10k log entries
  snapshotChunkSize: 1024 * 1024, // 1 MB chunks
  dataDir: './data',
};

export function createConfig(overrides: Partial<RaftConfig>): RaftConfig {
  const config = { ...DEFAULT_CONFIG, ...overrides } as RaftConfig;

  // Validate required fields
  if (!config.nodeId) {
    throw new Error('nodeId is required in configuration');
  }
  if (!config.peers) {
    throw new Error('peers is required in configuration');
  }

  // Validate timing constraints
  if (config.electionTimeoutMin >= config.electionTimeoutMax) {
    throw new Error('electionTimeoutMin must be less than electionTimeoutMax');
  }
  if (config.heartbeatInterval >= config.electionTimeoutMin) {
    throw new Error('heartbeatInterval must be less than electionTimeoutMin');
  }

  return config;
}
