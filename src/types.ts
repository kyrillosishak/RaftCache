/**
 * Core type definitions for the Raft-based distributed cache
 */

// Node identifier type
export type NodeId = string;

// Node state in Raft protocol
export enum NodeState {
  Leader = 'Leader',
  Follower = 'Follower',
  Candidate = 'Candidate',
}

// Log entry in the Raft log
export interface LogEntry {
  term: number;
  index: number;
  command: Buffer;
}

// Cache command types
export enum CommandType {
  SET = 'SET',
  DELETE = 'DELETE',
  BATCH = 'BATCH',
}

// Cache command structure
export interface CacheCommand {
  type: CommandType;
  key?: Buffer;
  value?: Buffer;
  ttl?: number; // TTL in milliseconds
  batch?: CacheCommand[];
}

// Cache entry stored in the state machine
export interface CacheEntry {
  key: Buffer;
  value: Buffer;
  createdAt: number; // timestamp in ms
  expiresAt?: number; // timestamp in ms
  lastAccessedAt: number; // timestamp in ms
  size: number; // memory footprint in bytes
}

// Result of a cache operation
export interface CacheResult {
  success: boolean;
  value?: Buffer;
  error?: string;
}

// Cache snapshot for log compaction
export interface CacheSnapshot {
  entries: Map<string, CacheEntry>; // key as string for serialization
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  timestamp: number;
  memoryUsage: number;
}

// Cache statistics for monitoring
export interface CacheStats {
  entryCount: number;
  memoryUsage: number;
  maxMemory: number;
  hitRate: number;
  evictionCount: number;
  expirationCount: number;
}

// Node status for monitoring
export interface NodeStatus {
  nodeId: NodeId;
  state: NodeState;
  currentTerm: number;
  leaderId?: NodeId;
  logSize: number;
  commitIndex: number;
  lastApplied: number;
  clusterMembers: NodeId[];
  cacheStats: CacheStats;
}

// Cluster status for client API
export interface ClusterStatus {
  leaderId?: NodeId;
  nodes: NodeStatus[];
}

// RequestVote RPC structures
export interface RequestVoteRequest {
  term: number;
  candidateId: NodeId;
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteResponse {
  term: number;
  voteGranted: boolean;
}

// AppendEntries RPC structures
export interface AppendEntriesRequest {
  term: number;
  leaderId: NodeId;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

export interface AppendEntriesResponse {
  term: number;
  success: boolean;
  matchIndex: number;
}

// InstallSnapshot RPC structures
export interface InstallSnapshotRequest {
  term: number;
  leaderId: NodeId;
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  offset: number;
  data: Buffer;
  done: boolean;
}

export interface InstallSnapshotResponse {
  term: number;
}

// Persistent state that must be saved to durable storage
export interface PersistentState {
  currentTerm: number;
  votedFor?: NodeId;
  log: LogEntry[];
}

// Serializable version of PersistentState for storage
export interface SerializedPersistentState {
  currentTerm: number;
  votedFor?: string;
  log: Array<{
    term: number;
    index: number;
    command: string; // base64 encoded
  }>;
}

// Helper functions for serialization/deserialization
export function serializePersistentState(state: PersistentState): string {
  const serialized: SerializedPersistentState = {
    currentTerm: state.currentTerm,
    votedFor: state.votedFor,
    log: state.log.map(entry => ({
      term: entry.term,
      index: entry.index,
      command: entry.command.toString('base64'),
    })),
  };
  return JSON.stringify(serialized);
}

export function deserializePersistentState(data: string): PersistentState {
  const serialized: SerializedPersistentState = JSON.parse(data);
  return {
    currentTerm: serialized.currentTerm,
    votedFor: serialized.votedFor,
    log: serialized.log.map(entry => ({
      term: entry.term,
      index: entry.index,
      command: Buffer.from(entry.command, 'base64'),
    })),
  };
}

// Volatile state maintained by all nodes
export interface VolatileState {
  commitIndex: number;
  lastApplied: number;
  state: NodeState;
  leaderId?: NodeId;
  electionTimeout?: NodeJS.Timeout;
  heartbeatTimeout?: NodeJS.Timeout;
  expirationTimer?: NodeJS.Timeout;
  lastSnapshotIndex: number;
}

// Volatile state maintained only by leaders
export interface LeaderState {
  nextIndex: Map<NodeId, number>;
  matchIndex: Map<NodeId, number>;
}
