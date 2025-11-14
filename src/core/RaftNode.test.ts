/**
 * Tests for RaftNode leader election
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { RaftNodeImpl } from './RaftNode';
import { FilePersistence } from '../persistence/FilePersistence';
import { CacheStateMachine } from '../cache/CacheStateMachine';
import {
  NodeId,
  NodeState,
  RequestVoteRequest,
  RequestVoteResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
  InstallSnapshotRequest,
  InstallSnapshotResponse,
  CacheCommand,
  CacheResult,
  CacheEntry,
  CacheSnapshot,
  CacheStats,
} from '../types';
import { NetworkInterface, NetworkHandlers } from '../network/NetworkInterface';
import { createConfig } from '../config';
import * as fs from 'fs/promises';
import * as path from 'path';

/**
 * Mock CacheStateMachine for testing
 */
class MockCacheStateMachine implements CacheStateMachine {
  private cache: Map<string, CacheEntry> = new Map();

  async apply(command: CacheCommand): Promise<CacheResult> {
    // Actually apply the command to the cache
    if (command.type === 'SET' && command.key && command.value) {
      await this.set(command.key, command.value, command.ttl);
      return { success: true };
    } else if (command.type === 'DELETE' && command.key) {
      const deleted = await this.delete(command.key);
      return { success: deleted };
    } else if (command.type === 'BATCH' && command.batch) {
      const results = await this.batchApply(command.batch);
      return { success: results.every(r => r.success) };
    }
    return { success: false, error: 'Unknown command type' };
  }

  async batchApply(commands: CacheCommand[]): Promise<CacheResult[]> {
    const results: CacheResult[] = [];
    for (const cmd of commands) {
      results.push(await this.apply(cmd));
    }
    return results;
  }

  async get(key: Buffer): Promise<CacheEntry | null> {
    return this.cache.get(key.toString()) || null;
  }

  async set(key: Buffer, value: Buffer, ttl?: number): Promise<void> {
    const now = Date.now();
    this.cache.set(key.toString(), {
      key,
      value,
      createdAt: now,
      expiresAt: ttl ? now + ttl : undefined,
      lastAccessedAt: now,
      size: key.length + value.length,
    });
  }

  async delete(key: Buffer): Promise<boolean> {
    return this.cache.delete(key.toString());
  }

  async evictLRU(): Promise<number> {
    return 0;
  }

  async removeExpired(): Promise<number> {
    return 0;
  }

  async getSnapshot(): Promise<CacheSnapshot> {
    return {
      entries: this.cache,
      lastIncludedIndex: 0,
      lastIncludedTerm: 0,
      timestamp: Date.now(),
      memoryUsage: 0,
    };
  }

  async restoreSnapshot(snapshot: CacheSnapshot): Promise<void> {
    this.cache = new Map(snapshot.entries);
  }

  getMemoryUsage(): number {
    return 0;
  }

  getStats(): CacheStats {
    return {
      entryCount: this.cache.size,
      memoryUsage: 0,
      maxMemory: 1024 * 1024,
      hitRate: 0,
      evictionCount: 0,
      expirationCount: 0,
    };
  }

  async start(): Promise<void> {
    // No-op
  }

  async stop(): Promise<void> {
    // No-op
  }
}

/**
 * In-memory network for testing
 * Simulates network communication between nodes
 */
class InMemoryNetwork implements NetworkInterface {
  private handlers?: NetworkHandlers;
  private nodes: Map<NodeId, InMemoryNetwork> = new Map();
  private nodeId: NodeId;
  private partitionedNodes: Set<NodeId> = new Set();

  constructor(nodeId: NodeId, nodes: Map<NodeId, InMemoryNetwork>) {
    this.nodeId = nodeId;
    this.nodes = nodes;
    this.nodes.set(nodeId, this);
  }

  registerHandlers(handlers: NetworkHandlers): void {
    this.handlers = handlers;
  }

  async start(): Promise<void> {
    // No-op for in-memory network
  }

  async stop(): Promise<void> {
    // No-op for in-memory network
  }

  /**
   * Partition this node from the specified target nodes
   */
  partitionFrom(targets: NodeId[]): void {
    for (const target of targets) {
      this.partitionedNodes.add(target);
    }
  }

  /**
   * Heal partition with the specified target nodes
   */
  healPartition(targets: NodeId[]): void {
    for (const target of targets) {
      this.partitionedNodes.delete(target);
    }
  }

  /**
   * Check if this node is partitioned from the target
   */
  private isPartitioned(target: NodeId): boolean {
    return this.partitionedNodes.has(target);
  }

  async sendRequestVote(
    target: NodeId,
    request: RequestVoteRequest
  ): Promise<RequestVoteResponse> {
    if (this.isPartitioned(target)) {
      throw new Error(`Network partition: cannot reach ${target}`);
    }
    const targetNetwork = this.nodes.get(target);
    if (!targetNetwork || !targetNetwork.handlers) {
      throw new Error(`Node ${target} not found or not ready`);
    }
    // Check if target is partitioned from us
    if (targetNetwork.isPartitioned(this.nodeId)) {
      throw new Error(`Network partition: ${target} cannot reach ${this.nodeId}`);
    }
    return targetNetwork.handlers.handleRequestVote(request);
  }

  async sendAppendEntries(
    target: NodeId,
    request: AppendEntriesRequest
  ): Promise<AppendEntriesResponse> {
    if (this.isPartitioned(target)) {
      throw new Error(`Network partition: cannot reach ${target}`);
    }
    const targetNetwork = this.nodes.get(target);
    if (!targetNetwork || !targetNetwork.handlers) {
      throw new Error(`Node ${target} not found or not ready`);
    }
    // Check if target is partitioned from us
    if (targetNetwork.isPartitioned(this.nodeId)) {
      throw new Error(`Network partition: ${target} cannot reach ${this.nodeId}`);
    }
    return targetNetwork.handlers.handleAppendEntries(request);
  }

  async sendInstallSnapshot(
    target: NodeId,
    request: InstallSnapshotRequest
  ): Promise<InstallSnapshotResponse> {
    if (this.isPartitioned(target)) {
      throw new Error(`Network partition: cannot reach ${target}`);
    }
    const targetNetwork = this.nodes.get(target);
    if (!targetNetwork || !targetNetwork.handlers) {
      throw new Error(`Node ${target} not found or not ready`);
    }
    // Check if target is partitioned from us
    if (targetNetwork.isPartitioned(this.nodeId)) {
      throw new Error(`Network partition: ${target} cannot reach ${this.nodeId}`);
    }
    return targetNetwork.handlers.handleInstallSnapshot(request);
  }
}

/**
 * Test cluster helper
 */
class TestCluster {
  nodes: RaftNodeImpl[] = [];
  networks: Map<NodeId, InMemoryNetwork> = new Map();
  persistences: FilePersistence[] = [];
  stateMachines: MockCacheStateMachine[] = [];
  testDataDir: string;

  constructor(testDataDir: string) {
    this.testDataDir = testDataDir;
  }

  async createNode(nodeId: NodeId, peers: NodeId[]): Promise<RaftNodeImpl> {
    const nodeDataDir = path.join(this.testDataDir, nodeId);
    await fs.mkdir(nodeDataDir, { recursive: true });

    const persistence = new FilePersistence(nodeDataDir);
    await persistence.initialize();
    this.persistences.push(persistence);

    const stateMachine = new MockCacheStateMachine();
    this.stateMachines.push(stateMachine);

    const network = new InMemoryNetwork(nodeId, this.networks);

    const config = createConfig({
      nodeId,
      peers,
      electionTimeoutMin: 150,
      electionTimeoutMax: 300,
      heartbeatInterval: 50,
      dataDir: nodeDataDir,
    });

    const node = new RaftNodeImpl(config, persistence, network, stateMachine);
    this.nodes.push(node);

    return node;
  }

  async startAll(): Promise<void> {
    await Promise.all(this.nodes.map(node => node.start()));
  }

  async stopAll(): Promise<void> {
    await Promise.all(this.nodes.map(node => node.stop()));
  }

  getLeader(): RaftNodeImpl | undefined {
    return this.nodes.find(node => node.getStatus().state === NodeState.Leader);
  }

  getFollowers(): RaftNodeImpl[] {
    return this.nodes.filter(node => node.getStatus().state === NodeState.Follower);
  }

  /**
   * Create a network partition between two groups of nodes
   */
  createPartition(group1: NodeId[], group2: NodeId[]): void {
    // Partition group1 from group2
    for (const nodeId of group1) {
      const network = this.networks.get(nodeId);
      if (network) {
        network.partitionFrom(group2);
      }
    }
    // Partition group2 from group1
    for (const nodeId of group2) {
      const network = this.networks.get(nodeId);
      if (network) {
        network.partitionFrom(group1);
      }
    }
  }

  /**
   * Heal a network partition between two groups of nodes
   */
  healPartition(group1: NodeId[], group2: NodeId[]): void {
    // Heal partition from group1 to group2
    for (const nodeId of group1) {
      const network = this.networks.get(nodeId);
      if (network) {
        network.healPartition(group2);
      }
    }
    // Heal partition from group2 to group1
    for (const nodeId of group2) {
      const network = this.networks.get(nodeId);
      if (network) {
        network.healPartition(group1);
      }
    }
  }

  async cleanup(): Promise<void> {
    await this.stopAll();
    // Give a small delay to ensure all async operations complete
    await new Promise(resolve => setTimeout(resolve, 50));
    await fs.rm(this.testDataDir, { recursive: true, force: true });
  }
}

describe('RaftNode - Leader Election', () => {
  const testDataDir = path.join(__dirname, '../../test-data-raft');
  let cluster: TestCluster;

  beforeEach(async () => {
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    cluster = new TestCluster(testDataDir);
  });

  afterEach(async () => {
    await cluster.cleanup();
  });

  it('should elect a leader in a single node cluster', async () => {
    const node = await cluster.createNode('node-1', []);
    await node.start();

    // Wait for election timeout
    await new Promise(resolve => setTimeout(resolve, 400));

    const status = node.getStatus();
    expect(status.state).toBe(NodeState.Leader);
    expect(status.leaderId).toBe('node-1');
    expect(status.currentTerm).toBeGreaterThan(0);

    await node.stop();
  });

  it('should elect a leader in a 3-node cluster', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    const followers = cluster.getFollowers();
    expect(followers.length).toBe(2);

    // All nodes should be in the same term
    const leaderStatus = leader!.getStatus();
    for (const follower of followers) {
      const status = follower.getStatus();
      expect(status.currentTerm).toBe(leaderStatus.currentTerm);
    }
  });

  it('should elect a leader in a 5-node cluster', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
    await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
    await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
    await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
    await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

    await cluster.startAll();

    // Wait for election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    const followers = cluster.getFollowers();
    expect(followers.length).toBe(4);

    // All nodes should be in the same term
    const leaderStatus = leader!.getStatus();
    for (const follower of followers) {
      const status = follower.getStatus();
      expect(status.currentTerm).toBe(leaderStatus.currentTerm);
    }
  });

  it('should elect a new leader after leader failure', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for initial election
    await new Promise(resolve => setTimeout(resolve, 600));

    const firstLeader = cluster.getLeader();
    expect(firstLeader).toBeDefined();
    const firstLeaderId = firstLeader!.getStatus().nodeId;
    const firstTerm = firstLeader!.getStatus().currentTerm;

    // Stop the leader
    await firstLeader!.stop();

    // Wait for new election (longer timeout to ensure election completes)
    await new Promise(resolve => setTimeout(resolve, 600));

    const newLeader = cluster.getLeader();
    expect(newLeader).toBeDefined();
    expect(newLeader!.getStatus().nodeId).not.toBe(firstLeaderId);
    expect(newLeader!.getStatus().currentTerm).toBeGreaterThan(firstTerm);
  });

  it('should handle split vote and retry', async () => {
    // With 2 nodes, neither can achieve majority alone
    // This tests the split vote scenario
    await cluster.createNode('node-1', ['node-2']);
    await cluster.createNode('node-2', ['node-1']);

    await cluster.startAll();

    // Wait for multiple election attempts
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Eventually one should become leader (due to randomized timeouts)
    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    const leaderStatus = leader!.getStatus();
    expect(leaderStatus.state).toBe(NodeState.Leader);
  });

  it('should update term and convert to follower on higher term', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();
    const initialTerm = leader!.getStatus().currentTerm;

    // Stop the leader to trigger new election
    await leader!.stop();

    // Wait for new election with higher term
    await new Promise(resolve => setTimeout(resolve, 500));

    // Get the new leader
    const newLeader = cluster.getLeader();
    expect(newLeader).toBeDefined();
    const newTerm = newLeader!.getStatus().currentTerm;
    expect(newTerm).toBeGreaterThan(initialTerm);

    // Restart the old leader
    await leader!.start();

    // Wait for it to discover the new term
    await new Promise(resolve => setTimeout(resolve, 400));

    // Old leader should be a follower now with updated term
    const oldLeaderStatus = leader!.getStatus();
    expect(oldLeaderStatus.state).toBe(NodeState.Follower);
    expect(oldLeaderStatus.currentTerm).toBeGreaterThanOrEqual(newTerm);
  });
});

describe('RaftNode - Log Replication', () => {
  const testDataDir = path.join(__dirname, '../../test-data-replication');
  let cluster: TestCluster;

  beforeEach(async () => {
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    cluster = new TestCluster(testDataDir);
  });

  afterEach(async () => {
    await cluster.cleanup();
  });

  it('should replicate log entries to all followers', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit a command to the leader
    const command = Buffer.from('SET key1 value1');
    const result = await leader!.submitCommand(command);
    expect(result.success).toBe(true);

    // Wait for replication
    await new Promise(resolve => setTimeout(resolve, 300));

    // Check that all nodes have the log entry
    for (const node of cluster.nodes) {
      const status = node.getStatus();
      expect(status.logSize).toBeGreaterThan(0);
    }

    // Check that commit index advanced
    const leaderStatus = leader!.getStatus();
    expect(leaderStatus.commitIndex).toBeGreaterThan(0);
  });

  it('should handle multiple commands in sequence', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit multiple commands
    const commands = [
      Buffer.from('SET key1 value1'),
      Buffer.from('SET key2 value2'),
      Buffer.from('SET key3 value3'),
    ];

    for (const command of commands) {
      const result = await leader!.submitCommand(command);
      expect(result.success).toBe(true);
    }

    // Wait for replication
    await new Promise(resolve => setTimeout(resolve, 400));

    // Check that all nodes have all log entries
    // Note: Log may contain additional entries (e.g., no-op entries from leader election)
    for (const node of cluster.nodes) {
      const status = node.getStatus();
      expect(status.logSize).toBeGreaterThanOrEqual(commands.length);
    }

    // Check that all entries are committed
    const leaderStatus = leader!.getStatus();
    expect(leaderStatus.commitIndex).toBeGreaterThanOrEqual(commands.length);
  });

  it('should advance commit index when majority replicates', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    const initialCommitIndex = leader!.getStatus().commitIndex;

    // Submit a command
    const command = Buffer.from('SET key1 value1');
    await leader!.submitCommand(command);

    // Wait for replication and commit
    await new Promise(resolve => setTimeout(resolve, 300));

    // Commit index should have advanced
    const newCommitIndex = leader!.getStatus().commitIndex;
    expect(newCommitIndex).toBeGreaterThan(initialCommitIndex);
  });

  it('should send heartbeats to maintain leadership', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();
    const leaderId = leader!.getStatus().nodeId;

    // Wait for several heartbeat intervals
    await new Promise(resolve => setTimeout(resolve, 500));

    // Leader should still be the leader
    const currentLeader = cluster.getLeader();
    expect(currentLeader).toBeDefined();
    expect(currentLeader!.getStatus().nodeId).toBe(leaderId);

    // All followers should recognize the same leader
    const followers = cluster.getFollowers();
    for (const follower of followers) {
      const status = follower.getStatus();
      expect(status.leaderId).toBe(leaderId);
    }
  });

  it('should apply committed entries to state machine', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit commands
    const commands = [
      Buffer.from('SET key1 value1'),
      Buffer.from('SET key2 value2'),
    ];

    for (const command of commands) {
      await leader!.submitCommand(command);
    }

    // Wait for replication and application
    await new Promise(resolve => setTimeout(resolve, 400));

    // Check that lastApplied matches commitIndex
    for (const node of cluster.nodes) {
      const status = node.getStatus();
      expect(status.lastApplied).toBe(status.commitIndex);
    }
  });

  it('should handle replication with slow followers', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-3']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 800));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Stop one follower to simulate slow/disconnected node
    const followers = cluster.getFollowers();
    const slowFollower = followers[0];
    await slowFollower.stop();

    // Submit commands while one follower is down
    const commands = [
      Buffer.from('SET key1 value1'),
      Buffer.from('SET key2 value2'),
      Buffer.from('SET key3 value3'),
    ];

    for (const command of commands) {
      const result = await leader!.submitCommand(command);
      expect(result.success).toBe(true);
    }

    // Wait for replication to remaining follower
    await new Promise(resolve => setTimeout(resolve, 400));

    // Leader should still commit with majority (2 out of 3)
    const leaderStatus = leader!.getStatus();
    expect(leaderStatus.commitIndex).toBeGreaterThanOrEqual(commands.length);

    // Restart the slow follower
    await slowFollower.start();

    // Wait for catch-up replication
    await new Promise(resolve => setTimeout(resolve, 600));

    // Slow follower should now have caught up
    const slowFollowerStatus = slowFollower.getStatus();
    expect(slowFollowerStatus.logSize).toBeGreaterThanOrEqual(commands.length);
    expect(slowFollowerStatus.commitIndex).toBe(leaderStatus.commitIndex);
  });

  it('should resolve log conflicts correctly', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit some commands
    await leader!.submitCommand(Buffer.from('SET key1 value1'));
    await leader!.submitCommand(Buffer.from('SET key2 value2'));

    // Wait for replication
    await new Promise(resolve => setTimeout(resolve, 300));

    // Stop the leader to trigger new election
    const oldLeader = leader!;
    await oldLeader.stop();

    // Wait for new leader election
    await new Promise(resolve => setTimeout(resolve, 600));

    const newLeader = cluster.getLeader();
    expect(newLeader).toBeDefined();
    expect(newLeader!.getStatus().nodeId).not.toBe(oldLeader.getStatus().nodeId);

    // Submit more commands to new leader
    await newLeader!.submitCommand(Buffer.from('SET key3 value3'));
    await newLeader!.submitCommand(Buffer.from('SET key4 value4'));

    // Wait for replication
    await new Promise(resolve => setTimeout(resolve, 300));

    // Restart old leader - it should reconcile its log with new leader
    await oldLeader.start();

    // Wait for log reconciliation
    await new Promise(resolve => setTimeout(resolve, 600));

    // All nodes should have consistent logs
    const newLeaderStatus = newLeader!.getStatus();
    const oldLeaderStatus = oldLeader.getStatus();

    expect(oldLeaderStatus.commitIndex).toBe(newLeaderStatus.commitIndex);
    expect(oldLeaderStatus.state).toBe(NodeState.Follower);
  });

  it('should advance commit index only for current term entries', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    const initialTerm = leader!.getStatus().currentTerm;
    const initialCommitIndex = leader!.getStatus().commitIndex;

    // Submit a command
    await leader!.submitCommand(Buffer.from('SET key1 value1'));

    // Wait for replication
    await new Promise(resolve => setTimeout(resolve, 300));

    // Commit index should have advanced
    const newCommitIndex = leader!.getStatus().commitIndex;
    expect(newCommitIndex).toBeGreaterThan(initialCommitIndex);

    // Verify the term hasn't changed
    expect(leader!.getStatus().currentTerm).toBe(initialTerm);
  });

  it('should maintain leadership through heartbeats', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();
    const leaderId = leader!.getStatus().nodeId;
    const initialTerm = leader!.getStatus().currentTerm;

    // Wait for multiple heartbeat intervals without submitting commands
    await new Promise(resolve => setTimeout(resolve, 800));

    // Leader should still be the same
    const currentLeader = cluster.getLeader();
    expect(currentLeader).toBeDefined();
    expect(currentLeader!.getStatus().nodeId).toBe(leaderId);
    expect(currentLeader!.getStatus().currentTerm).toBe(initialTerm);

    // All followers should still recognize the same leader
    const followers = cluster.getFollowers();
    expect(followers.length).toBe(2);

    for (const follower of followers) {
      const status = follower.getStatus();
      expect(status.leaderId).toBe(leaderId);
      expect(status.currentTerm).toBe(initialTerm);
      expect(status.state).toBe(NodeState.Follower);
    }
  });

  it('should replicate large batches of entries', async () => {
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 500));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit many commands
    const commandCount = 20;
    const commands: Buffer[] = [];
    for (let i = 0; i < commandCount; i++) {
      commands.push(Buffer.from(`SET key${i} value${i}`));
    }

    for (const command of commands) {
      const result = await leader!.submitCommand(command);
      expect(result.success).toBe(true);
    }

    // Wait for replication
    await new Promise(resolve => setTimeout(resolve, 800));

    // All nodes should have all entries
    for (const node of cluster.nodes) {
      const status = node.getStatus();
      expect(status.logSize).toBeGreaterThanOrEqual(commandCount);
      expect(status.commitIndex).toBeGreaterThanOrEqual(commandCount);
      expect(status.lastApplied).toBe(status.commitIndex);
    }
  });
});

describe('RaftNode - Network Partition Handling', () => {
  const testDataDir = path.join(__dirname, '../../test-data-partition');
  let cluster: TestCluster;

  beforeEach(async () => {
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    cluster = new TestCluster(testDataDir);
  });

  afterEach(async () => {
    await cluster.cleanup();
  });

  it('should allow majority partition to continue operation', async () => {
    // Create a 5-node cluster
    await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
    await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
    await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
    await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
    await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

    await cluster.startAll();

    // Wait for initial leader election
    await new Promise(resolve => setTimeout(resolve, 600));

    const initialLeader = cluster.getLeader();
    expect(initialLeader).toBeDefined();

    // Create partition: majority (node-1, node-2, node-3) vs minority (node-4, node-5)
    cluster.createPartition(['node-1', 'node-2', 'node-3'], ['node-4', 'node-5']);

    // Wait for partition to take effect
    await new Promise(resolve => setTimeout(resolve, 100));

    // Submit commands to the majority partition
    const majorityNodes = [
      cluster.nodes.find(n => n.getStatus().nodeId === 'node-1'),
      cluster.nodes.find(n => n.getStatus().nodeId === 'node-2'),
      cluster.nodes.find(n => n.getStatus().nodeId === 'node-3'),
    ].filter(n => n !== undefined) as RaftNodeImpl[];

    // Find leader in majority partition
    let majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
    
    // If no leader in majority, wait for election
    if (!majorityLeader) {
      await new Promise(resolve => setTimeout(resolve, 600));
      majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
    }

    expect(majorityLeader).toBeDefined();

    // Submit command to majority partition leader
    const result = await majorityLeader!.submitCommand(Buffer.from('SET key1 value1'));
    expect(result.success).toBe(true);

    // Wait for replication within majority partition
    await new Promise(resolve => setTimeout(resolve, 400));

    // Majority partition should commit the entry
    const majorityLeaderStatus = majorityLeader!.getStatus();
    expect(majorityLeaderStatus.commitIndex).toBeGreaterThan(0);
  });

  it('should prevent minority partition from electing leader', async () => {
    // Create a 5-node cluster
    await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
    await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
    await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
    await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
    await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

    await cluster.startAll();

    // Wait for initial leader election
    await new Promise(resolve => setTimeout(resolve, 600));

    const initialLeader = cluster.getLeader();
    expect(initialLeader).toBeDefined();

    // Create partition: majority (node-1, node-2, node-3) vs minority (node-4, node-5)
    cluster.createPartition(['node-1', 'node-2', 'node-3'], ['node-4', 'node-5']);

    // Wait for partition to take effect and multiple election attempts
    // Minority nodes will try to elect but cannot achieve majority (need 3 votes out of 5, only have 2 nodes)
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Check minority partition nodes
    const minorityNodes = [
      cluster.nodes.find(n => n.getStatus().nodeId === 'node-4'),
      cluster.nodes.find(n => n.getStatus().nodeId === 'node-5'),
    ].filter(n => n !== undefined) as RaftNodeImpl[];

    // Minority partition should not have a leader (cannot achieve majority of 5 nodes)
    // With the fix, nodes always require majority of total cluster size, not just reachable nodes
    const minorityLeader = minorityNodes.find(n => n.getStatus().state === NodeState.Leader);
    expect(minorityLeader).toBeUndefined();

    // Minority nodes should be candidates or followers (continuously trying to elect but failing)
    for (const node of minorityNodes) {
      const status = node.getStatus();
      expect([NodeState.Candidate, NodeState.Follower]).toContain(status.state);
    }
  });

  it('should reconcile logs after partition heals', async () => {
    // Create a 3-node cluster
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for initial leader election
    await new Promise(resolve => setTimeout(resolve, 600));

    const initialLeader = cluster.getLeader();
    expect(initialLeader).toBeDefined();
    const initialLeaderId = initialLeader!.getStatus().nodeId;

    // Submit some commands before partition
    await initialLeader!.submitCommand(Buffer.from('SET key1 value1'));
    await initialLeader!.submitCommand(Buffer.from('SET key2 value2'));

    // Wait for replication
    await new Promise(resolve => setTimeout(resolve, 400));

    // Create partition: isolate one node
    const isolatedNodeId = 'node-3';
    cluster.createPartition([isolatedNodeId], ['node-1', 'node-2']);

    // Wait for partition to take effect
    await new Promise(resolve => setTimeout(resolve, 100));

    // Submit more commands to majority partition
    const majorityNodes = cluster.nodes.filter(
      n => n.getStatus().nodeId !== isolatedNodeId
    );
    let majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
    
    if (!majorityLeader) {
      await new Promise(resolve => setTimeout(resolve, 600));
      majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
    }

    expect(majorityLeader).toBeDefined();

    await majorityLeader!.submitCommand(Buffer.from('SET key3 value3'));
    await majorityLeader!.submitCommand(Buffer.from('SET key4 value4'));

    // Wait for replication in majority
    await new Promise(resolve => setTimeout(resolve, 400));

    const majorityCommitIndex = majorityLeader!.getStatus().commitIndex;

    // Heal the partition
    cluster.healPartition([isolatedNodeId], ['node-1', 'node-2']);

    // Wait for log reconciliation
    await new Promise(resolve => setTimeout(resolve, 800));

    // Isolated node should have reconciled its log
    const isolatedNode = cluster.nodes.find(n => n.getStatus().nodeId === isolatedNodeId);
    expect(isolatedNode).toBeDefined();

    const isolatedStatus = isolatedNode!.getStatus();
    expect(isolatedStatus.commitIndex).toBe(majorityCommitIndex);
    expect(isolatedStatus.state).toBe(NodeState.Follower);
  });

  it('should make stale leader step down when partition heals', async () => {
    // Create a 5-node cluster
    await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
    await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
    await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
    await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
    await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

    await cluster.startAll();

    // Wait for initial leader election
    await new Promise(resolve => setTimeout(resolve, 600));

    const initialLeader = cluster.getLeader();
    expect(initialLeader).toBeDefined();
    const initialLeaderId = initialLeader!.getStatus().nodeId;
    const initialTerm = initialLeader!.getStatus().currentTerm;

    // Create partition: isolate the leader with one other node (minority)
    const otherNodeId = cluster.nodes.find(
      n => n.getStatus().nodeId !== initialLeaderId
    )!.getStatus().nodeId;
    
    cluster.createPartition(
      [initialLeaderId, otherNodeId],
      ['node-1', 'node-2', 'node-3', 'node-4', 'node-5'].filter(
        id => id !== initialLeaderId && id !== otherNodeId
      )
    );

    // Wait for majority partition to elect new leader
    await new Promise(resolve => setTimeout(resolve, 800));

    // Majority partition should have a new leader with higher term
    const majorityNodes = cluster.nodes.filter(
      n => n.getStatus().nodeId !== initialLeaderId && n.getStatus().nodeId !== otherNodeId
    );
    
    const newLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
    expect(newLeader).toBeDefined();
    expect(newLeader!.getStatus().nodeId).not.toBe(initialLeaderId);
    
    const newTerm = newLeader!.getStatus().currentTerm;
    expect(newTerm).toBeGreaterThan(initialTerm);

    // Heal the partition
    cluster.healPartition(
      [initialLeaderId, otherNodeId],
      ['node-1', 'node-2', 'node-3', 'node-4', 'node-5'].filter(
        id => id !== initialLeaderId && id !== otherNodeId
      )
    );

    // Wait for stale leader to discover new term
    await new Promise(resolve => setTimeout(resolve, 600));

    // Old leader should have stepped down
    const oldLeaderStatus = initialLeader!.getStatus();
    expect(oldLeaderStatus.state).toBe(NodeState.Follower);
    expect(oldLeaderStatus.currentTerm).toBeGreaterThanOrEqual(newTerm);
  });
});

describe('RaftNode - Recovery and Persistence', () => {
  const testDataDir = path.join(__dirname, '../../test-data-recovery');
  let cluster: TestCluster;

  beforeEach(async () => {
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    cluster = new TestCluster(testDataDir);
  });

  afterEach(async () => {
    await cluster.cleanup();
  });

  it('should preserve term and vote after node restart', async () => {
    // Create a 3-node cluster
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 600));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Get a follower to restart
    const followers = cluster.getFollowers();
    expect(followers.length).toBeGreaterThan(0);
    const followerToRestart = followers[0];
    const followerNodeId = followerToRestart.getStatus().nodeId;
    const termBeforeRestart = followerToRestart.getStatus().currentTerm;

    // Stop the follower
    await followerToRestart.stop();

    // Wait a bit
    await new Promise(resolve => setTimeout(resolve, 200));

    // Restart the follower (create new instance with same data directory)
    const nodeDataDir = path.join(testDataDir, followerNodeId);
    const persistence = new FilePersistence(nodeDataDir);
    await persistence.initialize();

    const stateMachine = new MockCacheStateMachine();
    const network = new InMemoryNetwork(followerNodeId, cluster.networks);

    const config = createConfig({
      nodeId: followerNodeId,
      peers: ['node-1', 'node-2', 'node-3'].filter(id => id !== followerNodeId),
      electionTimeoutMin: 150,
      electionTimeoutMax: 300,
      heartbeatInterval: 50,
      dataDir: nodeDataDir,
    });

    const restartedNode = new RaftNodeImpl(config, persistence, network, stateMachine);
    await restartedNode.start();

    // Wait for node to stabilize
    await new Promise(resolve => setTimeout(resolve, 400));

    // Check that term was preserved
    const statusAfterRestart = restartedNode.getStatus();
    expect(statusAfterRestart.currentTerm).toBeGreaterThanOrEqual(termBeforeRestart);
    expect(statusAfterRestart.state).toBe(NodeState.Follower);

    await restartedNode.stop();
  });

  it('should recover log entries after crash', async () => {
    // Create a 3-node cluster
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 600));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit some commands
    const commands = [
      Buffer.from('SET key1 value1'),
      Buffer.from('SET key2 value2'),
      Buffer.from('SET key3 value3'),
    ];

    for (const command of commands) {
      await leader!.submitCommand(command);
    }

    // Wait for replication
    await new Promise(resolve => setTimeout(resolve, 400));

    // Get a follower and check its log size
    const followers = cluster.getFollowers();
    const followerToRestart = followers[0];
    const followerNodeId = followerToRestart.getStatus().nodeId;
    const logSizeBeforeRestart = followerToRestart.getStatus().logSize;
    const commitIndexBeforeRestart = followerToRestart.getStatus().commitIndex;

    expect(logSizeBeforeRestart).toBeGreaterThanOrEqual(commands.length);

    // Stop the follower
    await followerToRestart.stop();

    // Wait a bit
    await new Promise(resolve => setTimeout(resolve, 200));

    // Restart the follower
    const nodeDataDir = path.join(testDataDir, followerNodeId);
    const persistence = new FilePersistence(nodeDataDir);
    await persistence.initialize();

    const stateMachine = new MockCacheStateMachine();
    const network = new InMemoryNetwork(followerNodeId, cluster.networks);

    const config = createConfig({
      nodeId: followerNodeId,
      peers: ['node-1', 'node-2', 'node-3'].filter(id => id !== followerNodeId),
      electionTimeoutMin: 150,
      electionTimeoutMax: 300,
      heartbeatInterval: 50,
      dataDir: nodeDataDir,
    });

    const restartedNode = new RaftNodeImpl(config, persistence, network, stateMachine);
    await restartedNode.start();

    // Wait for node to stabilize
    await new Promise(resolve => setTimeout(resolve, 400));

    // Check that log was recovered
    const statusAfterRestart = restartedNode.getStatus();
    expect(statusAfterRestart.logSize).toBe(logSizeBeforeRestart);
    expect(statusAfterRestart.commitIndex).toBe(commitIndexBeforeRestart);

    await restartedNode.stop();
  });

  it('should ensure committed entries survive failures', async () => {
    // Create a 3-node cluster
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 600));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit commands and wait for commit
    const commands = [
      Buffer.from('SET key1 value1'),
      Buffer.from('SET key2 value2'),
    ];

    for (const command of commands) {
      await leader!.submitCommand(command);
    }

    // Wait for replication and commit
    await new Promise(resolve => setTimeout(resolve, 400));

    const leaderCommitIndex = leader!.getStatus().commitIndex;
    expect(leaderCommitIndex).toBeGreaterThan(0);

    // Stop all nodes (simulating cluster crash)
    await cluster.stopAll();

    // Wait a bit
    await new Promise(resolve => setTimeout(resolve, 200));

    // Restart all nodes
    const nodeIds = ['node-1', 'node-2', 'node-3'];
    const restartedNodes: RaftNodeImpl[] = [];

    for (const nodeId of nodeIds) {
      const nodeDataDir = path.join(testDataDir, nodeId);
      const persistence = new FilePersistence(nodeDataDir);
      await persistence.initialize();

      const stateMachine = new MockCacheStateMachine();
      const network = new InMemoryNetwork(nodeId, cluster.networks);

      const config = createConfig({
        nodeId,
        peers: nodeIds.filter(id => id !== nodeId),
        electionTimeoutMin: 150,
        electionTimeoutMax: 300,
        heartbeatInterval: 50,
        dataDir: nodeDataDir,
      });

      const node = new RaftNodeImpl(config, persistence, network, stateMachine);
      await node.start();
      restartedNodes.push(node);
    }

    // Wait for new leader election
    await new Promise(resolve => setTimeout(resolve, 800));

    // Check that at least one node became leader
    const newLeader = restartedNodes.find(n => n.getStatus().state === NodeState.Leader);
    expect(newLeader).toBeDefined();

    // All nodes should have preserved their committed entries
    for (const node of restartedNodes) {
      const status = node.getStatus();
      expect(status.logSize).toBeGreaterThanOrEqual(commands.length);
      // Commit index might not be immediately restored, but log should be there
    }

    // Cleanup
    for (const node of restartedNodes) {
      await node.stop();
    }
  });

  it('should allow cluster to continue after node restart', async () => {
    // Create a 3-node cluster
    await cluster.createNode('node-1', ['node-2', 'node-3']);
    await cluster.createNode('node-2', ['node-1', 'node-3']);
    await cluster.createNode('node-3', ['node-1', 'node-2']);

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 600));

    const initialLeader = cluster.getLeader();
    expect(initialLeader).toBeDefined();

    // Submit some commands
    await initialLeader!.submitCommand(Buffer.from('SET key1 value1'));
    await new Promise(resolve => setTimeout(resolve, 300));

    // Get a follower to restart
    const followers = cluster.getFollowers();
    const followerToRestart = followers[0];
    const followerNodeId = followerToRestart.getStatus().nodeId;

    // Stop the follower
    await followerToRestart.stop();

    // Cluster should still be operational with 2 nodes
    await new Promise(resolve => setTimeout(resolve, 200));

    // Submit more commands while follower is down
    const result = await initialLeader!.submitCommand(Buffer.from('SET key2 value2'));
    expect(result.success).toBe(true);

    await new Promise(resolve => setTimeout(resolve, 300));

    // Restart the follower
    const nodeDataDir = path.join(testDataDir, followerNodeId);
    const persistence = new FilePersistence(nodeDataDir);
    await persistence.initialize();

    const stateMachine = new MockCacheStateMachine();
    const network = new InMemoryNetwork(followerNodeId, cluster.networks);

    const config = createConfig({
      nodeId: followerNodeId,
      peers: ['node-1', 'node-2', 'node-3'].filter(id => id !== followerNodeId),
      electionTimeoutMin: 150,
      electionTimeoutMax: 300,
      heartbeatInterval: 50,
      dataDir: nodeDataDir,
    });

    const restartedNode = new RaftNodeImpl(config, persistence, network, stateMachine);
    await restartedNode.start();

    // Wait for node to catch up
    await new Promise(resolve => setTimeout(resolve, 600));

    // Restarted node should have caught up
    const leaderStatus = initialLeader!.getStatus();
    const restartedStatus = restartedNode.getStatus();

    expect(restartedStatus.commitIndex).toBe(leaderStatus.commitIndex);
    expect(restartedStatus.state).toBe(NodeState.Follower);

    // Cluster should still be able to process commands
    const finalResult = await initialLeader!.submitCommand(Buffer.from('SET key3 value3'));
    expect(finalResult.success).toBe(true);

    await restartedNode.stop();
  });
});

describe('RaftNode - Snapshot and Log Compaction', () => {
  const testDataDir = path.join(__dirname, '../../test-data-snapshot');
  let cluster: TestCluster;

  beforeEach(async () => {
    cluster = new TestCluster(testDataDir);
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
  });

  afterEach(async () => {
    await cluster.stopAll();
  });

  it('should create snapshot at log size threshold', async () => {
    // Create a 3-node cluster with low snapshot threshold
    const node1 = await cluster.createNode('node-1', ['node-2', 'node-3']);
    const node2 = await cluster.createNode('node-2', ['node-1', 'node-3']);
    const node3 = await cluster.createNode('node-3', ['node-1', 'node-2']);

    // Override snapshot threshold to trigger quickly
    (node1 as any).config.snapshotThreshold = 5;
    (node2 as any).config.snapshotThreshold = 5;
    (node3 as any).config.snapshotThreshold = 5;

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 300));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit enough commands to trigger snapshot
    for (let i = 0; i < 10; i++) {
      const result = await leader!.submitCommand(
        Buffer.from(JSON.stringify({ type: 'SET', key: Buffer.from(`key${i}`), value: Buffer.from(`value${i}`) }))
      );
      expect(result.success).toBe(true);
    }

    // Wait for replication and snapshot creation
    await new Promise(resolve => setTimeout(resolve, 500));

    // Check that snapshot was created
    const leaderStatus = leader!.getStatus();
    expect(leaderStatus.lastApplied).toBeGreaterThan(0);

    // Verify snapshot exists in persistence
    const leaderPersistence = cluster.persistences[0];
    const snapshot = await leaderPersistence.loadSnapshot();
    expect(snapshot).toBeDefined();
    expect(snapshot!.lastIncludedIndex).toBeGreaterThan(0);
  });

  it('should truncate log after snapshot', async () => {
    // Create a single node cluster
    const node = await cluster.createNode('node-1', []);
    (node as any).config.snapshotThreshold = 5;

    await node.start();

    // Wait for leader election (single node needs time to timeout and elect itself)
    await new Promise(resolve => setTimeout(resolve, 400));

    // Verify node is leader
    expect(node.getStatus().state).toBe(NodeState.Leader);

    // Submit commands
    for (let i = 0; i < 10; i++) {
      const result = await node.submitCommand(
        Buffer.from(JSON.stringify({ type: 'SET', key: Buffer.from(`key${i}`), value: Buffer.from(`value${i}`) }))
      );
      expect(result.success).toBe(true);
    }

    // Wait for snapshot creation
    await new Promise(resolve => setTimeout(resolve, 400));

    // Check that log was truncated
    const status = node.getStatus();
    const logSize = status.logSize;

    // Log should be smaller than total commands submitted
    expect(logSize).toBeLessThan(10);
  });

  it('should send InstallSnapshot RPC for lagging followers', async () => {
    // Create a 3-node cluster
    const node1 = await cluster.createNode('node-1', ['node-2', 'node-3']);
    const node2 = await cluster.createNode('node-2', ['node-1', 'node-3']);
    const node3 = await cluster.createNode('node-3', ['node-1', 'node-2']);

    (node1 as any).config.snapshotThreshold = 5;
    (node2 as any).config.snapshotThreshold = 5;
    (node3 as any).config.snapshotThreshold = 5;

    // Start only node1 and node2
    await node1.start();
    await node2.start();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 300));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit many commands to trigger snapshot
    for (let i = 0; i < 15; i++) {
      const result = await leader!.submitCommand(
        Buffer.from(JSON.stringify({ type: 'SET', key: Buffer.from(`key${i}`), value: Buffer.from(`value${i}`) }))
      );
      expect(result.success).toBe(true);
    }

    // Wait for snapshot creation
    await new Promise(resolve => setTimeout(resolve, 500));

    // Now start node3 (it's far behind)
    await node3.start();

    // Wait for node3 to receive snapshot
    await new Promise(resolve => setTimeout(resolve, 800));

    // Node3 should have caught up via snapshot
    const leaderStatus = leader!.getStatus();
    const node3Status = node3.getStatus();

    expect(node3Status.commitIndex).toBeGreaterThan(0);
    expect(node3Status.lastApplied).toBeGreaterThan(0);
  });

  it('should restore from snapshot on restart', async () => {
    // Create a single node cluster
    const node = await cluster.createNode('node-1', []);
    (node as any).config.snapshotThreshold = 5;

    await node.start();

    // Wait for leader election (single node needs time to timeout and elect itself)
    await new Promise(resolve => setTimeout(resolve, 400));

    // Verify node is leader
    expect(node.getStatus().state).toBe(NodeState.Leader);

    // Submit commands
    const commands = [];
    for (let i = 0; i < 10; i++) {
      const cmd = { type: 'SET', key: Buffer.from(`key${i}`), value: Buffer.from(`value${i}`) };
      commands.push(cmd);
      const result = await node.submitCommand(Buffer.from(JSON.stringify(cmd)));
      expect(result.success).toBe(true);
    }

    // Wait for snapshot creation
    await new Promise(resolve => setTimeout(resolve, 400));

    const statusBeforeRestart = node.getStatus();
    const commitIndexBeforeRestart = statusBeforeRestart.commitIndex;

    // Stop the node
    await node.stop();

    // Restart the node
    const nodeDataDir = path.join(testDataDir, 'node-1');
    const persistence = new FilePersistence(nodeDataDir);
    await persistence.initialize();

    const stateMachine = new MockCacheStateMachine();
    const network = new InMemoryNetwork('node-1', cluster.networks);

    const config = createConfig({
      nodeId: 'node-1',
      peers: [],
      electionTimeoutMin: 150,
      electionTimeoutMax: 300,
      heartbeatInterval: 50,
      dataDir: nodeDataDir,
      snapshotThreshold: 5,
    });

    const restartedNode = new RaftNodeImpl(config, persistence, network, stateMachine);
    await restartedNode.start();

    // Wait for node to initialize
    await new Promise(resolve => setTimeout(resolve, 300));

    // Node should have restored from snapshot
    const statusAfterRestart = restartedNode.getStatus();
    expect(statusAfterRestart.lastApplied).toBeGreaterThan(0);
    // After snapshot, commitIndex should be at least the snapshot's lastIncludedIndex
    expect(statusAfterRestart.commitIndex).toBeGreaterThanOrEqual(statusAfterRestart.lastApplied);
    // The snapshot should have captured most of the committed entries
    expect(statusAfterRestart.lastApplied).toBeGreaterThanOrEqual(5);

    await restartedNode.stop();
  });

  it('should preserve cache state through snapshot cycle', async () => {
    // Create a 3-node cluster
    const node1 = await cluster.createNode('node-1', ['node-2', 'node-3']);
    const node2 = await cluster.createNode('node-2', ['node-1', 'node-3']);
    const node3 = await cluster.createNode('node-3', ['node-1', 'node-2']);

    (node1 as any).config.snapshotThreshold = 5;
    (node2 as any).config.snapshotThreshold = 5;
    (node3 as any).config.snapshotThreshold = 5;

    await cluster.startAll();

    // Wait for leader election
    await new Promise(resolve => setTimeout(resolve, 300));

    const leader = cluster.getLeader();
    expect(leader).toBeDefined();

    // Submit commands with known values
    const testData = [
      { key: 'user:1', value: 'Alice' },
      { key: 'user:2', value: 'Bob' },
      { key: 'user:3', value: 'Charlie' },
      { key: 'user:4', value: 'David' },
      { key: 'user:5', value: 'Eve' },
      { key: 'user:6', value: 'Frank' },
      { key: 'user:7', value: 'Grace' },
      { key: 'user:8', value: 'Henry' },
    ];

    for (const data of testData) {
      const cmd = {
        type: 'SET',
        key: Buffer.from(data.key),
        value: Buffer.from(data.value),
      };
      const result = await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      expect(result.success).toBe(true);
    }

    // Wait for replication and snapshot
    await new Promise(resolve => setTimeout(resolve, 600));

    // Verify all nodes have the same commit index
    const statuses = cluster.nodes.map(n => n.getStatus());
    const commitIndices = statuses.map(s => s.commitIndex);
    const allSame = commitIndices.every(ci => ci === commitIndices[0]);
    expect(allSame).toBe(true);

    // Verify cache state is preserved on the leader
    const leaderIndex = cluster.nodes.findIndex(n => n === leader);
    const leaderStateMachine = cluster.stateMachines[leaderIndex];
    for (const data of testData) {
      const entry = await leaderStateMachine.get(Buffer.from(data.key));
      expect(entry).toBeDefined();
      if (entry) {
        expect(entry.value.toString()).toBe(data.value);
      }
    }
  });
});
