/**
 * Raft Integration Tests
 * Tests complete leader election flow, log replication, network partitions, and node recovery
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { RaftNodeImpl } from '../core/RaftNode';
import { FilePersistence } from '../persistence/FilePersistence';
import { CacheStateMachineImpl } from '../cache/CacheStateMachine';
import { InMemoryNetwork } from '../network/InMemoryNetwork';
import { createConfig } from '../config';
import { NodeState, NodeId, CommandType } from '../types';
import * as fs from 'fs/promises';
import * as path from 'path';

/**
 * Test cluster helper for integration tests
 */
class IntegrationTestCluster {
  nodes: RaftNodeImpl[] = [];
  networks: Map<NodeId, InMemoryNetwork> = new Map();
  persistences: FilePersistence[] = [];
  stateMachines: CacheStateMachineImpl[] = [];
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

    const stateMachine = new CacheStateMachineImpl(1024 * 1024);
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

  createPartition(group1: NodeId[], group2: NodeId[]): void {
    for (const nodeId of group1) {
      const network = this.networks.get(nodeId);
      if (network) {
        network.partitionFrom(group2);
      }
    }
    for (const nodeId of group2) {
      const network = this.networks.get(nodeId);
      if (network) {
        network.partitionFrom(group1);
      }
    }
  }

  healPartition(group1: NodeId[], group2: NodeId[]): void {
    for (const nodeId of group1) {
      const network = this.networks.get(nodeId);
      if (network) {
        network.healPartition(group2);
      }
    }
    for (const nodeId of group2) {
      const network = this.networks.get(nodeId);
      if (network) {
        network.healPartition(group1);
      }
    }
  }

  async cleanup(): Promise<void> {
    await this.stopAll();
    await new Promise(resolve => setTimeout(resolve, 50));
    await fs.rm(this.testDataDir, { recursive: true, force: true });
  }
}

describe('Raft Integration Tests', () => {
  const testDataDir = path.join(__dirname, '../../test-data-integration');
  let cluster: IntegrationTestCluster;

  beforeEach(async () => {
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    cluster = new IntegrationTestCluster(testDataDir);
  });

  afterEach(async () => {
    await cluster.cleanup();
  });

  describe('Complete Leader Election Flow', () => {
    it('should complete full leader election in 3-node cluster', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const followers = cluster.getFollowers();
      expect(followers.length).toBe(2);

      const leaderStatus = leader!.getStatus();
      for (const follower of followers) {
        const status = follower.getStatus();
        expect(status.currentTerm).toBe(leaderStatus.currentTerm);
        expect(status.leaderId).toBe(leaderStatus.nodeId);
      }
    });

    it('should handle leader failure and re-election', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const firstLeader = cluster.getLeader();
      expect(firstLeader).toBeDefined();
      const firstLeaderId = firstLeader!.getStatus().nodeId;
      const firstTerm = firstLeader!.getStatus().currentTerm;

      const leaderIndex = cluster.nodes.indexOf(firstLeader!);
      await firstLeader!.stop();
      cluster.nodes.splice(leaderIndex, 1);
      
      await new Promise(resolve => setTimeout(resolve, 800));

      const newLeader = cluster.getLeader();
      expect(newLeader).toBeDefined();
      expect(newLeader!.getStatus().nodeId).not.toBe(firstLeaderId);
      expect(newLeader!.getStatus().currentTerm).toBeGreaterThan(firstTerm);
    });
  });

  describe('Complete Log Replication Flow', () => {
    it('should replicate entries from leader to all followers', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const commands = [
        { type: CommandType.SET, key: Buffer.from('key1'), value: Buffer.from('value1') },
        { type: CommandType.SET, key: Buffer.from('key2'), value: Buffer.from('value2') },
        { type: CommandType.SET, key: Buffer.from('key3'), value: Buffer.from('value3') },
      ];

      for (const cmd of commands) {
        const result = await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
        expect(result.success).toBe(true);
      }

      await new Promise(resolve => setTimeout(resolve, 400));

      const leaderStatus = leader!.getStatus();
      for (const node of cluster.nodes) {
        const status = node.getStatus();
        expect(status.commitIndex).toBe(leaderStatus.commitIndex);
        expect(status.lastApplied).toBe(status.commitIndex);
      }
    });

    it('should apply committed entries to state machine in order', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const commands = [
        { type: CommandType.SET, key: Buffer.from('counter'), value: Buffer.from('1') },
        { type: CommandType.SET, key: Buffer.from('counter'), value: Buffer.from('2') },
        { type: CommandType.SET, key: Buffer.from('counter'), value: Buffer.from('3') },
      ];

      for (const cmd of commands) {
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 400));

      const leaderIndex = cluster.nodes.indexOf(leader!);
      const leaderStateMachine = cluster.stateMachines[leaderIndex];
      const entry = await leaderStateMachine.get(Buffer.from('counter'));
      expect(entry).not.toBeNull();
      expect(entry!.value.toString()).toBe('3');
    });
  });

  describe('Network Partition Scenarios', () => {
    it('should allow majority partition to continue operation', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      cluster.createPartition(['node-1', 'node-2', 'node-3'], ['node-4', 'node-5']);
      await new Promise(resolve => setTimeout(resolve, 200));

      const majorityNodes = cluster.nodes.filter(n => 
        ['node-1', 'node-2', 'node-3'].includes(n.getStatus().nodeId)
      );

      let majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
      if (!majorityLeader) {
        await new Promise(resolve => setTimeout(resolve, 800));
        majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
      }

      expect(majorityLeader).toBeDefined();

      const cmd = { type: CommandType.SET, key: Buffer.from('partition-test'), value: Buffer.from('success') };
      const result = await majorityLeader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      expect(result.success).toBe(true);

      await new Promise(resolve => setTimeout(resolve, 300));
      expect(majorityLeader!.getStatus().commitIndex).toBeGreaterThan(0);
    });

    it('should prevent minority partition from making progress', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 600));

      cluster.createPartition(['node-1', 'node-2', 'node-3'], ['node-4', 'node-5']);
      await new Promise(resolve => setTimeout(resolve, 1500));

      const minorityNodes = cluster.nodes.filter(n => 
        ['node-4', 'node-5'].includes(n.getStatus().nodeId)
      );

      const minorityLeader = minorityNodes.find(n => n.getStatus().state === NodeState.Leader);
      expect(minorityLeader).toBeUndefined();
    });

    it('should reconcile logs after partition heals', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const initialLeader = cluster.getLeader();
      expect(initialLeader).toBeDefined();

      await initialLeader!.submitCommand(Buffer.from(JSON.stringify({
        type: CommandType.SET, key: Buffer.from('before'), value: Buffer.from('partition')
      })));
      await new Promise(resolve => setTimeout(resolve, 400));

      cluster.createPartition(['node-3'], ['node-1', 'node-2']);
      await new Promise(resolve => setTimeout(resolve, 200));

      const majorityNodes = cluster.nodes.filter(n => n.getStatus().nodeId !== 'node-3');
      let majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
      if (!majorityLeader) {
        await new Promise(resolve => setTimeout(resolve, 800));
        majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
      }

      expect(majorityLeader).toBeDefined();
      await majorityLeader!.submitCommand(Buffer.from(JSON.stringify({
        type: CommandType.SET, key: Buffer.from('during'), value: Buffer.from('partition')
      })));
      await new Promise(resolve => setTimeout(resolve, 300));

      const majorityCommitIndex = majorityLeader!.getStatus().commitIndex;

      cluster.healPartition(['node-3'], ['node-1', 'node-2']);
      await new Promise(resolve => setTimeout(resolve, 800));

      const isolatedNode = cluster.nodes.find(n => n.getStatus().nodeId === 'node-3');
      expect(isolatedNode!.getStatus().commitIndex).toBe(majorityCommitIndex);
    });
  });

  describe('Node Recovery After Crash', () => {
    it('should recover state after node restart', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const commands = [
        { type: CommandType.SET, key: Buffer.from('persistent1'), value: Buffer.from('value1') },
        { type: CommandType.SET, key: Buffer.from('persistent2'), value: Buffer.from('value2') },
      ];

      for (const cmd of commands) {
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }
      await new Promise(resolve => setTimeout(resolve, 500));

      const followers = cluster.getFollowers();
      const followerToRestart = followers[0];
      const followerNodeId = followerToRestart.getStatus().nodeId;
      const commitIndexBefore = followerToRestart.getStatus().commitIndex;

      await followerToRestart.stop();
      await new Promise(resolve => setTimeout(resolve, 200));

      const nodeDataDir = path.join(testDataDir, followerNodeId);
      const persistence = new FilePersistence(nodeDataDir);
      await persistence.initialize();

      const stateMachine = new CacheStateMachineImpl(1024 * 1024);
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
      await new Promise(resolve => setTimeout(resolve, 400));

      expect(restartedNode.getStatus().commitIndex).toBe(commitIndexBefore);
      expect(restartedNode.getStatus().state).toBe(NodeState.Follower);

      await restartedNode.stop();
    });

    it('should maintain cluster availability during node restart', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 600));

      const initialLeader = cluster.getLeader();
      expect(initialLeader).toBeDefined();

      const followers = cluster.getFollowers();
      const followerToRestart = followers[0];
      await followerToRestart.stop();

      await new Promise(resolve => setTimeout(resolve, 200));

      const cmd = { type: CommandType.SET, key: Buffer.from('during-restart'), value: Buffer.from('available') };
      const result = await initialLeader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      expect(result.success).toBe(true);

      await new Promise(resolve => setTimeout(resolve, 300));
      expect(initialLeader!.getStatus().commitIndex).toBeGreaterThan(0);
    });
  });
});
