/**
 * Property-Based Tests
 * Tests fundamental Raft properties: election safety, leader append-only,
 * log matching, state machine safety, and idempotency
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

class PropertyTestCluster {
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

  getLeaders(): RaftNodeImpl[] {
    return this.nodes.filter(node => node.getStatus().state === NodeState.Leader);
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

describe('Property-Based Tests', () => {
  const testDataDir = path.join(__dirname, '../../test-data-property');
  let cluster: PropertyTestCluster;

  beforeEach(async () => {
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    cluster = new PropertyTestCluster(testDataDir);
  });

  afterEach(async () => {
    await cluster.cleanup();
  });

  describe('Election Safety: At Most One Leader Per Term', () => {
    it('should never have more than one leader in the same term', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 600));

      const leaders = cluster.getLeaders();
      expect(leaders.length).toBeLessThanOrEqual(1);

      if (leaders.length === 1) {
        const leaderTerm = leaders[0].getStatus().currentTerm;

        for (const node of cluster.nodes) {
          const status = node.getStatus();
          if (status.state === NodeState.Leader) {
            expect(status.currentTerm).toBe(leaderTerm);
          }
        }
      }
    });

    it('should maintain single leader property across multiple elections', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();

      for (let round = 0; round < 2; round++) {
        await new Promise(resolve => setTimeout(resolve, 800));

        const leaders = cluster.getLeaders();
        expect(leaders.length).toBeLessThanOrEqual(1);

        if (leaders.length === 1) {
          const leaderIndex = cluster.nodes.indexOf(leaders[0]);
          await leaders[0].stop();
          cluster.nodes.splice(leaderIndex, 1);
          await new Promise(resolve => setTimeout(resolve, 200));
        }
      }
    });

    it('should prevent split-brain scenarios during network partitions', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 600));

      cluster.createPartition(['node-1', 'node-2', 'node-3'], ['node-4', 'node-5']);
      await new Promise(resolve => setTimeout(resolve, 1000));

      const allLeaders = cluster.getLeaders();
      const leaderTerms = allLeaders.map(l => l.getStatus().currentTerm);
      const uniqueTerms = new Set(leaderTerms);

      for (const term of uniqueTerms) {
        const leadersInTerm = allLeaders.filter(l => l.getStatus().currentTerm === term);
        expect(leadersInTerm.length).toBeLessThanOrEqual(1);
      }
    });
  });

  describe('Leader Append-Only Property', () => {
    it('should never delete or overwrite committed entries', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leaders = cluster.getLeaders();
      expect(leaders.length).toBe(1);
      const leader = leaders[0];

      const commands = [
        { type: CommandType.SET, key: Buffer.from('append-1'), value: Buffer.from('value-1') },
        { type: CommandType.SET, key: Buffer.from('append-2'), value: Buffer.from('value-2') },
        { type: CommandType.SET, key: Buffer.from('append-3'), value: Buffer.from('value-3') },
      ];

      for (const cmd of commands) {
        await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 400));

      const commitIndexAfterCommit = leader.getStatus().commitIndex;
      const logSizeAfterCommit = leader.getStatus().logSize;

      await new Promise(resolve => setTimeout(resolve, 500));

      const commitIndexLater = leader.getStatus().commitIndex;
      const logSizeLater = leader.getStatus().logSize;

      expect(commitIndexLater).toBeGreaterThanOrEqual(commitIndexAfterCommit);
      expect(logSizeLater).toBeGreaterThanOrEqual(logSizeAfterCommit);
    });

    it('should only append new entries to the log', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leaders = cluster.getLeaders();
      const leader = leaders[0];

      const initialLogSize = leader.getStatus().logSize;

      const cmd = { type: CommandType.SET, key: Buffer.from('test'), value: Buffer.from('value') };
      await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));

      await new Promise(resolve => setTimeout(resolve, 200));

      const newLogSize = leader.getStatus().logSize;
      expect(newLogSize).toBeGreaterThan(initialLogSize);
    });
  });

  describe('Log Matching Property', () => {
    it('should ensure logs match across nodes at same index and term', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leaders = cluster.getLeaders();
      const leader = leaders[0];

      const commands = [
        { type: CommandType.SET, key: Buffer.from('match-1'), value: Buffer.from('value-1') },
        { type: CommandType.SET, key: Buffer.from('match-2'), value: Buffer.from('value-2') },
      ];

      for (const cmd of commands) {
        await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 400));

      const leaderStatus = leader.getStatus();
      for (const node of cluster.nodes) {
        const status = node.getStatus();
        expect(status.commitIndex).toBe(leaderStatus.commitIndex);
      }
    });

    it('should maintain log consistency after leader changes', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const firstLeader = cluster.getLeaders()[0];
      expect(firstLeader).toBeDefined();
      const cmd1 = { type: CommandType.SET, key: Buffer.from('before'), value: Buffer.from('change') };
      await firstLeader.submitCommand(Buffer.from(JSON.stringify(cmd1)));
      await new Promise(resolve => setTimeout(resolve, 300));

      const commitIndexBefore = firstLeader.getStatus().commitIndex;

      await firstLeader.stop();
      await new Promise(resolve => setTimeout(resolve, 600));

      const newLeader = cluster.getLeaders()[0];
      expect(newLeader).toBeDefined();

      const cmd2 = { type: CommandType.SET, key: Buffer.from('after'), value: Buffer.from('change') };
      await newLeader.submitCommand(Buffer.from(JSON.stringify(cmd2)));
      await new Promise(resolve => setTimeout(resolve, 300));

      for (const node of cluster.nodes) {
        const status = node.getStatus();
        expect(status.commitIndex).toBeGreaterThanOrEqual(commitIndexBefore);
      }
    });
  });

  describe('State Machine Safety: Linearizability', () => {
    it('should apply committed entries in the same order on all nodes', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeaders()[0];
      expect(leader).toBeDefined();

      const commands = [
        { type: CommandType.SET, key: Buffer.from('counter'), value: Buffer.from('1') },
        { type: CommandType.SET, key: Buffer.from('counter'), value: Buffer.from('2') },
        { type: CommandType.SET, key: Buffer.from('counter'), value: Buffer.from('3') },
      ];

      for (const cmd of commands) {
        await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 600));

      const expectedValue = '3';
      for (const stateMachine of cluster.stateMachines) {
        const entry = await stateMachine.get(Buffer.from('counter'));
        expect(entry).not.toBeNull();
        expect(entry!.value.toString()).toBe(expectedValue);
      }
    });

    it('should ensure all nodes converge to the same state', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeaders()[0];
      expect(leader).toBeDefined();

      const testData = [
        { key: 'key1', value: 'value1' },
        { key: 'key2', value: 'value2' },
        { key: 'key3', value: 'value3' },
      ];

      for (const data of testData) {
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(data.key),
          value: Buffer.from(data.value),
        };
        await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 600));

      for (const data of testData) {
        const values = await Promise.all(
          cluster.stateMachines.map(sm => sm.get(Buffer.from(data.key)))
        );

        const allMatch = values.every(
          entry => entry && entry.value.toString() === data.value
        );
        expect(allMatch).toBe(true);
      }
    });
  });

  describe('Cache Operations Idempotency', () => {
    it('should produce same result when replaying operations', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeaders()[0];
      expect(leader).toBeDefined();

      const cmd = { type: CommandType.SET, key: Buffer.from('idempotent'), value: Buffer.from('test') };
      await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
      await new Promise(resolve => setTimeout(resolve, 400));

      const leaderIndex = cluster.nodes.indexOf(leader);
      const firstResult = await cluster.stateMachines[leaderIndex].get(Buffer.from('idempotent'));

      await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
      await new Promise(resolve => setTimeout(resolve, 400));

      const secondResult = await cluster.stateMachines[leaderIndex].get(Buffer.from('idempotent'));

      expect(firstResult).not.toBeNull();
      expect(secondResult).not.toBeNull();
      expect(firstResult!.value.toString()).toBe(secondResult!.value.toString());
    });

    it('should handle duplicate SET operations correctly', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeaders()[0];
      expect(leader).toBeDefined();

      const cmd = { type: CommandType.SET, key: Buffer.from('dup-key'), value: Buffer.from('value1') };
      await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
      await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
      await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));

      await new Promise(resolve => setTimeout(resolve, 500));

      for (const stateMachine of cluster.stateMachines) {
        const entry = await stateMachine.get(Buffer.from('dup-key'));
        expect(entry).not.toBeNull();
        expect(entry!.value.toString()).toBe('value1');
      }

      const stats = cluster.stateMachines[0].getStats();
      expect(stats.entryCount).toBe(1);
    });

    it('should handle DELETE operations idempotently', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeaders()[0];

      const setCmd = { type: CommandType.SET, key: Buffer.from('to-delete'), value: Buffer.from('value') };
      await leader.submitCommand(Buffer.from(JSON.stringify(setCmd)));
      await new Promise(resolve => setTimeout(resolve, 200));

      const deleteCmd = { type: CommandType.DELETE, key: Buffer.from('to-delete') };
      await leader.submitCommand(Buffer.from(JSON.stringify(deleteCmd)));
      await leader.submitCommand(Buffer.from(JSON.stringify(deleteCmd)));
      await leader.submitCommand(Buffer.from(JSON.stringify(deleteCmd)));

      await new Promise(resolve => setTimeout(resolve, 400));

      for (const stateMachine of cluster.stateMachines) {
        const entry = await stateMachine.get(Buffer.from('to-delete'));
        expect(entry).toBeNull();
      }
    });
  });
});
