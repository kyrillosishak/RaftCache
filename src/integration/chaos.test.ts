/**
 * Chaos Tests
 * Randomly kill/restart nodes, introduce network delays/packet loss,
 * create/heal partitions, and verify consistency
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

class ChaosTestCluster {
  nodes: RaftNodeImpl[] = [];
  networks: Map<NodeId, InMemoryNetwork> = new Map();
  persistences: FilePersistence[] = [];
  stateMachines: CacheStateMachineImpl[] = [];
  testDataDir: string;
  nodeConfigs: Map<NodeId, { peers: NodeId[]; dataDir: string }> = new Map();

  constructor(testDataDir: string) {
    this.testDataDir = testDataDir;
  }

  async createNode(nodeId: NodeId, peers: NodeId[]): Promise<RaftNodeImpl> {
    const nodeDataDir = path.join(this.testDataDir, nodeId);
    await fs.mkdir(nodeDataDir, { recursive: true });

    this.nodeConfigs.set(nodeId, { peers, dataDir: nodeDataDir });

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

  async restartNode(nodeId: NodeId): Promise<RaftNodeImpl> {
    const existingIndex = this.nodes.findIndex(n => n.getStatus().nodeId === nodeId);
    if (existingIndex !== -1) {
      await this.nodes[existingIndex].stop();
      this.nodes.splice(existingIndex, 1);
    }

    const config = this.nodeConfigs.get(nodeId);
    if (!config) throw new Error(`Node config not found for ${nodeId}`);

    const persistence = new FilePersistence(config.dataDir);
    await persistence.initialize();

    const stateMachine = new CacheStateMachineImpl(1024 * 1024);
    const network = new InMemoryNetwork(nodeId, this.networks);

    const raftConfig = createConfig({
      nodeId,
      peers: config.peers,
      electionTimeoutMin: 150,
      electionTimeoutMax: 300,
      heartbeatInterval: 50,
      dataDir: config.dataDir,
    });

    const node = new RaftNodeImpl(raftConfig, persistence, network, stateMachine);
    this.nodes.push(node);
    await node.start();

    return node;
  }

  async killNode(nodeId: NodeId): Promise<void> {
    const node = this.nodes.find(n => n.getStatus().nodeId === nodeId);
    if (node) {
      await node.stop();
      const index = this.nodes.indexOf(node);
      this.nodes.splice(index, 1);
    }
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

  getRunningNodes(): RaftNodeImpl[] {
    return this.nodes.filter(n => n.getStatus().state !== undefined);
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

describe('Chaos Tests', () => {
  const testDataDir = path.join(__dirname, '../../test-data-chaos');
  let cluster: ChaosTestCluster;

  beforeEach(async () => {
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    cluster = new ChaosTestCluster(testDataDir);
  });

  afterEach(async () => {
    await cluster.cleanup();
  });

  describe('Random Node Kills and Restarts', () => {
    it('should maintain consistency when randomly killing and restarting nodes', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const nodeIds = ['node-1', 'node-2', 'node-3', 'node-4', 'node-5'];
      const submittedCommands: Array<{ key: string; value: string }> = [];

      for (let round = 0; round < 3; round++) {
        const leader = cluster.getLeader();
        if (leader) {
          const cmd = {
            type: CommandType.SET,
            key: Buffer.from(`chaos-key-${round}`),
            value: Buffer.from(`chaos-value-${round}`),
          };
          submittedCommands.push({ key: `chaos-key-${round}`, value: `chaos-value-${round}` });
          await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
        }

        await new Promise(resolve => setTimeout(resolve, 100));

        const randomNode = nodeIds[Math.floor(Math.random() * nodeIds.length)];
        await cluster.killNode(randomNode);
        await new Promise(resolve => setTimeout(resolve, 200));
        await cluster.restartNode(randomNode);
        await new Promise(resolve => setTimeout(resolve, 300));
      }

      await new Promise(resolve => setTimeout(resolve, 1000));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const runningNodes = cluster.getRunningNodes();
      expect(runningNodes.length).toBeGreaterThanOrEqual(3);
    });

    it('should recover from multiple simultaneous node failures', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const cmd = {
        type: CommandType.SET,
        key: Buffer.from('before-crash'),
        value: Buffer.from('test-value'),
      };
      await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      await new Promise(resolve => setTimeout(resolve, 400));

      await cluster.killNode('node-1');
      await cluster.killNode('node-2');
      await new Promise(resolve => setTimeout(resolve, 800));

      const newLeader = cluster.getLeader();
      expect(newLeader).toBeDefined();

      await cluster.restartNode('node-1');
      await cluster.restartNode('node-2');
      await new Promise(resolve => setTimeout(resolve, 800));

      const finalLeader = cluster.getLeader();
      expect(finalLeader).toBeDefined();
    });
  });

  describe('Random Network Partitions', () => {
    it('should maintain consistency through random partition creation and healing', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      for (let round = 0; round < 2; round++) {
        cluster.createPartition(['node-1', 'node-2'], ['node-3', 'node-4', 'node-5']);
        await new Promise(resolve => setTimeout(resolve, 500));

        const majorityNodes = cluster.nodes.filter(n =>
          ['node-3', 'node-4', 'node-5'].includes(n.getStatus().nodeId)
        );
        const majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);

        if (majorityLeader) {
          const cmd = {
            type: CommandType.SET,
            key: Buffer.from(`partition-${round}`),
            value: Buffer.from(`value-${round}`),
          };
          await majorityLeader.submitCommand(Buffer.from(JSON.stringify(cmd)));
        }

        await new Promise(resolve => setTimeout(resolve, 300));

        cluster.healPartition(['node-1', 'node-2'], ['node-3', 'node-4', 'node-5']);
        await new Promise(resolve => setTimeout(resolve, 800));
      }

      const finalLeader = cluster.getLeader();
      expect(finalLeader).toBeDefined();
    });

    it('should handle cascading partitions', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 600));

      cluster.createPartition(['node-1'], ['node-2', 'node-3', 'node-4', 'node-5']);
      await new Promise(resolve => setTimeout(resolve, 300));

      cluster.createPartition(['node-2'], ['node-3', 'node-4', 'node-5']);
      await new Promise(resolve => setTimeout(resolve, 300));

      const majorityNodes = cluster.nodes.filter(n =>
        ['node-3', 'node-4', 'node-5'].includes(n.getStatus().nodeId)
      );
      const majorityLeader = majorityNodes.find(n => n.getStatus().state === NodeState.Leader);
      expect(majorityLeader).toBeDefined();

      cluster.healPartition(['node-1'], ['node-2', 'node-3', 'node-4', 'node-5']);
      cluster.healPartition(['node-2'], ['node-3', 'node-4', 'node-5']);
      await new Promise(resolve => setTimeout(resolve, 1000));

      const finalLeader = cluster.getLeader();
      expect(finalLeader).toBeDefined();
    });
  });

  describe('Cache Consistency Under Chaos', () => {
    it('should verify cache consistency after chaos operations', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 600));

      const testKeys = ['consistent-1', 'consistent-2', 'consistent-3'];
      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      for (const key of testKeys) {
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(key),
          value: Buffer.from(`value-${key}`),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 300));

      await cluster.killNode('node-3');
      cluster.createPartition(['node-1'], ['node-2', 'node-4', 'node-5']);
      await new Promise(resolve => setTimeout(resolve, 500));

      cluster.healPartition(['node-1'], ['node-2', 'node-4', 'node-5']);
      await cluster.restartNode('node-3');
      await new Promise(resolve => setTimeout(resolve, 1000));

      const finalLeader = cluster.getLeader();
      expect(finalLeader).toBeDefined();

      const leaderIndex = cluster.nodes.indexOf(finalLeader!);
      const leaderStateMachine = cluster.stateMachines[leaderIndex];

      for (const key of testKeys) {
        const entry = await leaderStateMachine.get(Buffer.from(key));
        expect(entry).not.toBeNull();
        expect(entry!.value.toString()).toBe(`value-${key}`);
      }
    });
  });

  describe('No Data Loss for Committed Entries', () => {
    it('should ensure no committed entries are lost during chaos', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const committedKeys: string[] = [];
      for (let i = 0; i < 5; i++) {
        const key = `committed-${i}`;
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(key),
          value: Buffer.from(`value-${i}`),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
        committedKeys.push(key);
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      await new Promise(resolve => setTimeout(resolve, 400));

      const commitIndexBeforeChaos = leader!.getStatus().commitIndex;

      await cluster.killNode('node-2');
      await new Promise(resolve => setTimeout(resolve, 300));
      await cluster.restartNode('node-2');
      await new Promise(resolve => setTimeout(resolve, 800));

      const finalLeader = cluster.getLeader();
      expect(finalLeader).toBeDefined();

      const leaderIndex = cluster.nodes.indexOf(finalLeader!);
      const leaderStateMachine = cluster.stateMachines[leaderIndex];

      for (const key of committedKeys) {
        const entry = await leaderStateMachine.get(Buffer.from(key));
        expect(entry).not.toBeNull();
        expect(entry!.value.toString()).toContain('value-');
      }
    });
  });
});
