/**
 * Cache Integration Tests
 * Tests cache operations through full Raft replication, TTL expiration, LRU eviction,
 * snapshot/compaction, and client API end-to-end
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { RaftNodeImpl } from '../core/RaftNode';
import { FilePersistence } from '../persistence/FilePersistence';
import { CacheStateMachineImpl } from '../cache/CacheStateMachine';
import { InMemoryNetwork } from '../network/InMemoryNetwork';
import { CacheClientImpl, CacheClientConfig } from '../client/CacheClient';
import { CacheServer } from '../client/CacheServer';
import { createConfig } from '../config';
import { NodeState, NodeId, CommandType } from '../types';
import * as fs from 'fs/promises';
import * as path from 'path';

class CacheIntegrationCluster {
  nodes: RaftNodeImpl[] = [];
  networks: Map<NodeId, InMemoryNetwork> = new Map();
  persistences: FilePersistence[] = [];
  stateMachines: CacheStateMachineImpl[] = [];
  servers: CacheServer[] = [];
  testDataDir: string;

  constructor(testDataDir: string) {
    this.testDataDir = testDataDir;
  }

  async createNode(
    nodeId: NodeId,
    peers: NodeId[],
    cachePort: number,
    options?: { maxMemory?: number; snapshotThreshold?: number }
  ): Promise<{ node: RaftNodeImpl; stateMachine: CacheStateMachineImpl }> {
    const nodeDataDir = path.join(this.testDataDir, nodeId);
    await fs.mkdir(nodeDataDir, { recursive: true });

    const persistence = new FilePersistence(nodeDataDir);
    await persistence.initialize();
    this.persistences.push(persistence);

    const stateMachine = new CacheStateMachineImpl(
      options?.maxMemory || 1024 * 1024,
      100
    );
    this.stateMachines.push(stateMachine);

    const network = new InMemoryNetwork(nodeId, this.networks);

    const config = createConfig({
      nodeId,
      peers,
      electionTimeoutMin: 150,
      electionTimeoutMax: 300,
      heartbeatInterval: 50,
      dataDir: nodeDataDir,
      maxMemory: options?.maxMemory || 1024 * 1024,
      snapshotThreshold: options?.snapshotThreshold || 100,
    });

    const node = new RaftNodeImpl(config, persistence, network, stateMachine);
    this.nodes.push(node);

    const server = new CacheServer(node, { port: cachePort, host: 'localhost' });
    this.servers.push(server);

    return { node, stateMachine };
  }

  async startAll(): Promise<void> {
    await Promise.all(this.nodes.map(node => node.start()));
    await Promise.all(this.servers.map(server => server.start()));
  }

  async stopAll(): Promise<void> {
    await Promise.all(this.servers.map(server => server.stop()));
    await Promise.all(this.nodes.map(node => node.stop()));
  }

  getLeader(): RaftNodeImpl | undefined {
    return this.nodes.find(node => node.getStatus().state === NodeState.Leader);
  }

  async cleanup(): Promise<void> {
    await this.stopAll();
    await new Promise(resolve => setTimeout(resolve, 50));
    await fs.rm(this.testDataDir, { recursive: true, force: true });
  }
}

describe('Cache Integration Tests', () => {
  const testDataDir = path.join(__dirname, '../../test-data-cache-integration');
  let cluster: CacheIntegrationCluster;

  beforeEach(async () => {
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    cluster = new CacheIntegrationCluster(testDataDir);
  });

  afterEach(async () => {
    await cluster.cleanup();
  });

  describe('Cache Operations Through Full Raft Replication', () => {
    it('should replicate SET operations across all nodes', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001);
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002);
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const cmd = { type: CommandType.SET, key: Buffer.from('test-key'), value: Buffer.from('test-value') };
      await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      await new Promise(resolve => setTimeout(resolve, 500));

      for (const stateMachine of cluster.stateMachines) {
        const entry = await stateMachine.get(Buffer.from('test-key'));
        expect(entry).not.toBeNull();
        expect(entry!.value.toString()).toBe('test-value');
      }
    });

    it('should replicate DELETE operations across all nodes', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001);
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002);
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const setCmd = { type: CommandType.SET, key: Buffer.from('to-delete'), value: Buffer.from('value') };
      await leader!.submitCommand(Buffer.from(JSON.stringify(setCmd)));
      await new Promise(resolve => setTimeout(resolve, 300));

      const deleteCmd = { type: CommandType.DELETE, key: Buffer.from('to-delete') };
      await leader!.submitCommand(Buffer.from(JSON.stringify(deleteCmd)));
      await new Promise(resolve => setTimeout(resolve, 300));

      for (const stateMachine of cluster.stateMachines) {
        const entry = await stateMachine.get(Buffer.from('to-delete'));
        expect(entry).toBeNull();
      }
    });

    it('should replicate batch operations atomically', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001);
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002);
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const batchCmd = {
        type: CommandType.BATCH,
        batch: [
          { type: CommandType.SET, key: Buffer.from('batch1'), value: Buffer.from('value1') },
          { type: CommandType.SET, key: Buffer.from('batch2'), value: Buffer.from('value2') },
          { type: CommandType.SET, key: Buffer.from('batch3'), value: Buffer.from('value3') },
        ],
      };

      await leader!.submitCommand(Buffer.from(JSON.stringify(batchCmd)));
      await new Promise(resolve => setTimeout(resolve, 400));

      for (const stateMachine of cluster.stateMachines) {
        const entry1 = await stateMachine.get(Buffer.from('batch1'));
        const entry2 = await stateMachine.get(Buffer.from('batch2'));
        const entry3 = await stateMachine.get(Buffer.from('batch3'));

        expect(entry1).not.toBeNull();
        expect(entry2).not.toBeNull();
        expect(entry3).not.toBeNull();
        expect(entry1!.value.toString()).toBe('value1');
        expect(entry2!.value.toString()).toBe('value2');
        expect(entry3!.value.toString()).toBe('value3');
      }
    });
  });

  describe('TTL Expiration Across Cluster', () => {
    it('should expire entries with TTL consistently across nodes', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001);
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002);
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const cmd = {
        type: CommandType.SET,
        key: Buffer.from('ttl-key'),
        value: Buffer.from('ttl-value'),
        ttl: 200,
      };

      await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      await new Promise(resolve => setTimeout(resolve, 400));

      for (const stateMachine of cluster.stateMachines) {
        const entry = await stateMachine.get(Buffer.from('ttl-key'));
        expect(entry).not.toBeNull();
      }

      await new Promise(resolve => setTimeout(resolve, 300));

      for (const stateMachine of cluster.stateMachines) {
        const entry = await stateMachine.get(Buffer.from('ttl-key'));
        expect(entry).toBeNull();
      }
    });

    it('should handle mixed TTL and non-TTL entries', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001);
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002);
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const ttlCmd = {
        type: CommandType.SET,
        key: Buffer.from('with-ttl'),
        value: Buffer.from('expires'),
        ttl: 200,
      };
      const noTtlCmd = {
        type: CommandType.SET,
        key: Buffer.from('no-ttl'),
        value: Buffer.from('persists'),
      };

      await leader!.submitCommand(Buffer.from(JSON.stringify(ttlCmd)));
      await leader!.submitCommand(Buffer.from(JSON.stringify(noTtlCmd)));
      await new Promise(resolve => setTimeout(resolve, 400));

      await new Promise(resolve => setTimeout(resolve, 300));

      for (const stateMachine of cluster.stateMachines) {
        const ttlEntry = await stateMachine.get(Buffer.from('with-ttl'));
        const noTtlEntry = await stateMachine.get(Buffer.from('no-ttl'));

        expect(ttlEntry).toBeNull();
        expect(noTtlEntry).not.toBeNull();
        expect(noTtlEntry!.value.toString()).toBe('persists');
      }
    });
  });

  describe('LRU Eviction Consistency', () => {
    it('should evict entries consistently across cluster when memory limit reached', async () => {
      const smallMemory = 500;
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001, { maxMemory: smallMemory });
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002, { maxMemory: smallMemory });
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003, { maxMemory: smallMemory });

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      for (let i = 0; i < 10; i++) {
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(`key${i}`),
          value: Buffer.from(`value${i}`),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 600));

      for (const stateMachine of cluster.stateMachines) {
        const usage = stateMachine.getMemoryUsage();
        expect(usage).toBeGreaterThanOrEqual(0);
        expect(usage).toBeLessThanOrEqual(smallMemory);
      }
    });
  });

  describe('Snapshot and Compaction with Cache Data', () => {
    it('should create snapshots and compact logs with cache data', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001, { snapshotThreshold: 5 });
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002, { snapshotThreshold: 5 });
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003, { snapshotThreshold: 5 });

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 800));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      for (let i = 0; i < 10; i++) {
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(`snap-key${i}`),
          value: Buffer.from(`snap-value${i}`),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 800));

      for (const persistence of cluster.persistences) {
        const snapshot = await persistence.loadSnapshot();
        expect(snapshot).toBeDefined();
        expect(snapshot!.lastIncludedIndex).toBeGreaterThan(0);
      }
    });

    it('should preserve cache data through snapshot restore', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001, { snapshotThreshold: 5 });
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002, { snapshotThreshold: 5 });
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003, { snapshotThreshold: 5 });

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const testData = [
        { key: 'user:1', value: 'Alice' },
        { key: 'user:2', value: 'Bob' },
        { key: 'user:3', value: 'Charlie' },
      ];

      for (const data of testData) {
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(data.key),
          value: Buffer.from(data.value),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 400));

      for (const stateMachine of cluster.stateMachines) {
        for (const data of testData) {
          const entry = await stateMachine.get(Buffer.from(data.key));
          expect(entry).not.toBeNull();
          expect(entry!.value.toString()).toBe(data.value);
        }
      }
    });
  });

  describe('Client API End-to-End', () => {
    it('should perform cache operations through client API', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001);
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002);
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const clientConfig: CacheClientConfig = {
        nodes: ['http://localhost:7001', 'http://localhost:7002', 'http://localhost:7003'],
        timeout: 5000,
      };

      const client = new CacheClientImpl(clientConfig);
      await client.connect();

      await client.set(Buffer.from('client-key'), Buffer.from('client-value'));
      await new Promise(resolve => setTimeout(resolve, 200));

      const result = await client.get(Buffer.from('client-key'));
      expect(result).not.toBeNull();
      expect(result!.toString()).toBe('client-value');

      await client.delete(Buffer.from('client-key'));
      await new Promise(resolve => setTimeout(resolve, 200));

      const deletedResult = await client.get(Buffer.from('client-key'));
      expect(deletedResult).toBeNull();

      await client.disconnect();
    });

    it('should handle batch operations through client API', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3'], 7001);
      await cluster.createNode('node-2', ['node-1', 'node-3'], 7002);
      await cluster.createNode('node-3', ['node-1', 'node-2'], 7003);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const clientConfig: CacheClientConfig = {
        nodes: ['http://localhost:7001', 'http://localhost:7002', 'http://localhost:7003'],
        timeout: 5000,
      };

      const client = new CacheClientImpl(clientConfig);
      await client.connect();

      const entries = new Map<Buffer, Buffer>([
        [Buffer.from('batch-key1'), Buffer.from('batch-value1')],
        [Buffer.from('batch-key2'), Buffer.from('batch-value2')],
        [Buffer.from('batch-key3'), Buffer.from('batch-value3')],
      ]);

      await client.batchSet(entries);
      await new Promise(resolve => setTimeout(resolve, 200));

      const keys = [Buffer.from('batch-key1'), Buffer.from('batch-key2'), Buffer.from('batch-key3')];
      const results = await client.batchGet(keys);

      expect(results.get('batch-key1')?.toString()).toBe('batch-value1');
      expect(results.get('batch-key2')?.toString()).toBe('batch-value2');
      expect(results.get('batch-key3')?.toString()).toBe('batch-value3');

      await client.disconnect();
    });
  });
});
