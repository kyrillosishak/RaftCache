/**
 * Tests for CacheClient
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { CacheClientImpl, CacheClientConfig } from './CacheClient';
import { CacheServer } from './CacheServer';
import { RaftNodeImpl } from '../core/RaftNode';
import { FilePersistence } from '../persistence/FilePersistence';
import { HttpNetwork } from '../network/HttpNetwork';
import { CacheStateMachineImpl } from '../cache/CacheStateMachine';
import { createConfig } from '../config';
import { CommandType, NodeId } from '../types';
import fs from 'fs';
import path from 'path';

describe('CacheClient', () => {
  let client: CacheClientImpl;
  let servers: CacheServer[] = [];
  let nodes: RaftNodeImpl[] = [];
  let networks: HttpNetwork[] = [];
  const testDataDir = './test-data-client';

  beforeEach(async () => {
    // Clean up test data directory
    if (fs.existsSync(testDataDir)) {
      fs.rmSync(testDataDir, { recursive: true });
    }

    // Create a 3-node cluster with HTTP networking
    const nodeIds = ['node-1', 'node-2', 'node-3'];
    const raftPorts = [5001, 5002, 5003];
    const cachePorts = [6001, 6002, 6003];
    
    // Build node address map for Raft communication
    const nodeAddresses = new Map<NodeId, string>();
    for (let i = 0; i < nodeIds.length; i++) {
      nodeAddresses.set(nodeIds[i], `http://localhost:${raftPorts[i]}`);
    }

    // Create and start all nodes
    for (let i = 0; i < nodeIds.length; i++) {
      const nodeId = nodeIds[i];
      const peers = nodeIds.filter(id => id !== nodeId);
      
      const config = createConfig({
        nodeId,
        peers,
        dataDir: path.join(testDataDir, nodeId),
        electionTimeoutMin: 150,
        electionTimeoutMax: 300,
        heartbeatInterval: 50,
      });

      const persistence = new FilePersistence(config.dataDir);
      const stateMachine = new CacheStateMachineImpl(config.maxMemory);
      
      // Create HTTP network for Raft communication
      const network = new HttpNetwork(nodeId, {
        port: raftPorts[i],
        host: 'localhost',
        nodeAddresses,
        timeout: 5000,
      });
      networks.push(network);

      const node = new RaftNodeImpl(config, persistence, network, stateMachine);
      nodes.push(node);

      // Start node (this starts the Raft network)
      await node.start();

      // Create cache server on different port
      const server = new CacheServer(node, { port: cachePorts[i], host: 'localhost' });
      await server.start();
      servers.push(server);
    }

    // Wait for leader election
    await waitForLeader(nodes);

    // Create client
    const clientConfig: CacheClientConfig = {
      nodes: cachePorts.map(port => `http://localhost:${port}`),
      timeout: 5000,
      retryAttempts: 3,
      retryDelay: 100,
    };

    client = new CacheClientImpl(clientConfig);
    await client.connect();
  });

  afterEach(async () => {
    // Disconnect client
    if (client) {
      await client.disconnect();
    }

    // Stop servers
    for (const server of servers) {
      await server.stop();
    }
    servers = [];

    // Stop nodes (this stops the Raft networks)
    for (const node of nodes) {
      await node.stop();
    }
    nodes = [];
    networks = [];

    // Clean up test data
    if (fs.existsSync(testDataDir)) {
      fs.rmSync(testDataDir, { recursive: true });
    }
  });

  describe('Basic Operations', () => {
    it('should set and get a value', async () => {
      const key = Buffer.from('test-key');
      const value = Buffer.from('test-value');

      await client.set(key, value);

      // Wait for replication
      await sleep(100);

      const result = await client.get(key);
      expect(result).not.toBeNull();
      expect(result!.toString()).toBe('test-value');
    });

    it('should return null for non-existent key', async () => {
      const key = Buffer.from('nonexistent');
      const result = await client.get(key);
      expect(result).toBeNull();
    });

    it('should delete a value', async () => {
      const key = Buffer.from('test-key');
      const value = Buffer.from('test-value');

      await client.set(key, value);
      await sleep(100);

      await client.delete(key);
      await sleep(100);

      const result = await client.get(key);
      expect(result).toBeNull();
    });

    it('should set value with TTL', async () => {
      const key = Buffer.from('ttl-key');
      const value = Buffer.from('ttl-value');
      const ttl = 100; // 100ms

      await client.set(key, value, ttl);
      await sleep(50);

      // Should still exist
      let result = await client.get(key);
      expect(result).not.toBeNull();

      // Wait for expiration
      await sleep(100);

      // Should be expired
      result = await client.get(key);
      expect(result).toBeNull();
    });
  });

  describe('Batch Operations', () => {
    it('should batch get multiple values', async () => {
      // Set up test data
      await client.set(Buffer.from('key1'), Buffer.from('value1'));
      await client.set(Buffer.from('key2'), Buffer.from('value2'));
      await client.set(Buffer.from('key3'), Buffer.from('value3'));
      await sleep(100);

      const keys = [
        Buffer.from('key1'),
        Buffer.from('key2'),
        Buffer.from('key3'),
        Buffer.from('nonexistent'),
      ];

      const results = await client.batchGet(keys);

      expect(results.size).toBe(4);
      expect(results.get('key1')?.toString()).toBe('value1');
      expect(results.get('key2')?.toString()).toBe('value2');
      expect(results.get('key3')?.toString()).toBe('value3');
      expect(results.get('nonexistent')).toBeNull();
    });

    it('should batch set multiple values', async () => {
      const entries = new Map<Buffer, Buffer>([
        [Buffer.from('batch-key1'), Buffer.from('batch-value1')],
        [Buffer.from('batch-key2'), Buffer.from('batch-value2')],
        [Buffer.from('batch-key3'), Buffer.from('batch-value3')],
      ]);

      await client.batchSet(entries);
      await sleep(100);

      const result1 = await client.get(Buffer.from('batch-key1'));
      const result2 = await client.get(Buffer.from('batch-key2'));
      const result3 = await client.get(Buffer.from('batch-key3'));

      expect(result1?.toString()).toBe('batch-value1');
      expect(result2?.toString()).toBe('batch-value2');
      expect(result3?.toString()).toBe('batch-value3');
    });

    it('should execute mixed batch operations', async () => {
      // Set up initial data
      await client.set(Buffer.from('existing'), Buffer.from('old-value'));
      await sleep(100);

      const commands = [
        {
          type: CommandType.SET,
          key: Buffer.from('new-key'),
          value: Buffer.from('new-value'),
        },
        {
          type: CommandType.DELETE,
          key: Buffer.from('existing'),
        },
      ];

      const results = await client.batchOp(commands);
      await sleep(100);

      expect(results).toHaveLength(2);

      const newValue = await client.get(Buffer.from('new-key'));
      const deletedValue = await client.get(Buffer.from('existing'));

      expect(newValue?.toString()).toBe('new-value');
      expect(deletedValue).toBeNull();
    });
  });

  describe('Leader Routing and Failover', () => {
    it('should route requests to leader', async () => {
      const key = Buffer.from('leader-test');
      const value = Buffer.from('leader-value');

      await client.set(key, value);
      await sleep(100);

      const result = await client.get(key);
      expect(result?.toString()).toBe('leader-value');
    });

    it('should handle leader failover', async () => {
      // Set initial value
      const key = Buffer.from('failover-test');
      const value = Buffer.from('failover-value');
      await client.set(key, value);
      await sleep(100);

      // Find and stop the leader
      const leader = nodes.find(n => n.getStatus().state === 'Leader');
      expect(leader).toBeDefined();
      
      const leaderIndex = nodes.indexOf(leader!);
      await leader!.stop();
      await servers[leaderIndex].stop();

      // Wait for new leader election
      await sleep(500);

      // Client should automatically discover new leader and continue working
      const result = await client.get(key);
      expect(result?.toString()).toBe('failover-value');

      // Should be able to set new values
      await client.set(Buffer.from('after-failover'), Buffer.from('new-value'));
      await sleep(100);

      const newResult = await client.get(Buffer.from('after-failover'));
      expect(newResult?.toString()).toBe('new-value');
    });
  });

  describe('Client Monitoring', () => {
    it('should track client-side metrics', async () => {
      const key = Buffer.from('metrics-test');
      const value = Buffer.from('metrics-value');

      await client.set(key, value);
      await client.get(key);
      await client.delete(key);

      const metrics = client.getMetrics();

      expect(metrics.requestCount).toBeGreaterThan(0);
      expect(metrics.averageLatency).toBeGreaterThan(0);
    });

    it('should get cluster status', async () => {
      const status = await client.getStatus();

      expect(status).toBeDefined();
      expect(status.leaderId).toBeDefined();
      expect(status.nodes).toBeDefined();
      expect(status.nodes.length).toBeGreaterThan(0);
    });
  });

  describe('Error Handling', () => {
    it('should throw error when not connected', async () => {
      const disconnectedClient = new CacheClientImpl({
        nodes: ['http://localhost:9999'],
      });

      await expect(
        disconnectedClient.get(Buffer.from('test'))
      ).rejects.toThrow('Client not connected');
    });

    it('should retry on transient failures', async () => {
      const key = Buffer.from('retry-test');
      const value = Buffer.from('retry-value');

      // This should succeed even with retries
      await client.set(key, value);
      await sleep(100);

      const result = await client.get(key);
      expect(result?.toString()).toBe('retry-value');
    });
  });
});

// Helper functions
async function waitForLeader(nodes: RaftNodeImpl[], timeout = 5000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const leader = nodes.find(n => n.getStatus().state === 'Leader');
    if (leader) {
      return;
    }
    await sleep(50);
  }
  throw new Error('No leader elected within timeout');
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
