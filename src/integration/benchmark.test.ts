/**
 * Performance Benchmarking Tests
 * Benchmarks throughput, latency, scalability, and identifies bottlenecks
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

class BenchmarkCluster {
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

    const stateMachine = new CacheStateMachineImpl(10 * 1024 * 1024);
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

  async cleanup(): Promise<void> {
    await this.stopAll();
    await new Promise(resolve => setTimeout(resolve, 50));
    await fs.rm(this.testDataDir, { recursive: true, force: true });
  }
}

interface BenchmarkResult {
  operationsPerSecond: number;
  totalOperations: number;
  durationMs: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  minLatencyMs: number;
  maxLatencyMs: number;
}

function calculatePercentile(sortedLatencies: number[], percentile: number): number {
  const index = Math.ceil((percentile / 100) * sortedLatencies.length) - 1;
  return sortedLatencies[Math.max(0, index)];
}

async function runBenchmark(
  cluster: BenchmarkCluster,
  operationCount: number,
  operationType: 'SET' | 'GET' | 'DELETE' | 'MIXED'
): Promise<BenchmarkResult> {
  const leader = cluster.getLeader();
  if (!leader) throw new Error('No leader found');

  const latencies: number[] = [];
  const startTime = Date.now();

  for (let i = 0; i < operationCount; i++) {
    const opStart = Date.now();

    if (operationType === 'SET' || operationType === 'MIXED') {
      const cmd = {
        type: CommandType.SET,
        key: Buffer.from(`bench-key-${i}`),
        value: Buffer.from(`bench-value-${i}`),
      };
      await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
    } else if (operationType === 'GET') {
      const leaderIndex = cluster.nodes.indexOf(leader);
      const stateMachine = cluster.stateMachines[leaderIndex];
      await stateMachine.get(Buffer.from(`bench-key-${i % 100}`));
    } else if (operationType === 'DELETE') {
      const cmd = {
        type: CommandType.DELETE,
        key: Buffer.from(`bench-key-${i}`),
      };
      await leader.submitCommand(Buffer.from(JSON.stringify(cmd)));
    }

    const opEnd = Date.now();
    latencies.push(opEnd - opStart);
  }

  const endTime = Date.now();
  const durationMs = endTime - startTime;

  latencies.sort((a, b) => a - b);

  return {
    operationsPerSecond: (operationCount / durationMs) * 1000,
    totalOperations: operationCount,
    durationMs,
    p50LatencyMs: calculatePercentile(latencies, 50),
    p95LatencyMs: calculatePercentile(latencies, 95),
    p99LatencyMs: calculatePercentile(latencies, 99),
    minLatencyMs: latencies[0],
    maxLatencyMs: latencies[latencies.length - 1],
  };
}

describe('Performance Benchmarking', () => {
  const testDataDir = path.join(__dirname, '../../test-data-benchmark');
  let cluster: BenchmarkCluster;

  beforeEach(async () => {
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    cluster = new BenchmarkCluster(testDataDir);
  });

  afterEach(async () => {
    await cluster.cleanup();
  });

  describe('Throughput Benchmarks', () => {
    it('should measure SET operations throughput', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const result = await runBenchmark(cluster, 50, 'SET');

      console.log('SET Throughput Benchmark:');
      console.log(`  Operations/sec: ${result.operationsPerSecond.toFixed(2)}`);
      console.log(`  Total operations: ${result.totalOperations}`);
      console.log(`  Duration: ${result.durationMs}ms`);

      expect(result.operationsPerSecond).toBeGreaterThan(0);
      expect(result.totalOperations).toBe(50);
    });

    it('should measure GET operations throughput', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      for (let i = 0; i < 100; i++) {
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(`bench-key-${i}`),
          value: Buffer.from(`bench-value-${i}`),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 500));

      const result = await runBenchmark(cluster, 100, 'GET');

      console.log('GET Throughput Benchmark:');
      console.log(`  Operations/sec: ${result.operationsPerSecond.toFixed(2)}`);
      console.log(`  Total operations: ${result.totalOperations}`);

      expect(result.operationsPerSecond).toBeGreaterThan(0);
    });

    it('should measure DELETE operations throughput', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      for (let i = 0; i < 50; i++) {
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(`bench-key-${i}`),
          value: Buffer.from(`bench-value-${i}`),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }

      await new Promise(resolve => setTimeout(resolve, 300));

      const result = await runBenchmark(cluster, 50, 'DELETE');

      console.log('DELETE Throughput Benchmark:');
      console.log(`  Operations/sec: ${result.operationsPerSecond.toFixed(2)}`);

      expect(result.operationsPerSecond).toBeGreaterThan(0);
    });
  });

  describe('Latency Benchmarks', () => {
    it('should measure p50, p95, p99 latencies for SET operations', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const result = await runBenchmark(cluster, 50, 'SET');

      console.log('SET Latency Benchmark:');
      console.log(`  p50: ${result.p50LatencyMs.toFixed(2)}ms`);
      console.log(`  p95: ${result.p95LatencyMs.toFixed(2)}ms`);
      console.log(`  p99: ${result.p99LatencyMs.toFixed(2)}ms`);
      console.log(`  min: ${result.minLatencyMs.toFixed(2)}ms`);
      console.log(`  max: ${result.maxLatencyMs.toFixed(2)}ms`);

      expect(result.p50LatencyMs).toBeGreaterThan(0);
      expect(result.p95LatencyMs).toBeGreaterThanOrEqual(result.p50LatencyMs);
      expect(result.p99LatencyMs).toBeGreaterThanOrEqual(result.p95LatencyMs);
    });

    it('should measure latency distribution', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const result = await runBenchmark(cluster, 30, 'SET');

      const latencyRange = result.maxLatencyMs - result.minLatencyMs;
      console.log('Latency Distribution:');
      console.log(`  Range: ${latencyRange.toFixed(2)}ms`);
      console.log(`  Variance: ${((result.p99LatencyMs - result.p50LatencyMs) / result.p50LatencyMs * 100).toFixed(2)}%`);

      expect(result.maxLatencyMs).toBeGreaterThanOrEqual(result.minLatencyMs);
    });
  });

  describe('Scalability Tests', () => {
    it('should test performance with 3-node cluster', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const result = await runBenchmark(cluster, 30, 'SET');

      console.log('3-Node Cluster Performance:');
      console.log(`  Throughput: ${result.operationsPerSecond.toFixed(2)} ops/sec`);
      console.log(`  p50 Latency: ${result.p50LatencyMs.toFixed(2)}ms`);

      expect(result.operationsPerSecond).toBeGreaterThan(0);
    });

    it('should test performance with 5-node cluster', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-2', ['node-1', 'node-3', 'node-4', 'node-5']);
      await cluster.createNode('node-3', ['node-1', 'node-2', 'node-4', 'node-5']);
      await cluster.createNode('node-4', ['node-1', 'node-2', 'node-3', 'node-5']);
      await cluster.createNode('node-5', ['node-1', 'node-2', 'node-3', 'node-4']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 600));

      const result = await runBenchmark(cluster, 30, 'SET');

      console.log('5-Node Cluster Performance:');
      console.log(`  Throughput: ${result.operationsPerSecond.toFixed(2)} ops/sec`);
      console.log(`  p50 Latency: ${result.p50LatencyMs.toFixed(2)}ms`);

      expect(result.operationsPerSecond).toBeGreaterThan(0);
    });

    it('should compare single-node vs multi-node performance', async () => {
      const singleNodeCluster = new BenchmarkCluster(path.join(testDataDir, 'single'));
      await singleNodeCluster.createNode('node-1', []);
      await singleNodeCluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 400));

      const singleResult = await runBenchmark(singleNodeCluster, 30, 'SET');

      await singleNodeCluster.cleanup();

      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);
      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const multiResult = await runBenchmark(cluster, 30, 'SET');

      console.log('Scalability Comparison:');
      console.log(`  Single-node throughput: ${singleResult.operationsPerSecond.toFixed(2)} ops/sec`);
      console.log(`  3-node throughput: ${multiResult.operationsPerSecond.toFixed(2)} ops/sec`);
      console.log(`  Single-node p50: ${singleResult.p50LatencyMs.toFixed(2)}ms`);
      console.log(`  3-node p50: ${multiResult.p50LatencyMs.toFixed(2)}ms`);

      expect(singleResult.operationsPerSecond).toBeGreaterThan(0);
      expect(multiResult.operationsPerSecond).toBeGreaterThan(0);
    });
  });

  describe('Bottleneck Identification', () => {
    it('should identify network replication overhead', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const smallPayloadStart = Date.now();
      for (let i = 0; i < 20; i++) {
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(`small-${i}`),
          value: Buffer.from('x'),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }
      const smallPayloadTime = Date.now() - smallPayloadStart;

      await new Promise(resolve => setTimeout(resolve, 200));

      const largePayloadStart = Date.now();
      for (let i = 0; i < 20; i++) {
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(`large-${i}`),
          value: Buffer.from('x'.repeat(1000)),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
      }
      const largePayloadTime = Date.now() - largePayloadStart;

      console.log('Payload Size Impact:');
      console.log(`  Small payload (20 ops): ${smallPayloadTime}ms`);
      console.log(`  Large payload (20 ops): ${largePayloadTime}ms`);
      console.log(`  Overhead ratio: ${(largePayloadTime / smallPayloadTime).toFixed(2)}x`);

      expect(smallPayloadTime).toBeGreaterThan(0);
      expect(largePayloadTime).toBeGreaterThan(0);
    });

    it('should measure commit latency overhead', async () => {
      await cluster.createNode('node-1', ['node-2', 'node-3']);
      await cluster.createNode('node-2', ['node-1', 'node-3']);
      await cluster.createNode('node-3', ['node-1', 'node-2']);

      await cluster.startAll();
      await new Promise(resolve => setTimeout(resolve, 500));

      const leader = cluster.getLeader();
      expect(leader).toBeDefined();

      const submitLatencies: number[] = [];

      for (let i = 0; i < 20; i++) {
        const start = Date.now();
        const cmd = {
          type: CommandType.SET,
          key: Buffer.from(`commit-test-${i}`),
          value: Buffer.from(`value-${i}`),
        };
        await leader!.submitCommand(Buffer.from(JSON.stringify(cmd)));
        submitLatencies.push(Date.now() - start);
      }

      submitLatencies.sort((a, b) => a - b);
      const avgSubmitLatency = submitLatencies.reduce((a, b) => a + b, 0) / submitLatencies.length;

      console.log('Commit Latency Analysis:');
      console.log(`  Average submit latency: ${avgSubmitLatency.toFixed(2)}ms`);
      console.log(`  Min: ${submitLatencies[0]}ms`);
      console.log(`  Max: ${submitLatencies[submitLatencies.length - 1]}ms`);

      expect(avgSubmitLatency).toBeGreaterThan(0);
    });
  });
});
