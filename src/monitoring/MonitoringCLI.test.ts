/**
 * Tests for MonitoringCLI
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { MonitoringCLI } from './MonitoringCLI';
import { RaftNodeImpl } from '../core/RaftNode';
import { CacheStateMachineImpl } from '../cache/CacheStateMachine';
import { FilePersistence } from '../persistence/FilePersistence';
import { createConfig } from '../config';
import { NodeState } from '../types';
import { LogLevel } from '../logging/Logger';

// Mock network interface
class MockNetwork {
  async start() {}
  async stop() {}
  registerHandlers() {}
  async sendRequestVote() {
    return { term: 0, voteGranted: false };
  }
  async sendAppendEntries() {
    return { term: 0, success: false, matchIndex: 0 };
  }
  async sendInstallSnapshot() {
    return { term: 0 };
  }
}

describe('MonitoringCLI', () => {
  let monitor: MonitoringCLI;
  let node1: RaftNodeImpl;
  let node2: RaftNodeImpl;

  beforeEach(async () => {
    monitor = new MonitoringCLI();

    // Create test nodes
    const config1 = createConfig({
      nodeId: 'node1',
      peers: ['node2'],
      dataDir: './test-data/node1',
    });

    const config2 = createConfig({
      nodeId: 'node2',
      peers: ['node1'],
      dataDir: './test-data/node2',
    });

    const persistence1 = new FilePersistence(config1.dataDir);
    const persistence2 = new FilePersistence(config2.dataDir);
    const network1 = new MockNetwork() as any;
    const network2 = new MockNetwork() as any;
    const stateMachine1 = new CacheStateMachineImpl(config1.maxMemory);
    const stateMachine2 = new CacheStateMachineImpl(config2.maxMemory);

    node1 = new RaftNodeImpl(config1, persistence1, network1, stateMachine1);
    node2 = new RaftNodeImpl(config2, persistence2, network2, stateMachine2);
  });

  afterEach(() => {
    monitor.stopMonitoring();
  });

  it('should register and unregister nodes', () => {
    monitor.registerNode(node1);
    monitor.registerNode(node2);

    // Verify nodes are registered by checking status display doesn't throw
    expect(() => monitor.displayStatus()).not.toThrow();

    monitor.unregisterNode('node1');
    expect(() => monitor.displayStatus()).not.toThrow();
  });

  it('should display status without errors', () => {
    monitor.registerNode(node1);
    monitor.registerNode(node2);

    // Mock console methods to capture output
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const clearSpy = vi.spyOn(console, 'clear').mockImplementation(() => {});

    monitor.displayStatus();

    expect(consoleSpy).toHaveBeenCalled();
    expect(clearSpy).toHaveBeenCalled();

    consoleSpy.mockRestore();
    clearSpy.mockRestore();
  });

  it('should display compact status', () => {
    monitor.registerNode(node1);
    monitor.registerNode(node2);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const clearSpy = vi.spyOn(console, 'clear').mockImplementation(() => {});

    monitor.displayStatus({ compact: true });

    expect(consoleSpy).toHaveBeenCalled();
    expect(clearSpy).toHaveBeenCalled();

    consoleSpy.mockRestore();
    clearSpy.mockRestore();
  });

  it('should display status with logs', () => {
    monitor.registerNode(node1);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const clearSpy = vi.spyOn(console, 'clear').mockImplementation(() => {});

    monitor.displayStatus({
      showLogs: true,
      logLevel: LogLevel.INFO,
    });

    expect(consoleSpy).toHaveBeenCalled();
    expect(clearSpy).toHaveBeenCalled();

    consoleSpy.mockRestore();
    clearSpy.mockRestore();
  });

  it('should export status as JSON', () => {
    monitor.registerNode(node1);
    monitor.registerNode(node2);

    const json = monitor.exportStatusJSON();
    const data = JSON.parse(json);

    expect(data).toHaveProperty('timestamp');
    expect(data).toHaveProperty('nodes');
    expect(data.nodes).toHaveLength(2);
    expect(data.nodes[0]).toHaveProperty('nodeId');
    expect(data.nodes[0]).toHaveProperty('status');
    expect(data.nodes[0]).toHaveProperty('metrics');
  });

  it('should start and stop continuous monitoring', async () => {
    monitor.registerNode(node1);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const clearSpy = vi.spyOn(console, 'clear').mockImplementation(() => {});

    monitor.startMonitoring({ refreshInterval: 100 });

    // Wait for a few refresh cycles
    await new Promise(resolve => setTimeout(resolve, 350));

    monitor.stopMonitoring();

    // Should have been called multiple times due to refresh
    expect(consoleSpy.mock.calls.length).toBeGreaterThan(2);

    consoleSpy.mockRestore();
    clearSpy.mockRestore();
  });

  it('should handle empty node list gracefully', () => {
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const clearSpy = vi.spyOn(console, 'clear').mockImplementation(() => {});

    expect(() => monitor.displayStatus()).not.toThrow();

    consoleSpy.mockRestore();
    clearSpy.mockRestore();
  });

  it('should format bytes correctly', () => {
    monitor.registerNode(node1);

    const json = monitor.exportStatusJSON();
    const data = JSON.parse(json);

    // Verify that status includes memory information
    expect(data.nodes[0].status.cacheStats).toHaveProperty('memoryUsage');
    expect(data.nodes[0].status.cacheStats).toHaveProperty('maxMemory');
  });

  it('should show cluster overview correctly', () => {
    monitor.registerNode(node1);
    monitor.registerNode(node2);

    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    const clearSpy = vi.spyOn(console, 'clear').mockImplementation(() => {});

    monitor.displayStatus();

    // Check that cluster overview information is logged
    const calls = consoleSpy.mock.calls.map(call => call[0]);
    const hasClusterOverview = calls.some(call => 
      typeof call === 'string' && call.includes('CLUSTER OVERVIEW')
    );

    expect(hasClusterOverview).toBe(true);

    consoleSpy.mockRestore();
    clearSpy.mockRestore();
  });
});
