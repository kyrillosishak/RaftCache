/**
 * Example usage of the MonitoringCLI
 * This demonstrates how to integrate monitoring into your Raft cluster
 */

import { RaftNodeImpl } from '../core/RaftNode';
import { CacheStateMachineImpl } from '../cache/CacheStateMachine';
import { FilePersistence } from '../persistence/FilePersistence';
import { createConfig } from '../config';
import { MonitoringCLI } from './MonitoringCLI';
import { LogLevel } from '../logging/Logger';

// Mock network interface for demonstration
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

/**
 * Example 1: Basic monitoring setup
 */
async function basicMonitoringExample() {
  console.log('=== Basic Monitoring Example ===\n');

  // Create a simple 3-node cluster
  const nodes: RaftNodeImpl[] = [];
  
  for (let i = 1; i <= 3; i++) {
    const nodeId = `node${i}`;
    const peers = ['node1', 'node2', 'node3'].filter(id => id !== nodeId);
    
    const config = createConfig({
      nodeId,
      peers,
      dataDir: `./data/${nodeId}`,
    });

    const persistence = new FilePersistence(config.dataDir);
    const network = new MockNetwork() as any;
    const stateMachine = new CacheStateMachineImpl(config.maxMemory);
    
    const node = new RaftNodeImpl(config, persistence, network, stateMachine);
    nodes.push(node);
  }

  // Create monitoring CLI
  const monitor = new MonitoringCLI();

  // Register all nodes
  for (const node of nodes) {
    monitor.registerNode(node);
  }

  // Display status once
  monitor.displayStatus();

  console.log('\nBasic monitoring example complete.');
}

/**
 * Example 2: Continuous monitoring with auto-refresh
 */
async function continuousMonitoringExample() {
  console.log('=== Continuous Monitoring Example ===\n');

  // Create nodes (simplified for example)
  const monitor = new MonitoringCLI();
  
  // Start continuous monitoring with 1-second refresh
  monitor.startMonitoring({
    refreshInterval: 1000,
    showLogs: true,
    logLevel: LogLevel.INFO,
  });

  // Let it run for 10 seconds
  await new Promise(resolve => setTimeout(resolve, 10000));

  // Stop monitoring
  monitor.stopMonitoring();

  console.log('\nContinuous monitoring example complete.');
}

/**
 * Example 3: Compact monitoring mode
 */
async function compactMonitoringExample() {
  console.log('=== Compact Monitoring Example ===\n');

  const monitor = new MonitoringCLI();
  
  // Display in compact mode (one line per node)
  monitor.displayStatus({ compact: true });

  console.log('\nCompact monitoring example complete.');
}

/**
 * Example 4: Export status as JSON
 */
async function exportStatusExample() {
  console.log('=== Export Status Example ===\n');

  const monitor = new MonitoringCLI();
  
  // Export current status as JSON
  const json = monitor.exportStatusJSON();
  console.log(json);

  console.log('\nExport status example complete.');
}

/**
 * Example 5: Filtered log monitoring
 */
async function filteredLogMonitoringExample() {
  console.log('=== Filtered Log Monitoring Example ===\n');

  const monitor = new MonitoringCLI();
  
  // Monitor with filtered logs (only ELECTION category)
  monitor.displayStatus({
    showLogs: true,
    logCategory: 'ELECTION',
    logLevel: LogLevel.INFO,
  });

  console.log('\nFiltered log monitoring example complete.');
}

/**
 * Example 6: Real-time monitoring with simulated activity
 */
async function realtimeMonitoringExample() {
  console.log('=== Real-time Monitoring Example ===\n');
  console.log('This example simulates a running cluster with activity.\n');

  // Create a simple cluster
  const nodes: RaftNodeImpl[] = [];
  
  for (let i = 1; i <= 3; i++) {
    const nodeId = `node${i}`;
    const peers = ['node1', 'node2', 'node3'].filter(id => id !== nodeId);
    
    const config = createConfig({
      nodeId,
      peers,
      dataDir: `./data/${nodeId}`,
    });

    const persistence = new FilePersistence(config.dataDir);
    const network = new MockNetwork() as any;
    const stateMachine = new CacheStateMachineImpl(config.maxMemory);
    
    const node = new RaftNodeImpl(config, persistence, network, stateMachine);
    nodes.push(node);
  }

  // Start all nodes
  for (const node of nodes) {
    await node.start();
  }

  // Create and start monitoring
  const monitor = new MonitoringCLI();
  for (const node of nodes) {
    monitor.registerNode(node);
  }

  monitor.startMonitoring({
    refreshInterval: 2000,
    showLogs: true,
    logLevel: LogLevel.INFO,
  });

  // Simulate some cache operations
  console.log('Simulating cache operations...\n');
  
  // Let it run for 30 seconds
  await new Promise(resolve => setTimeout(resolve, 30000));

  // Stop monitoring and nodes
  monitor.stopMonitoring();
  for (const node of nodes) {
    await node.stop();
  }

  console.log('\nReal-time monitoring example complete.');
}

// Run examples
async function runExamples() {
  const examples = [
    { name: 'Basic Monitoring', fn: basicMonitoringExample },
    { name: 'Continuous Monitoring', fn: continuousMonitoringExample },
    { name: 'Compact Monitoring', fn: compactMonitoringExample },
    { name: 'Export Status', fn: exportStatusExample },
    { name: 'Filtered Log Monitoring', fn: filteredLogMonitoringExample },
    { name: 'Real-time Monitoring', fn: realtimeMonitoringExample },
  ];

  console.log('Raft Monitoring CLI Examples\n');
  console.log('Available examples:');
  examples.forEach((ex, i) => {
    console.log(`  ${i + 1}. ${ex.name}`);
  });
  console.log();

  // Run first example by default
  // In practice, you would select which example to run
  await examples[0].fn();
}

// Export for use in other modules
export {
  basicMonitoringExample,
  continuousMonitoringExample,
  compactMonitoringExample,
  exportStatusExample,
  filteredLogMonitoringExample,
  realtimeMonitoringExample,
};

// Run if executed directly
if (require.main === module) {
  runExamples().catch(console.error);
}
