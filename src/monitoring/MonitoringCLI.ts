/**
 * MonitoringCLI - CLI tool for monitoring Raft cluster and cache status
 */

import { RaftNodeImpl } from '../core/RaftNode';
import { NodeState } from '../types';
import { LogLevel } from '../logging/Logger';

export interface MonitoringOptions {
  refreshInterval?: number; // in milliseconds
  showLogs?: boolean;
  logLevel?: LogLevel;
  logCategory?: string;
  compact?: boolean;
}

export class MonitoringCLI {
  private nodes: Map<string, RaftNodeImpl> = new Map();
  private refreshTimer?: NodeJS.Timeout;
  private running: boolean = false;

  /**
   * Register a node for monitoring
   */
  registerNode(node: RaftNodeImpl): void {
    const status = node.getStatus();
    this.nodes.set(status.nodeId, node);
  }

  /**
   * Unregister a node from monitoring
   */
  unregisterNode(nodeId: string): void {
    this.nodes.delete(nodeId);
  }

  /**
   * Start continuous monitoring with auto-refresh
   */
  startMonitoring(options: MonitoringOptions = {}): void {
    if (this.running) {
      return;
    }

    this.running = true;
    const interval = options.refreshInterval || 2000;

    // Initial display
    this.displayStatus(options);

    // Set up refresh timer
    this.refreshTimer = setInterval(() => {
      if (this.running) {
        this.displayStatus(options);
      }
    }, interval);
  }

  /**
   * Stop continuous monitoring
   */
  stopMonitoring(): void {
    this.running = false;
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = undefined;
    }
  }

  /**
   * Display current status (one-time)
   */
  displayStatus(options: MonitoringOptions = {}): void {
    if (options.compact) {
      this.displayCompactStatus(options);
    } else {
      this.displayDetailedStatus(options);
    }
  }

  /**
   * Display detailed status with all information
   */
  private displayDetailedStatus(options: MonitoringOptions): void {
    // Clear console for refresh
    console.clear();

    const timestamp = new Date().toISOString();
    console.log('='.repeat(80));
    console.log(`RAFT CLUSTER MONITORING - ${timestamp}`);
    console.log('='.repeat(80));
    console.log();

    // Cluster overview
    this.displayClusterOverview();
    console.log();

    // Individual node status
    for (const [nodeId, node] of this.nodes.entries()) {
      this.displayNodeStatus(nodeId, node);
      console.log();
    }

    // Performance metrics
    this.displayPerformanceMetrics();
    console.log();

    // Recent logs (if enabled)
    if (options.showLogs) {
      this.displayRecentLogs(options);
      console.log();
    }

    console.log('='.repeat(80));
  }

  /**
   * Display compact status (single line per node)
   */
  private displayCompactStatus(options: MonitoringOptions): void {
    console.clear();
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] Cluster Status:`);
    console.log();

    for (const [nodeId, node] of this.nodes.entries()) {
      const status = node.getStatus();
      const metrics = node.getMetrics();
      
      const stateIcon = this.getStateIcon(status.state);
      const leaderInfo = status.state === NodeState.Leader ? '(LEADER)' : 
                        status.leaderId ? `(follows ${status.leaderId})` : '(no leader)';
      
      console.log(
        `${stateIcon} ${nodeId.padEnd(15)} | ` +
        `Term: ${status.currentTerm.toString().padStart(3)} | ` +
        `Log: ${status.logSize.toString().padStart(5)} | ` +
        `Commit: ${status.commitIndex.toString().padStart(5)} | ` +
        `Cache: ${status.cacheStats.entryCount.toString().padStart(5)} entries | ` +
        `Hit Rate: ${(status.cacheStats.hitRate * 100).toFixed(1)}% | ` +
        `${leaderInfo}`
      );
    }
  }

  /**
   * Display cluster overview
   */
  private displayClusterOverview(): void {
    console.log('CLUSTER OVERVIEW');
    console.log('-'.repeat(80));

    const statuses = Array.from(this.nodes.values()).map(n => n.getStatus());
    const leader = statuses.find(s => s.state === NodeState.Leader);
    const followers = statuses.filter(s => s.state === NodeState.Follower);
    const candidates = statuses.filter(s => s.state === NodeState.Candidate);

    console.log(`Total Nodes:     ${statuses.length}`);
    console.log(`Leader:          ${leader ? leader.nodeId : 'NONE'}`);
    console.log(`Followers:       ${followers.length}`);
    console.log(`Candidates:      ${candidates.length}`);
    
    if (leader) {
      console.log(`Current Term:    ${leader.currentTerm}`);
      console.log(`Commit Index:    ${leader.commitIndex}`);
    }
  }

  /**
   * Display individual node status
   */
  private displayNodeStatus(nodeId: string, node: RaftNodeImpl): void {
    const status = node.getStatus();
    const stateIcon = this.getStateIcon(status.state);

    console.log(`NODE: ${nodeId} ${stateIcon}`);
    console.log('-'.repeat(80));

    // Raft status
    console.log('Raft Status:');
    console.log(`  State:           ${status.state}`);
    console.log(`  Current Term:    ${status.currentTerm}`);
    console.log(`  Leader ID:       ${status.leaderId || 'unknown'}`);
    console.log(`  Log Size:        ${status.logSize} entries`);
    console.log(`  Commit Index:    ${status.commitIndex}`);
    console.log(`  Last Applied:    ${status.lastApplied}`);
    
    // Log replication progress (for leaders)
    if (status.state === NodeState.Leader) {
      console.log();
      this.displayReplicationProgress(node);
    }

    // Cache statistics
    console.log();
    console.log('Cache Statistics:');
    console.log(`  Entries:         ${status.cacheStats.entryCount}`);
    console.log(`  Memory Usage:    ${this.formatBytes(status.cacheStats.memoryUsage)} / ${this.formatBytes(status.cacheStats.maxMemory)}`);
    console.log(`  Memory %:        ${((status.cacheStats.memoryUsage / status.cacheStats.maxMemory) * 100).toFixed(2)}%`);
    console.log(`  Hit Rate:        ${(status.cacheStats.hitRate * 100).toFixed(2)}%`);
    console.log(`  Evictions:       ${status.cacheStats.evictionCount}`);
    console.log(`  Expirations:     ${status.cacheStats.expirationCount}`);
  }

  /**
   * Display log replication progress for leader
   */
  private displayReplicationProgress(node: RaftNodeImpl): void {
    const metrics = node.getMetrics();
    const status = node.getStatus();

    console.log('Log Replication Progress:');
    
    if (status.state !== NodeState.Leader) {
      console.log('  (Not a leader)');
      return;
    }

    const peers = status.clusterMembers.filter(id => id !== status.nodeId);
    
    if (peers.length === 0) {
      console.log('  (No peers to replicate to)');
      return;
    }

    // Display replication latencies per peer
    for (const peer of peers) {
      const latencies = metrics.raft.replicationLatencies.get(peer);
      if (latencies && latencies.length > 0) {
        const avgLatency = latencies.reduce((a, b) => a + b, 0) / latencies.length;
        const lastLatency = latencies[latencies.length - 1];
        console.log(`  ${peer.padEnd(15)} - Avg: ${avgLatency.toFixed(1)}ms, Last: ${lastLatency.toFixed(1)}ms`);
      } else {
        console.log(`  ${peer.padEnd(15)} - No data`);
      }
    }

    console.log(`  Failed Replications: ${metrics.raft.failedReplicationCount}`);
  }

  /**
   * Display performance metrics
   */
  private displayPerformanceMetrics(): void {
    console.log('PERFORMANCE METRICS');
    console.log('-'.repeat(80));

    // Aggregate metrics from all nodes
    let totalElections = 0;
    let totalRpcSent = 0;
    let totalRpcReceived = 0;
    let totalRpcFailures = 0;
    let totalCacheOps = 0;
    let avgReplicationLatency = 0;
    let avgElectionDuration = 0;
    let nodeCount = 0;

    for (const node of this.nodes.values()) {
      const metrics = node.getMetrics();
      totalElections += metrics.raft.electionCount;
      totalRpcSent += metrics.raft.rpcCounts.requestVoteSent + 
                      metrics.raft.rpcCounts.appendEntriesSent + 
                      metrics.raft.rpcCounts.installSnapshotSent;
      totalRpcReceived += metrics.raft.rpcCounts.requestVoteReceived + 
                          metrics.raft.rpcCounts.appendEntriesReceived + 
                          metrics.raft.rpcCounts.installSnapshotReceived;
      totalRpcFailures += metrics.raft.rpcFailures.requestVote + 
                          metrics.raft.rpcFailures.appendEntries + 
                          metrics.raft.rpcFailures.installSnapshot;
      totalCacheOps += metrics.cache.setOperations + 
                       metrics.cache.getOperations + 
                       metrics.cache.deleteOperations + 
                       metrics.cache.batchOperations;
      avgReplicationLatency += metrics.raft.averageReplicationLatency;
      avgElectionDuration += metrics.raft.averageElectionDuration;
      nodeCount++;
    }

    if (nodeCount > 0) {
      avgReplicationLatency /= nodeCount;
      avgElectionDuration /= nodeCount;
    }

    console.log('Raft Metrics:');
    console.log(`  Total Elections:         ${totalElections}`);
    console.log(`  Avg Election Duration:   ${avgElectionDuration.toFixed(1)}ms`);
    console.log(`  Avg Replication Latency: ${avgReplicationLatency.toFixed(1)}ms`);
    console.log(`  Total RPCs Sent:         ${totalRpcSent}`);
    console.log(`  Total RPCs Received:     ${totalRpcReceived}`);
    console.log(`  Total RPC Failures:      ${totalRpcFailures}`);
    
    console.log();
    console.log('Cache Metrics:');
    console.log(`  Total Operations:        ${totalCacheOps}`);

    // Show detailed cache metrics from first node (representative)
    if (this.nodes.size > 0) {
      const firstNode = Array.from(this.nodes.values())[0];
      const metrics = firstNode.getMetrics();
      console.log(`  SET Operations:          ${metrics.cache.setOperations}`);
      console.log(`  GET Operations:          ${metrics.cache.getOperations}`);
      console.log(`  DELETE Operations:       ${metrics.cache.deleteOperations}`);
      console.log(`  BATCH Operations:        ${metrics.cache.batchOperations}`);
      console.log(`  Cache Hits:              ${metrics.cache.hitCount}`);
      console.log(`  Cache Misses:            ${metrics.cache.missCount}`);
      console.log(`  Hit Rate:                ${(metrics.cache.hitRate * 100).toFixed(2)}%`);
    }
  }

  /**
   * Display recent logs
   */
  private displayRecentLogs(options: MonitoringOptions): void {
    console.log('RECENT OPERATIONS');
    console.log('-'.repeat(80));

    const filter: any = {};
    if (options.logLevel !== undefined) {
      filter.level = options.logLevel;
    }
    if (options.logCategory) {
      filter.category = options.logCategory;
    }
    // Show logs from last 10 seconds
    filter.since = Date.now() - 10000;

    // Collect logs from all nodes
    const allLogs: Array<{ nodeId: string; entry: any }> = [];
    for (const [nodeId, node] of this.nodes.entries()) {
      const logs = node.getLogHistory(filter);
      for (const entry of logs) {
        allLogs.push({ nodeId, entry });
      }
    }

    // Sort by timestamp
    allLogs.sort((a, b) => a.entry.timestamp - b.entry.timestamp);

    // Display last 20 logs
    const recentLogs = allLogs.slice(-20);
    
    if (recentLogs.length === 0) {
      console.log('  (No recent logs)');
    } else {
      for (const { nodeId, entry } of recentLogs) {
        const time = new Date(entry.timestamp).toISOString().substr(11, 12);
        const level = LogLevel[entry.level].padEnd(5);
        const category = entry.category.padEnd(12);
        console.log(`  [${time}] [${nodeId.padEnd(10)}] [${level}] [${category}] ${entry.message}`);
      }
    }
  }

  /**
   * Get icon for node state
   */
  private getStateIcon(state: NodeState): string {
    switch (state) {
      case NodeState.Leader:
        return '👑';
      case NodeState.Follower:
        return '👤';
      case NodeState.Candidate:
        return '🗳️';
      default:
        return '❓';
    }
  }

  /**
   * Format bytes to human-readable string
   */
  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`;
  }

  /**
   * Export status as JSON
   */
  exportStatusJSON(): string {
    const data: any = {
      timestamp: new Date().toISOString(),
      nodes: [],
    };

    for (const [nodeId, node] of this.nodes.entries()) {
      const status = node.getStatus();
      const metrics = node.getMetrics();
      
      data.nodes.push({
        nodeId,
        status,
        metrics,
      });
    }

    return JSON.stringify(data, null, 2);
  }
}
