/**
 * MetricsCollector - Collects and tracks metrics for Raft and cache operations
 */

export interface RaftMetrics {
  // Election metrics
  electionCount: number;
  electionDurations: number[]; // in milliseconds
  lastElectionDuration?: number;
  averageElectionDuration: number;

  // Replication metrics
  replicationLatencies: Map<string, number[]>; // peer -> latencies in ms
  averageReplicationLatency: number;
  failedReplicationCount: number;

  // State transition metrics
  stateTransitions: Array<{
    from: string;
    to: string;
    term: number;
    timestamp: number;
  }>;

  // RPC metrics
  rpcCounts: {
    requestVoteSent: number;
    requestVoteReceived: number;
    appendEntriesSent: number;
    appendEntriesReceived: number;
    installSnapshotSent: number;
    installSnapshotReceived: number;
  };

  rpcFailures: {
    requestVote: number;
    appendEntries: number;
    installSnapshot: number;
  };
}

export interface CacheMetrics {
  // Operation counts
  setOperations: number;
  getOperations: number;
  deleteOperations: number;
  batchOperations: number;

  // Cache performance
  hitCount: number;
  missCount: number;
  hitRate: number;

  // Eviction and expiration
  evictionCount: number;
  expirationCount: number;
  evictionsByMemoryPressure: number;

  // Memory tracking
  currentMemoryUsage: number;
  maxMemoryUsage: number;
  peakMemoryUsage: number;
}

export class MetricsCollector {
  // Raft metrics
  private raftMetrics: RaftMetrics = {
    electionCount: 0,
    electionDurations: [],
    averageElectionDuration: 0,
    replicationLatencies: new Map(),
    averageReplicationLatency: 0,
    failedReplicationCount: 0,
    stateTransitions: [],
    rpcCounts: {
      requestVoteSent: 0,
      requestVoteReceived: 0,
      appendEntriesSent: 0,
      appendEntriesReceived: 0,
      installSnapshotSent: 0,
      installSnapshotReceived: 0,
    },
    rpcFailures: {
      requestVote: 0,
      appendEntries: 0,
      installSnapshot: 0,
    },
  };

  // Cache metrics
  private cacheMetrics: CacheMetrics = {
    setOperations: 0,
    getOperations: 0,
    deleteOperations: 0,
    batchOperations: 0,
    hitCount: 0,
    missCount: 0,
    hitRate: 0,
    evictionCount: 0,
    expirationCount: 0,
    evictionsByMemoryPressure: 0,
    currentMemoryUsage: 0,
    maxMemoryUsage: 0,
    peakMemoryUsage: 0,
  };

  // Election tracking
  private currentElectionStart?: number;

  // Replication tracking
  private replicationStarts: Map<string, number> = new Map(); // requestId -> start time

  /**
   * Record the start of an election
   */
  recordElectionStart(): void {
    this.currentElectionStart = Date.now();
  }

  /**
   * Record the completion of an election
   */
  recordElectionComplete(): void {
    if (this.currentElectionStart) {
      const duration = Date.now() - this.currentElectionStart;
      this.raftMetrics.electionCount++;
      this.raftMetrics.electionDurations.push(duration);
      this.raftMetrics.lastElectionDuration = duration;

      // Keep only last 100 election durations
      if (this.raftMetrics.electionDurations.length > 100) {
        this.raftMetrics.electionDurations.shift();
      }

      // Calculate average
      const sum = this.raftMetrics.electionDurations.reduce((a, b) => a + b, 0);
      this.raftMetrics.averageElectionDuration =
        sum / this.raftMetrics.electionDurations.length;

      this.currentElectionStart = undefined;
    }
  }

  /**
   * Record a state transition
   */
  recordStateTransition(from: string, to: string, term: number): void {
    this.raftMetrics.stateTransitions.push({
      from,
      to,
      term,
      timestamp: Date.now(),
    });

    // Keep only last 100 transitions
    if (this.raftMetrics.stateTransitions.length > 100) {
      this.raftMetrics.stateTransitions.shift();
    }
  }

  /**
   * Record the start of a replication request
   */
  recordReplicationStart(peer: string, requestId: string): void {
    this.replicationStarts.set(requestId, Date.now());
  }

  /**
   * Record the completion of a replication request
   */
  recordReplicationComplete(peer: string, requestId: string, success: boolean): void {
    const startTime = this.replicationStarts.get(requestId);
    if (startTime) {
      const latency = Date.now() - startTime;

      if (success) {
        // Track latency per peer
        if (!this.raftMetrics.replicationLatencies.has(peer)) {
          this.raftMetrics.replicationLatencies.set(peer, []);
        }
        const latencies = this.raftMetrics.replicationLatencies.get(peer)!;
        latencies.push(latency);

        // Keep only last 100 latencies per peer
        if (latencies.length > 100) {
          latencies.shift();
        }

        // Calculate average across all peers
        let totalLatency = 0;
        let totalCount = 0;
        for (const peerLatencies of this.raftMetrics.replicationLatencies.values()) {
          totalLatency += peerLatencies.reduce((a, b) => a + b, 0);
          totalCount += peerLatencies.length;
        }
        this.raftMetrics.averageReplicationLatency =
          totalCount > 0 ? totalLatency / totalCount : 0;
      } else {
        this.raftMetrics.failedReplicationCount++;
      }

      this.replicationStarts.delete(requestId);
    }
  }

  /**
   * Record an RPC sent
   */
  recordRpcSent(rpcType: 'requestVote' | 'appendEntries' | 'installSnapshot'): void {
    switch (rpcType) {
      case 'requestVote':
        this.raftMetrics.rpcCounts.requestVoteSent++;
        break;
      case 'appendEntries':
        this.raftMetrics.rpcCounts.appendEntriesSent++;
        break;
      case 'installSnapshot':
        this.raftMetrics.rpcCounts.installSnapshotSent++;
        break;
    }
  }

  /**
   * Record an RPC received
   */
  recordRpcReceived(rpcType: 'requestVote' | 'appendEntries' | 'installSnapshot'): void {
    switch (rpcType) {
      case 'requestVote':
        this.raftMetrics.rpcCounts.requestVoteReceived++;
        break;
      case 'appendEntries':
        this.raftMetrics.rpcCounts.appendEntriesReceived++;
        break;
      case 'installSnapshot':
        this.raftMetrics.rpcCounts.installSnapshotReceived++;
        break;
    }
  }

  /**
   * Record an RPC failure
   */
  recordRpcFailure(rpcType: 'requestVote' | 'appendEntries' | 'installSnapshot'): void {
    this.raftMetrics.rpcFailures[rpcType]++;
  }

  /**
   * Record a cache operation
   */
  recordCacheOperation(
    operation: 'set' | 'get' | 'delete' | 'batch',
    hit?: boolean
  ): void {
    switch (operation) {
      case 'set':
        this.cacheMetrics.setOperations++;
        break;
      case 'get':
        this.cacheMetrics.getOperations++;
        if (hit !== undefined) {
          if (hit) {
            this.cacheMetrics.hitCount++;
          } else {
            this.cacheMetrics.missCount++;
          }
          // Calculate hit rate
          const total = this.cacheMetrics.hitCount + this.cacheMetrics.missCount;
          this.cacheMetrics.hitRate = total > 0 ? this.cacheMetrics.hitCount / total : 0;
        }
        break;
      case 'delete':
        this.cacheMetrics.deleteOperations++;
        break;
      case 'batch':
        this.cacheMetrics.batchOperations++;
        break;
    }
  }

  /**
   * Record a cache eviction
   */
  recordEviction(memoryPressure: boolean = false): void {
    this.cacheMetrics.evictionCount++;
    if (memoryPressure) {
      this.cacheMetrics.evictionsByMemoryPressure++;
    }
  }

  /**
   * Record a cache expiration
   */
  recordExpiration(): void {
    this.cacheMetrics.expirationCount++;
  }

  /**
   * Update memory usage metrics
   */
  updateMemoryUsage(current: number, max: number): void {
    this.cacheMetrics.currentMemoryUsage = current;
    this.cacheMetrics.maxMemoryUsage = max;
    if (current > this.cacheMetrics.peakMemoryUsage) {
      this.cacheMetrics.peakMemoryUsage = current;
    }
  }

  /**
   * Get Raft metrics
   */
  getRaftMetrics(): RaftMetrics {
    return { ...this.raftMetrics };
  }

  /**
   * Get cache metrics
   */
  getCacheMetrics(): CacheMetrics {
    return { ...this.cacheMetrics };
  }

  /**
   * Get all metrics
   */
  getAllMetrics(): { raft: RaftMetrics; cache: CacheMetrics } {
    return {
      raft: this.getRaftMetrics(),
      cache: this.getCacheMetrics(),
    };
  }

  /**
   * Reset all metrics
   */
  reset(): void {
    this.raftMetrics = {
      electionCount: 0,
      electionDurations: [],
      averageElectionDuration: 0,
      replicationLatencies: new Map(),
      averageReplicationLatency: 0,
      failedReplicationCount: 0,
      stateTransitions: [],
      rpcCounts: {
        requestVoteSent: 0,
        requestVoteReceived: 0,
        appendEntriesSent: 0,
        appendEntriesReceived: 0,
        installSnapshotSent: 0,
        installSnapshotReceived: 0,
      },
      rpcFailures: {
        requestVote: 0,
        appendEntries: 0,
        installSnapshot: 0,
      },
    };

    this.cacheMetrics = {
      setOperations: 0,
      getOperations: 0,
      deleteOperations: 0,
      batchOperations: 0,
      hitCount: 0,
      missCount: 0,
      hitRate: 0,
      evictionCount: 0,
      expirationCount: 0,
      evictionsByMemoryPressure: 0,
      currentMemoryUsage: 0,
      maxMemoryUsage: 0,
      peakMemoryUsage: 0,
    };

    this.currentElectionStart = undefined;
    this.replicationStarts.clear();
  }
}
