/**
 * RaftNode - Core Raft protocol implementation
 */

import {
  NodeId,
  NodeState,
  NodeStatus,
  RequestVoteRequest,
  RequestVoteResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
  InstallSnapshotRequest,
  InstallSnapshotResponse,
  LogEntry,
  VolatileState,
  LeaderState,
} from '../types';
import { RaftConfig } from '../config';
import { PersistenceInterface } from '../persistence/PersistenceInterface';
import { NetworkInterface } from '../network/NetworkInterface';
import { CacheStateMachine } from '../cache/CacheStateMachine';
import { MetricsCollector } from '../metrics/MetricsCollector';
import { Logger, LogLevel } from '../logging/Logger';

/**
 * RaftNodeImpl - Implementation of the Raft consensus protocol
 */
export class RaftNodeImpl {
  // Configuration
  private readonly config: RaftConfig;
  private readonly nodeId: NodeId;
  private readonly peers: NodeId[];

  // Dependencies
  private readonly persistence: PersistenceInterface;
  private readonly network: NetworkInterface;
  private readonly stateMachine: CacheStateMachine;

  // Metrics and logging
  private readonly metrics: MetricsCollector;
  private readonly logger: Logger;

  // Persistent state (must be persisted before responding to RPCs)
  private currentTerm: number = 0;
  private votedFor?: NodeId;
  private log: LogEntry[] = [];

  // Volatile state (all nodes)
  private volatile: VolatileState = {
    commitIndex: 0,
    lastApplied: 0,
    state: NodeState.Follower,
    leaderId: undefined,
    electionTimeout: undefined,
    heartbeatTimeout: undefined,
    expirationTimer: undefined,
    lastSnapshotIndex: 0,
  };

  // Volatile state (leaders only)
  private leaderState?: LeaderState;

  // Control flags
  private running: boolean = false;

  constructor(
    config: RaftConfig,
    persistence: PersistenceInterface,
    network: NetworkInterface,
    stateMachine: CacheStateMachine
  ) {
    this.config = config;
    this.nodeId = config.nodeId;
    this.peers = config.peers;
    this.persistence = persistence;
    this.network = network;
    this.stateMachine = stateMachine;

    // Initialize metrics and logging
    const logLevel = process.env.DEBUG ? LogLevel.DEBUG : LogLevel.INFO;
    this.metrics = new MetricsCollector();
    this.logger = new Logger(this.nodeId, logLevel);

    this.logger.info('INIT', `RaftNode initialized with peers: ${this.peers.join(', ')}`);
  }

  /**
   * Start the Raft node
   */
  async start(): Promise<void> {
    if (this.running) {
      this.logger.warn('LIFECYCLE', 'RaftNode already running');
      return;
    }

    this.logger.info('LIFECYCLE', `Starting RaftNode ${this.nodeId}`);
    this.running = true;

    // Load persistent state
    await this.loadPersistentState();

    // Register RPC handlers
    this.network.registerHandlers({
      handleRequestVote: this.handleRequestVote.bind(this),
      handleAppendEntries: this.handleAppendEntries.bind(this),
      handleInstallSnapshot: this.handleInstallSnapshot.bind(this),
    });

    // Start network layer
    await this.network.start();

    // Start state machine
    await this.stateMachine.start();

    // Start as follower
    await this.becomeFollower(this.currentTerm);

    this.logger.info('LIFECYCLE', `RaftNode started in term ${this.currentTerm}`);
  }

  /**
   * Stop the Raft node
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.logger.info('LIFECYCLE', `Stopping RaftNode ${this.nodeId}`);
    this.running = false;

    // Clear all timers
    this.clearTimers();

    // Stop state machine
    await this.stateMachine.stop();

    // Stop network layer
    await this.network.stop();

    this.logger.info('LIFECYCLE', `RaftNode stopped`);
  }

  /**
   * Submit a command to be replicated through Raft
   * Only succeeds if this node is the leader
   */
  async submitCommand(command: Buffer): Promise<{ success: boolean; error?: string }> {
    if (this.volatile.state !== NodeState.Leader) {
      this.logger.warn('COMMAND', 'Command rejected: not the leader', {
        currentLeader: this.volatile.leaderId,
      });
      return {
        success: false,
        error: `Not the leader. Current leader: ${this.volatile.leaderId || 'unknown'}`,
      };
    }

    try {
      // Create new log entry with current term
      const newIndex = this.log.length > 0 ? this.log[this.log.length - 1].index + 1 : 1;
      const logEntry: LogEntry = {
        term: this.currentTerm,
        index: newIndex,
        command,
      };

      this.logger.debug('COMMAND', `Appending log entry at index ${newIndex}`, {
        term: this.currentTerm,
      });

      // Append to local log
      this.log.push(logEntry);

      // Persist log entry before replication (Req 3.2)
      await this.persistence.appendLogEntries([logEntry]);
      this.logger.debug('COMMAND', `Persisted log entry at index ${newIndex}`);

      // Trigger replication to followers
      this.sendAppendEntriesToAllPeers();

      // For single-node clusters, immediately commit and apply
      if (this.peers.length === 0) {
        this.advanceCommitIndex();
      }

      // TODO: Return future/promise for commit notification
      // For now, return success immediately after persistence
      return { success: true };
    } catch (error) {
      this.logger.error('COMMAND', 'Failed to submit command', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  /**
   * Get current node status for monitoring
   */
  getStatus(): NodeStatus {
    const cacheStats = this.stateMachine.getStats();

    return {
      nodeId: this.nodeId,
      state: this.volatile.state,
      currentTerm: this.currentTerm,
      leaderId: this.volatile.leaderId,
      logSize: this.log.length,
      commitIndex: this.volatile.commitIndex,
      lastApplied: this.volatile.lastApplied,
      clusterMembers: [this.nodeId, ...this.peers],
      cacheStats,
    };
  }

  /**
   * Get metrics for monitoring
   */
  getMetrics() {
    return this.metrics.getAllMetrics();
  }

  /**
   * Get log history for debugging
   */
  getLogHistory(filter?: { level?: LogLevel; category?: string; since?: number }) {
    return this.logger.getHistory(filter);
  }

  /**
   * Handle RequestVote RPC from candidate
   */
  async handleRequestVote(request: RequestVoteRequest): Promise<RequestVoteResponse> {
    // Record RPC received
    this.metrics.recordRpcReceived('requestVote');
    this.logger.logRpcRequest('RequestVote', 'received', request.candidateId, {
      term: request.term,
      lastLogIndex: request.lastLogIndex,
      lastLogTerm: request.lastLogTerm,
    });

    // Reply false if term < currentTerm (§5.1)
    if (request.term < this.currentTerm) {
      this.logger.debug('ELECTION', `Rejecting vote for ${request.candidateId}: stale term`, {
        requestTerm: request.term,
        currentTerm: this.currentTerm,
      });
      return {
        term: this.currentTerm,
        voteGranted: false,
      };
    }

    // If RPC request contains term T > currentTerm, set currentTerm = T and convert to follower (§5.1)
    if (request.term > this.currentTerm) {
      this.logger.info('ELECTION', `Discovered higher term from ${request.candidateId}`, {
        newTerm: request.term,
        oldTerm: this.currentTerm,
      });
      await this.becomeFollower(request.term);
    }

    // Determine if candidate's log is at least as up-to-date as receiver's log (§5.4)
    const lastLogIndex = this.log.length > 0 ? this.log[this.log.length - 1].index : 0;
    const lastLogTerm = this.log.length > 0 ? this.log[this.log.length - 1].term : 0;

    const candidateLogUpToDate =
      request.lastLogTerm > lastLogTerm ||
      (request.lastLogTerm === lastLogTerm && request.lastLogIndex >= lastLogIndex);

    // Grant vote if:
    // 1. votedFor is null or candidateId
    // 2. candidate's log is at least as up-to-date as receiver's log
    const canGrantVote =
      (this.votedFor === undefined || this.votedFor === request.candidateId) &&
      candidateLogUpToDate;

    if (canGrantVote) {
      this.logger.info('ELECTION', `Granting vote to ${request.candidateId}`, {
        term: request.term,
      });

      // Update votedFor and persist before granting vote
      this.votedFor = request.candidateId;
      try {
        await this.persistence.saveState(this.currentTerm, this.votedFor);
        this.logger.debug('ELECTION', `Persisted vote for ${request.candidateId}`);
      } catch (error) {
        this.logger.error('ELECTION', 'Failed to persist vote', error);
        // If we can't persist, we must not grant the vote to maintain safety
        return {
          term: this.currentTerm,
          voteGranted: false,
        };
      }

      // Reset election timeout when granting vote
      this.resetElectionTimeout();

      return {
        term: this.currentTerm,
        voteGranted: true,
      };
    } else {
      const reason = !candidateLogUpToDate
        ? 'candidate log not up-to-date'
        : `already voted for ${this.votedFor}`;
      this.logger.debug('ELECTION', `Rejecting vote for ${request.candidateId}: ${reason}`);

      return {
        term: this.currentTerm,
        voteGranted: false,
      };
    }
  }

  /**
   * Handle AppendEntries RPC from leader
   */
  async handleAppendEntries(request: AppendEntriesRequest): Promise<AppendEntriesResponse> {
    // Record RPC received
    this.metrics.recordRpcReceived('appendEntries');
    
    const isHeartbeat = request.entries.length === 0;
    if (!isHeartbeat) {
      this.logger.logRpcRequest('AppendEntries', 'received', request.leaderId, {
        entries: request.entries.length,
        prevLogIndex: request.prevLogIndex,
        prevLogTerm: request.prevLogTerm,
        leaderCommit: request.leaderCommit,
      });
    }

    // Reply false if term < currentTerm (§5.1)
    if (request.term < this.currentTerm) {
      this.logger.debug('REPLICATION', `Rejecting AppendEntries: stale term`, {
        requestTerm: request.term,
        currentTerm: this.currentTerm,
        leader: request.leaderId,
      });
      return {
        term: this.currentTerm,
        success: false,
        matchIndex: 0,
      };
    }

    // If RPC request contains term T > currentTerm, set currentTerm = T and convert to follower (§5.1)
    if (request.term > this.currentTerm) {
      this.logger.info('REPLICATION', `Discovered higher term from ${request.leaderId}`, {
        newTerm: request.term,
        oldTerm: this.currentTerm,
      });
      await this.becomeFollower(request.term, request.leaderId);
    } else if (this.volatile.state !== NodeState.Follower) {
      // If we're a candidate with same term, step down to follower
      await this.becomeFollower(request.term, request.leaderId);
    } else {
      // Update leader ID if we're already a follower
      this.volatile.leaderId = request.leaderId;
    }

    // Reset election timeout on valid AppendEntries (Req 1.3)
    this.resetElectionTimeout();

    // Check if log contains an entry at prevLogIndex with prevLogTerm (§5.3)
    if (request.prevLogIndex > 0) {
      const prevEntry = this.log.find(entry => entry.index === request.prevLogIndex);

      if (!prevEntry) {
        // Log doesn't contain entry at prevLogIndex
        this.logger.debug('REPLICATION', `Log doesn't contain entry at prevLogIndex`, {
          prevLogIndex: request.prevLogIndex,
        });
        return {
          term: this.currentTerm,
          success: false,
          matchIndex: 0,
        };
      }

      if (prevEntry.term !== request.prevLogTerm) {
        // Log contains entry at prevLogIndex but with different term
        this.logger.debug('REPLICATION', `Log conflict at prevLogIndex`, {
          prevLogIndex: request.prevLogIndex,
          existingTerm: prevEntry.term,
          expectedTerm: request.prevLogTerm,
        });

        // Delete the conflicting entry and all that follow it (§5.3)
        const conflictIndex = this.log.findIndex(entry => entry.index === request.prevLogIndex);
        if (conflictIndex >= 0) {
          this.log = this.log.slice(0, conflictIndex);
          await this.persistence.truncateLog(request.prevLogIndex);
          this.logger.debug('REPLICATION', `Truncated log from index ${request.prevLogIndex}`);
        }

        return {
          term: this.currentTerm,
          success: false,
          matchIndex: 0,
        };
      }
    }

    // Append new entries (§5.3)
    if (request.entries.length > 0) {
      // Find where to start appending
      let appendIndex = 0;
      for (let i = 0; i < request.entries.length; i++) {
        const newEntry = request.entries[i];
        const existingEntry = this.log.find(entry => entry.index === newEntry.index);

        if (existingEntry) {
          // If an existing entry conflicts with a new one (same index but different terms),
          // delete the existing entry and all that follow it (§5.3)
          if (existingEntry.term !== newEntry.term) {
            this.logger.debug('REPLICATION', `Conflict at index ${newEntry.index}`, {
              existingTerm: existingEntry.term,
              newTerm: newEntry.term,
            });

            const conflictIndex = this.log.findIndex(entry => entry.index === newEntry.index);
            if (conflictIndex >= 0) {
              this.log = this.log.slice(0, conflictIndex);
              await this.persistence.truncateLog(newEntry.index);
              this.logger.debug('REPLICATION', `Truncated log from index ${newEntry.index}`);
            }

            appendIndex = i;
            break;
          }
          // Entry already exists with same term, skip it
        } else {
          // Entry doesn't exist, start appending from here
          appendIndex = i;
          break;
        }
      }

      // Append new entries
      const entriesToAppend = request.entries.slice(appendIndex);
      if (entriesToAppend.length > 0) {
        this.log.push(...entriesToAppend);

        // Persist log changes before responding (Req 3.2)
        try {
          await this.persistence.appendLogEntries(entriesToAppend);
          this.logger.debug('REPLICATION', `Appended ${entriesToAppend.length} entries to log`);
        } catch (error) {
          this.logger.error('REPLICATION', 'Failed to persist log entries', error);
          return {
            term: this.currentTerm,
            success: false,
            matchIndex: 0,
          };
        }
      }
    }

    // Update commitIndex (§5.3)
    if (request.leaderCommit > this.volatile.commitIndex) {
      const lastNewEntryIndex =
        request.entries.length > 0
          ? request.entries[request.entries.length - 1].index
          : request.prevLogIndex;

      const newCommitIndex = Math.min(request.leaderCommit, lastNewEntryIndex);

      if (newCommitIndex > this.volatile.commitIndex) {
        this.logger.debug('COMMIT', `Updating commit index`, {
          from: this.volatile.commitIndex,
          to: newCommitIndex,
        });
        this.volatile.commitIndex = newCommitIndex;

        // Apply committed entries to state machine
        this.applyCommittedEntries();
      }
    }

    // Calculate matchIndex for response
    const matchIndex =
      request.entries.length > 0
        ? request.entries[request.entries.length - 1].index
        : request.prevLogIndex;

    return {
      term: this.currentTerm,
      success: true,
      matchIndex,
    };
  }

  /**
   * Handle InstallSnapshot RPC from leader
   * Receives snapshot chunks and restores cache state (Req 10.4)
   */
  async handleInstallSnapshot(request: InstallSnapshotRequest): Promise<InstallSnapshotResponse> {
    // Record RPC received
    this.metrics.recordRpcReceived('installSnapshot');
    this.logger.logRpcRequest('InstallSnapshot', 'received', request.leaderId, {
      lastIncludedIndex: request.lastIncludedIndex,
      lastIncludedTerm: request.lastIncludedTerm,
      offset: request.offset,
      done: request.done,
    });

    // Reply immediately if term < currentTerm
    if (request.term < this.currentTerm) {
      this.logger.debug('SNAPSHOT', `Rejecting InstallSnapshot: stale term`, {
        requestTerm: request.term,
        currentTerm: this.currentTerm,
      });
      return {
        term: this.currentTerm,
      };
    }

    // If RPC request contains term T > currentTerm, set currentTerm = T and convert to follower
    if (request.term > this.currentTerm) {
      this.logger.info('SNAPSHOT', `Discovered higher term from ${request.leaderId}`, {
        newTerm: request.term,
        oldTerm: this.currentTerm,
      });
      await this.becomeFollower(request.term, request.leaderId);
    } else if (this.volatile.state !== NodeState.Follower) {
      await this.becomeFollower(request.term, request.leaderId);
    } else {
      this.volatile.leaderId = request.leaderId;
    }

    // Reset election timeout
    this.resetElectionTimeout();

    try {
      // For simplicity, we'll handle the entire snapshot in one chunk
      // In a production system, you'd accumulate chunks based on offset
      if (request.done) {
        this.logger.info('SNAPSHOT', `Receiving snapshot from leader`, {
          lastIncludedIndex: request.lastIncludedIndex,
          lastIncludedTerm: request.lastIncludedTerm,
          dataSize: request.data.length,
        });

        // Deserialize snapshot from data buffer
        const snapshotData = JSON.parse(request.data.toString('utf-8'));
        
        // Convert serialized entries back to Map with Buffers
        const entries = new Map<string, any>();
        for (const [key, entry] of Object.entries(snapshotData.entries)) {
          entries.set(key, {
            key: Buffer.from((entry as any).key, 'base64'),
            value: Buffer.from((entry as any).value, 'base64'),
            createdAt: (entry as any).createdAt,
            expiresAt: (entry as any).expiresAt,
            lastAccessedAt: (entry as any).lastAccessedAt,
            size: (entry as any).size,
          });
        }

        const snapshot = {
          entries,
          lastIncludedIndex: request.lastIncludedIndex,
          lastIncludedTerm: request.lastIncludedTerm,
          timestamp: snapshotData.timestamp,
          memoryUsage: snapshotData.memoryUsage,
        };

        // Save snapshot to persistent storage
        await this.persistence.saveSnapshot(snapshot);

        // Restore cache state from snapshot
        await this.stateMachine.restoreSnapshot(snapshot);

        // Discard log entries covered by snapshot
        this.log = this.log.filter(entry => entry.index > request.lastIncludedIndex);

        // Update volatile state
        this.volatile.lastSnapshotIndex = request.lastIncludedIndex;
        this.volatile.lastApplied = request.lastIncludedIndex;
        this.volatile.commitIndex = Math.max(this.volatile.commitIndex, request.lastIncludedIndex);

        this.logger.info('SNAPSHOT', `Snapshot installed successfully`, {
          lastIncludedIndex: request.lastIncludedIndex,
          entries: snapshot.entries.size,
          remainingLogEntries: this.log.length,
        });
      }

      return {
        term: this.currentTerm,
      };
    } catch (error) {
      this.logger.error('SNAPSHOT', 'Failed to install snapshot', error);
      return {
        term: this.currentTerm,
      };
    }
  }

  /**
   * Transition to Follower state
   * Updates term if higher, persists state, and resets election timeout
   */
  private async becomeFollower(term: number, leaderId?: NodeId): Promise<void> {
    const previousState = this.volatile.state;
    const stateChanged = previousState !== NodeState.Follower;
    const termChanged = term > this.currentTerm;

    this.logger.logStateTransition(previousState, NodeState.Follower, term);
    if (stateChanged) {
      this.metrics.recordStateTransition(previousState, NodeState.Follower, term);
    }

    // Update state
    this.volatile.state = NodeState.Follower;
    this.volatile.leaderId = leaderId;

    // Update term if higher
    if (termChanged) {
      this.currentTerm = term;
      this.votedFor = undefined; // Clear vote for new term

      // Persist term and vote
      try {
        await this.persistence.saveState(this.currentTerm, this.votedFor);
        this.logger.debug('STATE', `Persisted term ${this.currentTerm} and cleared vote`);
      } catch (error) {
        this.logger.error('STATE', 'Failed to persist state', error);
        throw error;
      }
    }

    // Clear leader-specific state
    this.leaderState = undefined;

    // Clear heartbeat timer (followers don't send heartbeats)
    if (this.volatile.heartbeatTimeout) {
      clearTimeout(this.volatile.heartbeatTimeout);
      this.volatile.heartbeatTimeout = undefined;
    }

    // Reset election timeout
    this.resetElectionTimeout();
  }

  /**
   * Transition to Candidate state
   * Increments term, votes for self, and starts election
   */
  private async becomeCandidate(): Promise<void> {
    const previousState = this.volatile.state;
    const newTerm = this.currentTerm + 1;

    this.logger.logStateTransition(previousState, NodeState.Candidate, newTerm);
    this.metrics.recordStateTransition(previousState, NodeState.Candidate, newTerm);
    this.metrics.recordElectionStart();

    // Update state
    this.volatile.state = NodeState.Candidate;
    this.volatile.leaderId = undefined;

    // Increment term and vote for self
    this.currentTerm = newTerm;
    this.votedFor = this.nodeId;

    // Persist term and vote before requesting votes
    try {
      await this.persistence.saveState(this.currentTerm, this.votedFor);
      this.logger.debug('STATE', `Persisted term ${this.currentTerm} and vote for self`);
    } catch (error) {
      this.logger.error('STATE', 'Failed to persist state', error);
      throw error;
    }

    // Clear leader-specific state
    this.leaderState = undefined;

    // Clear heartbeat timer
    if (this.volatile.heartbeatTimeout) {
      clearTimeout(this.volatile.heartbeatTimeout);
      this.volatile.heartbeatTimeout = undefined;
    }

    // Reset election timeout with randomization
    this.resetElectionTimeout();

    // Start election by sending RequestVote RPCs to all peers
    await this.startElection();
  }

  /**
   * Start election by sending RequestVote RPCs to all peers
   */
  private async startElection(): Promise<void> {
    const lastLogIndex = this.log.length > 0 ? this.log[this.log.length - 1].index : 0;
    const lastLogTerm = this.log.length > 0 ? this.log[this.log.length - 1].term : 0;

    const request: RequestVoteRequest = {
      term: this.currentTerm,
      candidateId: this.nodeId,
      lastLogIndex,
      lastLogTerm,
    };

    this.logger.logElection('start', this.currentTerm, {
      peers: this.peers.length,
      lastLogIndex,
      lastLogTerm,
    });

    // Count votes (start with 1 for self)
    let votesReceived = 1;
    const votesNeeded = Math.floor((this.peers.length + 1) / 2) + 1;

    this.logger.debug('ELECTION', `Need ${votesNeeded} votes out of ${this.peers.length + 1} nodes`);

    // Check if we already have majority (single node cluster)
    if (votesReceived >= votesNeeded) {
      this.logger.logElection('won', this.currentTerm, { votes: votesReceived, needed: votesNeeded });
      this.metrics.recordElectionComplete();
      this.becomeLeader();
      return;
    }

    // Send RequestVote RPCs to all peers in parallel
    const votePromises = this.peers.map(async (peer) => {
      try {
        this.metrics.recordRpcSent('requestVote');
        this.logger.logRpcRequest('RequestVote', 'sent', peer, { term: this.currentTerm });
        
        const response = await this.network.sendRequestVote(peer, request);
        this.logger.logRpcResponse('RequestVote', peer, response.voteGranted, { term: response.term });

        // Handle response (check for higher term first)
        if (response.term > this.currentTerm) {
          this.logger.info('ELECTION', `Discovered higher term in vote response`, {
            newTerm: response.term,
            peer,
          });
          await this.becomeFollower(response.term);
          return response;
        }

        // If vote was granted and we're still a candidate for the same term
        if (response.voteGranted && response.term === this.currentTerm && this.volatile.state === NodeState.Candidate) {
          votesReceived++;
          this.logger.debug('ELECTION', `Vote granted by ${peer}`, { totalVotes: votesReceived });

          // Check if we've achieved majority
          if (votesReceived >= votesNeeded && this.volatile.state === NodeState.Candidate) {
            this.logger.logElection('won', this.currentTerm, { votes: votesReceived, needed: votesNeeded });
            this.metrics.recordElectionComplete();
            this.becomeLeader();
          }
        }

        return response;
      } catch (error) {
        this.metrics.recordRpcFailure('requestVote');
        this.logger.warn('ELECTION', `Failed to send RequestVote to ${peer}`, error);
        return null;
      }
    });

    // Wait for all responses (or timeouts)
    await Promise.all(votePromises);

    // If we're still a candidate and didn't win, the election timeout will trigger a new election
    if (this.volatile.state === NodeState.Candidate) {
      this.logger.logElection('timeout', this.currentTerm, {
        votesReceived,
        votesNeeded,
      });
    }
  }

  /**
   * Transition to Leader state
   * Initializes leader-specific state and starts sending heartbeats
   */
  private becomeLeader(): void {
    const previousState = this.volatile.state;
    
    this.logger.logStateTransition(previousState, NodeState.Leader, this.currentTerm);
    this.metrics.recordStateTransition(previousState, NodeState.Leader, this.currentTerm);

    // Update state
    this.volatile.state = NodeState.Leader;
    this.volatile.leaderId = this.nodeId;

    // Initialize leader-specific state
    const lastLogIndex = this.log.length > 0 ? this.log[this.log.length - 1].index : 0;
    this.leaderState = {
      nextIndex: new Map(),
      matchIndex: new Map(),
    };

    // Initialize nextIndex and matchIndex for each peer
    for (const peer of this.peers) {
      // nextIndex initialized to leader's last log index + 1
      this.leaderState.nextIndex.set(peer, lastLogIndex + 1);
      // matchIndex initialized to 0
      this.leaderState.matchIndex.set(peer, 0);
    }

    // Clear election timeout (leaders don't need election timeouts)
    if (this.volatile.electionTimeout) {
      clearTimeout(this.volatile.electionTimeout);
      this.volatile.electionTimeout = undefined;
    }

    // Start sending heartbeats immediately
    this.resetHeartbeatTimeout();

    // Send initial empty AppendEntries (heartbeat) to establish authority
    this.sendAppendEntriesToAllPeers();
  }

  /**
   * Load persistent state from storage
   */
  private async loadPersistentState(): Promise<void> {
    try {
      const state = await this.persistence.loadState();
      this.currentTerm = state.currentTerm;
      this.votedFor = state.votedFor;

      // Try to load snapshot first (Req 10.5)
      const snapshot = await this.persistence.loadSnapshot();
      if (snapshot) {
        this.logger.info('PERSISTENCE', `Loading snapshot`, {
          lastIncludedIndex: snapshot.lastIncludedIndex,
          lastIncludedTerm: snapshot.lastIncludedTerm,
          entries: snapshot.entries.size,
        });

        // Restore cache state from snapshot
        await this.stateMachine.restoreSnapshot(snapshot);

        // Update volatile state
        this.volatile.lastSnapshotIndex = snapshot.lastIncludedIndex;
        this.volatile.lastApplied = snapshot.lastIncludedIndex;
        this.volatile.commitIndex = snapshot.lastIncludedIndex;
      }

      // Load log entries (after snapshot if exists)
      const lastLogIndex = await this.persistence.getLastLogIndex();
      if (lastLogIndex > 0) {
        this.log = await this.persistence.getLogEntries(1, lastLogIndex);
      }

      // Replay log entries after snapshot
      if (snapshot && this.log.length > 0) {
        this.logger.info('PERSISTENCE', `Replaying ${this.log.length} log entries after snapshot`);
        // The log entries will be applied when the node starts and commitIndex is set
      }

      this.logger.info('PERSISTENCE', `Loaded persistent state`, {
        term: this.currentTerm,
        votedFor: this.votedFor,
        logSize: this.log.length,
        lastApplied: this.volatile.lastApplied,
      });
    } catch (error) {
      this.logger.warn('PERSISTENCE', 'Failed to load persistent state, starting fresh', error);
      // Start with default values (already initialized)
    }
  }

  /**
   * Reset election timeout with randomization
   * Timeout is randomized between electionTimeoutMin and electionTimeoutMax
   * to prevent split votes
   */
  private resetElectionTimeout(): void {
    // Clear existing timeout
    if (this.volatile.electionTimeout) {
      clearTimeout(this.volatile.electionTimeout);
    }

    // Calculate random timeout in range [min, max]
    const min = this.config.electionTimeoutMin;
    const max = this.config.electionTimeoutMax;
    const timeout = min + Math.random() * (max - min);

    this.logger.debug('TIMER', `Setting election timeout to ${timeout.toFixed(0)}ms`);

    // Set new timeout
    this.volatile.electionTimeout = setTimeout(() => {
      this.onElectionTimeout();
    }, timeout);
  }

  /**
   * Reset heartbeat timeout (for leaders)
   * Leaders send periodic heartbeats to maintain authority
   */
  private resetHeartbeatTimeout(): void {
    // Clear existing timeout
    if (this.volatile.heartbeatTimeout) {
      clearTimeout(this.volatile.heartbeatTimeout);
    }

    this.logger.debug('TIMER', `Setting heartbeat timeout to ${this.config.heartbeatInterval}ms`);

    // Set new timeout
    this.volatile.heartbeatTimeout = setTimeout(() => {
      this.onHeartbeatTimeout();
    }, this.config.heartbeatInterval);
  }



  /**
   * Handle election timeout - transition to candidate and start election
   */
  private onElectionTimeout(): void {
    if (!this.running) {
      return;
    }

    this.logger.info('ELECTION', 'Election timeout - starting election');
    this.becomeCandidate();
  }

  /**
   * Handle heartbeat timeout - send heartbeats to followers
   */
  private onHeartbeatTimeout(): void {
    if (!this.running || this.volatile.state !== NodeState.Leader) {
      return;
    }

    this.logger.debug('HEARTBEAT', 'Sending heartbeats to followers');
    this.sendAppendEntriesToAllPeers();
    this.resetHeartbeatTimeout();
  }

  /**
   * Send AppendEntries RPCs to all peers
   * Called by leader to replicate log entries or send heartbeats
   */
  private sendAppendEntriesToAllPeers(): void {
    if (this.volatile.state !== NodeState.Leader || !this.leaderState) {
      return;
    }

    // Send AppendEntries to each peer in parallel
    for (const peer of this.peers) {
      this.sendAppendEntriesToPeer(peer);
    }
  }

  /**
   * Send AppendEntries RPC to a specific peer
   * Includes appropriate log entries based on nextIndex
   * Sends InstallSnapshot if follower is far behind (Req 10.4)
   */
  private async sendAppendEntriesToPeer(peer: NodeId): Promise<void> {
    if (this.volatile.state !== NodeState.Leader || !this.leaderState) {
      return;
    }

    const nextIndex = this.leaderState.nextIndex.get(peer) || 1;

    // Check if nextIndex is covered by snapshot
    if (nextIndex <= this.volatile.lastSnapshotIndex) {
      // Follower is far behind, send snapshot instead
      this.logger.info('REPLICATION', `Follower ${peer} is behind snapshot, sending InstallSnapshot`, {
        nextIndex,
        lastSnapshotIndex: this.volatile.lastSnapshotIndex,
      });
      await this.sendInstallSnapshotToPeer(peer);
      return;
    }

    // Determine prevLogIndex and prevLogTerm for consistency check
    const prevLogIndex = nextIndex - 1;
    let prevLogTerm = 0;

    if (prevLogIndex > 0) {
      // Check if prevLogIndex is in snapshot
      if (prevLogIndex === this.volatile.lastSnapshotIndex) {
        // Get term from snapshot metadata
        const snapshotMeta = await this.persistence.getLatestSnapshot();
        if (snapshotMeta) {
          prevLogTerm = snapshotMeta.lastIncludedTerm;
        }
      } else {
        // Find the log entry at prevLogIndex
        const prevEntry = this.log.find(entry => entry.index === prevLogIndex);
        if (prevEntry) {
          prevLogTerm = prevEntry.term;
        } else {
          // Log entry not found and not in snapshot - send snapshot
          this.logger.warn('REPLICATION', `Log entry at index ${prevLogIndex} not found, sending snapshot to ${peer}`);
          await this.sendInstallSnapshotToPeer(peer);
          return;
        }
      }
    }

    // Get entries to send (from nextIndex onwards)
    const entries = this.log.filter(entry => entry.index >= nextIndex);

    // Create AppendEntries request
    const request: AppendEntriesRequest = {
      term: this.currentTerm,
      leaderId: this.nodeId,
      prevLogIndex,
      prevLogTerm,
      entries,
      leaderCommit: this.volatile.commitIndex,
    };

    const isHeartbeat = entries.length === 0;
    const requestId = `${peer}-${Date.now()}`;
    
    if (!isHeartbeat) {
      this.logger.logReplication('start', peer, {
        entries: entries.length,
        nextIndex,
      });
      this.metrics.recordReplicationStart(peer, requestId);
    }

    try {
      this.metrics.recordRpcSent('appendEntries');
      const response = await this.network.sendAppendEntries(peer, request);
      
      if (!isHeartbeat) {
        this.metrics.recordReplicationComplete(peer, requestId, response.success);
        this.logger.logReplication(response.success ? 'success' : 'failure', peer, {
          matchIndex: response.matchIndex,
        });
      }
      
      await this.handleAppendEntriesResponse(peer, request, response);
    } catch (error) {
      this.metrics.recordRpcFailure('appendEntries');
      if (!isHeartbeat) {
        this.metrics.recordReplicationComplete(peer, requestId, false);
        this.logger.logReplication('failure', peer, error);
      }
    }
  }

  /**
   * Handle AppendEntries response from a follower
   * Updates matchIndex and nextIndex, and advances commit index
   */
  private async handleAppendEntriesResponse(
    peer: NodeId,
    request: AppendEntriesRequest,
    response: AppendEntriesResponse
  ): Promise<void> {
    // If we're no longer the leader, ignore the response
    if (this.volatile.state !== NodeState.Leader || !this.leaderState) {
      return;
    }

    // If response contains higher term, step down to follower (Req 1.5)
    if (response.term > this.currentTerm) {
      this.logger.info('REPLICATION', `Discovered higher term in AppendEntries response`, {
        newTerm: response.term,
        peer,
      });
      await this.becomeFollower(response.term);
      return;
    }

    // Ignore stale responses
    if (response.term < this.currentTerm) {
      return;
    }

    if (response.success) {
      // Update matchIndex and nextIndex on success (Req 2.5)
      const newMatchIndex = request.prevLogIndex + request.entries.length;
      const newNextIndex = newMatchIndex + 1;

      this.leaderState.matchIndex.set(peer, newMatchIndex);
      this.leaderState.nextIndex.set(peer, newNextIndex);

      if (request.entries.length > 0) {
        this.logger.debug('REPLICATION', `Successfully replicated to ${peer}`, {
          matchIndex: newMatchIndex,
          nextIndex: newNextIndex,
        });
      }

      // Try to advance commit index
      this.advanceCommitIndex();
    } else {
      // Log inconsistency - decrement nextIndex and retry (Req 2.5)
      const currentNextIndex = this.leaderState.nextIndex.get(peer) || 1;
      const newNextIndex = Math.max(1, currentNextIndex - 1);
      this.leaderState.nextIndex.set(peer, newNextIndex);

      this.logger.logReplication('retry', peer, {
        oldNextIndex: currentNextIndex,
        newNextIndex,
      });

      // Retry immediately with earlier entries
      this.sendAppendEntriesToPeer(peer);
    }
  }

  /**
   * Advance commit index based on majority replication
   * Only commits entries from current term (Req 2.4, 3.5)
   */
  private advanceCommitIndex(): void {
    if (this.volatile.state !== NodeState.Leader || !this.leaderState) {
      return;
    }

    // Find the highest index that is replicated on a majority of servers
    // and is from the current term
    const matchIndices = Array.from(this.leaderState.matchIndex.values());
    // Include leader's own log
    const lastLogIndex = this.log.length > 0 ? this.log[this.log.length - 1].index : 0;
    matchIndices.push(lastLogIndex);

    // Sort in descending order
    matchIndices.sort((a, b) => b - a);

    // Find the median (majority threshold)
    const majorityIndex = Math.floor(matchIndices.length / 2);
    const newCommitIndex = matchIndices[majorityIndex];

    // Only commit entries from current term (safety requirement)
    if (newCommitIndex > this.volatile.commitIndex) {
      const entryToCommit = this.log.find(entry => entry.index === newCommitIndex);
      if (entryToCommit && entryToCommit.term === this.currentTerm) {
        this.logger.logCommit(newCommitIndex, entryToCommit.term);
        this.volatile.commitIndex = newCommitIndex;

        // Apply committed entries to state machine
        this.applyCommittedEntries();
      }
    }
  }

  /**
   * Apply committed entries to the state machine
   * Applies all entries from lastApplied + 1 to commitIndex
   */
  private async applyCommittedEntries(): Promise<void> {
    while (this.volatile.lastApplied < this.volatile.commitIndex) {
      const nextIndex = this.volatile.lastApplied + 1;
      const entry = this.log.find(e => e.index === nextIndex);

      if (!entry) {
        this.logger.error('APPLY', `Cannot find log entry at index ${nextIndex}`);
        break;
      }

      this.logger.debug('APPLY', `Applying log entry at index ${nextIndex}`);

      try {
        // Deserialize command from buffer
        // The command buffer should contain a serialized CacheCommand
        const commandStr = entry.command.toString('utf-8');
        const command = JSON.parse(commandStr);

        // Apply to state machine
        await this.stateMachine.apply(command);
        this.volatile.lastApplied = nextIndex;
        this.logger.logApply(nextIndex, true);
      } catch (error) {
        this.logger.error('APPLY', `Failed to apply log entry at index ${nextIndex}`, error);
        this.logger.logApply(nextIndex, false);
        // Continue applying other entries
        this.volatile.lastApplied = nextIndex;
      }
    }

    // Check if we need to create a snapshot (Req 10.1)
    await this.checkSnapshotThreshold();
  }

  /**
   * Check if log size exceeds threshold and create snapshot if needed
   * Monitors log size vs configured threshold (Req 10.1)
   */
  private async checkSnapshotThreshold(): Promise<void> {
    // Only create snapshots if we have applied entries
    if (this.volatile.lastApplied === 0) {
      return;
    }

    // Calculate actual log size (entries after last snapshot)
    const logSize = this.log.length;

    // Check if we've exceeded the snapshot threshold
    if (logSize >= this.config.snapshotThreshold) {
      this.logger.info('SNAPSHOT', `Log size ${logSize} exceeds threshold ${this.config.snapshotThreshold}, creating snapshot`);
      await this.createSnapshot();
    }
  }

  /**
   * Create a snapshot of the current cache state
   * Includes all cache entries with metadata (Req 10.1, 10.2)
   */
  private async createSnapshot(): Promise<void> {
    try {
      this.logger.info('SNAPSHOT', `Creating snapshot at index ${this.volatile.lastApplied}`);

      // Get snapshot from state machine
      const cacheSnapshot = await this.stateMachine.getSnapshot();

      // Find the term of the last applied entry
      const lastAppliedEntry = this.log.find(e => e.index === this.volatile.lastApplied);
      const lastIncludedTerm = lastAppliedEntry ? lastAppliedEntry.term : 0;

      // Set snapshot metadata
      cacheSnapshot.lastIncludedIndex = this.volatile.lastApplied;
      cacheSnapshot.lastIncludedTerm = lastIncludedTerm;

      // Persist snapshot to durable storage (Req 10.2)
      await this.persistence.saveSnapshot(cacheSnapshot);

      // Update volatile state
      this.volatile.lastSnapshotIndex = this.volatile.lastApplied;

      this.logger.info('SNAPSHOT', `Snapshot created successfully`, {
        lastIncludedIndex: cacheSnapshot.lastIncludedIndex,
        lastIncludedTerm: cacheSnapshot.lastIncludedTerm,
        entries: cacheSnapshot.entries.size,
        memoryUsage: cacheSnapshot.memoryUsage,
      });

      // Truncate log entries covered by snapshot (will be done in next subtask)
      await this.truncateLogAfterSnapshot(cacheSnapshot.lastIncludedIndex);
    } catch (error) {
      this.logger.error('SNAPSHOT', 'Failed to create snapshot', error);
      throw error;
    }
  }

  /**
   * Truncate log entries covered by snapshot
   * Keeps entries after lastIncludedIndex (Req 10.3)
   */
  private async truncateLogAfterSnapshot(lastIncludedIndex: number): Promise<void> {
    try {
      // Keep only entries after the snapshot
      const entriesToKeep = this.log.filter(entry => entry.index > lastIncludedIndex);
      const removedCount = this.log.length - entriesToKeep.length;

      if (removedCount > 0) {
        this.logger.info('SNAPSHOT', `Truncating ${removedCount} log entries covered by snapshot`, {
          lastIncludedIndex,
          remainingEntries: entriesToKeep.length,
        });

        // Update in-memory log
        this.log = entriesToKeep;

        // Note: Persistence layer handles truncation during saveSnapshot
        // The FilePersistence implementation already removes entries covered by snapshot

        this.logger.debug('SNAPSHOT', `Log truncation complete`, {
          newLogSize: this.log.length,
        });
      }
    } catch (error) {
      this.logger.error('SNAPSHOT', 'Failed to truncate log after snapshot', error);
      throw error;
    }
  }

  /**
   * Send InstallSnapshot RPC to a follower that is far behind
   * Supports chunked snapshot transfer for large snapshots (Req 10.4)
   */
  private async sendInstallSnapshotToPeer(peer: NodeId): Promise<void> {
    if (this.volatile.state !== NodeState.Leader) {
      return;
    }

    try {
      // Load the latest snapshot
      const snapshot = await this.persistence.loadSnapshot();
      if (!snapshot) {
        this.logger.warn('SNAPSHOT', `No snapshot available to send to ${peer}`);
        return;
      }

      this.logger.info('SNAPSHOT', `Sending snapshot to ${peer}`, {
        lastIncludedIndex: snapshot.lastIncludedIndex,
        lastIncludedTerm: snapshot.lastIncludedTerm,
        entries: snapshot.entries.size,
      });

      // Serialize snapshot to buffer
      // Convert Map entries to serializable format
      const entries: Record<string, any> = {};
      snapshot.entries.forEach((entry, key) => {
        entries[key] = {
          key: entry.key.toString('base64'),
          value: entry.value.toString('base64'),
          createdAt: entry.createdAt,
          expiresAt: entry.expiresAt,
          lastAccessedAt: entry.lastAccessedAt,
          size: entry.size,
        };
      });

      const snapshotData = {
        entries,
        timestamp: snapshot.timestamp,
        memoryUsage: snapshot.memoryUsage,
      };

      const dataBuffer = Buffer.from(JSON.stringify(snapshotData), 'utf-8');

      // For simplicity, send entire snapshot in one chunk
      // In production, you'd split into chunks based on config.snapshotChunkSize
      const request: InstallSnapshotRequest = {
        term: this.currentTerm,
        leaderId: this.nodeId,
        lastIncludedIndex: snapshot.lastIncludedIndex,
        lastIncludedTerm: snapshot.lastIncludedTerm,
        offset: 0,
        data: dataBuffer,
        done: true,
      };

      this.metrics.recordRpcSent('installSnapshot');
      const response = await this.network.sendInstallSnapshot(peer, request);

      // Handle response
      if (response.term > this.currentTerm) {
        this.logger.info('SNAPSHOT', `Discovered higher term in InstallSnapshot response`, {
          newTerm: response.term,
          peer,
        });
        await this.becomeFollower(response.term);
        return;
      }

      // Update nextIndex and matchIndex for the peer
      if (this.leaderState) {
        this.leaderState.nextIndex.set(peer, snapshot.lastIncludedIndex + 1);
        this.leaderState.matchIndex.set(peer, snapshot.lastIncludedIndex);
        this.logger.info('SNAPSHOT', `Snapshot sent successfully to ${peer}`, {
          nextIndex: snapshot.lastIncludedIndex + 1,
        });
      }
    } catch (error) {
      this.metrics.recordRpcFailure('installSnapshot');
      this.logger.error('SNAPSHOT', `Failed to send snapshot to ${peer}`, error);
    }
  }

  /**
   * Clear all timers
   */
  private clearTimers(): void {
    if (this.volatile.electionTimeout) {
      clearTimeout(this.volatile.electionTimeout);
      this.volatile.electionTimeout = undefined;
    }
    if (this.volatile.heartbeatTimeout) {
      clearTimeout(this.volatile.heartbeatTimeout);
      this.volatile.heartbeatTimeout = undefined;
    }
    if (this.volatile.expirationTimer) {
      clearTimeout(this.volatile.expirationTimer);
      this.volatile.expirationTimer = undefined;
    }
  }
}
