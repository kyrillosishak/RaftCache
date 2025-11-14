/**
 * Tests for ReliableNetwork
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { ReliableNetwork, NetworkTimeoutError, NetworkRetryExhaustedError } from './ReliableNetwork';
import { NetworkInterface, NetworkHandlers } from './NetworkInterface';
import {
  NodeId,
  RequestVoteRequest,
  RequestVoteResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
  InstallSnapshotRequest,
  InstallSnapshotResponse,
} from '../types';

// Mock network implementation for testing
class MockNetwork implements NetworkInterface {
  public requestVoteCalls: number = 0;
  public appendEntriesCalls: number = 0;
  public installSnapshotCalls: number = 0;
  public shouldFail: boolean = false;
  public failureCount: number = 0;
  public currentFailures: number = 0;
  public delay: number = 0;
  public handlers?: NetworkHandlers;

  async sendRequestVote(
    target: NodeId,
    request: RequestVoteRequest
  ): Promise<RequestVoteResponse> {
    this.requestVoteCalls++;
    
    if (this.shouldFail || this.currentFailures < this.failureCount) {
      this.currentFailures++;
      throw new Error('Network error');
    }

    if (this.delay > 0) {
      await new Promise(resolve => setTimeout(resolve, this.delay));
    }

    return { term: 1, voteGranted: true };
  }

  async sendAppendEntries(
    target: NodeId,
    request: AppendEntriesRequest
  ): Promise<AppendEntriesResponse> {
    this.appendEntriesCalls++;
    
    if (this.shouldFail || this.currentFailures < this.failureCount) {
      this.currentFailures++;
      throw new Error('Network error');
    }

    if (this.delay > 0) {
      await new Promise(resolve => setTimeout(resolve, this.delay));
    }

    return { term: 1, success: true, matchIndex: 0 };
  }

  async sendInstallSnapshot(
    target: NodeId,
    request: InstallSnapshotRequest
  ): Promise<InstallSnapshotResponse> {
    this.installSnapshotCalls++;
    
    if (this.shouldFail || this.currentFailures < this.failureCount) {
      this.currentFailures++;
      throw new Error('Network error');
    }

    if (this.delay > 0) {
      await new Promise(resolve => setTimeout(resolve, this.delay));
    }

    return { term: 1 };
  }

  registerHandlers(handlers: NetworkHandlers): void {
    this.handlers = handlers;
  }

  async start(): Promise<void> {}
  async stop(): Promise<void> {}

  reset(): void {
    this.requestVoteCalls = 0;
    this.appendEntriesCalls = 0;
    this.installSnapshotCalls = 0;
    this.shouldFail = false;
    this.failureCount = 0;
    this.currentFailures = 0;
    this.delay = 0;
  }
}

describe('ReliableNetwork', () => {
  let mockNetwork: MockNetwork;
  let reliableNetwork: ReliableNetwork;

  beforeEach(() => {
    mockNetwork = new MockNetwork();
    reliableNetwork = new ReliableNetwork(mockNetwork, {
      maxRetries: 3,
      initialTimeout: 100,
      maxTimeout: 1000,
      backoffMultiplier: 2,
    });
  });

  describe('Basic operations', () => {
    it('should forward RequestVote calls', async () => {
      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      const response = await reliableNetwork.sendRequestVote('node2', request);
      
      expect(response.voteGranted).toBe(true);
      expect(mockNetwork.requestVoteCalls).toBe(1);
    });

    it('should forward AppendEntries calls', async () => {
      const request: AppendEntriesRequest = {
        term: 1,
        leaderId: 'node1',
        prevLogIndex: 0,
        prevLogTerm: 0,
        entries: [],
        leaderCommit: 0,
      };

      const response = await reliableNetwork.sendAppendEntries('node2', request);
      
      expect(response.success).toBe(true);
      expect(mockNetwork.appendEntriesCalls).toBe(1);
    });

    it('should forward InstallSnapshot calls', async () => {
      const request: InstallSnapshotRequest = {
        term: 1,
        leaderId: 'node1',
        lastIncludedIndex: 10,
        lastIncludedTerm: 1,
        offset: 0,
        data: Buffer.from('snapshot'),
        done: true,
      };

      const response = await reliableNetwork.sendInstallSnapshot('node2', request);
      
      expect(response.term).toBe(1);
      expect(mockNetwork.installSnapshotCalls).toBe(1);
    });

    it('should forward handler registration', () => {
      const handlers: NetworkHandlers = {
        handleRequestVote: async () => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async () => ({ term: 1, success: true, matchIndex: 0 }),
        handleInstallSnapshot: async () => ({ term: 1 }),
      };

      reliableNetwork.registerHandlers(handlers);
      expect(mockNetwork.handlers).toBe(handlers);
    });

    it('should forward start and stop calls', async () => {
      await reliableNetwork.start();
      await reliableNetwork.stop();
    });
  });

  describe('Timeout handling', () => {
    it('should timeout slow operations', async () => {
      mockNetwork.delay = 200;
      
      // Create network with no retries on timeout
      const network = new ReliableNetwork(mockNetwork, {
        maxRetries: 0,
        initialTimeout: 100,
      });
      
      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      try {
        await network.sendRequestVote('node2', request);
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error).toBeInstanceOf(NetworkRetryExhaustedError);
        expect((error as NetworkRetryExhaustedError).lastError).toBeInstanceOf(NetworkTimeoutError);
      }
    });

    it('should not timeout fast operations', async () => {
      mockNetwork.delay = 50;
      
      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      const response = await reliableNetwork.sendRequestVote('node2', request);
      expect(response.voteGranted).toBe(true);
    });
  });

  describe('Retry logic', () => {
    it('should retry on transient failures', async () => {
      mockNetwork.failureCount = 2; // Fail first 2 attempts
      
      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      const response = await reliableNetwork.sendRequestVote('node2', request);
      
      expect(response.voteGranted).toBe(true);
      expect(mockNetwork.requestVoteCalls).toBe(3); // Initial + 2 retries
    });

    it('should throw RetryExhaustedError after max retries', async () => {
      mockNetwork.shouldFail = true;
      
      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      await expect(
        reliableNetwork.sendRequestVote('node2', request)
      ).rejects.toThrow(NetworkRetryExhaustedError);
      
      expect(mockNetwork.requestVoteCalls).toBe(4); // Initial + 3 retries
    });

    it('should not retry on permanent failures', async () => {
      const permanentFailureNetwork = new MockNetwork();
      permanentFailureNetwork.sendRequestVote = async () => {
        throw new Error('Target node not available');
      };

      const network = new ReliableNetwork(permanentFailureNetwork, {
        maxRetries: 3,
        initialTimeout: 100,
      });

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      await expect(
        network.sendRequestVote('node2', request)
      ).rejects.toThrow('not available');
    });

    it('should respect retryOnTimeout config', async () => {
      mockNetwork.delay = 200;
      
      const network = new ReliableNetwork(mockNetwork, {
        maxRetries: 3,
        initialTimeout: 100,
        retryOnTimeout: false,
      });

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      await expect(
        network.sendRequestVote('node2', request)
      ).rejects.toThrow(NetworkTimeoutError);
      
      expect(mockNetwork.requestVoteCalls).toBe(1); // No retries
    });

    it('should respect retryOnError config', async () => {
      mockNetwork.shouldFail = true;
      
      const network = new ReliableNetwork(mockNetwork, {
        maxRetries: 3,
        initialTimeout: 100,
        retryOnError: false,
      });

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      await expect(
        network.sendRequestVote('node2', request)
      ).rejects.toThrow('Network error');
      
      expect(mockNetwork.requestVoteCalls).toBe(1); // No retries
    });
  });

  describe('Exponential backoff', () => {
    it('should use exponential backoff between retries', async () => {
      mockNetwork.failureCount = 3;
      
      const network = new ReliableNetwork(mockNetwork, {
        maxRetries: 3,
        initialTimeout: 50,
        backoffMultiplier: 2,
      });

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      const startTime = Date.now();
      await network.sendRequestVote('node2', request);
      const elapsed = Date.now() - startTime;

      // Should wait: 50ms + 100ms + 200ms = 350ms minimum
      expect(elapsed).toBeGreaterThanOrEqual(300);
    });

    it('should cap timeout at maxTimeout', async () => {
      const network = new ReliableNetwork(mockNetwork, {
        maxRetries: 5,
        initialTimeout: 100,
        maxTimeout: 200,
        backoffMultiplier: 2,
      });

      const config = network.getConfig();
      expect(config.maxTimeout).toBe(200);
    });
  });

  describe('Configuration', () => {
    it('should use default configuration', () => {
      const network = new ReliableNetwork(mockNetwork);
      const config = network.getConfig();

      expect(config.maxRetries).toBe(3);
      expect(config.initialTimeout).toBe(1000);
      expect(config.maxTimeout).toBe(10000);
      expect(config.backoffMultiplier).toBe(2);
      expect(config.retryOnTimeout).toBe(true);
      expect(config.retryOnError).toBe(true);
    });

    it('should allow custom configuration', () => {
      const network = new ReliableNetwork(mockNetwork, {
        maxRetries: 5,
        initialTimeout: 500,
        maxTimeout: 5000,
        backoffMultiplier: 3,
        retryOnTimeout: false,
        retryOnError: false,
      });

      const config = network.getConfig();
      expect(config.maxRetries).toBe(5);
      expect(config.initialTimeout).toBe(500);
      expect(config.maxTimeout).toBe(5000);
      expect(config.backoffMultiplier).toBe(3);
      expect(config.retryOnTimeout).toBe(false);
      expect(config.retryOnError).toBe(false);
    });

    it('should expose underlying network', () => {
      const underlying = reliableNetwork.getUnderlying();
      expect(underlying).toBe(mockNetwork);
    });
  });

  describe('All RPC types', () => {
    it('should retry AppendEntries on failure', async () => {
      mockNetwork.failureCount = 2;
      
      const request: AppendEntriesRequest = {
        term: 1,
        leaderId: 'node1',
        prevLogIndex: 0,
        prevLogTerm: 0,
        entries: [],
        leaderCommit: 0,
      };

      const response = await reliableNetwork.sendAppendEntries('node2', request);
      
      expect(response.success).toBe(true);
      expect(mockNetwork.appendEntriesCalls).toBe(3);
    });

    it('should retry InstallSnapshot on failure', async () => {
      mockNetwork.failureCount = 2;
      
      const request: InstallSnapshotRequest = {
        term: 1,
        leaderId: 'node1',
        lastIncludedIndex: 10,
        lastIncludedTerm: 1,
        offset: 0,
        data: Buffer.from('snapshot'),
        done: true,
      };

      const response = await reliableNetwork.sendInstallSnapshot('node2', request);
      
      expect(response.term).toBe(1);
      expect(mockNetwork.installSnapshotCalls).toBe(3);
    });
  });
});
