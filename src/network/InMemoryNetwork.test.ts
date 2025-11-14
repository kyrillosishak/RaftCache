/**
 * Tests for InMemoryNetwork
 */

import { describe, it, expect, afterEach } from 'vitest';
import { InMemoryNetwork } from './InMemoryNetwork';
import {
  RequestVoteRequest,
  RequestVoteResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
} from '../types';

describe('InMemoryNetwork', () => {
  afterEach(() => {
    InMemoryNetwork.clearAll();
    InMemoryNetwork.reset();
  });

  describe('Basic message passing', () => {
    it('should send and receive RequestVote messages', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');

      const mockResponse: RequestVoteResponse = {
        term: 1,
        voteGranted: true,
      };

      network2.registerHandlers({
        handleRequestVote: async (req) => mockResponse,
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      const response = await network1.sendRequestVote('node2', request);
      expect(response).toEqual(mockResponse);
    });

    it('should send and receive AppendEntries messages', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');

      const mockResponse: AppendEntriesResponse = {
        term: 1,
        success: true,
        matchIndex: 5,
      };

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 0, voteGranted: false }),
        handleAppendEntries: async (req) => mockResponse,
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      const request: AppendEntriesRequest = {
        term: 1,
        leaderId: 'node1',
        prevLogIndex: 4,
        prevLogTerm: 1,
        entries: [],
        leaderCommit: 4,
      };

      const response = await network1.sendAppendEntries('node2', request);
      expect(response).toEqual(mockResponse);
    });
  });

  describe('Simulated delays', () => {
    it('should add delay to message delivery', async () => {
      const network1 = new InMemoryNetwork('node1', { baseDelay: 50 });
      const network2 = new InMemoryNetwork('node2');

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      const startTime = Date.now();
      await network1.sendRequestVote('node2', {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      });
      const elapsed = Date.now() - startTime;

      // Should take at least baseDelay * 2 (request + response)
      expect(elapsed).toBeGreaterThanOrEqual(90);
    });

    it('should add jitter to delays', async () => {
      const network1 = new InMemoryNetwork('node1', {
        baseDelay: 10,
        jitter: 0.5,
      });
      const network2 = new InMemoryNetwork('node2');

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      const times: number[] = [];
      for (let i = 0; i < 5; i++) {
        const startTime = Date.now();
        await network1.sendRequestVote('node2', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        });
        times.push(Date.now() - startTime);
      }

      // Times should vary due to jitter
      const uniqueTimes = new Set(times);
      expect(uniqueTimes.size).toBeGreaterThan(1);
    });
  });

  describe('Simulated failures', () => {
    it('should drop messages based on drop rate', async () => {
      const network1 = new InMemoryNetwork('node1', { dropRate: 1.0 });
      const network2 = new InMemoryNetwork('node2');

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      await expect(
        network1.sendRequestVote('node2', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        })
      ).rejects.toThrow('Message dropped');
    });

    it('should fail when target node is not available', async () => {
      const network1 = new InMemoryNetwork('node1');
      await network1.start();

      await expect(
        network1.sendRequestVote('node2', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        })
      ).rejects.toThrow('not available');
    });

    it('should fail when sender node is failed', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      InMemoryNetwork.failNode('node1');

      await expect(
        network1.sendRequestVote('node2', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        })
      ).rejects.toThrow('is failed');
    });

    it('should fail when target node is failed', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      InMemoryNetwork.failNode('node2');

      await expect(
        network1.sendRequestVote('node2', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        })
      ).rejects.toThrow('is failed');
    });

    it('should recover failed nodes', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      InMemoryNetwork.failNode('node2');
      await expect(
        network1.sendRequestVote('node2', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        })
      ).rejects.toThrow();

      InMemoryNetwork.recoverNode('node2');
      const response = await network1.sendRequestVote('node2', {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      });
      expect(response.voteGranted).toBe(true);
    });
  });

  describe('Network partitions', () => {
    it('should partition two nodes', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      network1.partition('node1', 'node2');

      await expect(
        network1.sendRequestVote('node2', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        })
      ).rejects.toThrow('Network partition');
    });

    it('should heal partitions', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      network1.partition('node1', 'node2');
      await expect(
        network1.sendRequestVote('node2', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        })
      ).rejects.toThrow();

      network1.healPartition('node1', 'node2');
      const response = await network1.sendRequestVote('node2', {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      });
      expect(response.voteGranted).toBe(true);
    });

    it('should partition a node from all others', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');
      const network3 = new InMemoryNetwork('node3');

      const handlers = {
        handleRequestVote: async (req: any) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req: any) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req: any) => ({ term: 0 }),
      };

      network2.registerHandlers(handlers);
      network3.registerHandlers(handlers);

      await network1.start();
      await network2.start();
      await network3.start();

      InMemoryNetwork.partitionNode('node1', ['node1', 'node2', 'node3']);

      await expect(
        network1.sendRequestVote('node2', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        })
      ).rejects.toThrow('Network partition');

      await expect(
        network1.sendRequestVote('node3', {
          term: 1,
          candidateId: 'node1',
          lastLogIndex: 0,
          lastLogTerm: 0,
        })
      ).rejects.toThrow('Network partition');
    });

    it('should heal a node from all partitions', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');
      const network3 = new InMemoryNetwork('node3');

      const handlers = {
        handleRequestVote: async (req: any) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req: any) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req: any) => ({ term: 0 }),
      };

      network2.registerHandlers(handlers);
      network3.registerHandlers(handlers);

      await network1.start();
      await network2.start();
      await network3.start();

      InMemoryNetwork.partitionNode('node1', ['node1', 'node2', 'node3']);
      InMemoryNetwork.healNode('node1', ['node1', 'node2', 'node3']);

      const response2 = await network1.sendRequestVote('node2', {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      });
      expect(response2.voteGranted).toBe(true);

      const response3 = await network1.sendRequestVote('node3', {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      });
      expect(response3.voteGranted).toBe(true);
    });
  });

  describe('Test utilities', () => {
    it('should reset all partitions and failures', async () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');

      network2.registerHandlers({
        handleRequestVote: async (req) => ({ term: 1, voteGranted: true }),
        handleAppendEntries: async (req) => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      network1.partition('node1', 'node2');
      InMemoryNetwork.failNode('node2');

      InMemoryNetwork.reset();

      const response = await network1.sendRequestVote('node2', {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      });
      expect(response.voteGranted).toBe(true);
    });

    it('should track all network instances', () => {
      const network1 = new InMemoryNetwork('node1');
      const network2 = new InMemoryNetwork('node2');
      const network3 = new InMemoryNetwork('node3');

      const instances = InMemoryNetwork.getInstances();
      expect(instances.size).toBe(3);
      expect(instances.has('node1')).toBe(true);
      expect(instances.has('node2')).toBe(true);
      expect(instances.has('node3')).toBe(true);
    });

    it('should clear all instances', () => {
      new InMemoryNetwork('node1');
      new InMemoryNetwork('node2');

      expect(InMemoryNetwork.getInstances().size).toBe(2);

      InMemoryNetwork.clearAll();

      expect(InMemoryNetwork.getInstances().size).toBe(0);
    });
  });
});
