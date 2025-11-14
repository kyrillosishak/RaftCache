/**
 * Tests for HttpNetwork
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { HttpNetwork } from './HttpNetwork';
import {
  RequestVoteRequest,
  RequestVoteResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
  InstallSnapshotRequest,
  InstallSnapshotResponse,
} from '../types';

describe('HttpNetwork', () => {
  let network1: HttpNetwork;
  let network2: HttpNetwork;

  beforeEach(async () => {
    // Create two networks for testing
    const nodeAddresses = new Map([
      ['node1', 'http://localhost:8001'],
      ['node2', 'http://localhost:8002'],
    ]);

    network1 = new HttpNetwork('node1', {
      port: 8001,
      host: 'localhost',
      nodeAddresses,
      timeout: 1000,
    });

    network2 = new HttpNetwork('node2', {
      port: 8002,
      host: 'localhost',
      nodeAddresses,
      timeout: 1000,
    });
  });

  afterEach(async () => {
    await network1.stop();
    await network2.stop();
  });

  describe('Server lifecycle', () => {
    it('should start and stop server', async () => {
      await network1.start();
      expect(network1.getAddress()).toContain('8001');
      await network1.stop();
    });

    it('should handle multiple start/stop cycles', async () => {
      await network1.start();
      await network1.stop();
      
      // Recreate network for second cycle
      const nodeAddresses = new Map([
        ['node1', 'http://localhost:8001'],
      ]);
      network1 = new HttpNetwork('node1', {
        port: 8001,
        host: 'localhost',
        nodeAddresses,
      });
      
      await network1.start();
      await network1.stop();
    });
  });

  describe('RPC communication', () => {
    it('should send and receive RequestVote', async () => {
      const mockResponse: RequestVoteResponse = {
        term: 2,
        voteGranted: true,
      };

      network2.registerHandlers({
        handleRequestVote: async (req) => {
          expect(req.term).toBe(1);
          expect(req.candidateId).toBe('node1');
          return mockResponse;
        },
        handleAppendEntries: async () => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async () => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 5,
        lastLogTerm: 1,
      };

      const response = await network1.sendRequestVote('node2', request);
      
      expect(response.term).toBe(2);
      expect(response.voteGranted).toBe(true);
    });

    it('should send and receive AppendEntries', async () => {
      const mockResponse: AppendEntriesResponse = {
        term: 1,
        success: true,
        matchIndex: 10,
      };

      network2.registerHandlers({
        handleRequestVote: async () => ({ term: 0, voteGranted: false }),
        handleAppendEntries: async (req) => {
          expect(req.term).toBe(1);
          expect(req.leaderId).toBe('node1');
          expect(req.entries.length).toBe(2);
          return mockResponse;
        },
        handleInstallSnapshot: async () => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      const request: AppendEntriesRequest = {
        term: 1,
        leaderId: 'node1',
        prevLogIndex: 8,
        prevLogTerm: 1,
        entries: [
          { term: 1, index: 9, command: Buffer.from('cmd1') },
          { term: 1, index: 10, command: Buffer.from('cmd2') },
        ],
        leaderCommit: 8,
      };

      const response = await network1.sendAppendEntries('node2', request);
      
      expect(response.term).toBe(1);
      expect(response.success).toBe(true);
      expect(response.matchIndex).toBe(10);
    });

    it('should send and receive InstallSnapshot', async () => {
      const mockResponse: InstallSnapshotResponse = {
        term: 1,
      };

      network2.registerHandlers({
        handleRequestVote: async () => ({ term: 0, voteGranted: false }),
        handleAppendEntries: async () => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => {
          expect(req.term).toBe(1);
          expect(req.leaderId).toBe('node1');
          expect(Buffer.isBuffer(req.data)).toBe(true);
          expect(req.data.toString()).toBe('snapshot data');
          return mockResponse;
        },
      });

      await network1.start();
      await network2.start();

      const request: InstallSnapshotRequest = {
        term: 1,
        leaderId: 'node1',
        lastIncludedIndex: 100,
        lastIncludedTerm: 1,
        offset: 0,
        data: Buffer.from('snapshot data'),
        done: true,
      };

      const response = await network1.sendInstallSnapshot('node2', request);
      
      expect(response.term).toBe(1);
    });
  });

  describe('Error handling', () => {
    it('should handle missing node address', async () => {
      await network1.start();

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      await expect(
        network1.sendRequestVote('node3', request)
      ).rejects.toThrow('No address configured');
    });

    it('should handle connection refused', async () => {
      await network1.start();
      // network2 not started

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      await expect(
        network1.sendRequestVote('node2', request)
      ).rejects.toThrow();
    });

    it('should handle timeout', async () => {
      const nodeAddresses = new Map([
        ['node1', 'http://localhost:8003'],
        ['node2', 'http://localhost:8004'],
      ]);

      const slowNetwork = new HttpNetwork('node2', {
        port: 8004,
        host: 'localhost',
        nodeAddresses,
        timeout: 100,
      });

      const fastNetwork = new HttpNetwork('node1', {
        port: 8003,
        host: 'localhost',
        nodeAddresses,
        timeout: 100,
      });

      slowNetwork.registerHandlers({
        handleRequestVote: async (req) => {
          // Simulate slow handler
          await new Promise(resolve => setTimeout(resolve, 200));
          return { term: 1, voteGranted: true };
        },
        handleAppendEntries: async () => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async () => ({ term: 0 }),
      });

      await fastNetwork.start();
      await slowNetwork.start();

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      await expect(
        fastNetwork.sendRequestVote('node2', request)
      ).rejects.toThrow('timeout');

      await fastNetwork.stop();
      await slowNetwork.stop();
    });

    it('should handle missing handlers', async () => {
      await network1.start();
      await network2.start();
      // network2 has no handlers registered

      const request: RequestVoteRequest = {
        term: 1,
        candidateId: 'node1',
        lastLogIndex: 0,
        lastLogTerm: 0,
      };

      await expect(
        network1.sendRequestVote('node2', request)
      ).rejects.toThrow();
    });
  });

  describe('Serialization', () => {
    it('should serialize and deserialize Buffer data', async () => {
      const testData = Buffer.from('test snapshot data');

      network2.registerHandlers({
        handleRequestVote: async () => ({ term: 0, voteGranted: false }),
        handleAppendEntries: async () => ({ term: 0, success: false, matchIndex: 0 }),
        handleInstallSnapshot: async (req) => {
          expect(Buffer.isBuffer(req.data)).toBe(true);
          expect(req.data.equals(testData)).toBe(true);
          return { term: 1 };
        },
      });

      await network1.start();
      await network2.start();

      const request: InstallSnapshotRequest = {
        term: 1,
        leaderId: 'node1',
        lastIncludedIndex: 10,
        lastIncludedTerm: 1,
        offset: 0,
        data: testData,
        done: true,
      };

      await network1.sendInstallSnapshot('node2', request);
    });

    it('should serialize and deserialize log entries with Buffer commands', async () => {
      const cmd1 = Buffer.from('command 1');
      const cmd2 = Buffer.from('command 2');

      network2.registerHandlers({
        handleRequestVote: async () => ({ term: 0, voteGranted: false }),
        handleAppendEntries: async (req) => {
          expect(req.entries.length).toBe(2);
          expect(Buffer.isBuffer(req.entries[0].command)).toBe(true);
          expect(Buffer.isBuffer(req.entries[1].command)).toBe(true);
          expect(req.entries[0].command.equals(cmd1)).toBe(true);
          expect(req.entries[1].command.equals(cmd2)).toBe(true);
          return { term: 1, success: true, matchIndex: 2 };
        },
        handleInstallSnapshot: async () => ({ term: 0 }),
      });

      await network1.start();
      await network2.start();

      const request: AppendEntriesRequest = {
        term: 1,
        leaderId: 'node1',
        prevLogIndex: 0,
        prevLogTerm: 0,
        entries: [
          { term: 1, index: 1, command: cmd1 },
          { term: 1, index: 2, command: cmd2 },
        ],
        leaderCommit: 0,
      };

      await network1.sendAppendEntries('node2', request);
    });
  });

  describe('Configuration', () => {
    it('should expose node addresses', () => {
      const addresses = network1.getNodeAddresses();
      expect(addresses.size).toBe(2);
      expect(addresses.get('node1')).toBe('http://localhost:8001');
      expect(addresses.get('node2')).toBe('http://localhost:8002');
    });

    it('should use custom timeout', async () => {
      const nodeAddresses = new Map([
        ['node1', 'http://localhost:8005'],
      ]);

      const customNetwork = new HttpNetwork('node1', {
        port: 8005,
        host: 'localhost',
        nodeAddresses,
        timeout: 50,
      });

      await customNetwork.start();
      await customNetwork.stop();
    });
  });
});
