/**
 * Tests for FilePersistence implementation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'fs/promises';
import * as path from 'path';
import { FilePersistence } from './FilePersistence';
import { LogEntry, CacheSnapshot, CacheEntry } from '../types';

describe('FilePersistence', () => {
  const testDataDir = path.join(__dirname, '../../test-data');
  let persistence: FilePersistence;

  beforeEach(async () => {
    // Clean up test directory
    await fs.rm(testDataDir, { recursive: true, force: true });
    await fs.mkdir(testDataDir, { recursive: true });
    
    persistence = new FilePersistence(testDataDir);
    await persistence.initialize();
  });

  afterEach(async () => {
    // Clean up test directory
    await fs.rm(testDataDir, { recursive: true, force: true });
  });

  describe('State persistence', () => {
    it('should save and load state', async () => {
      await persistence.saveState(5, 'node-1');
      
      const state = await persistence.loadState();
      expect(state.currentTerm).toBe(5);
      expect(state.votedFor).toBe('node-1');
    });

    it('should save state without votedFor', async () => {
      await persistence.saveState(3);
      
      const state = await persistence.loadState();
      expect(state.currentTerm).toBe(3);
      expect(state.votedFor).toBeUndefined();
    });

    it('should return initial state when file does not exist', async () => {
      const state = await persistence.loadState();
      expect(state.currentTerm).toBe(0);
      expect(state.votedFor).toBeUndefined();
    });

    it('should overwrite previous state', async () => {
      await persistence.saveState(1, 'node-1');
      await persistence.saveState(2, 'node-2');
      
      const state = await persistence.loadState();
      expect(state.currentTerm).toBe(2);
      expect(state.votedFor).toBe('node-2');
    });
  });

  describe('Log persistence', () => {
    it('should append and retrieve log entries', async () => {
      const entries: LogEntry[] = [
        { term: 1, index: 1, command: Buffer.from('cmd1') },
        { term: 1, index: 2, command: Buffer.from('cmd2') },
        { term: 2, index: 3, command: Buffer.from('cmd3') },
      ];
      
      await persistence.appendLogEntries(entries);
      
      const retrieved = await persistence.getLogEntries(1, 3);
      expect(retrieved).toHaveLength(3);
      expect(retrieved[0].term).toBe(1);
      expect(retrieved[0].index).toBe(1);
      expect(retrieved[0].command.toString()).toBe('cmd1');
      expect(retrieved[2].term).toBe(2);
      expect(retrieved[2].command.toString()).toBe('cmd3');
    });

    it('should get last log index', async () => {
      const entries: LogEntry[] = [
        { term: 1, index: 1, command: Buffer.from('cmd1') },
        { term: 1, index: 2, command: Buffer.from('cmd2') },
      ];
      
      await persistence.appendLogEntries(entries);
      
      const lastIndex = await persistence.getLastLogIndex();
      expect(lastIndex).toBe(2);
    });

    it('should get last log term', async () => {
      const entries: LogEntry[] = [
        { term: 1, index: 1, command: Buffer.from('cmd1') },
        { term: 2, index: 2, command: Buffer.from('cmd2') },
      ];
      
      await persistence.appendLogEntries(entries);
      
      const lastTerm = await persistence.getLastLogTerm();
      expect(lastTerm).toBe(2);
    });

    it('should return 0 for empty log', async () => {
      const lastIndex = await persistence.getLastLogIndex();
      const lastTerm = await persistence.getLastLogTerm();
      
      expect(lastIndex).toBe(0);
      expect(lastTerm).toBe(0);
    });

    it('should truncate log from specified index', async () => {
      const entries: LogEntry[] = [
        { term: 1, index: 1, command: Buffer.from('cmd1') },
        { term: 1, index: 2, command: Buffer.from('cmd2') },
        { term: 2, index: 3, command: Buffer.from('cmd3') },
        { term: 2, index: 4, command: Buffer.from('cmd4') },
      ];
      
      await persistence.appendLogEntries(entries);
      await persistence.truncateLog(3);
      
      const lastIndex = await persistence.getLastLogIndex();
      expect(lastIndex).toBe(2);
      
      const retrieved = await persistence.getLogEntries(1, 4);
      expect(retrieved).toHaveLength(2);
      expect(retrieved[1].index).toBe(2);
    });

    it('should handle range queries correctly', async () => {
      const entries: LogEntry[] = [
        { term: 1, index: 1, command: Buffer.from('cmd1') },
        { term: 1, index: 2, command: Buffer.from('cmd2') },
        { term: 2, index: 3, command: Buffer.from('cmd3') },
        { term: 2, index: 4, command: Buffer.from('cmd4') },
      ];
      
      await persistence.appendLogEntries(entries);
      
      const range = await persistence.getLogEntries(2, 3);
      expect(range).toHaveLength(2);
      expect(range[0].index).toBe(2);
      expect(range[1].index).toBe(3);
    });
  });

  describe('Snapshot persistence', () => {
    it('should save and load snapshot', async () => {
      const entries = new Map<string, CacheEntry>();
      entries.set('key1', {
        key: Buffer.from('key1'),
        value: Buffer.from('value1'),
        createdAt: Date.now(),
        lastAccessedAt: Date.now(),
        size: 10,
      });
      entries.set('key2', {
        key: Buffer.from('key2'),
        value: Buffer.from('value2'),
        createdAt: Date.now(),
        expiresAt: Date.now() + 10000,
        lastAccessedAt: Date.now(),
        size: 10,
      });
      
      const snapshot: CacheSnapshot = {
        entries,
        lastIncludedIndex: 10,
        lastIncludedTerm: 2,
        timestamp: Date.now(),
        memoryUsage: 1024,
      };
      
      await persistence.saveSnapshot(snapshot);
      
      const loaded = await persistence.loadSnapshot();
      expect(loaded).not.toBeNull();
      expect(loaded!.lastIncludedIndex).toBe(10);
      expect(loaded!.lastIncludedTerm).toBe(2);
      expect(loaded!.entries.size).toBe(2);
      expect(loaded!.entries.get('key1')?.value.toString()).toBe('value1');
      expect(loaded!.entries.get('key2')?.value.toString()).toBe('value2');
      expect(loaded!.entries.get('key2')?.expiresAt).toBeDefined();
    });

    it('should return null when no snapshot exists', async () => {
      const snapshot = await persistence.loadSnapshot();
      expect(snapshot).toBeNull();
    });

    it('should get latest snapshot metadata', async () => {
      const snapshot1: CacheSnapshot = {
        entries: new Map(),
        lastIncludedIndex: 5,
        lastIncludedTerm: 1,
        timestamp: Date.now(),
        memoryUsage: 0,
      };
      
      const snapshot2: CacheSnapshot = {
        entries: new Map(),
        lastIncludedIndex: 10,
        lastIncludedTerm: 2,
        timestamp: Date.now(),
        memoryUsage: 0,
      };
      
      await persistence.saveSnapshot(snapshot1);
      await persistence.saveSnapshot(snapshot2);
      
      const metadata = await persistence.getLatestSnapshot();
      expect(metadata).not.toBeNull();
      expect(metadata!.lastIncludedIndex).toBe(10);
      expect(metadata!.lastIncludedTerm).toBe(2);
    });

    it('should truncate log after snapshot', async () => {
      const logEntries: LogEntry[] = [
        { term: 1, index: 1, command: Buffer.from('cmd1') },
        { term: 1, index: 2, command: Buffer.from('cmd2') },
        { term: 2, index: 3, command: Buffer.from('cmd3') },
        { term: 2, index: 4, command: Buffer.from('cmd4') },
        { term: 2, index: 5, command: Buffer.from('cmd5') },
      ];
      
      await persistence.appendLogEntries(logEntries);
      
      const snapshot: CacheSnapshot = {
        entries: new Map(),
        lastIncludedIndex: 3,
        lastIncludedTerm: 2,
        timestamp: Date.now(),
        memoryUsage: 0,
      };
      
      await persistence.saveSnapshot(snapshot);
      
      // Log should only contain entries after snapshot
      const entries = await persistence.getLogEntries(4, 5);
      expect(entries).toHaveLength(2);
      expect(entries[0].index).toBe(4);
      expect(entries[1].index).toBe(5);
    });
  });

  describe('Durability after simulated crashes', () => {
    it('should persist state across instances', async () => {
      await persistence.saveState(5, 'node-1');
      
      // Create new instance (simulating restart)
      const newPersistence = new FilePersistence(testDataDir);
      await newPersistence.initialize();
      
      const state = await newPersistence.loadState();
      expect(state.currentTerm).toBe(5);
      expect(state.votedFor).toBe('node-1');
    });

    it('should persist log entries across instances', async () => {
      const entries: LogEntry[] = [
        { term: 1, index: 1, command: Buffer.from('cmd1') },
        { term: 1, index: 2, command: Buffer.from('cmd2') },
      ];
      
      await persistence.appendLogEntries(entries);
      
      // Create new instance
      const newPersistence = new FilePersistence(testDataDir);
      await newPersistence.initialize();
      
      const retrieved = await newPersistence.getLogEntries(1, 2);
      expect(retrieved).toHaveLength(2);
      expect(retrieved[0].command.toString()).toBe('cmd1');
      expect(retrieved[1].command.toString()).toBe('cmd2');
    });

    it('should persist snapshots across instances', async () => {
      const entries = new Map<string, CacheEntry>();
      entries.set('key1', {
        key: Buffer.from('key1'),
        value: Buffer.from('value1'),
        createdAt: Date.now(),
        lastAccessedAt: Date.now(),
        size: 10,
      });
      
      const snapshot: CacheSnapshot = {
        entries,
        lastIncludedIndex: 10,
        lastIncludedTerm: 2,
        timestamp: Date.now(),
        memoryUsage: 1024,
      };
      
      await persistence.saveSnapshot(snapshot);
      
      // Create new instance
      const newPersistence = new FilePersistence(testDataDir);
      await newPersistence.initialize();
      
      const loaded = await newPersistence.loadSnapshot();
      expect(loaded).not.toBeNull();
      expect(loaded!.lastIncludedIndex).toBe(10);
      expect(loaded!.entries.get('key1')?.value.toString()).toBe('value1');
    });
  });
});
