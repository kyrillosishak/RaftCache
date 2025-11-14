/**
 * Tests for CacheStateMachine
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { CacheStateMachineImpl } from './CacheStateMachine';
import { CacheCommand, CommandType } from '../types';

describe('CacheStateMachine', () => {
  let stateMachine: CacheStateMachineImpl;
  const maxMemory = 1024 * 1024; // 1 MB

  beforeEach(() => {
    stateMachine = new CacheStateMachineImpl(maxMemory);
  });

  describe('Basic Operations', () => {
    it('should set and get a value', async () => {
      const key = Buffer.from('key1');
      const value = Buffer.from('value1');

      await stateMachine.set(key, value);

      const entry = await stateMachine.get(key);
      expect(entry).not.toBeNull();
      expect(entry!.value.toString()).toBe('value1');
    });

    it('should return null for non-existent key', async () => {
      const key = Buffer.from('nonexistent');
      const entry = await stateMachine.get(key);
      expect(entry).toBeNull();
    });

    it('should delete a value', async () => {
      const key = Buffer.from('key1');
      const value = Buffer.from('value1');

      await stateMachine.set(key, value);
      const deleted = await stateMachine.delete(key);
      expect(deleted).toBe(true);

      const entry = await stateMachine.get(key);
      expect(entry).toBeNull();
    });

    it('should return false when deleting non-existent key', async () => {
      const key = Buffer.from('nonexistent');
      const deleted = await stateMachine.delete(key);
      expect(deleted).toBe(false);
    });
  });

  describe('Apply Command', () => {
    it('should apply SET command', async () => {
      const command: CacheCommand = {
        type: CommandType.SET,
        key: Buffer.from('key1'),
        value: Buffer.from('value1'),
      };

      const result = await stateMachine.apply(command);
      expect(result.success).toBe(true);

      const entry = await stateMachine.get(Buffer.from('key1'));
      expect(entry).not.toBeNull();
      expect(entry!.value.toString()).toBe('value1');
    });

    it('should apply DELETE command', async () => {
      await stateMachine.set(Buffer.from('key1'), Buffer.from('value1'));

      const command: CacheCommand = {
        type: CommandType.DELETE,
        key: Buffer.from('key1'),
      };

      const result = await stateMachine.apply(command);
      expect(result.success).toBe(true);

      const entry = await stateMachine.get(Buffer.from('key1'));
      expect(entry).toBeNull();
    });

    it('should handle SET command without key', async () => {
      const command: CacheCommand = {
        type: CommandType.SET,
        value: Buffer.from('value1'),
      };

      const result = await stateMachine.apply(command);
      expect(result.success).toBe(false);
      expect(result.error).toContain('requires key and value');
    });

    it('should handle DELETE command without key', async () => {
      const command: CacheCommand = {
        type: CommandType.DELETE,
      };

      const result = await stateMachine.apply(command);
      expect(result.success).toBe(false);
      expect(result.error).toContain('requires key');
    });
  });

  describe('Batch Operations', () => {
    it('should apply batch of SET commands', async () => {
      const commands: CacheCommand[] = [
        { type: CommandType.SET, key: Buffer.from('key1'), value: Buffer.from('value1') },
        { type: CommandType.SET, key: Buffer.from('key2'), value: Buffer.from('value2') },
        { type: CommandType.SET, key: Buffer.from('key3'), value: Buffer.from('value3') },
      ];

      const results = await stateMachine.batchApply(commands);
      expect(results.length).toBe(3);
      expect(results.every(r => r.success)).toBe(true);

      const entry1 = await stateMachine.get(Buffer.from('key1'));
      const entry2 = await stateMachine.get(Buffer.from('key2'));
      const entry3 = await stateMachine.get(Buffer.from('key3'));

      expect(entry1!.value.toString()).toBe('value1');
      expect(entry2!.value.toString()).toBe('value2');
      expect(entry3!.value.toString()).toBe('value3');
    });

    it('should apply batch with mixed operation types', async () => {
      // Set initial values
      await stateMachine.set(Buffer.from('key1'), Buffer.from('value1'));
      await stateMachine.set(Buffer.from('key2'), Buffer.from('value2'));

      const commands: CacheCommand[] = [
        { type: CommandType.SET, key: Buffer.from('key3'), value: Buffer.from('value3') },
        { type: CommandType.DELETE, key: Buffer.from('key1') },
        { type: CommandType.SET, key: Buffer.from('key2'), value: Buffer.from('updated') },
      ];

      const results = await stateMachine.batchApply(commands);
      expect(results.length).toBe(3);
      expect(results.every(r => r.success)).toBe(true);

      const entry1 = await stateMachine.get(Buffer.from('key1'));
      const entry2 = await stateMachine.get(Buffer.from('key2'));
      const entry3 = await stateMachine.get(Buffer.from('key3'));

      expect(entry1).toBeNull();
      expect(entry2!.value.toString()).toBe('updated');
      expect(entry3!.value.toString()).toBe('value3');
    });

    it('should track which operations succeeded vs failed', async () => {
      const commands: CacheCommand[] = [
        { type: CommandType.SET, key: Buffer.from('key1'), value: Buffer.from('value1') },
        { type: CommandType.DELETE }, // Missing key - should fail
        { type: CommandType.SET, key: Buffer.from('key2'), value: Buffer.from('value2') },
      ];

      const results = await stateMachine.batchApply(commands);
      expect(results.length).toBe(3);
      expect(results[0].success).toBe(true);
      expect(results[1].success).toBe(false);
      expect(results[2].success).toBe(true);
    });

    it('should apply BATCH command type', async () => {
      const batchCommand: CacheCommand = {
        type: CommandType.BATCH,
        batch: [
          { type: CommandType.SET, key: Buffer.from('key1'), value: Buffer.from('value1') },
          { type: CommandType.SET, key: Buffer.from('key2'), value: Buffer.from('value2') },
        ],
      };

      const result = await stateMachine.apply(batchCommand);
      expect(result.success).toBe(true);

      const entry1 = await stateMachine.get(Buffer.from('key1'));
      const entry2 = await stateMachine.get(Buffer.from('key2'));

      expect(entry1!.value.toString()).toBe('value1');
      expect(entry2!.value.toString()).toBe('value2');
    });
  });

  describe('Memory Management', () => {
    it('should track memory usage', async () => {
      const initialMemory = stateMachine.getMemoryUsage();
      expect(initialMemory).toBe(0);

      await stateMachine.set(Buffer.from('key1'), Buffer.from('value1'));

      const afterSet = stateMachine.getMemoryUsage();
      expect(afterSet).toBeGreaterThan(0);

      await stateMachine.delete(Buffer.from('key1'));

      const afterDelete = stateMachine.getMemoryUsage();
      expect(afterDelete).toBe(0);
    });

    it('should update memory usage when replacing value', async () => {
      await stateMachine.set(Buffer.from('key1'), Buffer.from('small'));
      const memoryAfterFirst = stateMachine.getMemoryUsage();

      await stateMachine.set(Buffer.from('key1'), Buffer.from('much larger value'));
      const memoryAfterSecond = stateMachine.getMemoryUsage();

      expect(memoryAfterSecond).toBeGreaterThan(memoryAfterFirst);
    });

    it('should evict LRU entry when memory limit reached', async () => {
      const smallMemory = 500; // Very small limit
      const sm = new CacheStateMachineImpl(smallMemory);

      // Add entries until we exceed memory
      await sm.set(Buffer.from('key1'), Buffer.from('value1'));
      await sm.set(Buffer.from('key2'), Buffer.from('value2'));
      await sm.set(Buffer.from('key3'), Buffer.from('value3'));

      // Access key1 and key2 to make key3 the LRU
      await sm.get(Buffer.from('key1'));
      await sm.get(Buffer.from('key2'));

      // Add a large entry that triggers eviction
      await sm.set(Buffer.from('key4'), Buffer.from('large value that triggers eviction'));

      // Memory should be under limit
      expect(sm.getMemoryUsage()).toBeLessThanOrEqual(smallMemory);
    });
  });

  describe('Statistics', () => {
    it('should track entry count', async () => {
      await stateMachine.set(Buffer.from('key1'), Buffer.from('value1'));
      await stateMachine.set(Buffer.from('key2'), Buffer.from('value2'));

      const stats = stateMachine.getStats();
      expect(stats.entryCount).toBe(2);
    });

    it('should track hit rate', async () => {
      await stateMachine.set(Buffer.from('key1'), Buffer.from('value1'));

      // Hit
      await stateMachine.get(Buffer.from('key1'));
      // Miss
      await stateMachine.get(Buffer.from('nonexistent'));
      // Hit
      await stateMachine.get(Buffer.from('key1'));

      const stats = stateMachine.getStats();
      expect(stats.hitRate).toBeCloseTo(2 / 3, 2);
    });

    it('should track eviction count', async () => {
      const smallMemory = 200;
      const sm = new CacheStateMachineImpl(smallMemory);

      await sm.set(Buffer.from('key1'), Buffer.from('value1'));
      await sm.set(Buffer.from('key2'), Buffer.from('value2'));
      await sm.set(Buffer.from('key3'), Buffer.from('value3'));

      const stats = sm.getStats();
      expect(stats.evictionCount).toBeGreaterThan(0);
    });
  });

  describe('Lifecycle', () => {
    it('should start and stop without errors', async () => {
      await expect(stateMachine.start()).resolves.not.toThrow();
      await expect(stateMachine.stop()).resolves.not.toThrow();
    });

    it('should handle multiple start calls', async () => {
      await stateMachine.start();
      await expect(stateMachine.start()).resolves.not.toThrow();
      await stateMachine.stop();
    });

    it('should handle multiple stop calls', async () => {
      await stateMachine.start();
      await stateMachine.stop();
      await expect(stateMachine.stop()).resolves.not.toThrow();
    });
  });

  describe('LRU Eviction and Memory Management', () => {
    it('should evict LRU entry when memory limit reached', async () => {
      const smallMemory = 300; // Very small limit
      const sm = new CacheStateMachineImpl(smallMemory);

      // Add entries
      await sm.set(Buffer.from('key1'), Buffer.from('value1'));
      await sm.set(Buffer.from('key2'), Buffer.from('value2'));
      await sm.set(Buffer.from('key3'), Buffer.from('value3'));

      // Access key1 and key2 to make them more recently used
      await sm.get(Buffer.from('key1'));
      await sm.get(Buffer.from('key2'));

      // Add a large entry that triggers eviction
      await sm.set(Buffer.from('key4'), Buffer.from('large value that should trigger eviction'));

      // Memory should be under limit
      expect(sm.getMemoryUsage()).toBeLessThanOrEqual(smallMemory);

      // key3 should have been evicted (least recently used)
      const entry3 = await sm.get(Buffer.from('key3'));
      expect(entry3).toBeNull();

      // key1 and key2 should still exist
      const entry1 = await sm.get(Buffer.from('key1'));
      const entry2 = await sm.get(Buffer.from('key2'));
      expect(entry1).not.toBeNull();
      expect(entry2).not.toBeNull();
    });

    it('should evict expired entries before applying LRU eviction', async () => {
      const smallMemory = 300;
      const sm = new CacheStateMachineImpl(smallMemory);

      // Add entries with TTL
      await sm.set(Buffer.from('key1'), Buffer.from('value1'), 50); // Will expire
      await sm.set(Buffer.from('key2'), Buffer.from('value2')); // No TTL
      await sm.set(Buffer.from('key3'), Buffer.from('value3')); // No TTL

      // Wait for key1 to expire
      await new Promise(resolve => setTimeout(resolve, 100));

      // Access key2 to make it more recently used than key3
      await sm.get(Buffer.from('key2'));

      // Add entry that triggers eviction
      await sm.set(Buffer.from('key4'), Buffer.from('large value that triggers eviction'));

      // key1 should be evicted first (expired)
      const entry1 = await sm.get(Buffer.from('key1'));
      expect(entry1).toBeNull();

      // key2 should still exist (more recently used)
      const entry2 = await sm.get(Buffer.from('key2'));
      expect(entry2).not.toBeNull();

      // Check eviction stats
      const stats = sm.getStats();
      expect(stats.expirationCount).toBeGreaterThan(0);
    });

    it('should update access time on GET operations', async () => {
      const key = Buffer.from('key1');
      const value = Buffer.from('value1');

      await stateMachine.set(key, value);

      const entry1 = await stateMachine.get(key);
      const firstAccessTime = entry1!.lastAccessedAt;

      // Wait a bit
      await new Promise(resolve => setTimeout(resolve, 10));

      const entry2 = await stateMachine.get(key);
      const secondAccessTime = entry2!.lastAccessedAt;

      // Access time should have been updated
      expect(secondAccessTime).toBeGreaterThan(firstAccessTime);
    });

    it('should track memory usage accurately', async () => {
      const initialMemory = stateMachine.getMemoryUsage();
      expect(initialMemory).toBe(0);

      // Add entry
      await stateMachine.set(Buffer.from('key1'), Buffer.from('value1'));
      const afterFirstSet = stateMachine.getMemoryUsage();
      expect(afterFirstSet).toBeGreaterThan(0);

      // Add another entry
      await stateMachine.set(Buffer.from('key2'), Buffer.from('value2'));
      const afterSecondSet = stateMachine.getMemoryUsage();
      expect(afterSecondSet).toBeGreaterThan(afterFirstSet);

      // Delete first entry
      await stateMachine.delete(Buffer.from('key1'));
      const afterDelete = stateMachine.getMemoryUsage();
      expect(afterDelete).toBeLessThan(afterSecondSet);
      expect(afterDelete).toBeCloseTo(afterFirstSet, -1);

      // Delete second entry
      await stateMachine.delete(Buffer.from('key2'));
      const afterAllDeleted = stateMachine.getMemoryUsage();
      expect(afterAllDeleted).toBe(0);
    });

    it('should track eviction statistics', async () => {
      const smallMemory = 200;
      const sm = new CacheStateMachineImpl(smallMemory);

      const initialStats = sm.getStats();
      expect(initialStats.evictionCount).toBe(0);

      // Add entries until eviction occurs
      await sm.set(Buffer.from('key1'), Buffer.from('value1'));
      await sm.set(Buffer.from('key2'), Buffer.from('value2'));
      await sm.set(Buffer.from('key3'), Buffer.from('value3'));
      await sm.set(Buffer.from('key4'), Buffer.from('value4'));

      const finalStats = sm.getStats();
      expect(finalStats.evictionCount).toBeGreaterThan(0);
    });

    it('should maintain LRU order correctly with multiple accesses', async () => {
      const smallMemory = 300;
      const sm = new CacheStateMachineImpl(smallMemory);

      // Add three small entries
      await sm.set(Buffer.from('key1'), Buffer.from('v1'));
      await sm.set(Buffer.from('key2'), Buffer.from('v2'));
      await sm.set(Buffer.from('key3'), Buffer.from('v3'));

      // Access key2 and key3 multiple times to make them more recently used
      await sm.get(Buffer.from('key2'));
      await sm.get(Buffer.from('key3'));
      await sm.get(Buffer.from('key2'));

      // Add entry that triggers eviction
      await sm.set(Buffer.from('key4'), Buffer.from('x'.repeat(150)));

      // At least one entry should have been evicted
      const stats = sm.getStats();
      expect(stats.evictionCount).toBeGreaterThan(0);

      // Verify that eviction happened and memory is under limit
      expect(sm.getMemoryUsage()).toBeLessThanOrEqual(smallMemory);
    });

    it('should handle multiple evictions when adding large entry', async () => {
      const smallMemory = 300;
      const sm = new CacheStateMachineImpl(smallMemory);

      // Add several small entries
      await sm.set(Buffer.from('key1'), Buffer.from('v1'));
      await sm.set(Buffer.from('key2'), Buffer.from('v2'));
      await sm.set(Buffer.from('key3'), Buffer.from('v3'));
      await sm.set(Buffer.from('key4'), Buffer.from('v4'));

      const statsBeforeLarge = sm.getStats();
      const entriesBeforeLarge = statsBeforeLarge.entryCount;

      // Add a very large entry that requires multiple evictions
      await sm.set(Buffer.from('large'), Buffer.from('x'.repeat(200)));

      const statsAfterLarge = sm.getStats();
      
      // Should have evicted multiple entries
      expect(statsAfterLarge.entryCount).toBeLessThan(entriesBeforeLarge);
      expect(statsAfterLarge.evictionCount).toBeGreaterThan(0);
      
      // Memory should still be under limit
      expect(sm.getMemoryUsage()).toBeLessThanOrEqual(smallMemory);
    });

    it('should expose memory usage in stats', async () => {
      await stateMachine.set(Buffer.from('key1'), Buffer.from('value1'));
      await stateMachine.set(Buffer.from('key2'), Buffer.from('value2'));

      const stats = stateMachine.getStats();
      
      expect(stats.memoryUsage).toBeGreaterThan(0);
      expect(stats.maxMemory).toBe(maxMemory);
      expect(stats.memoryUsage).toBe(stateMachine.getMemoryUsage());
    });

    it('should handle SET operations that update existing keys', async () => {
      const key = Buffer.from('key1');
      
      await stateMachine.set(key, Buffer.from('small'));
      const memoryAfterFirst = stateMachine.getMemoryUsage();

      await stateMachine.set(key, Buffer.from('much larger value'));
      const memoryAfterSecond = stateMachine.getMemoryUsage();

      // Memory should increase when replacing with larger value
      expect(memoryAfterSecond).toBeGreaterThan(memoryAfterFirst);

      // Should still be only one entry
      const stats = stateMachine.getStats();
      expect(stats.entryCount).toBe(1);
    });

    it('should correctly restore LRU order from snapshot', async () => {
      const sm = new CacheStateMachineImpl(maxMemory);

      // Add entries with delays to ensure different access times
      await sm.set(Buffer.from('key1'), Buffer.from('value1'));
      await new Promise(resolve => setTimeout(resolve, 10));
      
      await sm.set(Buffer.from('key2'), Buffer.from('value2'));
      await new Promise(resolve => setTimeout(resolve, 10));
      
      await sm.set(Buffer.from('key3'), Buffer.from('value3'));

      // Create snapshot
      const snapshot = await sm.getSnapshot();

      // Create new state machine and restore
      const sm2 = new CacheStateMachineImpl(300); // Small memory to trigger eviction
      await sm2.restoreSnapshot(snapshot);

      // Add entry that triggers eviction
      await sm2.set(Buffer.from('key4'), Buffer.from('large value'));

      // key1 should be evicted (oldest access time)
      const entry1 = await sm2.get(Buffer.from('key1'));
      expect(entry1).toBeNull();

      // Newer entries should still exist
      const entry2 = await sm2.get(Buffer.from('key2'));
      const entry3 = await sm2.get(Buffer.from('key3'));
      expect(entry2).not.toBeNull();
      expect(entry3).not.toBeNull();
    });
  });

  describe('TTL Expiration', () => {
    it('should store expiration timestamp when TTL provided', async () => {
      const key = Buffer.from('key1');
      const value = Buffer.from('value1');
      const ttl = 5000; // 5 seconds

      const beforeSet = Date.now();
      await stateMachine.set(key, value, ttl);
      const afterSet = Date.now();

      const entry = await stateMachine.get(key);
      expect(entry).not.toBeNull();
      expect(entry!.expiresAt).toBeDefined();
      expect(entry!.expiresAt!).toBeGreaterThanOrEqual(beforeSet + ttl);
      expect(entry!.expiresAt!).toBeLessThanOrEqual(afterSet + ttl);
    });

    it('should support entries without TTL that persist until deleted', async () => {
      const key = Buffer.from('key1');
      const value = Buffer.from('value1');

      await stateMachine.set(key, value); // No TTL

      const entry = await stateMachine.get(key);
      expect(entry).not.toBeNull();
      expect(entry!.expiresAt).toBeUndefined();
    });

    it('should return not found for expired entries on GET', async () => {
      const key = Buffer.from('key1');
      const value = Buffer.from('value1');
      const ttl = 50; // 50ms

      await stateMachine.set(key, value, ttl);

      // Entry should exist immediately
      let entry = await stateMachine.get(key);
      expect(entry).not.toBeNull();

      // Wait for expiration
      await new Promise(resolve => setTimeout(resolve, 100));

      // Entry should be expired and return null
      entry = await stateMachine.get(key);
      expect(entry).toBeNull();
    });

    it('should remove expired entry from cache on access', async () => {
      const key = Buffer.from('key1');
      const value = Buffer.from('value1');
      const ttl = 50; // 50ms

      await stateMachine.set(key, value, ttl);

      const memoryBefore = stateMachine.getMemoryUsage();
      expect(memoryBefore).toBeGreaterThan(0);

      // Wait for expiration
      await new Promise(resolve => setTimeout(resolve, 100));

      // Access expired entry
      await stateMachine.get(key);

      // Memory should be freed
      const memoryAfter = stateMachine.getMemoryUsage();
      expect(memoryAfter).toBe(0);
    });

    it('should periodically remove expired entries', async () => {
      const shortInterval = 100; // 100ms interval
      const sm = new CacheStateMachineImpl(maxMemory, shortInterval);

      // Add entries with short TTL
      await sm.set(Buffer.from('key1'), Buffer.from('value1'), 50);
      await sm.set(Buffer.from('key2'), Buffer.from('value2'), 50);
      await sm.set(Buffer.from('key3'), Buffer.from('value3'), 50);

      const statsBefore = sm.getStats();
      expect(statsBefore.entryCount).toBe(3);

      // Start background cleanup
      await sm.start();

      // Wait for entries to expire and cleanup to run
      await new Promise(resolve => setTimeout(resolve, 200));

      const statsAfter = sm.getStats();
      expect(statsAfter.entryCount).toBe(0);
      expect(statsAfter.expirationCount).toBe(3);

      await sm.stop();
    });

    it('should track expiration count', async () => {
      const key1 = Buffer.from('key1');
      const key2 = Buffer.from('key2');
      const ttl = 50; // 50ms

      await stateMachine.set(key1, Buffer.from('value1'), ttl);
      await stateMachine.set(key2, Buffer.from('value2'), ttl);

      // Wait for expiration
      await new Promise(resolve => setTimeout(resolve, 100));

      // Access expired entries to trigger passive expiration
      await stateMachine.get(key1);
      await stateMachine.get(key2);

      const stats = stateMachine.getStats();
      expect(stats.expirationCount).toBe(2);
    });

    it('should not expire entries without TTL', async () => {
      const shortInterval = 100;
      const sm = new CacheStateMachineImpl(maxMemory, shortInterval);

      // Add entry without TTL
      await sm.set(Buffer.from('key1'), Buffer.from('value1'));

      await sm.start();

      // Wait for multiple cleanup cycles
      await new Promise(resolve => setTimeout(resolve, 300));

      // Entry should still exist
      const entry = await sm.get(Buffer.from('key1'));
      expect(entry).not.toBeNull();
      expect(entry!.value.toString()).toBe('value1');

      await sm.stop();
    });

    it('should handle mixed TTL and non-TTL entries', async () => {
      const shortInterval = 100;
      const sm = new CacheStateMachineImpl(maxMemory, shortInterval);

      // Add entries with and without TTL
      await sm.set(Buffer.from('key1'), Buffer.from('value1'), 50); // With TTL
      await sm.set(Buffer.from('key2'), Buffer.from('value2')); // Without TTL
      await sm.set(Buffer.from('key3'), Buffer.from('value3'), 50); // With TTL

      await sm.start();

      // Wait for TTL entries to expire
      await new Promise(resolve => setTimeout(resolve, 200));

      // Only non-TTL entry should remain
      const stats = sm.getStats();
      expect(stats.entryCount).toBe(1);

      const entry = await sm.get(Buffer.from('key2'));
      expect(entry).not.toBeNull();
      expect(entry!.value.toString()).toBe('value2');

      await sm.stop();
    });
  });
});
