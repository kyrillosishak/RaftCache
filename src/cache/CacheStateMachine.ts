/**
 * CacheStateMachine - Cache-specific state machine implementation
 */

import { CacheCommand, CacheResult, CacheEntry, CacheSnapshot, CacheStats, CommandType } from '../types';
import { MetricsCollector } from '../metrics/MetricsCollector';
import { Logger } from '../logging/Logger';

export interface CacheStateMachineConfig {
  maxMemory: number;
  expirationInterval?: number;
  metrics?: MetricsCollector;
  logger?: Logger;
}

/**
 * Node in the doubly-linked list for LRU tracking
 */
class LRUNode {
  key: string;
  prev: LRUNode | null = null;
  next: LRUNode | null = null;

  constructor(key: string) {
    this.key = key;
  }
}

/**
 * Doubly-linked list for LRU tracking
 * Most recently used items are at the head, least recently used at the tail
 */
class LRUList {
  private head: LRUNode | null = null;
  private tail: LRUNode | null = null;
  private nodeMap: Map<string, LRUNode> = new Map();

  /**
   * Move a node to the front (most recently used)
   */
  moveToFront(key: string): void {
    let node = this.nodeMap.get(key);

    if (!node) {
      // Create new node
      node = new LRUNode(key);
      this.nodeMap.set(key, node);
    } else if (node === this.head) {
      // Already at front
      return;
    } else {
      // Remove from current position
      this.remove(node);
    }

    // Add to front
    node.next = this.head;
    node.prev = null;

    if (this.head) {
      this.head.prev = node;
    }

    this.head = node;

    if (!this.tail) {
      this.tail = node;
    }
  }

  /**
   * Remove a node from the list
   */
  private remove(node: LRUNode): void {
    if (node.prev) {
      node.prev.next = node.next;
    } else {
      this.head = node.next;
    }

    if (node.next) {
      node.next.prev = node.prev;
    } else {
      this.tail = node.prev;
    }

    node.prev = null;
    node.next = null;
  }

  /**
   * Remove a node by key
   */
  removeByKey(key: string): void {
    const node = this.nodeMap.get(key);
    if (node) {
      this.remove(node);
      this.nodeMap.delete(key);
    }
  }

  /**
   * Get the least recently used key (tail of the list)
   */
  getLRU(): string | null {
    return this.tail ? this.tail.key : null;
  }

  /**
   * Get the size of the list
   */
  size(): number {
    return this.nodeMap.size;
  }

  /**
   * Clear the list
   */
  clear(): void {
    this.head = null;
    this.tail = null;
    this.nodeMap.clear();
  }
}

export interface CacheStateMachine {
  /**
   * Apply a single cache command to the state machine
   */
  apply(command: CacheCommand): Promise<CacheResult>;

  /**
   * Apply multiple commands atomically (batch operation)
   */
  batchApply(commands: CacheCommand[]): Promise<CacheResult[]>;

  /**
   * Get a value from the cache (does not go through Raft)
   */
  get(key: Buffer): Promise<CacheEntry | null>;

  /**
   * Set a value in the cache (internal use, should be called via apply)
   */
  set(key: Buffer, value: Buffer, ttl?: number): Promise<void>;

  /**
   * Delete a value from the cache (internal use, should be called via apply)
   */
  delete(key: Buffer): Promise<boolean>;

  /**
   * Evict least recently used entries to free memory
   */
  evictLRU(): Promise<number>;

  /**
   * Remove expired entries from the cache
   */
  removeExpired(): Promise<number>;

  /**
   * Create a snapshot of the current cache state
   */
  getSnapshot(): Promise<CacheSnapshot>;

  /**
   * Restore cache state from a snapshot
   */
  restoreSnapshot(snapshot: CacheSnapshot): Promise<void>;

  /**
   * Get current memory usage in bytes
   */
  getMemoryUsage(): number;

  /**
   * Get cache statistics for monitoring
   */
  getStats(): CacheStats;

  /**
   * Start background tasks (TTL expiration, etc.)
   */
  start(): Promise<void>;

  /**
   * Stop background tasks
   */
  stop(): Promise<void>;
}

/**
 * CacheStateMachineImpl - Implementation of the cache state machine
 */
export class CacheStateMachineImpl implements CacheStateMachine {
  // In-memory cache storage (key as string for Map compatibility)
  private cache: Map<string, CacheEntry> = new Map();

  // LRU tracking
  private lruList: LRUList = new LRUList();

  // Memory tracking
  private memoryUsage: number = 0;
  private readonly maxMemory: number;

  // Statistics
  private hitCount: number = 0;
  private missCount: number = 0;
  private evictionCount: number = 0;
  private expirationCount: number = 0;

  // Background tasks
  private expirationTimer?: NodeJS.Timeout;
  private running: boolean = false;
  private readonly expirationInterval: number;

  // Metrics and logging
  private readonly metrics?: MetricsCollector;
  private readonly logger?: Logger;

  constructor(
    maxMemory: number,
    expirationInterval: number = 1000,
    metrics?: MetricsCollector,
    logger?: Logger
  ) {
    this.maxMemory = maxMemory;
    this.expirationInterval = expirationInterval;
    this.metrics = metrics;
    this.logger = logger;
  }

  /**
   * Apply a single cache command to the state machine
   */
  async apply(command: CacheCommand): Promise<CacheResult> {
    try {
      switch (command.type) {
        case CommandType.SET:
          if (!command.key || !command.value) {
            return { success: false, error: 'SET command requires key and value' };
          }
          await this.set(command.key, command.value, command.ttl);
          this.metrics?.recordCacheOperation('set');
          this.logger?.logCacheOperation('SET', command.key.toString('base64').substring(0, 16), true, {
            ttl: command.ttl,
          });
          return { success: true };

        case CommandType.DELETE:
          if (!command.key) {
            return { success: false, error: 'DELETE command requires key' };
          }
          const deleted = await this.delete(command.key);
          this.metrics?.recordCacheOperation('delete');
          this.logger?.logCacheOperation('DELETE', command.key.toString('base64').substring(0, 16), deleted);
          return { success: deleted };

        case CommandType.BATCH:
          if (!command.batch) {
            return { success: false, error: 'BATCH command requires batch array' };
          }
          const results = await this.batchApply(command.batch);
          this.metrics?.recordCacheOperation('batch');
          // Batch is successful if all operations succeeded
          const allSuccess = results.every(r => r.success);
          return { success: allSuccess };

        default:
          return { success: false, error: `Unknown command type: ${command.type}` };
      }
    } catch (error) {
      this.logger?.error('CACHE', 'Failed to apply command', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  /**
   * Apply multiple commands atomically (batch operation)
   */
  async batchApply(commands: CacheCommand[]): Promise<CacheResult[]> {
    const results: CacheResult[] = [];

    for (const command of commands) {
      const result = await this.apply(command);
      results.push(result);
    }

    return results;
  }

  /**
   * Get a value from the cache (does not go through Raft)
   */
  async get(key: Buffer): Promise<CacheEntry | null> {
    const keyStr = key.toString('base64');
    const entry = this.cache.get(keyStr);

    if (!entry) {
      this.missCount++;
      this.metrics?.recordCacheOperation('get', false);
      return null;
    }

    // Check if entry has expired
    const now = Date.now();
    if (entry.expiresAt && entry.expiresAt <= now) {
      // Entry expired, remove it
      this.cache.delete(keyStr);
      this.lruList.removeByKey(keyStr);
      this.memoryUsage -= entry.size;
      this.expirationCount++;
      this.missCount++;
      this.metrics?.recordCacheOperation('get', false);
      this.metrics?.recordExpiration();
      this.logger?.debug('CACHE', `Entry expired on access`, {
        key: keyStr.substring(0, 16),
      });
      return null;
    }

    // Update last accessed time and move to front of LRU list
    entry.lastAccessedAt = now;
    this.lruList.moveToFront(keyStr);
    this.hitCount++;
    this.metrics?.recordCacheOperation('get', true);

    return entry;
  }

  /**
   * Set a value in the cache (internal use, should be called via apply)
   */
  async set(key: Buffer, value: Buffer, ttl?: number): Promise<void> {
    const keyStr = key.toString('base64');
    const now = Date.now();

    // Calculate entry size (key + value + metadata overhead)
    const entrySize = key.length + value.length + 64; // 64 bytes for metadata

    // Check if we need to evict entries to make room
    while (this.memoryUsage + entrySize > this.maxMemory && this.cache.size > 0) {
      await this.evictLRU();
    }

    // Remove old entry if it exists
    const oldEntry = this.cache.get(keyStr);
    if (oldEntry) {
      this.memoryUsage -= oldEntry.size;
    }

    // Create new entry
    const entry: CacheEntry = {
      key,
      value,
      createdAt: now,
      expiresAt: ttl ? now + ttl : undefined,
      lastAccessedAt: now,
      size: entrySize,
    };

    // Store entry and add to LRU list (most recently used)
    this.cache.set(keyStr, entry);
    this.lruList.moveToFront(keyStr);
    this.memoryUsage += entrySize;

    // Update memory metrics
    this.metrics?.updateMemoryUsage(this.memoryUsage, this.maxMemory);
  }

  /**
   * Delete a value from the cache (internal use, should be called via apply)
   */
  async delete(key: Buffer): Promise<boolean> {
    const keyStr = key.toString('base64');
    const entry = this.cache.get(keyStr);

    if (!entry) {
      return false;
    }

    this.cache.delete(keyStr);
    this.lruList.removeByKey(keyStr);
    this.memoryUsage -= entry.size;
    return true;
  }

  /**
   * Evict least recently used entries to free memory
   * Prioritizes expired entries first, then applies LRU eviction
   */
  async evictLRU(): Promise<number> {
    if (this.cache.size === 0) {
      return 0;
    }

    const now = Date.now();

    // First, try to evict expired entries
    for (const [key, entry] of this.cache.entries()) {
      if (entry.expiresAt && entry.expiresAt <= now) {
        this.cache.delete(key);
        this.lruList.removeByKey(key);
        this.memoryUsage -= entry.size;
        this.expirationCount++;
        this.metrics?.recordExpiration();
        this.logger?.debug('CACHE', 'Evicted expired entry', {
          key: key.substring(0, 16),
        });
        return 1;
      }
    }

    // No expired entries, evict LRU entry
    const lruKey = this.lruList.getLRU();
    if (lruKey) {
      const entry = this.cache.get(lruKey);
      if (entry) {
        this.cache.delete(lruKey);
        this.lruList.removeByKey(lruKey);
        this.memoryUsage -= entry.size;
        this.evictionCount++;
        this.metrics?.recordEviction(true);
        this.logger?.logEviction('lru', 1);
        return 1;
      }
    }

    return 0;
  }

  /**
   * Remove expired entries from the cache
   */
  async removeExpired(): Promise<number> {
    const now = Date.now();
    let removedCount = 0;

    for (const [key, entry] of this.cache.entries()) {
      if (entry.expiresAt && entry.expiresAt <= now) {
        this.cache.delete(key);
        this.lruList.removeByKey(key);
        this.memoryUsage -= entry.size;
        this.expirationCount++;
        this.metrics?.recordExpiration();
        removedCount++;
      }
    }

    if (removedCount > 0) {
      this.logger?.logEviction('ttl', removedCount);
    }

    return removedCount;
  }

  /**
   * Create a snapshot of the current cache state
   */
  async getSnapshot(): Promise<CacheSnapshot> {
    return {
      entries: new Map(this.cache),
      lastIncludedIndex: 0, // Will be set by RaftNode
      lastIncludedTerm: 0, // Will be set by RaftNode
      timestamp: Date.now(),
      memoryUsage: this.memoryUsage,
    };
  }

  /**
   * Restore cache state from a snapshot
   */
  async restoreSnapshot(snapshot: CacheSnapshot): Promise<void> {
    this.cache = new Map(snapshot.entries);
    this.memoryUsage = snapshot.memoryUsage;

    // Rebuild LRU list based on lastAccessedAt timestamps
    this.lruList.clear();
    const entries = Array.from(this.cache.entries());
    // Sort by lastAccessedAt (oldest first)
    entries.sort((a, b) => a[1].lastAccessedAt - b[1].lastAccessedAt);
    // Add to LRU list in order (oldest to newest)
    for (const [key] of entries) {
      this.lruList.moveToFront(key);
    }
  }

  /**
   * Get current memory usage in bytes
   */
  getMemoryUsage(): number {
    return this.memoryUsage;
  }

  /**
   * Get cache statistics for monitoring
   */
  getStats(): CacheStats {
    const totalRequests = this.hitCount + this.missCount;
    const hitRate = totalRequests > 0 ? this.hitCount / totalRequests : 0;

    return {
      entryCount: this.cache.size,
      memoryUsage: this.memoryUsage,
      maxMemory: this.maxMemory,
      hitRate,
      evictionCount: this.evictionCount,
      expirationCount: this.expirationCount,
    };
  }

  /**
   * Start background tasks (TTL expiration, etc.)
   */
  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    this.running = true;

    // Start periodic expiration cleanup
    this.scheduleExpirationCheck();

    this.logger?.info('CACHE', 'Background tasks started', {
      expirationInterval: this.expirationInterval,
    });
  }

  /**
   * Schedule the next expiration check
   */
  private scheduleExpirationCheck(): void {
    if (!this.running) {
      return;
    }

    this.expirationTimer = setTimeout(async () => {
      try {
        const removed = await this.removeExpired();
        if (removed > 0) {
          this.logger?.debug('CACHE', `Periodic expiration removed ${removed} entries`);
        }
      } catch (error) {
        this.logger?.error('CACHE', 'Error during periodic expiration', error);
      }

      // Schedule next check
      this.scheduleExpirationCheck();
    }, this.expirationInterval);
  }

  /**
   * Stop background tasks
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;

    if (this.expirationTimer) {
      clearTimeout(this.expirationTimer);
      this.expirationTimer = undefined;
    }
  }
}
