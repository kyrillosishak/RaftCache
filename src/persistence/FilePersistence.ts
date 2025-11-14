/**
 * File-based implementation of PersistenceInterface
 * Provides durable storage for Raft state, log entries, and snapshots
 */

import * as fs from 'fs/promises';
import * as fsSync from 'fs';
import * as path from 'path';
import { PersistenceInterface, PersistedState } from './PersistenceInterface';
import { NodeId, LogEntry, CacheSnapshot, CacheEntry } from '../types';

export class FilePersistence implements PersistenceInterface {
  private readonly dataDir: string;
  private readonly stateFile: string;
  private readonly logFile: string;
  private readonly snapshotDir: string;
  
  // In-memory cache of log entries for performance
  private logCache: LogEntry[] = [];
  private lastSnapshotIndex: number = 0;

  constructor(dataDir: string) {
    this.dataDir = dataDir;
    this.stateFile = path.join(dataDir, 'state.json');
    this.logFile = path.join(dataDir, 'log.json');
    this.snapshotDir = path.join(dataDir, 'snapshots');
  }

  /**
   * Initialize the persistence layer
   * Creates necessary directories if they don't exist
   */
  async initialize(): Promise<void> {
    await fs.mkdir(this.dataDir, { recursive: true });
    await fs.mkdir(this.snapshotDir, { recursive: true });
    
    // Load log into memory cache
    await this.loadLogCache();
  }

  /**
   * Save current term and vote information with atomic write and fsync
   */
  async saveState(term: number, votedFor?: NodeId): Promise<void> {
    try {
      // Ensure directory exists
      await fs.mkdir(this.dataDir, { recursive: true });
      
      const state: PersistedState = { currentTerm: term, votedFor };
      const data = JSON.stringify(state, null, 2);
      
      // Atomic write: write to temp file, fsync, then rename
      const tempFile = `${this.stateFile}.tmp`;
      await fs.writeFile(tempFile, data, 'utf8');
      
      // Ensure data is written to disk
      const fd = await fs.open(tempFile, 'r+');
      await fd.sync();
      await fd.close();
      
      // Atomic rename
      await fs.rename(tempFile, this.stateFile);
      
      // Sync directory to ensure rename is durable
      const dirFd = await fs.open(this.dataDir, 'r');
      await dirFd.sync();
      await dirFd.close();
    } catch (error) {
      // Silently fail if directory was deleted during cleanup
      if ((error as any).code !== 'ENOENT') {
        throw error;
      }
    }
  }

  /**
   * Load persistent state from storage
   */
  async loadState(): Promise<PersistedState> {
    try {
      const data = await fs.readFile(this.stateFile, 'utf8');
      return JSON.parse(data);
    } catch (error: any) {
      if (error.code === 'ENOENT') {
        // File doesn't exist, return initial state
        return { currentTerm: 0 };
      }
      throw error;
    }
  }

  /**
   * Load log cache from disk
   */
  private async loadLogCache(): Promise<void> {
    try {
      const data = await fs.readFile(this.logFile, 'utf8');
      const parsed = JSON.parse(data);
      this.logCache = parsed.entries.map((entry: any) => ({
        term: entry.term,
        index: entry.index,
        command: Buffer.from(entry.command, 'base64'),
      }));
      this.lastSnapshotIndex = parsed.lastSnapshotIndex || 0;
    } catch (error: any) {
      if (error.code === 'ENOENT') {
        // File doesn't exist, start with empty log
        this.logCache = [];
        this.lastSnapshotIndex = 0;
      } else {
        throw error;
      }
    }
  }

  /**
   * Save log cache to disk with fsync
   */
  private async saveLogCache(): Promise<void> {
    try {
      // Ensure directory exists
      await fs.mkdir(this.dataDir, { recursive: true });
      
      const data = JSON.stringify({
        lastSnapshotIndex: this.lastSnapshotIndex,
        entries: this.logCache.map(entry => ({
          term: entry.term,
          index: entry.index,
          command: entry.command.toString('base64'),
        })),
      }, null, 2);
      
      const tempFile = `${this.logFile}.tmp`;
      await fs.writeFile(tempFile, data, 'utf8');
      
      const fd = await fs.open(tempFile, 'r+');
      await fd.sync();
      await fd.close();
      
      await fs.rename(tempFile, this.logFile);
      
      const dirFd = await fs.open(this.dataDir, 'r');
      await dirFd.sync();
      await dirFd.close();
    } catch (error) {
      // Silently fail if directory was deleted during cleanup
      if ((error as any).code !== 'ENOENT') {
        throw error;
      }
    }
  }

  /**
   * Append log entries to persistent storage
   */
  async appendLogEntries(entries: LogEntry[]): Promise<void> {
    if (entries.length === 0) return;
    
    // Add to in-memory cache
    this.logCache.push(...entries);
    
    // Persist to disk
    await this.saveLogCache();
  }

  /**
   * Get log entries in the specified range [startIndex, endIndex]
   */
  async getLogEntries(startIndex: number, endIndex: number): Promise<LogEntry[]> {
    // Adjust for snapshot offset
    const adjustedStart = startIndex - this.lastSnapshotIndex - 1;
    const adjustedEnd = endIndex - this.lastSnapshotIndex - 1;
    
    if (adjustedStart < 0 || adjustedStart >= this.logCache.length) {
      return [];
    }
    
    const end = Math.min(adjustedEnd + 1, this.logCache.length);
    return this.logCache.slice(adjustedStart, end);
  }

  /**
   * Get the index of the last log entry
   */
  async getLastLogIndex(): Promise<number> {
    if (this.logCache.length === 0) {
      return this.lastSnapshotIndex;
    }
    return this.logCache[this.logCache.length - 1].index;
  }

  /**
   * Get the term of the last log entry
   */
  async getLastLogTerm(): Promise<number> {
    if (this.logCache.length === 0) {
      // Return term from snapshot if available
      const snapshot = await this.getLatestSnapshot();
      return snapshot?.lastIncludedTerm || 0;
    }
    return this.logCache[this.logCache.length - 1].term;
  }

  /**
   * Truncate log from the specified index (inclusive)
   */
  async truncateLog(fromIndex: number): Promise<void> {
    // Adjust for snapshot offset
    const adjustedIndex = fromIndex - this.lastSnapshotIndex - 1;
    
    if (adjustedIndex < 0) {
      // Truncating before snapshot, clear entire log
      this.logCache = [];
    } else if (adjustedIndex < this.logCache.length) {
      // Truncate from the specified position
      this.logCache = this.logCache.slice(0, adjustedIndex);
    }
    
    // Persist changes
    await this.saveLogCache();
  }

  /**
   * Save a snapshot to persistent storage
   */
  async saveSnapshot(snapshot: CacheSnapshot): Promise<void> {
    // Ensure snapshot directory exists
    await fs.mkdir(this.snapshotDir, { recursive: true });
    
    const snapshotFile = path.join(
      this.snapshotDir,
      `snapshot-${snapshot.lastIncludedIndex}-${snapshot.lastIncludedTerm}.json`
    );
    
    // Convert Map to serializable format
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
    
    const data = JSON.stringify({
      entries,
      lastIncludedIndex: snapshot.lastIncludedIndex,
      lastIncludedTerm: snapshot.lastIncludedTerm,
      timestamp: snapshot.timestamp,
      memoryUsage: snapshot.memoryUsage,
    }, null, 2);
    
    // Write with atomic rename
    const tempFile = `${snapshotFile}.tmp`;
    await fs.writeFile(tempFile, data, 'utf8');
    
    const fd = await fs.open(tempFile, 'r+');
    await fd.sync();
    await fd.close();
    
    await fs.rename(tempFile, snapshotFile);
    
    const dirFd = await fs.open(this.snapshotDir, 'r');
    await dirFd.sync();
    await dirFd.close();
    
    // Update last snapshot index and truncate log
    this.lastSnapshotIndex = snapshot.lastIncludedIndex;
    
    // Remove log entries covered by snapshot
    this.logCache = this.logCache.filter(
      entry => entry.index > snapshot.lastIncludedIndex
    );
    await this.saveLogCache();
    
    // Clean up old snapshots
    await this.cleanupOldSnapshots(snapshot.lastIncludedIndex);
  }

  /**
   * Load a snapshot from persistent storage
   */
  async loadSnapshot(): Promise<CacheSnapshot | null> {
    try {
      const files = await fs.readdir(this.snapshotDir);
      const snapshotFiles = files.filter(f => f.startsWith('snapshot-') && f.endsWith('.json'));
    
    if (snapshotFiles.length === 0) {
      return null;
    }
    
    // Find the latest snapshot
    let latestFile = snapshotFiles[0];
    let latestIndex = this.parseSnapshotIndex(latestFile);
    
    for (const file of snapshotFiles) {
      const index = this.parseSnapshotIndex(file);
      if (index > latestIndex) {
        latestIndex = index;
        latestFile = file;
      }
    }
    
    const snapshotPath = path.join(this.snapshotDir, latestFile);
    const data = await fs.readFile(snapshotPath, 'utf8');
    const parsed = JSON.parse(data);
    
    // Convert back to Map with Buffers
    const entries = new Map<string, CacheEntry>();
    for (const [key, entry] of Object.entries(parsed.entries)) {
      entries.set(key, {
        key: Buffer.from((entry as any).key, 'base64'),
        value: Buffer.from((entry as any).value, 'base64'),
        createdAt: (entry as any).createdAt,
        expiresAt: (entry as any).expiresAt,
        lastAccessedAt: (entry as any).lastAccessedAt,
        size: (entry as any).size,
      });
    }
    
    return {
      entries,
      lastIncludedIndex: parsed.lastIncludedIndex,
      lastIncludedTerm: parsed.lastIncludedTerm,
      timestamp: parsed.timestamp,
      memoryUsage: parsed.memoryUsage,
    };
    } catch (error: any) {
      if (error.code === 'ENOENT') {
        // Directory doesn't exist, no snapshot available
        return null;
      }
      throw error;
    }
  }

  /**
   * Get the latest snapshot metadata
   */
  async getLatestSnapshot(): Promise<{ lastIncludedIndex: number; lastIncludedTerm: number } | null> {
    try {
      const files = await fs.readdir(this.snapshotDir);
      const snapshotFiles = files.filter(f => f.startsWith('snapshot-') && f.endsWith('.json'));
    
    if (snapshotFiles.length === 0) {
      return null;
    }
    
    let latestFile = snapshotFiles[0];
    let latestIndex = this.parseSnapshotIndex(latestFile);
    let latestTerm = this.parseSnapshotTerm(latestFile);
    
    for (const file of snapshotFiles) {
      const index = this.parseSnapshotIndex(file);
      if (index > latestIndex) {
        latestIndex = index;
        latestTerm = this.parseSnapshotTerm(file);
        latestFile = file;
      }
    }
    
    return { lastIncludedIndex: latestIndex, lastIncludedTerm: latestTerm };
    } catch (error: any) {
      if (error.code === 'ENOENT') {
        // Directory doesn't exist, no snapshot available
        return null;
      }
      throw error;
    }
  }

  /**
   * Parse snapshot index from filename
   */
  private parseSnapshotIndex(filename: string): number {
    const match = filename.match(/snapshot-(\d+)-(\d+)\.json/);
    return match ? parseInt(match[1], 10) : 0;
  }

  /**
   * Parse snapshot term from filename
   */
  private parseSnapshotTerm(filename: string): number {
    const match = filename.match(/snapshot-(\d+)-(\d+)\.json/);
    return match ? parseInt(match[2], 10) : 0;
  }

  /**
   * Clean up old snapshots, keeping only the latest
   */
  private async cleanupOldSnapshots(currentIndex: number): Promise<void> {
    try {
      const files = await fs.readdir(this.snapshotDir);
      const snapshotFiles = files.filter(f => f.startsWith('snapshot-') && f.endsWith('.json'));
      
      for (const file of snapshotFiles) {
        const index = this.parseSnapshotIndex(file);
        if (index < currentIndex) {
          await fs.unlink(path.join(this.snapshotDir, file));
        }
      }
    } catch (error: any) {
      if (error.code === 'ENOENT') {
        // Directory doesn't exist, nothing to clean up
        return;
      }
      throw error;
    }
  }
}
