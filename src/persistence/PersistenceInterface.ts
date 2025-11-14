/**
 * PersistenceInterface - Abstraction for durable storage
 */

import { NodeId, LogEntry, CacheSnapshot } from '../types';

/**
 * Persistent state returned by loadState (term and vote only, log managed separately)
 */
export interface PersistedState {
  currentTerm: number;
  votedFor?: NodeId;
}

export interface PersistenceInterface {
  /**
   * Save current term and vote information
   * Must be durable before returning (fsync or equivalent)
   */
  saveState(term: number, votedFor?: NodeId): Promise<void>;

  /**
   * Load persistent state from storage
   */
  loadState(): Promise<PersistedState>;

  /**
   * Append log entries to persistent storage
   * Must be durable before returning
   */
  appendLogEntries(entries: LogEntry[]): Promise<void>;

  /**
   * Get log entries in the specified range [startIndex, endIndex]
   */
  getLogEntries(startIndex: number, endIndex: number): Promise<LogEntry[]>;

  /**
   * Get the index of the last log entry
   */
  getLastLogIndex(): Promise<number>;

  /**
   * Get the term of the last log entry
   */
  getLastLogTerm(): Promise<number>;

  /**
   * Truncate log from the specified index (inclusive)
   */
  truncateLog(fromIndex: number): Promise<void>;

  /**
   * Save a snapshot to persistent storage
   */
  saveSnapshot(snapshot: CacheSnapshot): Promise<void>;

  /**
   * Load a snapshot from persistent storage
   */
  loadSnapshot(): Promise<CacheSnapshot | null>;

  /**
   * Get the latest snapshot metadata
   */
  getLatestSnapshot(): Promise<{ lastIncludedIndex: number; lastIncludedTerm: number } | null>;
}
