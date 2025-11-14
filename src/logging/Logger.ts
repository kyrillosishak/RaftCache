/**
 * Logger - Enhanced logging system for Raft and cache operations
 */

export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
}

export interface LogEntry {
  timestamp: number;
  level: LogLevel;
  nodeId: string;
  category: string;
  message: string;
  data?: any;
}

export class Logger {
  private readonly nodeId: string;
  private readonly minLevel: LogLevel;
  private readonly logHistory: LogEntry[] = [];
  private readonly maxHistorySize: number = 1000;

  constructor(nodeId: string, minLevel: LogLevel = LogLevel.INFO) {
    this.nodeId = nodeId;
    this.minLevel = minLevel;
  }

  /**
   * Log a debug message
   */
  debug(category: string, message: string, data?: any): void {
    this.log(LogLevel.DEBUG, category, message, data);
  }

  /**
   * Log an info message
   */
  info(category: string, message: string, data?: any): void {
    this.log(LogLevel.INFO, category, message, data);
  }

  /**
   * Log a warning message
   */
  warn(category: string, message: string, data?: any): void {
    this.log(LogLevel.WARN, category, message, data);
  }

  /**
   * Log an error message
   */
  error(category: string, message: string, data?: any): void {
    this.log(LogLevel.ERROR, category, message, data);
  }

  /**
   * Log a state transition
   */
  logStateTransition(from: string, to: string, term: number): void {
    this.info('STATE', `Transition: ${from} -> ${to}`, { term });
  }

  /**
   * Log an election event
   */
  logElection(event: 'start' | 'won' | 'lost' | 'timeout', term: number, data?: any): void {
    this.info('ELECTION', `Election ${event} for term ${term}`, data);
  }

  /**
   * Log an RPC request
   */
  logRpcRequest(
    rpcType: string,
    direction: 'sent' | 'received',
    peer: string,
    data?: any
  ): void {
    const message = `${rpcType} ${direction} ${direction === 'sent' ? 'to' : 'from'} ${peer}`;
    this.debug('RPC', message, data);
  }

  /**
   * Log an RPC response
   */
  logRpcResponse(rpcType: string, peer: string, success: boolean, data?: any): void {
    const message = `${rpcType} response from ${peer}: ${success ? 'success' : 'failure'}`;
    this.debug('RPC', message, data);
  }

  /**
   * Log a replication event
   */
  logReplication(
    event: 'start' | 'success' | 'failure' | 'retry',
    peer: string,
    data?: any
  ): void {
    const level = event === 'failure' ? LogLevel.WARN : LogLevel.DEBUG;
    const message = `Replication ${event} for peer ${peer}`;
    this.log(level, 'REPLICATION', message, data);
  }

  /**
   * Log a cache operation
   */
  logCacheOperation(
    operation: string,
    key: string,
    success: boolean,
    data?: any
  ): void {
    const message = `Cache ${operation} for key ${key}: ${success ? 'success' : 'failure'}`;
    this.debug('CACHE', message, data);
  }

  /**
   * Log a cache eviction
   */
  logEviction(reason: 'lru' | 'memory' | 'ttl', count: number): void {
    this.info('CACHE', `Evicted ${count} entries (reason: ${reason})`);
  }

  /**
   * Log a commit event
   */
  logCommit(index: number, term: number): void {
    this.debug('COMMIT', `Committed entry at index ${index}`, { term });
  }

  /**
   * Log an apply event
   */
  logApply(index: number, success: boolean): void {
    const message = `Applied entry at index ${index}: ${success ? 'success' : 'failure'}`;
    this.debug('APPLY', message);
  }

  /**
   * Core logging method
   */
  private log(level: LogLevel, category: string, message: string, data?: any): void {
    if (level < this.minLevel) {
      return;
    }

    const entry: LogEntry = {
      timestamp: Date.now(),
      level,
      nodeId: this.nodeId,
      category,
      message,
      data,
    };

    // Add to history
    this.logHistory.push(entry);
    if (this.logHistory.length > this.maxHistorySize) {
      this.logHistory.shift();
    }

    // Output to console
    this.outputToConsole(entry);
  }

  /**
   * Output log entry to console
   */
  private outputToConsole(entry: LogEntry): void {
    const timestamp = new Date(entry.timestamp).toISOString();
    const levelStr = LogLevel[entry.level].padEnd(5);
    const categoryStr = entry.category.padEnd(12);
    const prefix = `[${timestamp}] [${this.nodeId}] [${levelStr}] [${categoryStr}]`;
    const message = `${prefix} ${entry.message}`;

    switch (entry.level) {
      case LogLevel.DEBUG:
        if (entry.data) {
          console.debug(message, entry.data);
        } else {
          console.debug(message);
        }
        break;
      case LogLevel.INFO:
        if (entry.data) {
          console.log(message, entry.data);
        } else {
          console.log(message);
        }
        break;
      case LogLevel.WARN:
        if (entry.data) {
          console.warn(message, entry.data);
        } else {
          console.warn(message);
        }
        break;
      case LogLevel.ERROR:
        if (entry.data) {
          console.error(message, entry.data);
        } else {
          console.error(message);
        }
        break;
    }
  }

  /**
   * Get log history
   */
  getHistory(
    filter?: {
      level?: LogLevel;
      category?: string;
      since?: number;
    }
  ): LogEntry[] {
    let filtered = this.logHistory;

    if (filter) {
      if (filter.level !== undefined) {
        filtered = filtered.filter(entry => entry.level >= filter.level!);
      }
      if (filter.category) {
        filtered = filtered.filter(entry => entry.category === filter.category);
      }
      if (filter.since) {
        filtered = filtered.filter(entry => entry.timestamp >= filter.since!);
      }
    }

    return filtered;
  }

  /**
   * Clear log history
   */
  clearHistory(): void {
    this.logHistory.length = 0;
  }
}
