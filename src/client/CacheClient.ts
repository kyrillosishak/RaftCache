/**
 * CacheClient - Client API for cache operations
 */

import http from 'http';
import { NodeId, CacheCommand, CacheResult, ClusterStatus, CommandType } from '../types';

export interface CacheClient {
  /**
   * Connect to the cache cluster
   */
  connect(): Promise<void>;

  /**
   * Disconnect from the cache cluster
   */
  disconnect(): Promise<void>;

  /**
   * Get a value from the cache
   */
  get(key: Buffer): Promise<Buffer | null>;

  /**
   * Set a value in the cache with optional TTL
   */
  set(key: Buffer, value: Buffer, ttl?: number): Promise<void>;

  /**
   * Delete a value from the cache
   */
  delete(key: Buffer): Promise<void>;

  /**
   * Get multiple values in a single request
   */
  batchGet(keys: Buffer[]): Promise<Map<string, Buffer>>;

  /**
   * Set multiple values in a single request
   */
  batchSet(entries: Map<Buffer, Buffer>, ttl?: number): Promise<void>;

  /**
   * Execute multiple mixed operations in a single request
   */
  batchOp(commands: CacheCommand[]): Promise<CacheResult[]>;

  /**
   * Get cluster status
   */
  getStatus(): Promise<ClusterStatus>;
}

export interface CacheClientConfig {
  nodes: NodeId[]; // List of node addresses (e.g., ["http://localhost:3001", "http://localhost:3002"])
  timeout?: number; // Request timeout in ms
  retryAttempts?: number; // Number of retry attempts
  retryDelay?: number; // Delay between retries in ms
}

/**
 * CacheClientImpl - Implementation of the cache client
 */
export class CacheClientImpl implements CacheClient {
  private config: Required<CacheClientConfig>;
  private currentLeader?: NodeId;
  private agent: http.Agent;
  private connected: boolean = false;

  // Client-side metrics
  private metrics = {
    requestCount: 0,
    errorCount: 0,
    totalLatency: 0,
    redirectCount: 0,
  };

  constructor(config: CacheClientConfig) {
    this.config = {
      nodes: config.nodes,
      timeout: config.timeout ?? 5000,
      retryAttempts: config.retryAttempts ?? 3,
      retryDelay: config.retryDelay ?? 100,
    };

    // Create HTTP agent with connection pooling
    this.agent = new http.Agent({
      keepAlive: true,
      maxSockets: 50,
      maxFreeSockets: 10,
      timeout: this.config.timeout,
    });
  }

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    // Discover the current leader
    await this.discoverLeader();
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }

    this.agent.destroy();
    this.connected = false;
    this.currentLeader = undefined;
  }

  async get(key: Buffer): Promise<Buffer | null> {
    const command: CacheCommand = {
      type: CommandType.SET, // Will be handled as GET on server
      key,
    };

    const result = await this.executeCommand('/cache/get', { key: key.toString('base64') });
    
    if (result.value) {
      return Buffer.from(result.value, 'base64');
    }
    return null;
  }

  async set(key: Buffer, value: Buffer, ttl?: number): Promise<void> {
    const command: CacheCommand = {
      type: CommandType.SET,
      key,
      value,
      ttl,
    };

    await this.executeCommand('/cache/set', {
      key: key.toString('base64'),
      value: value.toString('base64'),
      ttl,
    });
  }

  async delete(key: Buffer): Promise<void> {
    const command: CacheCommand = {
      type: CommandType.DELETE,
      key,
    };

    await this.executeCommand('/cache/delete', {
      key: key.toString('base64'),
    });
  }

  async batchGet(keys: Buffer[]): Promise<Map<string, Buffer>> {
    const result = await this.executeCommand('/cache/batch-get', {
      keys: keys.map(k => k.toString('base64')),
    });

    const resultMap = new Map<string, Buffer>();
    if (result.results) {
      for (const [keyBase64, value] of Object.entries(result.results)) {
        // Decode the key from base64 back to string
        const keyStr = Buffer.from(keyBase64, 'base64').toString();
        if (value) {
          resultMap.set(keyStr, Buffer.from(value as string, 'base64'));
        } else {
          resultMap.set(keyStr, null as any);
        }
      }
    }
    return resultMap;
  }

  async batchSet(entries: Map<Buffer, Buffer>, ttl?: number): Promise<void> {
    const entriesArray = Array.from(entries.entries()).map(([key, value]) => ({
      key: key.toString('base64'),
      value: value.toString('base64'),
    }));

    await this.executeCommand('/cache/batch-set', {
      entries: entriesArray,
      ttl,
    });
  }

  async batchOp(commands: CacheCommand[]): Promise<CacheResult[]> {
    const serializedCommands = commands.map(cmd => ({
      type: cmd.type,
      key: cmd.key?.toString('base64'),
      value: cmd.value?.toString('base64'),
      ttl: cmd.ttl,
      batch: cmd.batch,
    }));

    const result = await this.executeCommand('/cache/batch-op', {
      commands: serializedCommands,
    });

    return result.results || [];
  }

  async getStatus(): Promise<ClusterStatus> {
    const result = await this.executeCommand('/cache/status', {});
    return result as ClusterStatus;
  }

  /**
   * Get client-side metrics
   */
  getMetrics() {
    return {
      ...this.metrics,
      averageLatency: this.metrics.requestCount > 0 
        ? this.metrics.totalLatency / this.metrics.requestCount 
        : 0,
    };
  }

  /**
   * Discover the current leader by querying nodes
   */
  private async discoverLeader(): Promise<void> {
    // Try each node to find one that responds successfully
    // The leader will handle requests, followers will redirect
    for (const node of this.config.nodes) {
      try {
        // Try a simple status query
        await this.sendRequest(node, '/cache/status', {});
        // If we get here without error, this node can handle requests
        this.currentLeader = node;
        return;
      } catch (error) {
        // Try next node
        continue;
      }
    }

    // If no responsive node found, just use the first node
    // and let the retry logic handle it
    this.currentLeader = this.config.nodes[0];
  }

  /**
   * Query a specific node for its status
   */
  private async queryNodeStatus(node: NodeId): Promise<any> {
    return this.sendRequest(node, '/raft/status', {});
  }

  /**
   * Execute a cache command with leader routing and retry logic
   */
  private async executeCommand(path: string, payload: any): Promise<any> {
    if (!this.connected) {
      throw new Error('Client not connected. Call connect() first.');
    }

    let lastError: Error | undefined;
    let attempts = 0;

    while (attempts < this.config.retryAttempts) {
      attempts++;

      try {
        const startTime = Date.now();
        
        // Try current leader first, or try all nodes in round-robin
        const target = this.currentLeader || this.config.nodes[(attempts - 1) % this.config.nodes.length];
        const result = await this.sendRequest(target, path, payload);

        // Update metrics
        this.metrics.requestCount++;
        this.metrics.totalLatency += Date.now() - startTime;

        // Cache the successful node as leader
        this.currentLeader = target;

        return result;
      } catch (error) {
        lastError = error as Error;
        this.metrics.errorCount++;

        // Check if it's a redirect error or connection error
        if (lastError.message.includes('Not the leader') || 
            lastError.message.includes('Connection failed') ||
            lastError.message.includes('ECONNREFUSED') ||
            lastError.message.includes('ECONNRESET')) {
          this.metrics.redirectCount++;
          
          // Try next node in the list
          const currentIndex = this.config.nodes.indexOf(this.currentLeader || '');
          const nextIndex = (currentIndex + 1) % this.config.nodes.length;
          this.currentLeader = this.config.nodes[nextIndex];
          
          // Don't count this as a retry attempt for leader redirects or connection errors
          if (attempts < this.config.retryAttempts) {
            attempts--;
          }
          continue;
        }

        // For other errors, wait before retry
        if (attempts < this.config.retryAttempts) {
          await this.sleep(this.config.retryDelay * attempts);
        }
      }
    }

    throw lastError || new Error('Failed to execute command after retries');
  }

  /**
   * Send HTTP request to a node
   */
  private async sendRequest(node: NodeId, path: string, payload: any): Promise<any> {
    const url = `${node}${path}`;
    const body = JSON.stringify(payload);

    return new Promise((resolve, reject) => {
      const req = http.request(
        url,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
          },
          agent: this.agent,
          timeout: this.config.timeout,
        },
        (res) => {
          let data = '';

          res.on('data', (chunk) => {
            data += chunk;
          });

          res.on('end', () => {
            try {
              if (res.statusCode !== 200) {
                reject(new Error(`HTTP ${res.statusCode}: ${data}`));
                return;
              }

              const response = JSON.parse(data);
              
              // Check if response indicates not leader
              if (response.success === false && response.error) {
                reject(new Error(response.error));
                return;
              }

              resolve(response);
            } catch (error) {
              reject(error);
            }
          });
        }
      );

      req.on('error', (error: any) => {
        // Handle connection errors specially
        if (error.code === 'ECONNREFUSED' || error.code === 'ECONNRESET') {
          reject(new Error(`Connection failed: ${error.code}`));
        } else {
          reject(error);
        }
      });

      req.on('timeout', () => {
        req.destroy();
        reject(new Error(`Request timeout after ${this.config.timeout}ms`));
      });

      req.write(body);
      req.end();
    });
  }

  /**
   * Sleep for specified milliseconds
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
