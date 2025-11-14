/**
 * CacheServer - HTTP server that exposes cache operations
 * Wraps RaftNode and provides REST API for cache operations
 */

import http from 'http';
import { RaftNodeImpl } from '../core/RaftNode';
import { CacheCommand, CommandType, CacheResult } from '../types';

export interface CacheServerConfig {
  port: number;
  host?: string;
}

/**
 * CacheServer provides HTTP endpoints for cache operations
 */
export class CacheServer {
  private raftNode: RaftNodeImpl;
  private config: Required<CacheServerConfig>;
  private server?: http.Server;

  constructor(raftNode: RaftNodeImpl, config: CacheServerConfig) {
    this.raftNode = raftNode;
    this.config = {
      port: config.port,
      host: config.host ?? '0.0.0.0',
    };
  }

  async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server = http.createServer(async (req, res) => {
        await this.handleRequest(req, res);
      });

      this.server.on('error', (error) => {
        reject(error);
      });

      this.server.listen(this.config.port, this.config.host, () => {
        resolve();
      });
    });
  }

  async stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.server || !this.server.listening) {
        resolve();
        return;
      }

      this.server.close((error) => {
        if (error && (error as any).code !== 'ERR_SERVER_NOT_RUNNING') {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  private async handleRequest(
    req: http.IncomingMessage,
    res: http.ServerResponse
  ): Promise<void> {
    try {
      if (req.method !== 'POST') {
        res.writeHead(405, { 'Content-Type': 'text/plain' });
        res.end('Method Not Allowed');
        return;
      }

      let result: any;

      // Status endpoints don't require a body
      if (req.url === '/cache/status' || req.url === '/raft/status') {
        result = await this.handleGetStatus();
      } else {
        // Read request body for other endpoints
        const body = await this.readBody(req);
        const payload = body ? JSON.parse(body) : {};

        switch (req.url) {
          case '/cache/get':
            result = await this.handleGet(payload);
            break;
          case '/cache/set':
            result = await this.handleSet(payload);
            break;
          case '/cache/delete':
            result = await this.handleDelete(payload);
            break;
          case '/cache/batch-get':
            result = await this.handleBatchGet(payload);
            break;
          case '/cache/batch-set':
            result = await this.handleBatchSet(payload);
            break;
          case '/cache/batch-op':
            result = await this.handleBatchOp(payload);
            break;
          default:
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('Not Found');
            return;
        }
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end(`Internal Server Error: ${(error as Error).message}`);
    }
  }

  private async handleGet(payload: any): Promise<any> {
    const key = Buffer.from(payload.key, 'base64');
    
    // For GET operations, we can read directly from the state machine
    // This is safe because the state machine reflects committed state
    const stateMachine = (this.raftNode as any).stateMachine;
    const entry = await stateMachine.get(key);
    
    if (entry) {
      return {
        success: true,
        value: entry.value.toString('base64'),
      };
    }
    
    return {
      success: true,
      value: null,
    };
  }

  private async handleSet(payload: any): Promise<any> {
    const key = Buffer.from(payload.key, 'base64');
    const value = Buffer.from(payload.value, 'base64');
    const ttl = payload.ttl;

    const command: CacheCommand = {
      type: CommandType.SET,
      key,
      value,
      ttl,
    };

    const commandBuffer = Buffer.from(JSON.stringify({
      type: command.type,
      key: command.key?.toString('base64'),
      value: command.value?.toString('base64'),
      ttl: command.ttl,
    }));

    const result = await this.raftNode.submitCommand(commandBuffer);
    return result;
  }

  private async handleDelete(payload: any): Promise<any> {
    const key = Buffer.from(payload.key, 'base64');

    const command: CacheCommand = {
      type: CommandType.DELETE,
      key,
    };

    const commandBuffer = Buffer.from(JSON.stringify({
      type: command.type,
      key: command.key?.toString('base64'),
    }));

    const result = await this.raftNode.submitCommand(commandBuffer);
    return result;
  }

  private async handleBatchGet(payload: any): Promise<any> {
    const keys = payload.keys.map((k: string) => Buffer.from(k, 'base64'));
    
    const stateMachine = (this.raftNode as any).stateMachine;
    const results: Record<string, string | null> = {};
    
    for (const key of keys) {
      const entry = await stateMachine.get(key);
      const keyStr = key.toString('base64');
      results[keyStr] = entry ? entry.value.toString('base64') : null;
    }
    
    return {
      success: true,
      results,
    };
  }

  private async handleBatchSet(payload: any): Promise<any> {
    const entries = payload.entries.map((e: any) => ({
      key: Buffer.from(e.key, 'base64'),
      value: Buffer.from(e.value, 'base64'),
    }));
    const ttl = payload.ttl;

    // Create batch command
    const batchCommands: CacheCommand[] = entries.map((e: any) => ({
      type: CommandType.SET,
      key: e.key,
      value: e.value,
      ttl,
    }));

    const command: CacheCommand = {
      type: CommandType.BATCH,
      batch: batchCommands,
    };

    const commandBuffer = Buffer.from(JSON.stringify({
      type: command.type,
      batch: command.batch?.map(c => ({
        type: c.type,
        key: c.key?.toString('base64'),
        value: c.value?.toString('base64'),
        ttl: c.ttl,
      })),
    }));

    const result = await this.raftNode.submitCommand(commandBuffer);
    return result;
  }

  private async handleBatchOp(payload: any): Promise<any> {
    const commands: CacheCommand[] = payload.commands.map((c: any) => ({
      type: c.type,
      key: c.key ? Buffer.from(c.key, 'base64') : undefined,
      value: c.value ? Buffer.from(c.value, 'base64') : undefined,
      ttl: c.ttl,
      batch: c.batch,
    }));

    const command: CacheCommand = {
      type: CommandType.BATCH,
      batch: commands,
    };

    const commandBuffer = Buffer.from(JSON.stringify({
      type: command.type,
      batch: command.batch?.map(c => ({
        type: c.type,
        key: c.key?.toString('base64'),
        value: c.value?.toString('base64'),
        ttl: c.ttl,
      })),
    }));

    const result = await this.raftNode.submitCommand(commandBuffer);
    
    // For batch operations, we need to return individual results
    // For now, return success for all if the batch was committed
    if (result.success) {
      const results: CacheResult[] = commands.map(() => ({
        success: true,
      }));
      return { success: true, results };
    }
    
    return result;
  }

  private async handleGetStatus(): Promise<any> {
    const status = this.raftNode.getStatus();
    
    // Build cluster status
    const clusterStatus = {
      leaderId: status.leaderId,
      nodes: [status],
      success: true,
    };
    
    return clusterStatus;
  }

  private readBody(req: http.IncomingMessage): Promise<string> {
    return new Promise((resolve, reject) => {
      let body = '';
      req.on('data', (chunk) => {
        body += chunk;
      });
      req.on('end', () => {
        resolve(body);
      });
      req.on('error', (error) => {
        reject(error);
      });
    });
  }

  getAddress(): string {
    if (!this.server || !this.server.listening) {
      throw new Error('Server not started');
    }
    const addr = this.server.address();
    if (typeof addr === 'string') {
      return addr;
    }
    return `http://${addr?.address}:${addr?.port}`;
  }
}
