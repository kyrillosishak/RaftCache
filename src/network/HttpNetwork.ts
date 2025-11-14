/**
 * HttpNetwork - HTTP-based network implementation for production
 * 
 * Features:
 * - HTTP/JSON transport
 * - Connection pooling
 * - Request/response serialization
 * - Error handling
 */

import http from 'http';
import {
  NodeId,
  RequestVoteRequest,
  RequestVoteResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
  InstallSnapshotRequest,
  InstallSnapshotResponse,
} from '../types';
import { NetworkInterface, NetworkHandlers } from './NetworkInterface';

export interface HttpNetworkConfig {
  /** Port to listen on */
  port: number;
  /** Host to bind to */
  host?: string;
  /** Map of node IDs to their HTTP addresses */
  nodeAddresses: Map<NodeId, string>;
  /** Request timeout in milliseconds */
  timeout?: number;
}

/**
 * HttpNetwork provides HTTP-based RPC communication for production
 */
export class HttpNetwork implements NetworkInterface {
  private nodeId: NodeId;
  private config: Required<Omit<HttpNetworkConfig, 'nodeAddresses'>> & {
    nodeAddresses: Map<NodeId, string>;
  };
  private handlers?: NetworkHandlers;
  private server?: http.Server;
  private agent: http.Agent;

  constructor(nodeId: NodeId, config: HttpNetworkConfig) {
    this.nodeId = nodeId;
    this.config = {
      port: config.port,
      host: config.host ?? '0.0.0.0',
      nodeAddresses: config.nodeAddresses,
      timeout: config.timeout ?? 5000,
    };

    // Create HTTP agent with connection pooling
    this.agent = new http.Agent({
      keepAlive: true,
      maxSockets: 50,
      maxFreeSockets: 10,
      timeout: this.config.timeout,
    });
  }

  registerHandlers(handlers: NetworkHandlers): void {
    this.handlers = handlers;
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
        this.agent.destroy();
        resolve();
        return;
      }

      this.server.close((error) => {
        this.agent.destroy();
        if (error && (error as any).code !== 'ERR_SERVER_NOT_RUNNING') {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  async sendRequestVote(
    target: NodeId,
    request: RequestVoteRequest
  ): Promise<RequestVoteResponse> {
    return this.sendRpc<RequestVoteRequest, RequestVoteResponse>(
      target,
      '/raft/request-vote',
      request
    );
  }

  async sendAppendEntries(
    target: NodeId,
    request: AppendEntriesRequest
  ): Promise<AppendEntriesResponse> {
    return this.sendRpc<AppendEntriesRequest, AppendEntriesResponse>(
      target,
      '/raft/append-entries',
      request
    );
  }

  async sendInstallSnapshot(
    target: NodeId,
    request: InstallSnapshotRequest
  ): Promise<InstallSnapshotResponse> {
    return this.sendRpc<InstallSnapshotRequest, InstallSnapshotResponse>(
      target,
      '/raft/install-snapshot',
      request
    );
  }

  private async sendRpc<TRequest, TResponse>(
    target: NodeId,
    path: string,
    request: TRequest
  ): Promise<TResponse> {
    const address = this.config.nodeAddresses.get(target);
    if (!address) {
      throw new Error(`No address configured for node ${target}`);
    }

    const url = `${address}${path}`;
    const body = JSON.stringify(this.serializeRequest(request));

    return new Promise((resolve, reject) => {
      const req = http.request(
        url,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
            'X-Raft-Node-Id': this.nodeId,
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
              resolve(this.deserializeResponse(response));
            } catch (error) {
              reject(error);
            }
          });
        }
      );

      req.on('error', (error) => {
        reject(error);
      });

      req.on('timeout', () => {
        req.destroy();
        reject(new Error(`Request timeout after ${this.config.timeout}ms`));
      });

      req.write(body);
      req.end();
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

      if (!this.handlers) {
        res.writeHead(503, { 'Content-Type': 'text/plain' });
        res.end('Service Unavailable: No handlers registered');
        return;
      }

      // Read request body
      const body = await this.readBody(req);
      const request = JSON.parse(body);

      let response: any;

      switch (req.url) {
        case '/raft/request-vote':
          response = await this.handlers.handleRequestVote(
            this.deserializeRequest(request)
          );
          break;
        case '/raft/append-entries':
          response = await this.handlers.handleAppendEntries(
            this.deserializeRequest(request)
          );
          break;
        case '/raft/install-snapshot':
          response = await this.handlers.handleInstallSnapshot(
            this.deserializeRequest(request)
          );
          break;
        default:
          res.writeHead(404, { 'Content-Type': 'text/plain' });
          res.end('Not Found');
          return;
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(this.serializeResponse(response)));
    } catch (error) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end(`Internal Server Error: ${(error as Error).message}`);
    }
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

  private serializeRequest(request: any): any {
    // Convert Buffer fields to base64 strings for JSON serialization
    if (request.data && Buffer.isBuffer(request.data)) {
      return {
        ...request,
        data: request.data.toString('base64'),
        _dataIsBuffer: true,
      };
    }
    if (request.entries) {
      return {
        ...request,
        entries: request.entries.map((entry: any) => ({
          ...entry,
          command: Buffer.isBuffer(entry.command)
            ? entry.command.toString('base64')
            : entry.command,
          _commandIsBuffer: Buffer.isBuffer(entry.command),
        })),
      };
    }
    return request;
  }

  private deserializeRequest(request: any): any {
    // Convert base64 strings back to Buffers
    if (request._dataIsBuffer && typeof request.data === 'string') {
      return {
        ...request,
        data: Buffer.from(request.data, 'base64'),
      };
    }
    if (request.entries) {
      return {
        ...request,
        entries: request.entries.map((entry: any) => ({
          ...entry,
          command: entry._commandIsBuffer
            ? Buffer.from(entry.command, 'base64')
            : entry.command,
        })),
      };
    }
    return request;
  }

  private serializeResponse(response: any): any {
    return response;
  }

  private deserializeResponse(response: any): any {
    return response;
  }

  /**
   * Get the server address
   */
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

  /**
   * Get node addresses map
   */
  getNodeAddresses(): Map<NodeId, string> {
    return new Map(this.config.nodeAddresses);
  }
}
