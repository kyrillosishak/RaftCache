/**
 * InMemoryNetwork - In-memory network implementation for testing
 * 
 * Supports:
 * - Message passing between nodes
 * - Simulated delays
 * - Simulated failures
 * - Network partition simulation
 */

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

interface NetworkMessage {
  from: NodeId;
  to: NodeId;
  type: 'RequestVote' | 'AppendEntries' | 'InstallSnapshot';
  request: RequestVoteRequest | AppendEntriesRequest | InstallSnapshotRequest;
  resolve: (response: any) => void;
  reject: (error: Error) => void;
}

export interface NetworkConfig {
  /** Base delay for message delivery in ms */
  baseDelay?: number;
  /** Random jitter to add to delays (0-1) */
  jitter?: number;
  /** Probability of message drop (0-1) */
  dropRate?: number;
}

/**
 * InMemoryNetwork provides a simulated network for testing Raft
 */
export class InMemoryNetwork implements NetworkInterface {
  private nodeId: NodeId;
  private handlers?: NetworkHandlers;
  private config: Required<NetworkConfig>;
  private partitions: Set<string> = new Set();
  private nodeFailures: Set<NodeId> = new Set();
  private messageQueue: NetworkMessage[] = [];
  private running: boolean = false;

  // Static registry of all network instances
  private static instances: Map<NodeId, InMemoryNetwork> = new Map();
  
  // Optional external network registry (for test clusters)
  private externalNetworks?: Map<NodeId, InMemoryNetwork>;

  constructor(nodeId: NodeId, configOrNetworks?: NetworkConfig | Map<NodeId, InMemoryNetwork>) {
    this.nodeId = nodeId;
    
    // Check if second parameter is a Map (external networks) or NetworkConfig
    if (configOrNetworks instanceof Map) {
      this.externalNetworks = configOrNetworks;
      this.config = {
        baseDelay: 0,
        jitter: 0,
        dropRate: 0,
      };
      this.externalNetworks.set(nodeId, this);
    } else {
      const config = configOrNetworks || {};
      this.config = {
        baseDelay: config.baseDelay ?? 0,
        jitter: config.jitter ?? 0,
        dropRate: config.dropRate ?? 0,
      };
    }

    InMemoryNetwork.instances.set(nodeId, this);
  }

  registerHandlers(handlers: NetworkHandlers): void {
    this.handlers = handlers;
  }

  async start(): Promise<void> {
    this.running = true;
  }

  async stop(): Promise<void> {
    this.running = false;
    InMemoryNetwork.instances.delete(this.nodeId);
  }

  async sendRequestVote(
    target: NodeId,
    request: RequestVoteRequest
  ): Promise<RequestVoteResponse> {
    return this.sendMessage(target, 'RequestVote', request);
  }

  async sendAppendEntries(
    target: NodeId,
    request: AppendEntriesRequest
  ): Promise<AppendEntriesResponse> {
    return this.sendMessage(target, 'AppendEntries', request);
  }

  async sendInstallSnapshot(
    target: NodeId,
    request: InstallSnapshotRequest
  ): Promise<InstallSnapshotResponse> {
    return this.sendMessage(target, 'InstallSnapshot', request);
  }

  private async sendMessage<T>(
    target: NodeId,
    type: NetworkMessage['type'],
    request: any
  ): Promise<T> {
    // Check if sender is failed
    if (this.nodeFailures.has(this.nodeId)) {
      throw new Error(`Node ${this.nodeId} is failed`);
    }

    // Check if target is failed
    if (this.nodeFailures.has(target)) {
      throw new Error(`Target node ${target} is failed`);
    }

    // Check if nodes are partitioned
    if (this.isPartitioned(this.nodeId, target)) {
      throw new Error(`Network partition between ${this.nodeId} and ${target}`);
    }

    // Simulate message drop
    if (Math.random() < this.config.dropRate) {
      throw new Error(`Message dropped from ${this.nodeId} to ${target}`);
    }

    // Get target network instance (check external networks first, then static registry)
    const targetNetwork = this.externalNetworks?.get(target) ?? InMemoryNetwork.instances.get(target);
    if (!targetNetwork || !targetNetwork.running) {
      throw new Error(`Target node ${target} not available`);
    }

    if (!targetNetwork.handlers) {
      throw new Error(`Target node ${target} has no handlers registered`);
    }

    // Calculate delay
    const delay = this.calculateDelay();

    // Send message with delay
    return new Promise<T>((resolve, reject) => {
      setTimeout(async () => {
        try {
          let response: any;
          
          switch (type) {
            case 'RequestVote':
              response = await targetNetwork.handlers!.handleRequestVote(
                request as RequestVoteRequest
              );
              break;
            case 'AppendEntries':
              response = await targetNetwork.handlers!.handleAppendEntries(
                request as AppendEntriesRequest
              );
              break;
            case 'InstallSnapshot':
              response = await targetNetwork.handlers!.handleInstallSnapshot(
                request as InstallSnapshotRequest
              );
              break;
          }

          // Simulate response delay
          const responseDelay = this.calculateDelay();
          setTimeout(() => resolve(response), responseDelay);
        } catch (error) {
          reject(error);
        }
      }, delay);
    });
  }

  private calculateDelay(): number {
    const jitter = this.config.jitter * Math.random();
    return this.config.baseDelay * (1 + jitter);
  }

  private isPartitioned(from: NodeId, to: NodeId): boolean {
    const key1 = `${from}-${to}`;
    const key2 = `${to}-${from}`;
    return this.partitions.has(key1) || this.partitions.has(key2);
  }

  // Test utilities

  /**
   * Create a network partition between two nodes
   */
  partition(node1: NodeId, node2: NodeId): void {
    this.partitions.add(`${node1}-${node2}`);
  }

  /**
   * Partition this node from a list of other nodes
   */
  partitionFrom(nodes: NodeId[]): void {
    for (const node of nodes) {
      this.partition(this.nodeId, node);
    }
  }

  /**
   * Heal a network partition between two nodes
   */
  healPartition(node1: NodeId | NodeId[], node2?: NodeId): void {
    if (Array.isArray(node1)) {
      // Called with array of nodes - heal partition from this node to all nodes in array
      for (const node of node1) {
        this.partitions.delete(`${this.nodeId}-${node}`);
        this.partitions.delete(`${node}-${this.nodeId}`);
      }
    } else if (node2) {
      // Called with two node IDs
      this.partitions.delete(`${node1}-${node2}`);
      this.partitions.delete(`${node2}-${node1}`);
    }
  }

  /**
   * Partition a node from all other nodes
   */
  static partitionNode(nodeId: NodeId, allNodes: NodeId[]): void {
    const network = InMemoryNetwork.instances.get(nodeId);
    if (!network) return;

    for (const other of allNodes) {
      if (other !== nodeId) {
        network.partition(nodeId, other);
      }
    }
  }

  /**
   * Heal all partitions for a node
   */
  static healNode(nodeId: NodeId, allNodes: NodeId[]): void {
    const network = InMemoryNetwork.instances.get(nodeId);
    if (!network) return;

    for (const other of allNodes) {
      if (other !== nodeId) {
        network.healPartition(nodeId, other);
      }
    }
  }

  /**
   * Simulate node failure
   */
  static failNode(nodeId: NodeId): void {
    // Mark node as failed in all network instances
    for (const network of InMemoryNetwork.instances.values()) {
      network.nodeFailures.add(nodeId);
    }
  }

  /**
   * Recover a failed node
   */
  static recoverNode(nodeId: NodeId): void {
    // Remove failure status from all network instances
    for (const network of InMemoryNetwork.instances.values()) {
      network.nodeFailures.delete(nodeId);
    }
  }

  /**
   * Clear all partitions and failures
   */
  static reset(): void {
    for (const network of InMemoryNetwork.instances.values()) {
      network.partitions.clear();
      network.nodeFailures.clear();
    }
  }

  /**
   * Get all active network instances
   */
  static getInstances(): Map<NodeId, InMemoryNetwork> {
    return InMemoryNetwork.instances;
  }

  /**
   * Clear all network instances (for test cleanup)
   */
  static clearAll(): void {
    InMemoryNetwork.instances.clear();
  }
}
