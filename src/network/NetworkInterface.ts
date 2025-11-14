/**
 * NetworkInterface - Abstraction for RPC communication between nodes
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

export interface NetworkInterface {
  /**
   * Send RequestVote RPC to target node
   */
  sendRequestVote(
    target: NodeId,
    request: RequestVoteRequest
  ): Promise<RequestVoteResponse>;

  /**
   * Send AppendEntries RPC to target node
   */
  sendAppendEntries(
    target: NodeId,
    request: AppendEntriesRequest
  ): Promise<AppendEntriesResponse>;

  /**
   * Send InstallSnapshot RPC to target node
   */
  sendInstallSnapshot(
    target: NodeId,
    request: InstallSnapshotRequest
  ): Promise<InstallSnapshotResponse>;

  /**
   * Register RPC handlers for incoming requests
   */
  registerHandlers(handlers: NetworkHandlers): void;

  /**
   * Start listening for incoming RPC requests
   */
  start(): Promise<void>;

  /**
   * Stop the network interface
   */
  stop(): Promise<void>;
}

export interface NetworkHandlers {
  handleRequestVote: (request: RequestVoteRequest) => Promise<RequestVoteResponse>;
  handleAppendEntries: (request: AppendEntriesRequest) => Promise<AppendEntriesResponse>;
  handleInstallSnapshot: (request: InstallSnapshotRequest) => Promise<InstallSnapshotResponse>;
}
