/**
 * ReliableNetwork - Adds timeout and retry logic to NetworkInterface
 * 
 * Wraps any NetworkInterface implementation with:
 * - Configurable RPC timeout
 * - Exponential backoff for retries
 * - Graceful error handling
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

export interface RetryConfig {
  /** Maximum number of retry attempts */
  maxRetries?: number;
  /** Initial timeout in milliseconds */
  initialTimeout?: number;
  /** Maximum timeout in milliseconds */
  maxTimeout?: number;
  /** Backoff multiplier for exponential backoff */
  backoffMultiplier?: number;
  /** Whether to retry on timeout */
  retryOnTimeout?: boolean;
  /** Whether to retry on network errors */
  retryOnError?: boolean;
}

export class NetworkTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NetworkTimeoutError';
  }
}

export class NetworkRetryExhaustedError extends Error {
  constructor(message: string, public lastError: Error) {
    super(message);
    this.name = 'NetworkRetryExhaustedError';
  }
}

/**
 * ReliableNetwork wraps a NetworkInterface with timeout and retry logic
 */
export class ReliableNetwork implements NetworkInterface {
  private underlying: NetworkInterface;
  private config: Required<RetryConfig>;

  constructor(underlying: NetworkInterface, config: RetryConfig = {}) {
    this.underlying = underlying;
    this.config = {
      maxRetries: config.maxRetries ?? 3,
      initialTimeout: config.initialTimeout ?? 1000,
      maxTimeout: config.maxTimeout ?? 10000,
      backoffMultiplier: config.backoffMultiplier ?? 2,
      retryOnTimeout: config.retryOnTimeout ?? true,
      retryOnError: config.retryOnError ?? true,
    };
  }

  registerHandlers(handlers: NetworkHandlers): void {
    this.underlying.registerHandlers(handlers);
  }

  async start(): Promise<void> {
    return this.underlying.start();
  }

  async stop(): Promise<void> {
    return this.underlying.stop();
  }

  async sendRequestVote(
    target: NodeId,
    request: RequestVoteRequest
  ): Promise<RequestVoteResponse> {
    return this.withRetry(
      () => this.underlying.sendRequestVote(target, request),
      `RequestVote to ${target}`
    );
  }

  async sendAppendEntries(
    target: NodeId,
    request: AppendEntriesRequest
  ): Promise<AppendEntriesResponse> {
    return this.withRetry(
      () => this.underlying.sendAppendEntries(target, request),
      `AppendEntries to ${target}`
    );
  }

  async sendInstallSnapshot(
    target: NodeId,
    request: InstallSnapshotRequest
  ): Promise<InstallSnapshotResponse> {
    return this.withRetry(
      () => this.underlying.sendInstallSnapshot(target, request),
      `InstallSnapshot to ${target}`
    );
  }

  private async withRetry<T>(
    operation: () => Promise<T>,
    operationName: string
  ): Promise<T> {
    let lastError: Error | undefined;
    let currentTimeout = this.config.initialTimeout;

    for (let attempt = 0; attempt <= this.config.maxRetries; attempt++) {
      try {
        // Execute operation with timeout
        const result = await this.withTimeout(operation(), currentTimeout);
        return result;
      } catch (error) {
        lastError = error as Error;

        // If this was the last attempt, throw retry exhausted error
        if (attempt === this.config.maxRetries) {
          throw new NetworkRetryExhaustedError(
            `${operationName} failed after ${this.config.maxRetries + 1} attempts`,
            lastError
          );
        }

        // Check if we should retry
        const shouldRetry = this.shouldRetry(error as Error, attempt);
        
        if (!shouldRetry) {
          throw error;
        }

        // Wait before retrying (exponential backoff)
        await this.sleep(this.calculateBackoff(attempt));

        // Calculate next timeout with exponential backoff
        currentTimeout = Math.min(
          currentTimeout * this.config.backoffMultiplier,
          this.config.maxTimeout
        );
      }
    }

    // Should never reach here, but TypeScript needs it
    throw new NetworkRetryExhaustedError(
      `${operationName} failed after ${this.config.maxRetries + 1} attempts`,
      lastError!
    );
  }

  private async withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        setTimeout(() => {
          reject(new NetworkTimeoutError(`Operation timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  }

  private shouldRetry(error: Error, attempt: number): boolean {
    // Don't retry if we've exhausted attempts
    if (attempt >= this.config.maxRetries) {
      return false;
    }

    // Check if error is retryable based on config
    if (error instanceof NetworkTimeoutError) {
      return this.config.retryOnTimeout;
    }

    // Retry on network errors if configured
    if (this.config.retryOnError) {
      // Don't retry on certain errors that indicate permanent failures
      const nonRetryableErrors = [
        'not available',
        'no handlers',
      ];

      const errorMessage = error.message.toLowerCase();
      const isPermanentFailure = nonRetryableErrors.some(msg => 
        errorMessage.includes(msg)
      );

      return !isPermanentFailure;
    }

    return false;
  }

  private calculateBackoff(attempt: number): number {
    // Exponential backoff: initialTimeout * (backoffMultiplier ^ attempt)
    const backoff = this.config.initialTimeout * 
      Math.pow(this.config.backoffMultiplier, attempt);
    
    return Math.min(backoff, this.config.maxTimeout);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Get the underlying network interface
   */
  getUnderlying(): NetworkInterface {
    return this.underlying;
  }

  /**
   * Get current retry configuration
   */
  getConfig(): Required<RetryConfig> {
    return { ...this.config };
  }
}
