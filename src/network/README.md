# Network Layer Implementation

This directory contains the network layer implementations for the Raft distributed cache system.

## Components

### NetworkInterface
The core abstraction for RPC communication between Raft nodes. All network implementations must implement this interface.

**Methods:**
- `sendRequestVote()` - Send RequestVote RPC
- `sendAppendEntries()` - Send AppendEntries RPC
- `sendInstallSnapshot()` - Send InstallSnapshot RPC
- `registerHandlers()` - Register RPC handlers
- `start()` - Start the network service
- `stop()` - Stop the network service

### InMemoryNetwork
In-memory network implementation for testing and development.

**Features:**
- Direct message passing between nodes (no actual network)
- Simulated network delays with configurable jitter
- Simulated message drops
- Network partition simulation
- Node failure simulation
- Test utilities for chaos testing

**Usage:**
```typescript
const network = new InMemoryNetwork('node1', {
  baseDelay: 10,      // 10ms base delay
  jitter: 0.5,        // 50% jitter
  dropRate: 0.01,     // 1% message drop rate
});

// Simulate partition
network.partition('node1', 'node2');

// Heal partition
network.healPartition('node1', 'node2');

// Simulate node failure
InMemoryNetwork.failNode('node1');

// Recover node
InMemoryNetwork.recoverNode('node1');
```

### ReliableNetwork
Wrapper that adds timeout and retry logic to any NetworkInterface implementation.

**Features:**
- Configurable RPC timeout
- Exponential backoff for retries
- Configurable retry behavior (on timeout, on error)
- Graceful error handling
- Distinguishes between transient and permanent failures

**Usage:**
```typescript
const underlying = new InMemoryNetwork('node1');
const reliable = new ReliableNetwork(underlying, {
  maxRetries: 3,
  initialTimeout: 1000,
  maxTimeout: 10000,
  backoffMultiplier: 2,
  retryOnTimeout: true,
  retryOnError: true,
});
```

**Retry Behavior:**
- Retries on transient failures (network errors, timeouts)
- Does not retry on permanent failures (node not available, no handlers)
- Uses exponential backoff: `initialTimeout * (backoffMultiplier ^ attempt)`
- Caps timeout at `maxTimeout`
- Throws `NetworkRetryExhaustedError` after max retries

### HttpNetwork
Production HTTP-based network implementation.

**Features:**
- HTTP/JSON transport
- Connection pooling with keep-alive
- Automatic serialization/deserialization
- Buffer support (base64 encoding for JSON)
- Configurable timeout
- Error handling

**Usage:**
```typescript
const nodeAddresses = new Map([
  ['node1', 'http://localhost:8001'],
  ['node2', 'http://localhost:8002'],
  ['node3', 'http://localhost:8003'],
]);

const network = new HttpNetwork('node1', {
  port: 8001,
  host: '0.0.0.0',
  nodeAddresses,
  timeout: 5000,
});

await network.start();

// Send RPC
const response = await network.sendRequestVote('node2', request);

await network.stop();
```

**Endpoints:**
- `POST /raft/request-vote` - RequestVote RPC
- `POST /raft/append-entries` - AppendEntries RPC
- `POST /raft/install-snapshot` - InstallSnapshot RPC

## Recommended Usage

### Development and Testing
Use `InMemoryNetwork` for unit tests and integration tests:

```typescript
const network = new InMemoryNetwork('node1');
```

### Production
Use `HttpNetwork` wrapped with `ReliableNetwork`:

```typescript
const httpNetwork = new HttpNetwork('node1', {
  port: 8001,
  nodeAddresses,
});

const network = new ReliableNetwork(httpNetwork, {
  maxRetries: 3,
  initialTimeout: 1000,
  maxTimeout: 10000,
});
```

## Testing

All network implementations have comprehensive test coverage:

```bash
# Test all network implementations
npm test -- src/network/

# Test specific implementation
npm test -- src/network/InMemoryNetwork.test.ts
npm test -- src/network/ReliableNetwork.test.ts
npm test -- src/network/HttpNetwork.test.ts
```

## Error Handling

### NetworkTimeoutError
Thrown when an RPC times out.

### NetworkRetryExhaustedError
Thrown when all retry attempts are exhausted. Contains the last error as `lastError` property.

### Connection Errors
- "No address configured for node X" - Node address not in configuration
- "Target node X not available" - Node is not running or unreachable
- "Network partition between X and Y" - Nodes are partitioned (InMemoryNetwork only)
- "Node X is failed" - Node is in failed state (InMemoryNetwork only)

## Performance Considerations

### Connection Pooling
`HttpNetwork` uses HTTP keep-alive with connection pooling:
- `maxSockets: 50` - Maximum concurrent connections
- `maxFreeSockets: 10` - Maximum idle connections to keep

### Serialization
- Buffer data is base64-encoded for JSON transport
- Log entries with Buffer commands are automatically serialized
- Minimal overhead for small messages

### Timeouts
- Default timeout: 5000ms (HttpNetwork)
- Default timeout: 1000ms (ReliableNetwork)
- Adjust based on network latency and cluster size

## Future Enhancements

Potential improvements for production deployments:

1. **TLS/SSL Support** - Secure communication between nodes
2. **Compression** - Reduce bandwidth for large snapshots
3. **Streaming** - Stream large snapshots in chunks
4. **Metrics** - Built-in metrics for latency, throughput, errors
5. **Circuit Breaker** - Prevent cascading failures
6. **Load Balancing** - Distribute load across multiple network interfaces
7. **gRPC Support** - Alternative to HTTP for better performance
