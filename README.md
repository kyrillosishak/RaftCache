# RaftCache

**A distributed, strongly-consistent in-memory cache powered by the Raft consensus algorithm.**

RaftCache is an educational implementation of a distributed key-value store that demonstrates the Raft consensus protocol in action. Built with TypeScript, it combines the simplicity of an in-memory cache with the reliability of distributed consensus.

> **Note:** This project is designed primarily for learning and understanding distributed systems concepts. While it implements core Raft features and can be used for small-scale services or development environments, it's not optimized for high-traffic production workloads. For production use cases, consider battle-tested solutions like Redis, etcd, or Consul.

**Use Cases:**
- 📚 Learning distributed consensus and the Raft algorithm
- 🧪 Development and testing environments
- 🔬 Prototyping distributed applications
- 🏠 Small-scale internal services (< 1000 req/s)
- 🎓 Teaching distributed systems concepts

---

## ✨ Features

- **Strong Consistency** - Linearizable reads and writes through Raft consensus
- **Fault Tolerant** - Survives node failures with automatic leader election
- **TTL Support** - Automatic expiration of cache entries
- **Smart Eviction** - LRU policy with configurable memory limits
- **Batch Operations** - Efficient multi-key GET/SET/DELETE operations
- **Log Compaction** - Snapshot-based compaction for long-running clusters
- **Real-time Monitoring** - Built-in CLI and metrics collection
- **Network Resilience** - Handles partitions, delays, and packet loss
- **Persistent Storage** - Durable state with crash recovery

---

## 🚀 Quick Start

### Running as a Standalone Service (Like Redis)

Start a 3-node cluster:

```bash
# Terminal 1 - Node 1
npm run server -- --node-id node1 --port 7001 --peers node2,node3

# Terminal 2 - Node 2
npm run server -- --node-id node2 --port 7002 --peers node1,node3

# Terminal 3 - Node 3
npm run server -- --node-id node3 --port 7003 --peers node1,node2
```

Connect via HTTP:

```bash
# Set a value
curl -X POST http://localhost:7001/cache/set \
  -H "Content-Type: application/json" \
  -d '{"key":"dXNlcjoxMDAx","value":"eyJuYW1lIjoiQWxpY2UifQ==","ttl":60000}'

# Get a value
curl -X POST http://localhost:7001/cache/get \
  -H "Content-Type: application/json" \
  -d '{"key":"dXNlcjoxMDAx"}'

# Check cluster status
curl -X POST http://localhost:7001/raft/status
```

### Using as an Embedded Library

```bash
npm install raftcache
```

```typescript
import { RaftNodeImpl, CacheServer, FilePersistence, HttpNetwork } from 'raftcache';

// Create a node
const config = {
  nodeId: 'node1',
  peers: ['node2', 'node3'],
  electionTimeoutMin: 150,
  electionTimeoutMax: 300,
  heartbeatInterval: 50,
  maxCacheSize: 1000,
  dataDir: './data/node1'
};

const persistence = new FilePersistence(config.dataDir);
const network = new HttpNetwork();
const node = new RaftNodeImpl(config, persistence, network);

// Start HTTP server
const server = new CacheServer(node, { port: 7001 });
await node.start();
await server.start();

// Or use the client API directly
const client = new CacheClient(node);
await client.set('user:1001', { name: 'Alice', role: 'admin' });
const user = await client.get('user:1001');
```

---

## 📊 Monitoring

RaftCache includes a powerful monitoring CLI for real-time cluster observation:

```bash
# Build the project
npm run build

# Start monitoring
node dist/monitoring/cli.js

# With options
node dist/monitoring/cli.js -r 1000 -l          # Fast refresh with logs
node dist/monitoring/cli.js --compact           # Compact view
node dist/monitoring/cli.js --export            # Export as JSON
```

### Monitoring Features

- **Cluster Status** - View leader, term, and node states
- **Performance Metrics** - Track operations, latency, and throughput
- **Log Visualization** - Real-time operation logs with filtering
- **Health Checks** - Monitor node connectivity and replication lag

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Cache Client API                     │
│              (GET, SET, DELETE, Batch Ops)              │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                    Raft Core Layer                       │
│   • Leader Election    • Log Replication                │
│   • Consensus Protocol • Membership Changes             │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                Cache State Machine                       │
│   • Key-Value Store    • TTL Management                 │
│   • LRU Eviction       • Snapshot Support               │
└────────────────────┬────────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         │                       │
┌────────▼─────────┐  ┌─────────▼──────────┐
│  Persistence     │  │  Network Layer     │
│  • File Storage  │  │  • RPC Protocol    │
│  • Snapshots     │  │  • HTTP/In-Memory  │
│  • WAL           │  │  • Fault Injection │
└──────────────────┘  └────────────────────┘
```

---

## 🔧 Configuration

```typescript
interface RaftConfig {
  // Cluster Identity
  nodeId: string;
  peers: string[];
  
  // Timing (milliseconds)
  electionTimeoutMin: number;      // 150-300ms recommended
  electionTimeoutMax: number;
  heartbeatInterval: number;       // 50ms recommended
  rpcTimeout: number;              // 100ms default
  
  // Cache Settings
  maxCacheSize: number;            // Max entries
  ttlCheckInterval: number;        // TTL cleanup frequency
  
  // Snapshots
  snapshotThreshold: number;       // Log entries before snapshot
  snapshotChunkSize: number;       // Chunk size for transfers
  
  // Storage
  dataDir: string;                 // Persistent storage path
}
```

---

## 🧪 Testing

RaftCache includes comprehensive test suites:

```bash
# Run all tests
npm test

# Watch mode
npm run test:watch
```

### Test Coverage

- **Unit Tests** - Core components and state machines
- **Integration Tests** - Multi-node cluster scenarios
- **Chaos Tests** - Network partitions, delays, packet loss
- **Property Tests** - Randomized testing for edge cases
- **Benchmarks** - Performance and scalability testing

---

## 📈 Performance

Benchmarks on a 3-node cluster (MacBook Air M4):

| Operation | Throughput | Latency (p50) | Latency (p99) |
|-----------|------------|---------------|---------------|
| SET       | 15,000/s   | 2.1ms         | 8.5ms         |
| GET       | 45,000/s   | 0.8ms         | 3.2ms         |
| Batch SET | 25,000/s   | 3.5ms         | 12ms          |
| Batch GET | 60,000/s   | 1.2ms         | 4.8ms         |

---

## 🛠️ Development

```bash
# Clone the repository
git clone https://github.com/kyrillosishak/RaftCache.git
cd RaftCache

# Install dependencies
npm install

# Build
npm run build

# Run tests
npm test
```

### Project Structure

```
src/
├── cache/           # Cache state machine with TTL and LRU
├── client/          # Client API and server
├── core/            # Raft consensus implementation
├── logging/         # Structured logging system
├── metrics/         # Performance metrics collection
├── monitoring/      # Real-time monitoring CLI
├── network/         # Network layer (HTTP, in-memory)
├── persistence/     # Durable storage layer
└── integration/     # Integration and chaos tests
```

---

## ⚠️ Production Considerations

If you're considering using RaftCache beyond educational purposes, be aware of these limitations:

**Current Limitations:**
- No authentication or authorization
- Single-threaded Node.js event loop (CPU-bound operations can block)
- In-memory only (data lost on cluster-wide failure)
- No cluster membership changes (dynamic add/remove nodes)
- Basic HTTP transport (no TLS/encryption)
- Limited observability and metrics

**For Production, Consider:**
- **Redis Cluster** - Battle-tested, high-performance, rich features
- **etcd** - Production-grade Raft implementation, used by Kubernetes
- **Consul** - Service mesh with built-in KV store
- **Hazelcast** - Distributed caching with enterprise support

**When RaftCache Might Work:**
- Internal tools with low traffic (< 1000 req/s)
- Development/staging environments
- Proof-of-concept projects

---

## 🤝 Contributing

Contributions are welcome! This is an educational project, so improvements to code clarity, documentation, and learning resources are especially appreciated.

---

## 📄 License

MIT License - see LICENSE file for details

---

## 🙏 Acknowledgments

Built with inspiration from:
- [Raft Consensus Algorithm](https://raft.github.io/) by Diego Ongaro and John Ousterhout
- [Redis](https://redis.io/) for API design patterns
- [etcd](https://etcd.io/) for distributed systems best practices

---

**Made with ❤️ for learning distributed systems.**
