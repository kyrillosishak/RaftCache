#!/usr/bin/env node
/**
 * RaftCache Server - Standalone cache server
 * Usage: node server.js --node-id node1 --port 7001 --peers node2:7002,node3:7003
 */

import { RaftNodeImpl } from './core/RaftNode';
import { CacheServer } from './client/CacheServer';
import { FilePersistence } from './persistence/FilePersistence';
import { HttpNetwork } from './network/HttpNetwork';
import { CacheStateMachineImpl } from './cache/CacheStateMachine';
import { RaftConfig } from './config';
import * as path from 'path';
import * as fs from 'fs';

interface ServerArgs {
  nodeId: string;
  port: number;
  raftPort?: number;
  peers: string[];
  dataDir?: string;
  electionTimeoutMin?: number;
  electionTimeoutMax?: number;
  heartbeatInterval?: number;
  maxCacheSize?: number;
  help?: boolean;
}

function parseArgs(): ServerArgs {
  const args = process.argv.slice(2);
  const parsed: any = {
    port: 7001,
    peers: [],
    electionTimeoutMin: 150,
    electionTimeoutMax: 300,
    heartbeatInterval: 50,
    maxCacheSize: 10000,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    switch (arg) {
      case '-h':
      case '--help':
        parsed.help = true;
        break;

      case '--node-id':
        parsed.nodeId = args[++i];
        break;

      case '--port':
      case '-p':
        parsed.port = parseInt(args[++i], 10);
        break;

      case '--raft-port':
        parsed.raftPort = parseInt(args[++i], 10);
        break;

      case '--peers':
        parsed.peers = args[++i].split(',').map((p: string) => p.trim());
        break;

      case '--data-dir':
        parsed.dataDir = args[++i];
        break;

      case '--election-timeout-min':
        parsed.electionTimeoutMin = parseInt(args[++i], 10);
        break;

      case '--election-timeout-max':
        parsed.electionTimeoutMax = parseInt(args[++i], 10);
        break;

      case '--heartbeat-interval':
        parsed.heartbeatInterval = parseInt(args[++i], 10);
        break;

      case '--max-cache-size':
        parsed.maxCacheSize = parseInt(args[++i], 10);
        break;
    }
  }

  return parsed;
}

function displayHelp(): void {
  console.log(`
RaftCache Server - Distributed In-Memory Cache

Usage: node server.js [options]

Required Options:
  --node-id <id>              Unique identifier for this node
  --port, -p <port>           Client API port (default: 7001)
  --peers <peer1,peer2,...>   Comma-separated list of peer node IDs

Optional:
  --raft-port <port>          Raft RPC port (default: port + 1000)
  --data-dir <path>           Directory for persistent storage (default: ./data/<node-id>)
  --election-timeout-min <ms> Minimum election timeout (default: 150)
  --election-timeout-max <ms> Maximum election timeout (default: 300)
  --heartbeat-interval <ms>   Heartbeat interval (default: 50)
  --max-cache-size <n>        Maximum cache entries (default: 10000)
  -h, --help                  Show this help message

Examples:
  # Start a 3-node cluster (auto-detects ports from node IDs)
  node server.js --node-id node1 --port 7001 --peers node2,node3
  node server.js --node-id node2 --port 7002 --peers node1,node3
  node server.js --node-id node3 --port 7003 --peers node1,node2

  # Explicit Raft ports
  node server.js --node-id node1 --port 7001 --raft-port 8001 --peers node2:8002,node3:8003

  # With custom settings
  node server.js --node-id node1 --port 7001 --peers node2,node3 \\
    --data-dir /var/lib/raftcache \\
    --max-cache-size 50000

API Endpoints:
  POST /cache/set         Set a key-value pair
  POST /cache/get         Get a value by key
  POST /cache/delete      Delete a key
  POST /cache/batch-set   Set multiple key-value pairs
  POST /cache/batch-get   Get multiple values
  POST /raft/status       Get cluster status
`);
}

async function main(): Promise<void> {
  const args = parseArgs();

  if (args.help) {
    displayHelp();
    process.exit(0);
  }

  if (!args.nodeId) {
    console.error('Error: --node-id is required');
    console.error('Run with --help for usage information');
    process.exit(1);
  }

  if (!args.peers || args.peers.length === 0) {
    console.error('Error: --peers is required');
    console.error('Run with --help for usage information');
    process.exit(1);
  }

  // Set default data directory
  const dataDir = args.dataDir || path.join(process.cwd(), 'data', args.nodeId);

  // Ensure data directory exists
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  // Build Raft configuration
  const config: RaftConfig = {
    nodeId: args.nodeId,
    peers: args.peers,
    electionTimeoutMin: args.electionTimeoutMin!,
    electionTimeoutMax: args.electionTimeoutMax!,
    heartbeatInterval: args.heartbeatInterval!,
    rpcTimeout: 100,
    maxMemory: args.maxCacheSize! * 1024, // Convert to bytes (rough estimate)
    ttlExpirationInterval: 1000,
    lruEvictionEnabled: true,
    snapshotThreshold: 1000,
    snapshotChunkSize: 1024 * 1024,
    dataDir,
  };

  // Calculate Raft port (default: client port + 1000)
  const raftPort = args.raftPort || args.port + 1000;

  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║                      RaftCache Server                      ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  console.log();
  console.log(`Node ID:           ${config.nodeId}`);
  console.log(`Client API Port:   ${args.port}`);
  console.log(`Raft RPC Port:     ${raftPort}`);
  console.log(`Peers:             ${config.peers.join(', ')}`);
  console.log(`Data Directory:    ${config.dataDir}`);
  console.log(`Max Memory:        ${Math.floor(config.maxMemory / 1024)} KB`);
  console.log(`Election Timeout:  ${config.electionTimeoutMin}-${config.electionTimeoutMax}ms`);
  console.log(`Heartbeat:         ${config.heartbeatInterval}ms`);
  console.log();

  try {
    // Build node addresses map for HTTP network (Raft RPC)
    const nodeAddresses = new Map<string, string>();
    
    // Register peer addresses
    // Peers can be specified as "nodeId" or "nodeId:raftPort"
    for (const peerId of config.peers) {
      const parts = peerId.split(':');
      const peerNodeId = parts[0];
      
      if (parts.length > 1) {
        // Explicit Raft port provided
        const peerRaftPort = parseInt(parts[1], 10);
        nodeAddresses.set(peerNodeId, `http://localhost:${peerRaftPort}`);
      } else {
        // Infer Raft port from node ID pattern
        // If node ID ends with a number (e.g., node1, node2), use that to calculate port
        const match = peerNodeId.match(/(\d+)$/);
        if (match) {
          const nodeNum = parseInt(match[1], 10);
          const peerClientPort = 7000 + nodeNum;
          const peerRaftPort = peerClientPort + 1000;
          nodeAddresses.set(peerNodeId, `http://localhost:${peerRaftPort}`);
        } else {
          // Fallback: use same offset as current node
          nodeAddresses.set(peerNodeId, `http://localhost:${raftPort}`);
        }
      }
    }

    // Initialize components
    const persistence = new FilePersistence(config.dataDir);
    const network = new HttpNetwork(config.nodeId, {
      port: raftPort,
      nodeAddresses,
    });
    const stateMachine = new CacheStateMachineImpl(config.maxMemory, config.ttlExpirationInterval);
    const node = new RaftNodeImpl(config, persistence, network, stateMachine);

    // Start the Raft node (starts Raft RPC server)
    console.log('Starting Raft node...');
    await node.start();
    console.log(`✓ Raft RPC server listening on port ${raftPort}`);

    // Start Client API server
    const server = new CacheServer(node, { port: args.port });
    console.log('Starting Client API server...');
    await server.start();
    console.log(`✓ Client API server listening on http://localhost:${args.port}`);
    console.log();
    console.log('Server is ready to accept connections');
    console.log('Press Ctrl+C to shutdown');
    console.log();

    // Handle graceful shutdown
    const shutdown = async () => {
      console.log();
      console.log('Shutting down gracefully...');
      
      try {
        await server.stop();
        console.log('✓ HTTP server stopped');
        
        await node.stop();
        console.log('✓ Raft node stopped');
        
        console.log('Goodbye!');
        process.exit(0);
      } catch (error) {
        console.error('Error during shutdown:', error);
        process.exit(1);
      }
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

// Run if executed directly
if (require.main === module) {
  main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

export { main };
