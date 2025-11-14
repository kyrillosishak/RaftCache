#!/usr/bin/env node
/**
 * CLI script for monitoring Raft cluster
 * Usage: node cli.js [options]
 */

import { MonitoringCLI, MonitoringOptions } from './MonitoringCLI';
import { LogLevel } from '../logging/Logger';

// Parse command line arguments
function parseArgs(): MonitoringOptions & { help?: boolean; export?: boolean } {
  const args = process.argv.slice(2);
  const options: any = {
    refreshInterval: 2000,
    showLogs: false,
    compact: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    
    switch (arg) {
      case '-h':
      case '--help':
        options.help = true;
        break;
      
      case '-r':
      case '--refresh':
        options.refreshInterval = parseInt(args[++i], 10);
        break;
      
      case '-l':
      case '--logs':
        options.showLogs = true;
        break;
      
      case '-c':
      case '--compact':
        options.compact = true;
        break;
      
      case '--log-level':
        const level = args[++i].toUpperCase();
        options.logLevel = LogLevel[level as keyof typeof LogLevel];
        break;
      
      case '--log-category':
        options.logCategory = args[++i];
        break;
      
      case '-e':
      case '--export':
        options.export = true;
        break;
    }
  }

  return options;
}

// Display help message
function displayHelp(): void {
  console.log(`
Raft Cluster Monitoring CLI

Usage: node cli.js [options]

Options:
  -h, --help              Show this help message
  -r, --refresh <ms>      Refresh interval in milliseconds (default: 2000)
  -l, --logs              Show recent operation logs
  -c, --compact           Use compact display mode (one line per node)
  --log-level <level>     Filter logs by level (DEBUG, INFO, WARN, ERROR)
  --log-category <cat>    Filter logs by category
  -e, --export            Export status as JSON and exit

Examples:
  node cli.js                           # Basic monitoring
  node cli.js -r 1000 -l                # Fast refresh with logs
  node cli.js -c                        # Compact mode
  node cli.js -l --log-level INFO       # Show INFO level logs
  node cli.js --export                  # Export status as JSON

Note: This CLI tool requires nodes to be registered programmatically.
      See the MonitoringCLI class documentation for integration details.
`);
}

// Main function
function main(): void {
  const options = parseArgs();

  if (options.help) {
    displayHelp();
    process.exit(0);
  }

  const monitor = new MonitoringCLI();

  // Note: In a real deployment, nodes would be registered here
  // For example:
  // monitor.registerNode(node1);
  // monitor.registerNode(node2);
  // monitor.registerNode(node3);

  if (options.export) {
    // Export status as JSON
    console.log(monitor.exportStatusJSON());
    process.exit(0);
  }

  // Start monitoring
  console.log('Starting Raft cluster monitoring...');
  console.log('Press Ctrl+C to exit');
  console.log();

  monitor.startMonitoring(options);

  // Handle graceful shutdown
  process.on('SIGINT', () => {
    console.log('\nStopping monitoring...');
    monitor.stopMonitoring();
    process.exit(0);
  });
}

// Run if executed directly
if (require.main === module) {
  main();
}

export { main };
