const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const readline = require('readline');

/**
 * Client for the Distributed Key-Value Store
 * 
 * This client can connect to any node in the cluster and perform
 * GET, PUT, and DELETE operations. The client includes:
 * 1. Interactive CLI interface
 * 2. Load balancing across multiple nodes
 * 3. Automatic failover if a node is unreachable
 * 4. Batch operations for performance testing
 */
class KVStoreClient {
    constructor(nodeAddresses = ['localhost:50051']) {
        this.nodeAddresses = nodeAddresses;
        this.clients = new Map(); // address -> grpc client
        this.currentNodeIndex = 0;

        // Load protocol buffer definitions
        this.loadProtoDefinitions();

        // Initialize connections to all provided nodes
        this.initializeConnections();

        console.log(`KV Store Client initialized with nodes: ${nodeAddresses.join(', ')}`);
    }

    /**
     * Load Protocol Buffer definitions
     */
    loadProtoDefinitions() {
        const PROTO_PATH = path.join(__dirname, 'kvstore.proto');
        const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true
        });

        this.proto = grpc.loadPackageDefinition(packageDefinition).kvstore;
    }

    /**
     * Initialize gRPC connections to all nodes
     */
    initializeConnections() {
        for (const address of this.nodeAddresses) {
            try {
                const client = new this.proto.KVStoreService(address, grpc.credentials.createInsecure());
                this.clients.set(address, client);
                console.log(`Connected to node at ${address}`);
            } catch (error) {
                console.error(`Failed to connect to node at ${address}:`, error.message);
            }
        }

        if (this.clients.size === 0) {
            throw new Error('Failed to connect to any nodes');
        }
    }

    /**
     * Get the next available client (round-robin load balancing)
     */
    getNextClient() {
        const addresses = Array.from(this.clients.keys());
        if (addresses.length === 0) {
            throw new Error('No available nodes to connect to');
        }

        const address = addresses[this.currentNodeIndex % addresses.length];
        this.currentNodeIndex = (this.currentNodeIndex + 1) % addresses.length;

        return {
            client: this.clients.get(address),
            address
        };
    }

    /**
     * Make a request with automatic failover
     */
    async makeRequest(operation, request) {
        const maxRetries = this.clients.size;
        let lastError = null;

        for (let attempt = 0; attempt < maxRetries; attempt++) {
            const { client, address } = this.getNextClient();

            try {
                const response = await new Promise((resolve, reject) => {
                    client[operation](request, (error, response) => {
                        if (error) {
                            reject(error);
                        } else {
                            resolve(response);
                        }
                    });
                });

                return { response, address };

            } catch (error) {
                console.warn(`Request to ${address} failed: ${error.message}`);
                lastError = error;

                // If this was a connection error, remove the client temporarily
                if (error.code === grpc.status.UNAVAILABLE) {
                    console.warn(`Node ${address} appears to be down, trying next node`);
                }
            }
        }

        throw new Error(`All nodes failed. Last error: ${lastError.message}`);
    }

    /**
     * PUT operation - store a key-value pair
     */
    async put(key, value) {
        try {
            console.log(`\n🔄 PUT ${key} = ${value}`);
            const start = Date.now();

            const { response, address } = await this.makeRequest('Put', { key, value });
            const duration = Date.now() - start;

            if (response.success) {
                console.log(`✅ Successfully stored ${key} via node ${address} (${duration}ms)`);
                console.log(`   Message: ${response.message}`);
                console.log(`   Timestamp: ${new Date(parseInt(response.timestamp)).toISOString()}`);
            } else {
                console.log(`❌ Failed to store ${key}: ${response.message}`);
            }

            return response;

        } catch (error) {
            console.error(`❌ PUT operation failed: ${error.message}`);
            throw error;
        }
    }

    /**
     * GET operation - retrieve a value by key
     */
    async get(key) {
        try {
            console.log(`\n🔍 GET ${key}`);
            const start = Date.now();

            const { response, address } = await this.makeRequest('Get', { key });
            const duration = Date.now() - start;

            if (response.found) {
                console.log(`✅ Found ${key} = "${response.value}" via node ${address} (${duration}ms)`);
                console.log(`   Message: ${response.message}`);
                console.log(`   Timestamp: ${new Date(parseInt(response.timestamp)).toISOString()}`);
            } else {
                console.log(`❌ Key ${key} not found: ${response.message} (${duration}ms)`);
            }

            return response;

        } catch (error) {
            console.error(`❌ GET operation failed: ${error.message}`);
            throw error;
        }
    }

    /**
     * DELETE operation - remove a key
     */
    async delete(key) {
        try {
            console.log(`\n🗑️  DELETE ${key}`);
            const start = Date.now();

            const { response, address } = await this.makeRequest('Delete', { key });
            const duration = Date.now() - start;

            if (response.success) {
                console.log(`✅ Successfully deleted ${key} via node ${address} (${duration}ms)`);
                console.log(`   Message: ${response.message}`);
            } else {
                console.log(`❌ Failed to delete ${key}: ${response.message}`);
            }

            return response;

        } catch (error) {
            console.error(`❌ DELETE operation failed: ${error.message}`);
            throw error;
        }
    }

    /**
     * Batch PUT operations for performance testing
     */
    async batchPut(keyValuePairs, concurrent = 5) {
        console.log(`\n📦 Batch PUT: ${keyValuePairs.length} operations with concurrency ${concurrent}`);
        const start = Date.now();

        const results = [];
        const chunks = [];

        // Split into chunks for concurrent processing
        for (let i = 0; i < keyValuePairs.length; i += concurrent) {
            chunks.push(keyValuePairs.slice(i, i + concurrent));
        }

        for (const chunk of chunks) {
            const chunkPromises = chunk.map(({ key, value }) =>
                this.put(key, value).catch(error => ({ error, key, value }))
            );

            const chunkResults = await Promise.all(chunkPromises);
            results.push(...chunkResults);
        }

        const duration = Date.now() - start;
        const successCount = results.filter(r => !r.error && r.success).length;

        console.log(`📊 Batch PUT completed: ${successCount}/${keyValuePairs.length} successful (${duration}ms total)`);
        console.log(`   Average: ${(duration / keyValuePairs.length).toFixed(2)}ms per operation`);

        return results;
    }

    /**
     * Batch GET operations for performance testing
     */
    async batchGet(keys, concurrent = 5) {
        console.log(`\n📦 Batch GET: ${keys.length} operations with concurrency ${concurrent}`);
        const start = Date.now();

        const results = [];
        const chunks = [];

        // Split into chunks for concurrent processing
        for (let i = 0; i < keys.length; i += concurrent) {
            chunks.push(keys.slice(i, i + concurrent));
        }

        for (const chunk of chunks) {
            const chunkPromises = chunk.map(key =>
                this.get(key).catch(error => ({ error, key }))
            );

            const chunkResults = await Promise.all(chunkPromises);
            results.push(...chunkResults);
        }

        const duration = Date.now() - start;
        const foundCount = results.filter(r => !r.error && r.found).length;

        console.log(`📊 Batch GET completed: ${foundCount}/${keys.length} found (${duration}ms total)`);
        console.log(`   Average: ${(duration / keys.length).toFixed(2)}ms per operation`);

        return results;
    }

    /**
     * Performance test with random data
     */
    async performanceTest(numOperations = 100) {
        console.log(`\n🚀 Performance Test: ${numOperations} operations`);

        // Generate test data
        const testData = [];
        for (let i = 0; i < numOperations; i++) {
            testData.push({
                key: `test_key_${i}`,
                value: `test_value_${i}_${Math.random().toString(36).substring(7)}`
            });
        }

        // Test PUT operations
        console.log('\n=== PUT Performance ===');
        const putStart = Date.now();
        await this.batchPut(testData, 10);
        const putDuration = Date.now() - putStart;

        // Test GET operations
        console.log('\n=== GET Performance ===');
        const keys = testData.map(item => item.key);
        const getStart = Date.now();
        await this.batchGet(keys, 10);
        const getDuration = Date.now() - getStart;

        // Summary
        console.log('\n📈 Performance Test Summary:');
        console.log(`   PUT: ${numOperations} ops in ${putDuration}ms (${(putDuration / numOperations).toFixed(2)}ms avg)`);
        console.log(`   GET: ${numOperations} ops in ${getDuration}ms (${(getDuration / numOperations).toFixed(2)}ms avg)`);
        console.log(`   Total: ${(putDuration + getDuration)}ms`);
    }

    /**
     * Interactive CLI interface
     */
    async startInteractiveCLI() {
        const rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });

        console.log('\n🖥️  KV Store Interactive Client');
        console.log('Commands:');
        console.log('  put <key> <value>  - Store a key-value pair');
        console.log('  get <key>          - Retrieve a value');
        console.log('  delete <key>       - Delete a key');
        console.log('  test <num>         - Run performance test');
        console.log('  nodes              - Show connected nodes');
        console.log('  help               - Show this help');
        console.log('  exit               - Exit the client\n');

        const askQuestion = () => {
            rl.question('kvstore> ', async (input) => {
                const parts = input.trim().split(' ');
                const command = parts[0].toLowerCase();

                try {
                    switch (command) {
                        case 'put':
                            if (parts.length < 3) {
                                console.log('Usage: put <key> <value>');
                            } else {
                                const key = parts[1];
                                const value = parts.slice(2).join(' ');
                                await this.put(key, value);
                            }
                            break;

                        case 'get':
                            if (parts.length < 2) {
                                console.log('Usage: get <key>');
                            } else {
                                await this.get(parts[1]);
                            }
                            break;

                        case 'delete':
                        case 'del':
                            if (parts.length < 2) {
                                console.log('Usage: delete <key>');
                            } else {
                                await this.delete(parts[1]);
                            }
                            break;

                        case 'test':
                            const numOps = parts.length > 1 ? parseInt(parts[1]) : 50;
                            if (isNaN(numOps)) {
                                console.log('Usage: test <number_of_operations>');
                            } else {
                                await this.performanceTest(numOps);
                            }
                            break;

                        case 'nodes':
                            console.log('\n🌐 Connected Nodes:');
                            for (const address of this.clients.keys()) {
                                console.log(`   - ${address}`);
                            }
                            break;

                        case 'help':
                            console.log('\nCommands:');
                            console.log('  put <key> <value>  - Store a key-value pair');
                            console.log('  get <key>          - Retrieve a value');
                            console.log('  delete <key>       - Delete a key');
                            console.log('  test <num>         - Run performance test');
                            console.log('  nodes              - Show connected nodes');
                            console.log('  help               - Show this help');
                            console.log('  exit               - Exit the client');
                            break;

                        case 'exit':
                        case 'quit':
                            console.log('👋 Goodbye!');
                            rl.close();
                            this.close();
                            return;

                        case '':
                            // Empty command, just continue
                            break;

                        default:
                            console.log(`Unknown command: ${command}. Type 'help' for available commands.`);
                    }
                } catch (error) {
                    console.error(`Error executing command: ${error.message}`);
                }

                askQuestion();
            });
        };

        askQuestion();
    }

    /**
     * Close all client connections
     */
    close() {
        console.log('Closing client connections...');
        for (const client of this.clients.values()) {
            client.close();
        }
        this.clients.clear();
    }
}

// Main execution

async function main() {
    // Parse command line arguments - FIXED VERSION
    const args = process.argv.slice(2);
    let nodeAddresses = ['localhost:50051', 'localhost:50052', 'localhost:50053'];

    // Only override nodeAddresses if explicit addresses are provided
    // Check if args contain actual addresses (contain ':' for port)
    const explicitAddresses = args.filter(arg =>
        arg.includes(':') &&
        !arg.startsWith('--') &&
        !isNaN(parseInt(arg.split(':')[1]))
    );

    if (explicitAddresses.length > 0) {
        nodeAddresses = explicitAddresses;
    }

    try {
        const client = new KVStoreClient(nodeAddresses);

        // Check if running in interactive mode or demo mode
        if (process.argv.includes('--demo')) {
            await runDemo(client);
        } else if (process.argv.includes('--test')) {
            const testIndex = process.argv.indexOf('--test');
            const numOps = parseInt(process.argv[testIndex + 1]) || 100;
            await client.performanceTest(numOps);
        } else {
            await client.startInteractiveCLI();
        }

    } catch (error) {
        console.error('Failed to start client:', error.message);
        process.exit(1);
    }
}

/**
 * Run a demonstration of the distributed key-value store
 */
async function runDemo(client) {
    console.log('\n🎬 Running Distributed KV Store Demo\n');

    try {
        // Demo 1: Basic operations
        console.log('=== Demo 1: Basic Operations ===');
        await client.put('user:1', 'Alice');
        await client.put('user:2', 'Bob');
        await client.put('config:timeout', '30');

        await client.get('user:1');
        await client.get('user:2');
        await client.get('config:timeout');
        await client.get('nonexistent');

        await client.delete('user:2');
        await client.get('user:2');

        // Demo 2: Data distribution
        console.log('\n=== Demo 2: Data Distribution Test ===');
        const distributionTest = [];
        for (let i = 0; i < 20; i++) {
            distributionTest.push({
                key: `item:${i}`,
                value: `data_${i}`
            });
        }

        await client.batchPut(distributionTest, 5);

        // Demo 3: Performance test  
        console.log('\n=== Demo 3: Performance Test ===');
        await client.performanceTest(50);

        console.log('\n🎉 Demo completed successfully!');

    } catch (error) {
        console.error('Demo failed:', error.message);
        throw error;
    } finally {
        // Clean up the client
        client.close();
    }
}

// Execute main function if this file is run directly
if (require.main === module) {
    main().catch(error => {
        console.error('Application failed:', error.message);
        process.exit(1);
    });
}

// Export the client class for use in other modules
module.exports = { KVStoreClient, runDemo };