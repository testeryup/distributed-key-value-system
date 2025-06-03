const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const ConsistentHash = require('./consistent-hash');

/**
 * Distributed Key-Value Store Node
 * 
 * This class represents a single node in our distributed system.
 * Each node can:
 * 1. Store key-value pairs locally
 * 2. Communicate with other nodes for replication
 * 3. Handle client requests (GET/PUT/DELETE)
 * 4. Participate in failure detection via heartbeats
 * 5. Recover data when rejoining the cluster
 */
class DistributedNode {
    constructor(nodeId, port, seedNodes = []) {
        // Node identification and network configuration
        this.nodeId = nodeId;
        this.port = port;
        this.address = 'localhost';

        // Local data storage - in production, this would be persistent storage
        this.localStorage = new Map(); // key -> {value, timestamp, version}

        // Cluster management
        this.consistentHash = new ConsistentHash(2); // Replication factor of 2
        this.knownNodes = new Map(); // nodeId -> {address, port, lastSeen, client}
        this.seedNodes = seedNodes; // Initial nodes to connect to

        // Server instances for different services
        this.kvServer = null;    // Client-facing service
        this.nodeServer = null;  // Node-to-node service

        // Failure detection and recovery
        this.heartbeatInterval = null;
        this.heartbeatFrequency = 5000; // 5 seconds
        this.nodeTimeout = 15000; // 15 seconds to consider a node dead

        // Load protocol buffer definitions
        this.loadProtoDefinitions();

        // State management
        this.isShuttingDown = false;

        console.log(`Initializing node ${this.nodeId} on port ${this.port}`);
    }

    /**
     * Load and compile Protocol Buffer definitions
     * This sets up the gRPC service definitions we'll use for communication
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
        console.log(`Protocol buffers loaded for node ${this.nodeId}`);
    }

    /**
     * Start the node - this initializes both gRPC servers and begins cluster operations
     */
    async start() {
        try {
            console.log(`Starting node ${this.nodeId}...`);

            // Start gRPC servers
            await this.startKVServer();
            await this.startNodeServer();

            // Add ourselves to the consistent hash ring
            this.consistentHash.addNode(this.nodeId, {
                node_id: this.nodeId,
                address: this.address,
                port: this.port,
                is_alive: true,
                last_heartbeat: Date.now()
            });

            // Connect to seed nodes to join the cluster
            await this.joinCluster();

            // Start heartbeat mechanism for failure detection
            this.startHeartbeat();

            console.log(`Node ${this.nodeId} successfully started and joined cluster`);

        } catch (error) {
            console.error(`Failed to start node ${this.nodeId}:`, error);
            throw error;
        }
    }

    /**
     * Start the client-facing gRPC server
     * This handles PUT/GET/DELETE operations from clients
     */
    async startKVServer() {
        return new Promise((resolve, reject) => {
            this.kvServer = new grpc.Server();

            // Implement the KVStoreService
            this.kvServer.addService(this.proto.KVStoreService.service, {
                Put: this.handlePut.bind(this),
                Get: this.handleGet.bind(this),
                Delete: this.handleDelete.bind(this)
            });

            const address = `${this.address}:${this.port}`;
            this.kvServer.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
                if (err) {
                    console.error(`Failed to bind KV server for node ${this.nodeId}:`, err);
                    reject(err);
                } else {
                    this.kvServer.start();
                    console.log(`KV Server for node ${this.nodeId} running on ${address}`);
                    resolve();
                }
            });
        });
    }

    /**
     * Start the node-to-node communication gRPC server
     * This handles internal cluster operations like replication and heartbeats
     */
    async startNodeServer() {
        return new Promise((resolve, reject) => {
            this.nodeServer = new grpc.Server();

            // Implement the NodeService
            this.nodeServer.addService(this.proto.NodeService.service, {
                Replicate: this.handleReplicate.bind(this),
                Heartbeat: this.handleHeartbeat.bind(this),
                SyncData: this.handleSyncData.bind(this),
                GetClusterState: this.handleGetClusterState.bind(this)
            });

            const nodePort = this.port + 1000; // Use different port for node communication
            const address = `${this.address}:${nodePort}`;

            this.nodeServer.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
                if (err) {
                    console.error(`Failed to bind Node server for node ${this.nodeId}:`, err);
                    reject(err);
                } else {
                    this.nodeServer.start();
                    console.log(`Node Server for node ${this.nodeId} running on ${address}`);
                    resolve();
                }
            });
        });
    }

    /**
     * Join the cluster by connecting to seed nodes
     * This allows the node to discover other nodes and synchronize state
     */
    async joinCluster() {
        if (this.seedNodes.length === 0) {
            console.log(`Node ${this.nodeId} starting as first node in cluster`);
            return;
        }

        console.log(`Node ${this.nodeId} attempting to join cluster via seed nodes:`, this.seedNodes);

        for (const seedNode of this.seedNodes) {
            try {
                await this.connectToNode(seedNode.nodeId, seedNode.address, seedNode.port + 1000);

                // Request cluster state from seed node
                const client = this.knownNodes.get(seedNode.nodeId).client;
                const response = await this.makeNodeCall(client, 'GetClusterState', {
                    requesting_node_id: this.nodeId
                });

                // Add all known nodes to our cluster view
                for (const nodeInfo of response.nodes) {
                    if (nodeInfo.node_id !== this.nodeId) {
                        await this.connectToNode(nodeInfo.node_id, nodeInfo.address, nodeInfo.port + 1000);
                        this.consistentHash.addNode(nodeInfo.node_id, nodeInfo);
                    }
                }

                console.log(`Node ${this.nodeId} successfully joined cluster with ${response.nodes.length} total nodes`);
                break;

            } catch (error) {
                console.warn(`Failed to connect to seed node ${seedNode.nodeId}:`, error.message);
                continue;
            }
        }
    }

    /**
     * Establish a gRPC connection to another node
     */
    async connectToNode(nodeId, address, port) {
        const nodeAddress = `${address}:${port}`;
        const client = new this.proto.NodeService(nodeAddress, grpc.credentials.createInsecure());

        // Test the connection
        await this.testConnection(client);

        this.knownNodes.set(nodeId, {
            address,
            port: port - 1000, // Store original port (KV service port)
            lastSeen: Date.now(),
            client
        });

        console.log(`Connected to node ${nodeId} at ${nodeAddress}`);
    }

    /**
     * Test if a gRPC connection is working
     */
    async testConnection(client) {
        return new Promise((resolve, reject) => {
            const deadline = new Date();
            deadline.setSeconds(deadline.getSeconds() + 5);

            client.waitForReady(deadline, (error) => {
                if (error) {
                    reject(error);
                } else {
                    resolve();
                }
            });
        });
    }

    /**
     * Make a gRPC call to another node with error handling
     */
    async makeNodeCall(client, method, request) {
        return new Promise((resolve, reject) => {
            client[method](request, (error, response) => {
                if (error) {
                    reject(error);
                } else {
                    resolve(response);
                }
            });
        });
    }

    /**
     * Handle PUT requests from clients
     * This operation needs to:
     * 1. Determine which nodes should store this key
     * 2. Store locally if this node is responsible
     * 3. Replicate to other responsible nodes
     */
    async handlePut(call, callback) {
        const { key, value } = call.request;
        const timestamp = Date.now();

        console.log(`PUT request received: ${key} = ${value}`);

        try {
            // Find all nodes that should store this key (including replicas)
            const replicaNodes = this.consistentHash.getReplicaNodes(key);
            console.log(`Key ${key} should be stored on nodes: ${replicaNodes.join(', ')}`);

            // Store locally if we're one of the replica nodes
            if (replicaNodes.includes(this.nodeId)) {
                this.localStorage.set(key, {
                    value,
                    timestamp,
                    version: `${this.nodeId}-${timestamp}` // Simple version vector
                });
                console.log(`Stored ${key} locally on node ${this.nodeId}`);
            }

            // Replicate to other nodes
            const replicationPromises = replicaNodes
                .filter(nodeId => nodeId !== this.nodeId) // Don't replicate to ourselves
                .map(async (nodeId) => {
                    if (!this.knownNodes.has(nodeId)) {
                        console.warn(`Cannot replicate to unknown node ${nodeId}`);
                        return false;
                    }

                    try {
                        const nodeInfo = this.knownNodes.get(nodeId);
                        const response = await this.makeNodeCall(nodeInfo.client, 'Replicate', {
                            data: { key, value, timestamp, version: `${this.nodeId}-${timestamp}` },
                            operation: 'PUT'
                        });

                        console.log(`Successfully replicated ${key} to node ${nodeId}`);
                        return response.success;
                    } catch (error) {
                        console.error(`Failed to replicate ${key} to node ${nodeId}:`, error.message);
                        return false;
                    }
                });

            const replicationResults = await Promise.all(replicationPromises);
            const successfulReplications = replicationResults.filter(result => result).length;

            // Consider the operation successful if we stored locally or replicated to at least one node
            const isSuccessful = replicaNodes.includes(this.nodeId) || successfulReplications > 0;

            callback(null, {
                success: isSuccessful,
                message: isSuccessful ?
                    `Successfully stored ${key} with ${successfulReplications} replications` :
                    `Failed to store ${key} - no replications successful`,
                timestamp
            });

        } catch (error) {
            console.error(`Error processing PUT ${key}:`, error);
            callback(null, {
                success: false,
                message: `Internal error: ${error.message}`,
                timestamp
            });
        }
    }

    /**
     * Handle GET requests from clients
     * This operation:
     * 1. Checks if the key is stored locally
     * 2. If not found locally, forwards to the responsible node
     * 3. Returns the most recent version if multiple replicas exist
     */
    async handleGet(call, callback) {
        const { key, is_forwarded = false } = call.request;

        console.log(`GET request received: ${key} (forwarded: ${is_forwarded})`);

        try {
            // Check if we have the key locally
            if (this.localStorage.has(key)) {
                const data = this.localStorage.get(key);
                console.log(`Found ${key} locally on node ${this.nodeId}`);

                callback(null, {
                    found: true,
                    value: data.value,
                    message: `Retrieved from node ${this.nodeId}`,
                    timestamp: data.timestamp
                });
                return;
            }

            // Get the replica nodes for this key
            const replicaNodes = this.consistentHash.getReplicaNodes(key);
            console.log(`Key ${key} should be on replica nodes: ${replicaNodes.join(', ')}`);

            // If we're one of the replica nodes and don't have the key, it doesn't exist
            if (replicaNodes.includes(this.nodeId)) {
                console.log(`Key ${key} not found locally and we're a replica node - key doesn't exist`);
                callback(null, {
                    found: false,
                    value: '',
                    message: `Key ${key} not found in cluster`,
                    timestamp: Date.now()
                });
                return;
            }

            // If this is already a forwarded request, don't forward again
            if (is_forwarded) {
                console.log(`Key ${key} not found and request was already forwarded`);
                callback(null, {
                    found: false,
                    value: '',
                    message: `Key ${key} not found on node ${this.nodeId}`,
                    timestamp: Date.now()
                });
                return;
            }

            // If we're not a replica node, forward to the PRIMARY replica node only
            const primaryNode = replicaNodes[0];

            if (!this.knownNodes.has(primaryNode)) {
                console.warn(`Primary node ${primaryNode} for key ${key} is not available`);
                callback(null, {
                    found: false,
                    value: '',
                    message: `Primary node ${primaryNode} for key ${key} is not available`,
                    timestamp: Date.now()
                });
                return;
            }

            try {
                const nodeInfo = this.knownNodes.get(primaryNode);
                const kvAddress = `${nodeInfo.address}:${nodeInfo.port}`;
                const kvClient = new this.proto.KVStoreService(kvAddress, grpc.credentials.createInsecure());

                console.log(`Forwarding GET ${key} to primary replica node ${primaryNode}`);
                const response = await this.makeNodeCall(kvClient, 'Get', {
                    key,
                    is_forwarded: true
                });

                kvClient.close();

                callback(null, {
                    found: response.found,
                    value: response.value || '',
                    message: response.found ?
                        `Retrieved from node ${primaryNode} via ${this.nodeId}` :
                        `Key ${key} not found in cluster`,
                    timestamp: response.timestamp
                });

            } catch (error) {
                console.error(`Failed to forward GET ${key} to primary node ${primaryNode}:`, error.message);
                callback(null, {
                    found: false,
                    value: '',
                    message: `Failed to retrieve ${key}: ${error.message}`,
                    timestamp: Date.now()
                });
            }

        } catch (error) {
            console.error(`Error processing GET ${key}:`, error);
            callback(null, {
                found: false,
                value: '',
                message: `Internal error: ${error.message}`,
                timestamp: Date.now()
            });
        }
    }
    /**
     * Handle DELETE requests from clients
     * Similar to PUT, but removes the key from all replica nodes
     */
    async handleDelete(call, callback) {
        const { key } = call.request;
        const timestamp = Date.now();

        console.log(`DELETE request received: ${key}`);

        try {
            const replicaNodes = this.consistentHash.getReplicaNodes(key);
            console.log(`Deleting ${key} from nodes: ${replicaNodes.join(', ')}`);

            let deletedLocally = false;

            // Delete locally if we're a replica node
            if (replicaNodes.includes(this.nodeId)) {
                deletedLocally = this.localStorage.delete(key);
                console.log(`${deletedLocally ? 'Deleted' : 'Key not found locally'} ${key} on node ${this.nodeId}`);
            }

            // Delete from other replica nodes
            const deletionPromises = replicaNodes
                .filter(nodeId => nodeId !== this.nodeId)
                .map(async (nodeId) => {
                    if (!this.knownNodes.has(nodeId)) {
                        console.warn(`Cannot delete from unknown node ${nodeId}`);
                        return false;
                    }

                    try {
                        const nodeInfo = this.knownNodes.get(nodeId);
                        const response = await this.makeNodeCall(nodeInfo.client, 'Replicate', {
                            data: { key, value: '', timestamp, version: `${this.nodeId}-${timestamp}` },
                            operation: 'DELETE'
                        });

                        console.log(`Successfully deleted ${key} from node ${nodeId}`);
                        return response.success;
                    } catch (error) {
                        console.error(`Failed to delete ${key} from node ${nodeId}:`, error.message);
                        return false;
                    }
                });

            const deletionResults = await Promise.all(deletionPromises);
            const successfulDeletions = deletionResults.filter(result => result).length;

            const isSuccessful = deletedLocally || successfulDeletions > 0;

            callback(null, {
                success: isSuccessful,
                message: isSuccessful ?
                    `Successfully deleted ${key} from ${successfulDeletions + (deletedLocally ? 1 : 0)} nodes` :
                    `Failed to delete ${key} - key may not exist`,
                timestamp: timestamp  // Add this line
            });

        } catch (error) {
            console.error(`Error processing DELETE ${key}:`, error);
            callback(null, {
                success: false,
                message: `Internal error: ${error.message}`
            });
        }
    }

    /**
     * Handle replication requests from other nodes
     * This is called when another node wants to replicate data to us
     */
    async handleReplicate(call, callback) {
        const { data, operation } = call.request;

        console.log(`Replication request: ${operation} ${data.key} from another node`);

        try {
            if (operation === 'PUT') {
                // Check if we already have this key with a newer timestamp
                const existing = this.localStorage.get(data.key);
                if (!existing || existing.timestamp < data.timestamp) {
                    this.localStorage.set(data.key, {
                        value: data.value,
                        timestamp: data.timestamp,
                        version: data.version
                    });
                    console.log(`Replicated PUT: ${data.key} = ${data.value}`);
                } else {
                    console.log(`Ignored older PUT for ${data.key} (existing: ${existing.timestamp}, incoming: ${data.timestamp})`);
                }
            } else if (operation === 'DELETE') {
                // For deletes, we always apply if the timestamp is newer
                const existing = this.localStorage.get(data.key);
                if (!existing || existing.timestamp < data.timestamp) {
                    this.localStorage.delete(data.key);
                    console.log(`Replicated DELETE: ${data.key}`);
                } else {
                    console.log(`Ignored older DELETE for ${data.key}`);
                }
            }

            callback(null, {
                success: true,
                message: `Successfully replicated ${operation} for ${data.key}`
            });

        } catch (error) {
            console.error(`Error handling replication:`, error);
            callback(null, {
                success: false,
                message: `Replication failed: ${error.message}`
            });
        }
    }

    /**
     * Handle heartbeat requests from other nodes
     * This is used for failure detection and cluster membership management
     */
    async handleHeartbeat(call, callback) {
        const { sender, known_nodes } = call.request;

        console.log(`Heartbeat received from node ${sender.node_id}`);

        try {
            // Update our knowledge of the sender
            if (!this.knownNodes.has(sender.node_id)) {
                await this.connectToNode(sender.node_id, sender.address, sender.port + 1000);
                this.consistentHash.addNode(sender.node_id, sender);
                console.log(`Discovered new node ${sender.node_id} via heartbeat`);
            } else {
                const nodeInfo = this.knownNodes.get(sender.node_id);
                nodeInfo.lastSeen = Date.now();
            }

            // Learn about other nodes from the sender
            for (const nodeInfo of known_nodes) {
                if (nodeInfo.node_id !== this.nodeId && !this.knownNodes.has(nodeInfo.node_id)) {
                    try {
                        await this.connectToNode(nodeInfo.node_id, nodeInfo.address, nodeInfo.port + 1000);
                        this.consistentHash.addNode(nodeInfo.node_id, nodeInfo);
                        console.log(`Learned about node ${nodeInfo.node_id} from ${sender.node_id}`);
                    } catch (error) {
                        console.warn(`Failed to connect to node ${nodeInfo.node_id} learned from heartbeat:`, error.message);
                    }
                }
            }

            // Respond with our information and known nodes
            const ourKnownNodes = Array.from(this.knownNodes.entries()).map(([nodeId, info]) => ({
                node_id: nodeId,
                address: info.address,
                port: info.port,
                is_alive: true,
                last_heartbeat: info.lastSeen
            }));

            callback(null, {
                responder: {
                    node_id: this.nodeId,
                    address: this.address,
                    port: this.port,
                    is_alive: true,
                    last_heartbeat: Date.now()
                },
                known_nodes: ourKnownNodes
            });

        } catch (error) {
            console.error(`Error handling heartbeat from ${sender.node_id}:`, error);
            callback(null, {
                responder: {
                    node_id: this.nodeId,
                    address: this.address,
                    port: this.port,
                    is_alive: true,
                    last_heartbeat: Date.now()
                },
                known_nodes: []
            });
        }
    }

    /**
     * Handle data synchronization requests
     * This is called when a node needs to recover data after rejoining the cluster
     */
    async handleSyncData(call, callback) {
        const { requesting_node_id, keys_to_sync } = call.request;

        console.log(`Data sync request from node ${requesting_node_id} for ${keys_to_sync.length} keys`);

        try {
            const syncData = [];

            if (keys_to_sync.length === 0) {
                // If no specific keys requested, send all our data
                for (const [key, data] of this.localStorage.entries()) {
                    syncData.push({
                        key,
                        value: data.value,
                        timestamp: data.timestamp,
                        version: data.version
                    });
                }
            } else {
                // Send only requested keys
                for (const key of keys_to_sync) {
                    if (this.localStorage.has(key)) {
                        const data = this.localStorage.get(key);
                        syncData.push({
                            key,
                            value: data.value,
                            timestamp: data.timestamp,
                            version: data.version
                        });
                    }
                }
            }

            console.log(`Sending ${syncData.length} key-value pairs to node ${requesting_node_id}`);

            callback(null, { data: syncData });

        } catch (error) {
            console.error(`Error handling sync request from ${requesting_node_id}:`, error);
            callback(null, { data: [] });
        }
    }

    /**
     * Handle cluster state requests
     * This provides information about all known nodes in the cluster
     */
    async handleGetClusterState(call, callback) {
        const { requesting_node_id } = call.request;

        console.log(`Cluster state request from node ${requesting_node_id}`);

        try {
            const clusterNodes = [{
                node_id: this.nodeId,
                address: this.address,
                port: this.port,
                is_alive: true,
                last_heartbeat: Date.now()
            }];

            // Add all known nodes
            for (const [nodeId, info] of this.knownNodes.entries()) {
                clusterNodes.push({
                    node_id: nodeId,
                    address: info.address,
                    port: info.port,
                    is_alive: true,
                    last_heartbeat: info.lastSeen
                });
            }

            callback(null, {
                nodes: clusterNodes,
                hash_ring: {} // Could include hash ring information if needed
            });

        } catch (error) {
            console.error(`Error handling cluster state request:`, error);
            callback(null, {
                nodes: [],
                hash_ring: {}
            });
        }
    }

    /**
     * Start the heartbeat mechanism for failure detection
     * Sends periodic heartbeats to all known nodes
     */
    startHeartbeat() {
        console.log(`Starting heartbeat mechanism for node ${this.nodeId}`);

        this.heartbeatInterval = setInterval(async () => {
            if (this.isShuttingDown) return;

            const currentTime = Date.now();
            const deadNodes = [];

            // Check for dead nodes first
            for (const [nodeId, info] of this.knownNodes.entries()) {
                if (currentTime - info.lastSeen > this.nodeTimeout) {
                    console.warn(`Node ${nodeId} appears to be dead (last seen ${currentTime - info.lastSeen}ms ago)`);
                    deadNodes.push(nodeId);
                }
            }

            // Remove dead nodes from cluster
            for (const nodeId of deadNodes) {
                this.removeDeadNode(nodeId);
            }

            // Send heartbeats to alive nodes
            const heartbeatPromises = Array.from(this.knownNodes.entries()).map(async ([nodeId, info]) => {
                try {
                    const knownNodesInfo = Array.from(this.knownNodes.entries()).map(([id, nodeInfo]) => ({
                        node_id: id,
                        address: nodeInfo.address,
                        port: nodeInfo.port,
                        is_alive: true,
                        last_heartbeat: nodeInfo.lastSeen
                    }));

                    const response = await this.makeNodeCall(info.client, 'Heartbeat', {
                        sender: {
                            node_id: this.nodeId,
                            address: this.address,
                            port: this.port,
                            is_alive: true,
                            last_heartbeat: currentTime
                        },
                        known_nodes: knownNodesInfo
                    });

                    // Update last seen time
                    info.lastSeen = currentTime;

                    console.log(`Heartbeat successful with node ${nodeId}`);

                } catch (error) {
                    console.warn(`Heartbeat failed with node ${nodeId}:`, error.message);
                    // Mark as potentially dead, will be removed in next cycle if still unreachable
                }
            });

            await Promise.allSettled(heartbeatPromises);

        }, this.heartbeatFrequency);
    }

    /**
     * Remove a dead node from the cluster
     * This includes removing it from the consistent hash ring and cleaning up connections
     */
    removeDeadNode(nodeId) {
        console.log(`Removing dead node ${nodeId} from cluster`);

        // Remove from consistent hash ring
        this.consistentHash.removeNode(nodeId);

        // Close gRPC connection and remove from known nodes
        const nodeInfo = this.knownNodes.get(nodeId);
        if (nodeInfo && nodeInfo.client) {
            nodeInfo.client.close();
        }
        this.knownNodes.delete(nodeId);

        console.log(`Node ${nodeId} removed from cluster`);
    }

    /**
     * Synchronize data after rejoining the cluster
     * This is called when a node starts up and needs to recover its data
     */
    async synchronizeData() {
        console.log(`Node ${this.nodeId} synchronizing data with cluster`);

        if (this.knownNodes.size === 0) {
            console.log('No other nodes available for synchronization');
            return;
        }

        // Request data from each known node
        for (const [nodeId, nodeInfo] of this.knownNodes.entries()) {
            try {
                console.log(`Requesting data sync from node ${nodeId}`);

                const response = await this.makeNodeCall(nodeInfo.client, 'SyncData', {
                    requesting_node_id: this.nodeId,
                    keys_to_sync: [] // Request all data
                });

                let syncedCount = 0;
                for (const keyValue of response.data) {
                    // Only sync if we don't have this key or if the incoming version is newer
                    const existing = this.localStorage.get(keyValue.key);
                    if (!existing || existing.timestamp < keyValue.timestamp) {
                        this.localStorage.set(keyValue.key, {
                            value: keyValue.value,
                            timestamp: keyValue.timestamp,
                            version: keyValue.version
                        });
                        syncedCount++;
                    }
                }

                console.log(`Synchronized ${syncedCount} keys from node ${nodeId}`);

            } catch (error) {
                console.warn(`Failed to sync data from node ${nodeId}:`, error.message);
            }
        }

        console.log(`Data synchronization completed. Local storage has ${this.localStorage.size} keys`);
    }

    /**
     * Get statistics about this node
     */
    getStats() {
        return {
            nodeId: this.nodeId,
            port: this.port,
            localKeys: this.localStorage.size,
            knownNodes: this.knownNodes.size,
            consistentHashStats: this.consistentHash.getStats()
        };
    }

    /**
     * Gracefully shutdown the node
     */
    async shutdown() {
        console.log(`Shutting down node ${this.nodeId}...`);

        this.isShuttingDown = true;

        // Stop heartbeat
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
        }

        // Close gRPC servers
        if (this.kvServer) {
            await new Promise((resolve) => {
                this.kvServer.tryShutdown(() => {
                    console.log(`KV Server for node ${this.nodeId} shut down`);
                    resolve();
                });
            });
        }

        if (this.nodeServer) {
            await new Promise((resolve) => {
                this.nodeServer.tryShutdown(() => {
                    console.log(`Node Server for node ${this.nodeId} shut down`);
                    resolve();
                });
            });
        }

        // Close connections to other nodes
        for (const [nodeId, nodeInfo] of this.knownNodes.entries()) {
            if (nodeInfo.client) {
                nodeInfo.client.close();
            }
        }

        console.log(`Node ${this.nodeId} shutdown complete`);
    }
}

module.exports = DistributedNode;