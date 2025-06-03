const crypto = require('crypto');

/**
 * Consistent Hashing implementation for distributed key-value store
 * This ensures even distribution of data across nodes and minimal reshuffling
 * when nodes are added or removed from the cluster
 */
class ConsistentHash {
    constructor(replicationFactor = 2) {
        this.ring = new Map(); // Hash ring: hash -> node_id
        this.nodes = new Map(); // Active nodes: node_id -> NodeInfo
        this.virtualNodes = 150; // Number of virtual nodes per physical node
        this.replicationFactor = replicationFactor; // Number of replicas per key
    }

    /**
     * Generate a consistent hash for a given input
     * Uses SHA-256 to ensure uniform distribution
     */
    hash(input) {
        return crypto.createHash('sha256').update(input).digest('hex');
    }

    /**
     * Convert hex hash to a number for ring positioning
     * Takes first 8 characters of hash for 32-bit positioning
     */
    hashToNumber(hash) {
        return parseInt(hash.substring(0, 8), 16);
    }

    /**
     * Add a node to the consistent hash ring
     * Creates virtual nodes to improve load distribution
     */
    addNode(nodeId, nodeInfo) {
        console.log(`Adding node ${nodeId} to hash ring`);

        // Store node information
        this.nodes.set(nodeId, nodeInfo);

        // Add virtual nodes to the ring
        for (let i = 0; i < this.virtualNodes; i++) {
            const virtualNodeKey = `${nodeId}:${i}`;
            const hash = this.hash(virtualNodeKey);
            const position = this.hashToNumber(hash);
            this.ring.set(position, nodeId);
        }

        // Sort the ring by hash values for efficient lookups
        this.sortRing();

        console.log(`Node ${nodeId} added with ${this.virtualNodes} virtual nodes`);
    }

    /**
     * Remove a node from the hash ring
     * Removes all virtual nodes associated with the physical node
     */
    removeNode(nodeId) {
        console.log(`Removing node ${nodeId} from hash ring`);

        // Remove from nodes map
        this.nodes.delete(nodeId);

        // Remove all virtual nodes from ring
        const toDelete = [];
        for (const [position, node] of this.ring.entries()) {
            if (node === nodeId) {
                toDelete.push(position);
            }
        }

        toDelete.forEach(position => this.ring.delete(position));

        console.log(`Node ${nodeId} removed, ${toDelete.length} virtual nodes cleaned up`);
    }

    /**
     * Sort the hash ring by position values
     * This enables efficient clockwise traversal for node selection
     */
    sortRing() {
        const sortedEntries = Array.from(this.ring.entries()).sort((a, b) => a[0] - b[0]);
        this.ring.clear();
        sortedEntries.forEach(([position, nodeId]) => {
            this.ring.set(position, nodeId);
        });
    }

    /**
     * Find the primary node responsible for a given key
     * Uses clockwise traversal from the key's hash position
     */
    getNode(key) {
        if (this.ring.size === 0) {
            throw new Error('No nodes available in the hash ring');
        }

        const keyHash = this.hash(key);
        const keyPosition = this.hashToNumber(keyHash);

        // Find the first node clockwise from the key position
        const sortedPositions = Array.from(this.ring.keys()).sort((a, b) => a - b);

        for (const position of sortedPositions) {
            if (position >= keyPosition) {
                return this.ring.get(position);
            }
        }

        // If no node found clockwise, wrap around to the first node
        return this.ring.get(sortedPositions[0]);
    }

    /**
     * Get all nodes that should store replicas of a key
     * Returns primary node plus (replicationFactor - 1) additional nodes
     */
    getReplicaNodes(key) {
        if (this.nodes.size === 0) {
            throw new Error('No nodes available in the cluster');
        }

        const keyHash = this.hash(key);
        const keyPosition = this.hashToNumber(keyHash);

        // Get all unique physical nodes in clockwise order
        const sortedPositions = Array.from(this.ring.keys()).sort((a, b) => a - b);
        const uniqueNodes = new Set();
        const replicaNodes = [];

        // Start from the key position and move clockwise
        let startIndex = 0;
        for (let i = 0; i < sortedPositions.length; i++) {
            if (sortedPositions[i] >= keyPosition) {
                startIndex = i;
                break;
            }
        }

        // Collect unique nodes starting from the key position
        for (let i = 0; i < sortedPositions.length && uniqueNodes.size < this.replicationFactor; i++) {
            const position = sortedPositions[(startIndex + i) % sortedPositions.length];
            const nodeId = this.ring.get(position);

            if (!uniqueNodes.has(nodeId)) {
                uniqueNodes.add(nodeId);
                replicaNodes.push(nodeId);
            }
        }

        // If we don't have enough unique nodes, add all available nodes
        if (replicaNodes.length < this.replicationFactor && this.nodes.size < this.replicationFactor) {
            console.warn(`Only ${this.nodes.size} nodes available, less than replication factor ${this.replicationFactor}`);
            return Array.from(this.nodes.keys());
        }

        return replicaNodes;
    }

    /**
     * Get all active nodes in the cluster
     */
    getAllNodes() {
        return Array.from(this.nodes.keys());
    }

    /**
     * Get node information by node ID
     */
    getNodeInfo(nodeId) {
        return this.nodes.get(nodeId);
    }

    /**
     * Check if a node exists in the cluster
     */
    hasNode(nodeId) {
        return this.nodes.has(nodeId);
    }

    /**
     * Get cluster statistics for monitoring
     */
    getStats() {
        return {
            totalNodes: this.nodes.size,
            totalVirtualNodes: this.ring.size,
            replicationFactor: this.replicationFactor,
            averageVirtualNodesPerNode: this.ring.size / this.nodes.size
        };
    }

    /**
     * Debug method to visualize the hash ring
     */
    debugRing() {
        console.log('\n=== Hash Ring Debug ===');
        console.log(`Total physical nodes: ${this.nodes.size}`);
        console.log(`Total virtual nodes: ${this.ring.size}`);
        console.log(`Replication factor: ${this.replicationFactor}`);

        const nodeDistribution = new Map();
        for (const nodeId of this.ring.values()) {
            nodeDistribution.set(nodeId, (nodeDistribution.get(nodeId) || 0) + 1);
        }

        console.log('\nVirtual node distribution:');
        for (const [nodeId, count] of nodeDistribution.entries()) {
            console.log(`  ${nodeId}: ${count} virtual nodes`);
        }
        console.log('========================\n');
    }
}

module.exports = ConsistentHash;