const DistributedNode = require('./node');

async function startCluster() {
    console.log('🚀 Starting Distributed KV Store Cluster\n');

    try {
        // Define the cluster topology
        const nodeConfigs = [
            { nodeId: 'node1', port: 50051, seedNodes: [] },
            { nodeId: 'node2', port: 50052, seedNodes: [{ nodeId: 'node1', address: 'localhost', port: 50051 }] },
            { nodeId: 'node3', port: 50053, seedNodes: [{ nodeId: 'node1', address: 'localhost', port: 50051 }] }
        ];

        const nodes = [];

        // Start each node
        for (const config of nodeConfigs) {
            console.log(`Starting ${config.nodeId}...`);
            const node = new DistributedNode(config.nodeId, config.port, config.seedNodes);
            await node.start();
            nodes.push(node);

            // Wait a bit between node startups to ensure proper cluster formation
            await new Promise(resolve => setTimeout(resolve, 2000));
        }

        console.log('\n✅ All nodes started successfully!');
        console.log('Cluster nodes:');
        nodes.forEach(node => {
            const stats = node.getStats();
            console.log(`  - ${stats.nodeId}: port ${stats.port}, ${stats.localKeys} keys, ${stats.knownNodes} known nodes`);
        });

        console.log('\n🔗 You can now connect clients to any of these addresses:');
        console.log('  - localhost:50051');
        console.log('  - localhost:50052');
        console.log('  - localhost:50053');

        // Handle graceful shutdown
        process.on('SIGINT', async () => {
            console.log('\n🛑 Shutting down cluster...');

            for (const node of nodes) {
                await node.shutdown();
            }

            console.log('✅ Cluster shutdown complete');
            process.exit(0);
        });

        // Keep the process running
        process.on('SIGTERM', async () => {
            console.log('\n🛑 Received SIGTERM, shutting down cluster...');

            for (const node of nodes) {
                await node.shutdown();
            }

            process.exit(0);
        });

    } catch (error) {
        console.error('❌ Failed to start cluster:', error.message);
        process.exit(1);
    }
}

// Run if this file is executed directly
if (require.main === module) {
    startCluster();
}

module.exports = { startCluster };