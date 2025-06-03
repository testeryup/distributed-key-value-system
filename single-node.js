const DistributedNode = require('./node');

async function startSingleNode() {
    const nodeId = process.argv[2] || 'node1';
    const port = parseInt(process.argv[3]) || 50051;

    // Define seed nodes based on the node being started
    let seedNodes = [];
    if (nodeId !== 'node1') {
        seedNodes = [{ nodeId: 'node1', address: 'localhost', port: 50051 }];
    }

    try {
        console.log(`🚀 Starting ${nodeId} on port ${port}`);

        const node = new DistributedNode(nodeId, port, seedNodes);
        await node.start();

        const stats = node.getStats();
        console.log(`✅ ${nodeId} started successfully!`);
        console.log(`   Local keys: ${stats.localKeys}`);
        console.log(`   Known nodes: ${stats.knownNodes}`);
        console.log(`   Cluster nodes: ${node.consistentHash.getAllNodes().join(', ')}`);

        // Handle graceful shutdown
        process.on('SIGINT', async () => {
            console.log(`\n🛑 Shutting down ${nodeId}...`);
            await node.shutdown();
            console.log(`✅ ${nodeId} shutdown complete`);
            process.exit(0);
        });

    } catch (error) {
        console.error(`❌ Failed to start ${nodeId}:`, error.message);
        process.exit(1);
    }
}

if (require.main === module) {
    startSingleNode();
}