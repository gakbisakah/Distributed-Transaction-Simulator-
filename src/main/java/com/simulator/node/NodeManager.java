package com.simulator.node;

import com.simulator.config.SystemConfig;
import com.simulator.model.NodeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manager untuk semua node dalam sistem distributed
 * Mengelola lifecycle node dan replicas
 */
public class NodeManager {
    private static final Logger logger = LoggerFactory.getLogger(NodeManager.class);
    
    private final Map<String, DistributedNode> nodes;
    private final SystemConfig config;
    private final Random random;
    
    public NodeManager(SystemConfig config) {
        this.config = config;
        this.nodes = new ConcurrentHashMap<>();
        this.random = new Random();
    }
    
    /**
     * Inisialisasi semua node
     */
    public void initializeAllNodes() {
        for (int i = 0; i < config.getNumberOfNodes(); i++) {
            String nodeId = "node-" + i;
            String host = "localhost";
            int port = 8000 + i;
            
            DistributedNode node = new DistributedNode(nodeId, host, port);
            nodes.put(nodeId, node);
            logger.info("Node {} diinisialisasi", nodeId);
        }
        
        logger.info("{} node berhasil diinisialisasi", nodes.size());
    }
    
    /**
     * Mendapatkan node berdasarkan ID
     */
    public DistributedNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }
    
    /**
     * Mendapatkan semua node
     */
    public Collection<DistributedNode> getAllNodes() {
        return Collections.unmodifiableCollection(nodes.values());
    }
    
    /**
     * Mendapatkan semua node ID
     */
    public Set<String> getAllNodeIds() {
        return new HashSet<>(nodes.keySet());
    }
    
    /**
     * Mendapatkan node aktif
     */
    public List<String> getActiveNodes() {
        return nodes.values().stream()
            .filter(node -> node.getStatus() == NodeStatus.ACTIVE)
            .map(DistributedNode::getNodeId)
            .collect(Collectors.toList());
    }
    
    /**
     * Mendapatkan node untuk resource tertentu (replication-aware)
     */
    public Set<DistributedNode> getNodesForResource(String resourceId) {
        Set<DistributedNode> resourceNodes = new HashSet<>();
        
        // Tentukan primary node berdasarkan hash dari resource ID
        int primaryIndex = Math.abs(resourceId.hashCode()) % config.getNumberOfNodes();
        resourceNodes.add(nodes.get("node-" + primaryIndex));
        
        // Tambahkan replica nodes
        for (int i = 1; i < config.getReplicationFactor(); i++) {
            int replicaIndex = (primaryIndex + i) % config.getNumberOfNodes();
            resourceNodes.add(nodes.get("node-" + replicaIndex));
        }
        
        return resourceNodes;
    }
    
    /**
     * Menandai node sebagai gagal
     */
    public void markNodeFailed(String nodeId) {
        DistributedNode node = nodes.get(nodeId);
        if (node != null && node.getStatus() != NodeStatus.FAILED) {
            node.setStatus(NodeStatus.FAILED);
            logger.warn("Node {} ditandai sebagai FAILED", nodeId);
            
            // Trigger failover
            handleNodeFailure(nodeId);
        }
    }
    
    /**
     * Menandai node sebagai suspected
     */
    public void markNodeSuspected(String nodeId) {
        DistributedNode node = nodes.get(nodeId);
        if (node != null && node.getStatus() == NodeStatus.ACTIVE) {
            node.setStatus(NodeStatus.SUSPECTED);
            logger.warn("Node {} ditandai sebagai SUSPECTED", nodeId);
        }
    }
    
    /**
     * Menandai node sebagai aktif
     */
    public void markNodeActive(String nodeId) {
        DistributedNode node = nodes.get(nodeId);
        if (node != null) {
            node.setStatus(NodeStatus.ACTIVE);
            logger.info("Node {} ditandai sebagai ACTIVE", nodeId);
        }
    }
    
    /**
     * Menandai node sebagai recovering
     */
    public void markNodeRecovering(String nodeId) {
        DistributedNode node = nodes.get(nodeId);
        if (node != null) {
            node.setStatus(NodeStatus.RECOVERING);
            logger.info("Node {} dalam mode RECOVERING", nodeId);
        }
    }
    
    /**
     * Mengatur latency untuk node (simulasi)
     */
    public void setNodeLatency(String nodeId, long latencyMs) {
        DistributedNode node = nodes.get(nodeId);
        if (node != null) {
            node.setLatencyOverride(latencyMs);
            logger.debug("Latency node {} diatur ke {} ms", nodeId, latencyMs);
        }
    }
    
    /**
     * Menangani failure node dengan failover
     */
    private void handleNodeFailure(String nodeId) {
        logger.info("Menangani failover untuk node {}", nodeId);
        // Implementasi failover logic
        // Redirect traffic ke replica nodes
    }
    
    /**
     * Mendapatkan status node
     */
    public NodeStatus getNodeStatus(String nodeId) {
        DistributedNode node = nodes.get(nodeId);
        return node != null ? node.getStatus() : null;
    }
    
    /**
     * Re-inisialisasi node
     */
    public void reinitializeNode(String nodeId) {
        DistributedNode node = nodes.get(nodeId);
        if (node != null) {
            node.setStatus(NodeStatus.RECOVERING);
            // Re-inisialisasi state
            node.setStatus(NodeStatus.ACTIVE);
            node.setLatencyOverride(0);
            logger.info("Node {} berhasil re-inisialisasi", nodeId);
        }
    }
    
    /**
     * Mendapatkan statistik semua node
     */
    public Map<String, Object> getAllNodeStats() {
        Map<String, Object> stats = new HashMap<>();
        
        stats.put("totalNodes", nodes.size());
        stats.put("activeNodes", getActiveNodes().size());
        stats.put("failedNodes", nodes.values().stream()
            .filter(n -> n.getStatus() == NodeStatus.FAILED).count());
        
        List<Map<String, Object>> nodeDetails = new ArrayList<>();
        for (DistributedNode node : nodes.values()) {
            nodeDetails.add(node.getStats());
        }
        stats.put("nodeDetails", nodeDetails);
        
        return stats;
    }
}