package com.simulator.fault;

import com.simulator.config.SystemConfig;
import com.simulator.model.NodeStatus;
import com.simulator.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Detektor kegagalan node dalam sistem distributed
 * Menggunakan failure detection berbasis timeout
 */
public class FailureDetector {
    private static final Logger logger = LoggerFactory.getLogger(FailureDetector.class);
    
    private final SystemConfig config;
    private final NodeManager nodeManager;
    private final Map<String, Long> lastHeartbeat;
    private final Map<String, Integer> failureCount;
    private final ScheduledExecutorService executor;
    
    public FailureDetector(SystemConfig config, NodeManager nodeManager) {
        this.config = config;
        this.nodeManager = nodeManager;
        this.lastHeartbeat = new ConcurrentHashMap<>();
        this.failureCount = new ConcurrentHashMap<>();
        this.executor = Executors.newSingleThreadScheduledExecutor();
    }
    
    /**
     * Memeriksa kesehatan semua node
     */
    public void checkNodeHealth() {
        for (String nodeId : nodeManager.getAllNodeIds()) {
            checkNode(nodeId);
        }
    }
    
    /**
     * Memeriksa kesehatan node tertentu
     */
    private void checkNode(String nodeId) {
        NodeStatus currentStatus = nodeManager.getNodeStatus(nodeId);
        
        if (currentStatus == NodeStatus.ACTIVE) {
            Long lastHeartbeatTime = lastHeartbeat.get(nodeId);
            
            if (lastHeartbeatTime != null) {
                long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatTime;
                long timeout = config.getHeartbeatIntervalSeconds() * 1000L * config.getFailureDetectionThreshold();
                
                if (timeSinceLastHeartbeat > timeout) {
                    int failures = failureCount.merge(nodeId, 1, Integer::sum);
                    logger.warn("Node {} tidak merespon heartbeat, failure count: {}/{}", 
                              nodeId, failures, config.getFailureDetectionThreshold());
                    
                    if (failures >= config.getFailureDetectionThreshold()) {
                        logger.error("Node {} dinyatakan FAILED setelah {} kegagalan berturut-turut", 
                                   nodeId, failures);
                        nodeManager.markNodeFailed(nodeId);
                        failureCount.remove(nodeId);
                    } else {
                        nodeManager.markNodeSuspected(nodeId);
                    }
                } else {
                    failureCount.remove(nodeId);
                    if (currentStatus == NodeStatus.SUSPECTED) {
                        nodeManager.markNodeActive(nodeId);
                        logger.info("Node {} kembali aktif", nodeId);
                    }
                }
            }
        }
    }
    
    /**
     * Menerima heartbeat dari node
     */
    public void receiveHeartbeat(String nodeId) {
        lastHeartbeat.put(nodeId, System.currentTimeMillis());
        NodeStatus status = nodeManager.getNodeStatus(nodeId);
        
        if (status == NodeStatus.SUSPECTED || status == NodeStatus.RECOVERING) {
            nodeManager.markNodeActive(nodeId);
            logger.info("Node {} pulih dan kembali aktif", nodeId);
        }
        
        failureCount.remove(nodeId);
    }
    
    /**
     * Menghentikan failure detector
     */
    public void stop() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}