package com.simulator.fault;

import com.simulator.config.SystemConfig;
import com.simulator.model.NodeStatus;
import com.simulator.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Injector untuk mensimulasikan berbagai jenis fault dalam sistem distributed
 */
public class FaultInjector {
    private static final Logger logger = LoggerFactory.getLogger(FaultInjector.class);
    
    private final SystemConfig config;
    private final NodeManager nodeManager;
    private final Random random;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running;
    private final FailureDetector failureDetector;
    private final RecoveryManager recoveryManager;
    
    public FaultInjector(SystemConfig config, NodeManager nodeManager) {
        this.config = config;
        this.nodeManager = nodeManager;
        this.random = new Random();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.running = new AtomicBoolean(false);
        this.failureDetector = new FailureDetector(config, nodeManager);
        this.recoveryManager = new RecoveryManager(config, nodeManager);
    }
    
    /**
     * Memulai injeksi fault secara random
     */
    public void startRandomFaultInjection() {
        running.set(true);
        
        // Jadwalkan failure detector
        scheduler.scheduleAtFixedRate(() -> {
            if (running.get()) {
                failureDetector.checkNodeHealth();
            }
        }, config.getHeartbeatIntervalSeconds(), config.getHeartbeatIntervalSeconds(), TimeUnit.SECONDS);
        
        // Jadwalkan random fault injection
        scheduler.scheduleAtFixedRate(() -> {
            if (running.get() && shouldInjectFault()) {
                injectRandomFault();
            }
        }, 10, 15, TimeUnit.SECONDS);
        
        // Recovery manager
        recoveryManager.startRecoveryService();
        
        logger.info("Fault injector dimulai dengan probabilitas kegagalan {}%", config.getNodeFailureProbability());
    }
    
    /**
     * Memeriksa apakah harus menginjeksi fault
     */
    private boolean shouldInjectFault() {
        return random.nextInt(100) < config.getNodeFailureProbability();
    }
    
    /**
     * Menginjeksi fault random
     */
    private void injectRandomFault() {
        FaultType faultType = FaultType.getRandom();
        String targetNode = getRandomActiveNode();
        
        if (targetNode != null) {
            logger.warn("Menginjeksi fault {} ke node {}", faultType, targetNode);
            
            switch (faultType) {
                case NODE_CRASH:
                    nodeManager.markNodeFailed(targetNode);
                    break;
                case NETWORK_PARTITION:
                    simulateNetworkPartition(targetNode);
                    break;
                case HIGH_LATENCY:
                    simulateHighLatency(targetNode);
                    break;
                case RESOURCE_EXHAUSTION:
                    simulateResourceExhaustion(targetNode);
                    break;
            }
        }
    }
    
    /**
     * Simulasi network partition
     */
    private void simulateNetworkPartition(String nodeId) {
        logger.warn("Simulasi network partition untuk node {}", nodeId);
        nodeManager.markNodeSuspected(nodeId);
        // Node masih bisa diakses tapi dengan delay tinggi
    }
    
    /**
     * Simulasi high latency
     */
    private void simulateHighLatency(String nodeId) {
        logger.warn("Simulasi high latency untuk node {}", nodeId);
        // Implementasi: meningkatkan delay komunikasi
        nodeManager.setNodeLatency(nodeId, 5000); // 5 detik delay
    }
    
    /**
     * Simulasi resource exhaustion
     */
    private void simulateResourceExhaustion(String nodeId) {
        logger.warn("Simulasi resource exhaustion untuk node {}", nodeId);
        nodeManager.markNodeSuspected(nodeId);
    }
    
    /**
     * Mendapatkan node aktif random
     */
    private String getRandomActiveNode() {
        return nodeManager.getActiveNodes().stream()
            .findAny()
            .orElse(null);
    }
    
    /**
     * Menghentikan fault injection
     */
    public void stop() {
        running.set(false);
        scheduler.shutdown();
        failureDetector.stop();
        recoveryManager.stopRecoveryService();
        logger.info("Fault injector dihentikan");
    }
    
    /**
     * Jenis-jenis fault yang dapat diinjeksi
     */
    private enum FaultType {
        NODE_CRASH,
        NETWORK_PARTITION,
        HIGH_LATENCY,
        RESOURCE_EXHAUSTION;
        
        private static final Random random = new Random();
        
        public static FaultType getRandom() {
            return values()[random.nextInt(values().length)];
        }
    }
}