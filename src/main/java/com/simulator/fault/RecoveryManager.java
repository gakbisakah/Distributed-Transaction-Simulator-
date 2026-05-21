package com.simulator.fault;

import com.simulator.config.SystemConfig;
import com.simulator.model.NodeStatus;
import com.simulator.model.Transaction;
import com.simulator.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manager untuk recovery node yang gagal
 * Mengimplementasikan auto-recovery dan state recovery
 */
public class RecoveryManager {
    private static final Logger logger = LoggerFactory.getLogger(RecoveryManager.class);
    
    private final SystemConfig config;
    private final NodeManager nodeManager;
    private final ScheduledExecutorService recoveryExecutor;
    
    public RecoveryManager(SystemConfig config, NodeManager nodeManager) {
        this.config = config;
        this.nodeManager = nodeManager;
        this.recoveryExecutor = Executors.newScheduledThreadPool(2);
    }
    
    /**
     * Memulai service recovery
     */
    public void startRecoveryService() {
        if (config.isEnableAutoRecovery()) {
            recoveryExecutor.scheduleAtFixedRate(() -> {
                recoverFailedNodes();
            }, 30, 30, TimeUnit.SECONDS);
            
            logger.info("Recovery service dimulai dengan interval 30 detik");
        }
    }
    
    /**
     * Memulihkan node yang gagal
     */
    private void recoverFailedNodes() {
        for (String nodeId : nodeManager.getAllNodeIds()) {
            NodeStatus status = nodeManager.getNodeStatus(nodeId);
            
            if (status == NodeStatus.FAILED) {
                logger.info("Memulai recovery untuk node {}", nodeId);
                boolean recovered = attemptRecovery(nodeId);
                
                if (recovered) {
                    logger.info("Node {} berhasil dipulihkan", nodeId);
                    nodeManager.markNodeActive(nodeId);
                    recoverPendingTransactions(nodeId);
                } else {
                    logger.warn("Gagal memulihkan node {}", nodeId);
                }
            }
        }
    }
    
    /**
     * Mencoba memulihkan node
     */
    private boolean attemptRecovery(String nodeId) {
        // Simulasi proses recovery
        try {
            // Simulasi delay recovery
            Thread.sleep(5000);
            
            // Re-inisialisasi node
            nodeManager.reinitializeNode(nodeId);
            
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    /**
     * Memulihkan transaksi yang pending pada node yang recovered
     */
    private void recoverPendingTransactions(String nodeId) {
        logger.info("Memulihkan transaksi pending untuk node {}", nodeId);
        
        // Implementasi: membaca dari Write-Ahead Log dan replay transaksi
        // yang belum selesai sebelum node failure
        
        // Simulasi recovery transaksi
        try {
            Thread.sleep(2000);
            logger.info("Transaksi pending pada node {} berhasil dipulihkan", nodeId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Menghentikan service recovery
     */
    public void stopRecoveryService() {
        recoveryExecutor.shutdown();
        try {
            if (!recoveryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                recoveryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            recoveryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}