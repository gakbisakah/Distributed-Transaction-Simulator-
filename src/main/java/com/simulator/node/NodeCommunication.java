package com.simulator.node;

import com.simulator.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Mengelola komunikasi antar node dalam sistem distributed
 * Mensimulasikan komunikasi jaringan dengan latensi dan potential failures
 */
public class NodeCommunication {
    private static final Logger logger = LoggerFactory.getLogger(NodeCommunication.class);
    
    private final NodeManager nodeManager;
    private final long defaultTimeoutMs;
    
    public NodeCommunication(NodeManager nodeManager, long defaultTimeoutMs) {
        this.nodeManager = nodeManager;
        this.defaultTimeoutMs = defaultTimeoutMs;
    }
    
    /**
     * Mengirim prepare request ke node
     */
    public CompletableFuture<Boolean> sendPrepare(String nodeId, Transaction transaction) {
        return CompletableFuture.supplyAsync(() -> {
            DistributedNode node = nodeManager.getNode(nodeId);
            
            if (node == null || node.getStatus().isFailed()) {
                logger.warn("Node {} tidak tersedia", nodeId);
                return false;
            }
            
            try {
                // Simulasi komunikasi jaringan
                Thread.sleep(50);
                
                boolean result = node.prepareTransaction(transaction);
                logger.debug("Prepare response dari {} untuk tx {}: {}", nodeId, transaction.getId(), result);
                return result;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }).orTimeout(defaultTimeoutMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Mengirim commit request ke node
     */
    public CompletableFuture<Boolean> sendCommit(String nodeId, Transaction transaction) {
        return CompletableFuture.supplyAsync(() -> {
            DistributedNode node = nodeManager.getNode(nodeId);
            
            if (node == null || node.getStatus().isFailed()) {
                logger.warn("Node {} tidak tersedia", nodeId);
                return false;
            }
            
            try {
                Thread.sleep(50);
                boolean result = node.commitTransaction(transaction);
                logger.debug("Commit response dari {} untuk tx {}: {}", nodeId, transaction.getId(), result);
                return result;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }).orTimeout(defaultTimeoutMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Mengirim abort request ke node
     */
    public CompletableFuture<Void> sendAbort(String nodeId, Transaction transaction) {
        return CompletableFuture.runAsync(() -> {
            DistributedNode node = nodeManager.getNode(nodeId);
            
            if (node != null && !node.getStatus().isFailed()) {
                node.abortTransaction(transaction);
                logger.debug("Abort sent to {} for tx {}", nodeId, transaction.getId());
            }
        }).orTimeout(defaultTimeoutMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Broadcast pesan ke semua node
     */
    public void broadcast(String message, String senderId) {
        for (String nodeId : nodeManager.getAllNodeIds()) {
            if (!nodeId.equals(senderId)) {
                sendMessage(nodeId, message);
            }
        }
    }
    
    /**
     * Mengirim pesan ke node tertentu
     */
    private void sendMessage(String nodeId, String message) {
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(30);
                logger.trace("Pesan '{}' terkirim ke node {}", message, nodeId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
}