package com.simulator.node;

import com.simulator.model.NodeStatus;
import com.simulator.model.Transaction;
import com.simulator.model.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Representasi node dalam sistem distributed
 * Setiap node dapat memproses transaksi secara independent
 */
public class DistributedNode {
    private static final Logger logger = LoggerFactory.getLogger(DistributedNode.class);
    
    private final String nodeId;
    private final String host;
    private final int port;
    private volatile NodeStatus status;
    private final Map<String, Transaction> preparedTransactions;
    private final Map<String, Transaction> committedTransactions;
    private final AtomicLong totalTransactionsProcessed;
    private long latencyOverride; // Untuk simulasi latency
    
    public DistributedNode(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.status = NodeStatus.ACTIVE;
        this.preparedTransactions = new ConcurrentHashMap<>();
        this.committedTransactions = new ConcurrentHashMap<>();
        this.totalTransactionsProcessed = new AtomicLong(0);
        this.latencyOverride = 0;
    }
    
    /**
     * Mempersiapkan transaksi (Prepare phase)
     */
    public boolean prepareTransaction(Transaction transaction) {
        if (status != NodeStatus.ACTIVE) {
            logger.warn("Node {} tidak aktif, tidak dapat prepare transaksi", nodeId);
            return false;
        }
        
        // Simulasi latency
        simulateLatency();
        
        // Validasi apakah bisa memproses transaksi
        boolean canPrepare = validateTransaction(transaction);
        
        if (canPrepare) {
            transaction.setStatus(TransactionStatus.PREPARED);
            preparedTransactions.put(transaction.getId(), transaction);
            logger.debug("Node {} menyiapkan transaksi {}", nodeId, transaction.getId());
        }
        
        return canPrepare;
    }
    
    /**
     * Melakukan commit transaksi
     */
    public boolean commitTransaction(Transaction transaction) {
        if (status != NodeStatus.ACTIVE) {
            logger.warn("Node {} tidak aktif, tidak dapat commit transaksi", nodeId);
            return false;
        }
        
        simulateLatency();
        
        Transaction preparedTx = preparedTransactions.remove(transaction.getId());
        if (preparedTx != null) {
            // Simulasi write ke storage
            boolean committed = writeToStorage(transaction);
            
            if (committed) {
                transaction.setStatus(TransactionStatus.COMMITTED);
                committedTransactions.put(transaction.getId(), transaction);
                totalTransactionsProcessed.incrementAndGet();
                logger.debug("Node {} commit transaksi {}", nodeId, transaction.getId());
                return true;
            }
        }
        
        logger.warn("Node {} gagal commit transaksi {}", nodeId, transaction.getId());
        return false;
    }
    
    /**
     * Membatalkan transaksi
     */
    public void abortTransaction(Transaction transaction) {
        if (status != NodeStatus.ACTIVE) {
            return;
        }
        
        simulateLatency();
        
        preparedTransactions.remove(transaction.getId());
        transaction.setStatus(TransactionStatus.ABORTED);
        logger.debug("Node {} abort transaksi {}", nodeId, transaction.getId());
    }
    
    /**
     * Validasi transaksi
     */
    private boolean validateTransaction(Transaction transaction) {
        // Simulasi validasi bisnis logic
        // 90% success rate untuk simulasi
        return Math.random() < 0.9;
    }
    
    /**
     * Simulasi write ke storage
     */
    private boolean writeToStorage(Transaction transaction) {
        // Simulasi I/O
        try {
            Thread.sleep(10);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    /**
     * Simulasi latency jaringan
     */
    private void simulateLatency() {
        if (latencyOverride > 0) {
            try {
                Thread.sleep(latencyOverride);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * Mengirim heartbeat ke coordinator
     */
    public void sendHeartbeat() {
        // Implementasi heartbeat
        logger.trace("Heartbeat dari node {}", nodeId);
    }
    
    // Getters and Setters
    public String getNodeId() {
        return nodeId;
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public NodeStatus getStatus() {
        return status;
    }
    
    public void setStatus(NodeStatus status) {
        this.status = status;
        logger.info("Node {} status berubah menjadi {}", nodeId, status);
    }
    
    public void setLatencyOverride(long latencyMs) {
        this.latencyOverride = latencyMs;
    }
    
    public long getTotalTransactionsProcessed() {
        return totalTransactionsProcessed.get();
    }
    
    public int getPreparedTransactionsCount() {
        return preparedTransactions.size();
    }
    
    public Map<String, Object> getStats() {
        return Map.of(
            "nodeId", nodeId,
            "status", status.toString(),
            "totalProcessed", totalTransactionsProcessed.get(),
            "preparedCount", preparedTransactions.size(),
            "committedCount", committedTransactions.size()
        );
    }
    
    @Override
    public String toString() {
        return String.format("DistributedNode{id='%s', host='%s', port=%d, status=%s}", 
                           nodeId, host, port, status);
    }
}