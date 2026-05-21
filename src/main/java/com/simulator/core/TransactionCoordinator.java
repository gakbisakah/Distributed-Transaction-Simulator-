package com.simulator.core;

import com.simulator.config.SystemConfig;
import com.simulator.lock.DistributedLock;
import com.simulator.lock.LockManager;
import com.simulator.model.NodeStatus;
import com.simulator.model.Transaction;
import com.simulator.model.TransactionStatus;
import com.simulator.node.NodeManager;
import com.simulator.node.DistributedNode;
import com.simulator.util.TimeoutManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Koordinator untuk distributed transaction (2PC - Two Phase Commit)
 * dengan fault tolerance
 */
public class TransactionCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(TransactionCoordinator.class);
    
    private final SystemConfig config;
    private final NodeManager nodeManager;
    private final TransactionManager transactionManager;
    private final LockManager lockManager;
    private final TimeoutManager timeoutManager;
    
    public TransactionCoordinator(SystemConfig config, NodeManager nodeManager, 
                                  TransactionManager transactionManager) {
        this.config = config;
        this.nodeManager = nodeManager;
        this.transactionManager = transactionManager;
        this.lockManager = new LockManager(config);
        this.timeoutManager = new TimeoutManager();
    }
    
    /**
     * Mengeksekusi transaksi distributed menggunakan 2PC
     */
    public boolean executeTransaction(Transaction transaction) {
        logger.info("Memulai koordinasi untuk transaksi {}", transaction.getId());
        
        // Daftarkan transaksi
        transactionManager.registerTransaction(transaction);
        
        // Phase 1: Prepare
        boolean prepared = preparePhase(transaction);
        
        if (!prepared) {
            logger.warn("Phase 1 prepare gagal untuk transaksi {}", transaction.getId());
            abortTransaction(transaction);
            return false;
        }
        
        // Phase 2: Commit
        boolean committed = commitPhase(transaction);
        
        if (committed) {
            transactionManager.updateTransactionStatus(transaction.getId(), TransactionStatus.COMMITTED);
            logger.info("Transaksi {} berhasil di-commit", transaction.getId());
            return true;
        } else {
            transactionManager.updateTransactionStatus(transaction.getId(), TransactionStatus.ABORTED);
            logger.error("Transaksi {} gagal di-commit", transaction.getId());
            return false;
        }
    }
    
    /**
     * Prepare phase - meminta semua node untuk mempersiapkan commit
     */
    private boolean preparePhase(Transaction transaction) {
        logger.info("Prepare phase untuk transaksi {}", transaction.getId());
        
        // Dapatkan node yang terlibat dalam transaksi
        Set<String> involvedNodes = getInvolvedNodes(transaction);
        
        // Acquire locks terlebih dahulu
        boolean locksAcquired = acquireLocks(transaction, involvedNodes);
        if (!locksAcquired) {
            logger.warn("Gagal mengakuisisi lock untuk transaksi {}", transaction.getId());
            releaseLocks(transaction);
            return false;
        }
        
        // Kirim prepare request ke semua node
        List<CompletableFuture<Boolean>> prepareFutures = new ArrayList<>();
        
        for (String nodeId : involvedNodes) {
            CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                DistributedNode node = nodeManager.getNode(nodeId);
                if (node == null || node.getStatus() != NodeStatus.ACTIVE) {
                    logger.warn("Node {} tidak tersedia", nodeId);
                    return false;
                }
                
                // Simulasi network delay
                simulateNetworkDelay();
                
                // Node mempersiapkan transaksi
                boolean prepared = node.prepareTransaction(transaction);
                logger.debug("Node {} prepare transaksi {}: {}", nodeId, transaction.getId(), prepared);
                return prepared;
            });
            prepareFutures.add(future);
        }
        
        // Tunggu semua prepare response dengan timeout
        boolean allPrepared = true;
        try {
            List<Boolean> results = CompletableFuture.allOf(prepareFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> prepareFutures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList()))
                .get(config.getTransactionTimeoutMs(), TimeUnit.MILLISECONDS);
            
            for (Boolean result : results) {
                if (!result) {
                    allPrepared = false;
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Timeout atau error saat prepare phase: {}", e.getMessage());
            allPrepared = false;
        }
        
        if (allPrepared) {
            logger.info("Semua node siap commit untuk transaksi {}", transaction.getId());
            // Simpan keputusan di WAL
            logPrepareDecision(transaction, true);
        } else {
            logger.warn("Tidak semua node siap commit untuk transaksi {}", transaction.getId());
            logPrepareDecision(transaction, false);
        }
        
        return allPrepared;
    }
    
    /**
     * Commit phase - meminta semua node melakukan commit
     */
    private boolean commitPhase(Transaction transaction) {
        logger.info("Commit phase untuk transaksi {}", transaction.getId());
        
        Set<String> involvedNodes = getInvolvedNodes(transaction);
        List<CompletableFuture<Boolean>> commitFutures = new ArrayList<>();
        
        for (String nodeId : involvedNodes) {
            CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                DistributedNode node = nodeManager.getNode(nodeId);
                if (node == null || node.getStatus() != NodeStatus.ACTIVE) {
                    logger.warn("Node {} tidak tersedia saat commit", nodeId);
                    return false;
                }
                
                simulateNetworkDelay();
                boolean committed = node.commitTransaction(transaction);
                logger.debug("Node {} commit transaksi {}: {}", nodeId, transaction.getId(), committed);
                return committed;
            });
            commitFutures.add(future);
        }
        
        // Tunggu semua commit dengan timeout
        boolean allCommitted = true;
        try {
            List<Boolean> results = CompletableFuture.allOf(commitFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> commitFutures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList()))
                .get(config.getTransactionTimeoutMs(), TimeUnit.MILLISECONDS);
            
            for (Boolean result : results) {
                if (!result) {
                    allCommitted = false;
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Timeout atau error saat commit phase: {}", e.getMessage());
            allCommitted = false;
        }
        
        // Release locks setelah commit/abort
        releaseLocks(transaction);
        
        if (allCommitted) {
            logger.info("Commit berhasil untuk transaksi {}", transaction.getId());
        } else {
            logger.error("Commit gagal untuk transaksi {}", transaction.getId());
        }
        
        return allCommitted;
    }
    
    /**
     * Membatalkan transaksi
     */
    private void abortTransaction(Transaction transaction) {
        logger.info("Membatalkan transaksi {}", transaction.getId());
        
        Set<String> involvedNodes = getInvolvedNodes(transaction);
        
        for (String nodeId : involvedNodes) {
            DistributedNode node = nodeManager.getNode(nodeId);
            if (node != null && node.getStatus() == NodeStatus.ACTIVE) {
                node.abortTransaction(transaction);
            }
        }
        
        releaseLocks(transaction);
        transactionManager.updateTransactionStatus(transaction.getId(), TransactionStatus.ABORTED);
    }
    
    /**
     * Mengakuisisi lock untuk transaksi
     */
    private boolean acquireLocks(Transaction transaction, Set<String> nodes) {
        for (String nodeId : nodes) {
            DistributedLock lock = new DistributedLock(
                transaction.getId(),
                nodeId,
                transaction.getResourceIds()
            );
            
            boolean acquired = lockManager.acquireLock(lock);
            if (!acquired) {
                logger.warn("Gagal acquire lock untuk node {} pada transaksi {}", nodeId, transaction.getId());
                return false;
            }
        }
        return true;
    }
    
    /**
     * Melepas semua lock
     */
    private void releaseLocks(Transaction transaction) {
        lockManager.releaseLocks(transaction.getId());
    }
    
    /**
     * Mendapatkan node yang terlibat dalam transaksi
     */
    private Set<String> getInvolvedNodes(Transaction transaction) {
        Set<String> nodes = new HashSet<>();
        for (String resourceId : transaction.getResourceIds()) {
            // Simulasi: node ditentukan berdasarkan hash dari resource ID
            int nodeIndex = Math.abs(resourceId.hashCode()) % config.getNumberOfNodes();
            nodes.add("node-" + nodeIndex);
        }
        return nodes;
    }
    
    /**
     * Simulasi network delay
     */
    private void simulateNetworkDelay() {
        try {
            if (config.getNetworkDelayMs() > 0) {
                Thread.sleep(config.getNetworkDelayMs());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Menyimpan keputusan prepare ke Write-Ahead Log
     */
    private void logPrepareDecision(Transaction transaction, boolean decision) {
        if (config.isEnableWriteAheadLog()) {
            logger.debug("WAL: Prepare decision for tx {} = {}", transaction.getId(), decision);
            // Implementasi WAL sebenarnya akan menulis ke disk
        }
    }
}