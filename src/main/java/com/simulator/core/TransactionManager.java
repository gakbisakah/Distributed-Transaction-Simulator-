package com.simulator.core;

import com.simulator.config.SystemConfig;
import com.simulator.model.Transaction;
import com.simulator.model.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mengelola lifecycle transaksi dan tracking status
 */
public class TransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);
    
    private final Map<String, Transaction> activeTransactions;
    private final Map<String, Transaction> completedTransactions;
    private final AtomicInteger totalTransactions;
    private final AtomicInteger successfulTransactions;
    private final AtomicInteger failedTransactions;
    private final SystemConfig config;
    
    public TransactionManager(SystemConfig config) {
        this.config = config;
        this.activeTransactions = new ConcurrentHashMap<>();
        this.completedTransactions = new ConcurrentHashMap<>();
        this.totalTransactions = new AtomicInteger(0);
        this.successfulTransactions = new AtomicInteger(0);
        this.failedTransactions = new AtomicInteger(0);
    }
    
    /**
     * Mendaftarkan transaksi baru
     */
    public void registerTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.INITIALIZED);
        transaction.setStartTime(System.currentTimeMillis());
        activeTransactions.put(transaction.getId(), transaction);
        totalTransactions.incrementAndGet();
        logger.debug("Transaksi {} didaftarkan", transaction.getId());
    }
    
    /**
     * Memperbarui status transaksi
     */
    public void updateTransactionStatus(String transactionId, TransactionStatus status) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction != null) {
            transaction.setStatus(status);
            logger.debug("Transaksi {} status diperbarui menjadi {}", transactionId, status);
            
            if (status == TransactionStatus.COMMITTED) {
                completeTransaction(transactionId, true);
            } else if (status == TransactionStatus.ABORTED) {
                completeTransaction(transactionId, false);
            }
        }
    }
    
    /**
     * Menyelesaikan transaksi
     */
    private void completeTransaction(String transactionId, boolean success) {
        Transaction transaction = activeTransactions.remove(transactionId);
        if (transaction != null) {
            transaction.setEndTime(System.currentTimeMillis());
            completedTransactions.put(transactionId, transaction);
            
            if (success) {
                successfulTransactions.incrementAndGet();
                logger.info("Transaksi {} berhasil diselesaikan", transactionId);
            } else {
                failedTransactions.incrementAndGet();
                logger.warn("Transaksi {} gagal diselesaikan", transactionId);
            }
        }
    }
    
    /**
     * Mendapatkan status transaksi
     */
    public TransactionStatus getTransactionStatus(String transactionId) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction != null) {
            return transaction.getStatus();
        }
        
        transaction = completedTransactions.get(transactionId);
        if (transaction != null) {
            return transaction.getStatus();
        }
        
        return null;
    }
    
    /**
     * Mendapatkan transaksi berdasarkan ID
     */
    public Transaction getTransaction(String transactionId) {
        Transaction tx = activeTransactions.get(transactionId);
        if (tx == null) {
            tx = completedTransactions.get(transactionId);
        }
        return tx;
    }
    
    /**
     * Memeriksa apakah transaksi timeout
     */
    public boolean isTransactionTimeout(String transactionId) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction != null) {
            long elapsed = System.currentTimeMillis() - transaction.getStartTime();
            return elapsed > config.getTransactionTimeoutMs();
        }
        return false;
    }
    
    /**
     * Mendapatkan statistik transaksi
     */
    public Map<String, Object> getStatistics() {
        return Map.of(
            "total", totalTransactions.get(),
            "successful", successfulTransactions.get(),
            "failed", failedTransactions.get(),
            "active", activeTransactions.size(),
            "completed", completedTransactions.size(),
            "successRate", getSuccessRate()
        );
    }
    
    /**
     * Mendapatkan success rate
     */
    public double getSuccessRate() {
        int total = totalTransactions.get();
        if (total == 0) return 0.0;
        return (successfulTransactions.get() * 100.0) / total;
    }
}