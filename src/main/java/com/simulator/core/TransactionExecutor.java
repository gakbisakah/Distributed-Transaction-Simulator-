package com.simulator.core;

import com.simulator.model.Transaction;
import com.simulator.model.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Eksekutor untuk menjalankan transaksi secara parallel
 */
public class TransactionExecutor implements Callable<Boolean> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionExecutor.class);
    
    private final Transaction transaction;
    private final TransactionCoordinator coordinator;
    
    public TransactionExecutor(Transaction transaction, TransactionCoordinator coordinator) {
        this.transaction = transaction;
        this.coordinator = coordinator;
    }
    
    @Override
    public Boolean call() throws Exception {
        logger.debug("Memulai eksekusi transaksi {}", transaction.getId());
        
        long startTime = System.currentTimeMillis();
        boolean success = coordinator.executeTransaction(transaction);
        long duration = System.currentTimeMillis() - startTime;
        
        if (success) {
            logger.info("Transaksi {} selesai dalam {} ms", transaction.getId(), duration);
        } else {
            logger.warn("Transaksi {} gagal setelah {} ms", transaction.getId(), duration);
        }
        
        return success;
    }
}