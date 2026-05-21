package com.simulator.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write-Ahead Log (WAL) untuk durability dan recovery
 * Merekam semua operasi sebelum dieksekusi
 */
public class WriteAheadLog {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);
    
    private final Queue<LogEntry> logEntries;
    private final Map<String, List<LogEntry>> transactionLogs;
    private final AtomicLong logSequenceNumber;
    private long lastCheckpoint;
    
    public WriteAheadLog() {
        this.logEntries = new ConcurrentLinkedQueue<>();
        this.transactionLogs = new HashMap<>();
        this.logSequenceNumber = new AtomicLong(0);
        this.lastCheckpoint = System.currentTimeMillis();
    }
    
    /**
     * Menulis log entry
     */
    public void writeLog(LogEntry entry) {
        long lsn = logSequenceNumber.incrementAndGet();
        entry.setLsn(lsn);
        entry.setTimestamp(System.currentTimeMillis());
        
        logEntries.add(entry);
        
        // Index by transaction
        transactionLogs.computeIfAbsent(entry.getTransactionId(), k -> new ArrayList<>())
            .add(entry);
        
        logger.trace("WAL: wrote entry {} for tx {}", entry.getLogType(), entry.getTransactionId());
        
        // Trigger checkpoint periodically
        if (shouldCheckpoint()) {
            checkpoint();
        }
    }
    
    /**
     * Membaca semua log untuk transaksi
     */
    public List<LogEntry> readTransactionLog(String transactionId) {
        return transactionLogs.getOrDefault(transactionId, Collections.emptyList());
    }
    
    /**
     * Recovery dengan replay log
     */
    public void recover() {
        logger.info("Memulai recovery dari WAL...");
        
        // Replay semua log untuk memulihkan state
        Map<String, TransactionState> transactionStates = new HashMap<>();
        
        for (LogEntry entry : logEntries) {
            TransactionState state = transactionStates.computeIfAbsent(
                entry.getTransactionId(), k -> new TransactionState()
            );
            
            switch (entry.getLogType()) {
                case BEGIN:
                    state.begin();
                    break;
                case PREPARE:
                    state.prepare();
                    break;
                case COMMIT:
                    state.commit();
                    break;
                case ABORT:
                    state.abort();
                    break;
            }
        }
        
        // Handle transaksi yang belum selesai
        for (Map.Entry<String, TransactionState> entry : transactionStates.entrySet()) {
            String txId = entry.getKey();
            TransactionState state = entry.getValue();
            
            if (state.isPrepared() && !state.isCommitted()) {
                logger.info("Transaction {} found in prepared state, re-committing", txId);
                // Re-commit transaksi yang sudah prepare
            } else if (!state.isCompleted()) {
                logger.info("Transaction {} found incomplete, aborting", txId);
                // Abort transaksi yang tidak selesai
            }
        }
        
        logger.info("Recovery selesai, processed {} log entries", logEntries.size());
    }
    
    /**
     * Memeriksa apakah perlu checkpoint
     */
    private boolean shouldCheckpoint() {
        return System.currentTimeMillis() - lastCheckpoint > 60000; // every minute
    }
    
    /**
     * Membuat checkpoint
     */
    private void checkpoint() {
        logger.info("Creating checkpoint...");
        lastCheckpoint = System.currentTimeMillis();
        // Simulasi: flush ke disk
        
        // Clean up old logs (keep last 1000 entries untuk demo)
        while (logEntries.size() > 1000) {
            LogEntry old = logEntries.poll();
            if (old != null) {
                List<LogEntry> txLogs = transactionLogs.get(old.getTransactionId());
                if (txLogs != null) {
                    txLogs.remove(old);
                    if (txLogs.isEmpty()) {
                        transactionLogs.remove(old.getTransactionId());
                    }
                }
            }
        }
    }
    
    /**
     * Mendapatkan statistik WAL
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "totalEntries", logEntries.size(),
            "activeTransactions", transactionLogs.size(),
            "lastCheckpoint", lastCheckpoint,
            "currentLSN", logSequenceNumber.get()
        );
    }
    
    /**
     * Inner class untuk tracking state transaksi saat recovery
     */
    private static class TransactionState {
        private boolean began = false;
        private boolean prepared = false;
        private boolean committed = false;
        private boolean aborted = false;
        
        void begin() { began = true; }
        void prepare() { prepared = true; }
        void commit() { committed = true; }
        void abort() { aborted = true; }
        
        boolean isPrepared() { return prepared && !committed && !aborted; }
        boolean isCommitted() { return committed; }
        boolean isCompleted() { return committed || aborted; }
    }
}