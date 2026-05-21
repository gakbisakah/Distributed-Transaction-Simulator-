package com.simulator.util;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generator untuk ID unik dalam sistem distributed
 */
public class IdGenerator {
    private static final AtomicLong transactionCounter = new AtomicLong(0);
    private static final AtomicLong nodeCounter = new AtomicLong(0);
    private static final String INSTANCE_ID = UUID.randomUUID().toString().substring(0, 8);
    
    /**
     * Menghasilkan ID transaksi unik
     */
    public static String generateTransactionId() {
        return String.format("TX-%s-%d", INSTANCE_ID, transactionCounter.incrementAndGet());
    }
    
    /**
     * Menghasilkan ID node unik
     */
    public static String generateNodeId() {
        return String.format("NODE-%s-%d", INSTANCE_ID, nodeCounter.incrementAndGet());
    }
    
    /**
     * Menghasilkan UUID sederhana
     */
    public static String generateUUID() {
        return UUID.randomUUID().toString();
    }
    
    /**
     * Reset counter (untuk testing)
     */
    public static void reset() {
        transactionCounter.set(0);
        nodeCounter.set(0);
    }
}