package com.simulator.lock;

import com.simulator.config.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manager untuk distributed lock dengan timeout dan deadlock detection
 */
public class LockManager {
    private static final Logger logger = LoggerFactory.getLogger(LockManager.class);
    
    private final Map<String, DistributedLock> activeLocks;
    private final Map<String, Set<String>> transactionLocks;
    private final Map<String, ReentrantLock> resourceLocks;
    private final SystemConfig config;
    private final ScheduledExecutorService cleanupExecutor;
    
    public LockManager(SystemConfig config) {
        this.config = config;
        this.activeLocks = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
        this.resourceLocks = new ConcurrentHashMap<>();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();
        
        // Schedule lock cleanup
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpiredLocks, 10, 10, TimeUnit.SECONDS);
    }
    
    /**
     * Mengakuisisi lock
     */
    public boolean acquireLock(DistributedLock lock) {
        String resourceId = lock.getResourceId();
        ReentrantLock resourceLock = resourceLocks.computeIfAbsent(resourceId, k -> new ReentrantLock());
        
        try {
            boolean acquired = resourceLock.tryLock(config.getLockTimeoutMs(), TimeUnit.MILLISECONDS);
            
            if (acquired) {
                lock.setAcquired(true);
                lock.setTimeout(config.getLockTimeoutMs());
                activeLocks.put(lock.getLockId(), lock);
                
                transactionLocks.computeIfAbsent(lock.getTransactionId(), k -> ConcurrentHashMap.newKeySet())
                    .add(lock.getLockId());
                
                logger.debug("Lock acquired: {}", lock);
                return true;
            } else {
                logger.warn("Failed to acquire lock for resource {}: timeout", resourceId);
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while acquiring lock for {}", resourceId);
            return false;
        }
    }
    
    /**
     * Melepas lock
     */
    public void releaseLock(String lockId) {
        DistributedLock lock = activeLocks.remove(lockId);
        if (lock != null) {
            String resourceId = lock.getResourceId();
            ReentrantLock resourceLock = resourceLocks.get(resourceId);
            
            if (resourceLock != null && resourceLock.isHeldByCurrentThread()) {
                resourceLock.unlock();
            }
            
            Set<String> locks = transactionLocks.get(lock.getTransactionId());
            if (locks != null) {
                locks.remove(lockId);
                if (locks.isEmpty()) {
                    transactionLocks.remove(lock.getTransactionId());
                }
            }
            
            logger.debug("Lock released: {}", lock);
        }
    }
    
    /**
     * Melepas semua lock untuk transaksi
     */
    public void releaseLocks(String transactionId) {
        Set<String> locks = transactionLocks.remove(transactionId);
        if (locks != null) {
            for (String lockId : locks) {
                releaseLock(lockId);
            }
            logger.debug("All locks released for transaction {}", transactionId);
        }
    }
    
    /**
     * Membersihkan lock yang expired
     */
    private void cleanupExpiredLocks() {
        for (DistributedLock lock : activeLocks.values()) {
            if (lock.isExpired()) {
                logger.warn("Lock expired, releasing: {}", lock);
                releaseLock(lock.getLockId());
            }
        }
    }
    
    /**
     * Mendeteksi deadlock (sederhana)
     */
    public boolean hasDeadlock(String transactionId) {
        // Implementasi deadlock detection sederhana
        // Dalam production, perlu algoritma seperti wait-for graph
        return false;
    }
    
    /**
     * Mendapatkan statistik lock
     */
    public Map<String, Object> getStats() {
        return Map.of(
            "activeLocks", activeLocks.size(),
            "activeTransactions", transactionLocks.size(),
            "totalResources", resourceLocks.size()
        );
    }
    
    /**
     * Menghentikan lock manager
     */
    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}