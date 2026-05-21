package com.simulator.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Manager untuk timeout handling dalam distributed system
 */
public class TimeoutManager {
    private static final Logger logger = LoggerFactory.getLogger(TimeoutManager.class);
    
    private final ScheduledExecutorService scheduler;
    private final Map<String, ScheduledFuture<?>> timeoutFutures;
    
    public TimeoutManager() {
        this.scheduler = Executors.newScheduledThreadPool(4);
        this.timeoutFutures = new ConcurrentHashMap<>();
    }
    
    /**
     * Menjadwalkan timeout untuk task
     */
    public void scheduleTimeout(String taskId, Runnable onTimeout, long timeoutMs) {
        ScheduledFuture<?> future = scheduler.schedule(() -> {
            logger.warn("Timeout untuk task {} setelah {} ms", taskId, timeoutMs);
            onTimeout.run();
            timeoutFutures.remove(taskId);
        }, timeoutMs, TimeUnit.MILLISECONDS);
        
        timeoutFutures.put(taskId, future);
    }
    
    /**
     * Membatalkan timeout untuk task
     */
    public void cancelTimeout(String taskId) {
        ScheduledFuture<?> future = timeoutFutures.remove(taskId);
        if (future != null) {
            future.cancel(false);
            logger.debug("Timeout untuk task {} dibatalkan", taskId);
        }
    }
    
    /**
     * Eksekusi dengan timeout
     */
    public <T> CompletableFuture<T> executeWithTimeout(Callable<T> task, long timeoutMs) {
        CompletableFuture<T> future = CompletableFuture.supplyAsync(() -> {
            try {
                return task.call();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
        
        return future.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Menghentikan timeout manager
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}