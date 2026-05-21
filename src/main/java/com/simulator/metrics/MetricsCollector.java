package com.simulator.metrics;

import com.simulator.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mengumpulkan metrics performa untuk monitoring
 */
public class MetricsCollector {
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);
    
    private final AtomicLong totalTransactions;
    private final AtomicLong successfulTransactions;
    private final AtomicLong failedTransactions;
    private final Queue<Long> latencies;
    private final ScheduledExecutorService scheduler;
    private boolean collecting;
    
    public MetricsCollector() {
        this.totalTransactions = new AtomicLong(0);
        this.successfulTransactions = new AtomicLong(0);
        this.failedTransactions = new AtomicLong(0);
        this.latencies = new ConcurrentLinkedQueue<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.collecting = true;
    }
    
    /**
     * Memulai collection metrics
     */
    public void startCollection() {
        scheduler.scheduleAtFixedRate(() -> {
            if (collecting) {
                printCurrentStats();
            }
        }, 10, 10, TimeUnit.SECONDS);
        
        logger.info("Metrics collector dimulai");
    }
    
    /**
     * Mencatat transaksi selesai
     */
    public void recordTransaction(Transaction transaction, boolean success, long duration) {
        totalTransactions.incrementAndGet();
        
        if (success) {
            successfulTransactions.incrementAndGet();
        } else {
            failedTransactions.incrementAndGet();
        }
        
        latencies.add(duration);
        
        // Keep hanya 1000 sample terakhir
        while (latencies.size() > 1000) {
            latencies.poll();
        }
    }
    
    /**
     * Mencatat transaksi gagal
     */
    public void recordFailedTransaction(Transaction transaction) {
        failedTransactions.incrementAndGet();
        totalTransactions.incrementAndGet();
    }
    
    /**
     * Mencetak statistik saat ini
     */
    private void printCurrentStats() {
        PerformanceStats stats = getPerformanceStats();
        logger.info("=== Performance Statistics ===");
        logger.info("Total Transactions: {}", stats.getTotalTransactions());
        logger.info("Successful: {}", stats.getSuccessfulTransactions());
        logger.info("Failed: {}", stats.getFailedTransactions());
        logger.info("Success Rate: {:.2f}%", stats.getSuccessRate());
        logger.info("Average Latency: {} ms", stats.getAverageLatency());
        logger.info("P95 Latency: {} ms", stats.getP95Latency());
        logger.info("Throughput: {:.2f} tx/sec", stats.getThroughput());
        logger.info("============================");
    }
    
    /**
     * Mendapatkan statistik performa
     */
    public PerformanceStats getPerformanceStats() {
        long total = totalTransactions.get();
        long success = successfulTransactions.get();
        long failed = failedTransactions.get();
        
        double avgLatency = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        
        // Calculate P95
        List<Long> sortedLatencies = new ArrayList<>(latencies);
        sortedLatencies.sort(Long::compareTo);
        long p95 = 0;
        if (!sortedLatencies.isEmpty()) {
            int index = (int) Math.ceil(95.0 / 100.0 * sortedLatencies.size()) - 1;
            p95 = sortedLatencies.get(Math.max(0, index));
        }
        
        return new PerformanceStats(
            total, success, failed,
            avgLatency, p95,
            calculateThroughput()
        );
    }
    
    /**
     * Menghitung throughput
     */
    private double calculateThroughput() {
        // Throughput sederhana: transactions per second over last minute
        return totalTransactions.get() / 60.0;
    }
    
    /**
     * Mencetak laporan final
     */
    public void printFinalReport() {
        collecting = false;
        logger.info("=== FINAL REPORT ===");
        printCurrentStats();
        logger.info("===================");
    }
    
    /**
     * Menghentikan collection metrics
     */
    public void stopCollection() {
        collecting = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}