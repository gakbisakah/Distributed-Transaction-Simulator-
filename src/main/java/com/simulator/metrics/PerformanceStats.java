package com.simulator.metrics;

/**
 * Data class untuk menyimpan statistik performa
 */
public class PerformanceStats {
    private final long totalTransactions;
    private final long successfulTransactions;
    private final long failedTransactions;
    private final double averageLatency;
    private final long p95Latency;
    private final double throughput;
    
    public PerformanceStats(long totalTransactions, long successfulTransactions, 
                           long failedTransactions, double averageLatency, 
                           long p95Latency, double throughput) {
        this.totalTransactions = totalTransactions;
        this.successfulTransactions = successfulTransactions;
        this.failedTransactions = failedTransactions;
        this.averageLatency = averageLatency;
        this.p95Latency = p95Latency;
        this.throughput = throughput;
    }
    
    public long getTotalTransactions() { return totalTransactions; }
    public long getSuccessfulTransactions() { return successfulTransactions; }
    public long getFailedTransactions() { return failedTransactions; }
    public double getSuccessRate() {
        if (totalTransactions == 0) return 0.0;
        return (successfulTransactions * 100.0) / totalTransactions;
    }
    public double getAverageLatency() { return averageLatency; }
    public long getP95Latency() { return p95Latency; }
    public double getThroughput() { return throughput; }
    
    @Override
    public String toString() {
        return String.format("PerformanceStats{total=%d, success=%d, failed=%d, " +
                           "successRate=%.2f%%, avgLatency=%.2fms, p95=%dms, throughput=%.2f}",
                           totalTransactions, successfulTransactions, failedTransactions,
                           getSuccessRate(), averageLatency, p95Latency, throughput);
    }
}