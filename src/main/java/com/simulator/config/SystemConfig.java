package com.simulator.config;

import java.util.concurrent.TimeUnit;

/**
 * Konfigurasi sistem untuk distributed transaction simulator
 */
public class SystemConfig {
    private int numberOfNodes = 5;
    private int replicationFactor = 3;
    private int transactionTimeoutSeconds = 30;
    private int maxParallelTransactions = 50;
    private long nodeFailureProbability = 10; // persentase 0-100
    private long networkDelayMs = 50;
    private long lockTimeoutMs = 5000;
    private boolean enableWriteAheadLog = true;
    private boolean enableAutoRecovery = true;
    private int heartbeatIntervalSeconds = 5;
    private int failureDetectionThreshold = 3;
    
    public SystemConfig() {
        // Konfigurasi default
    }
    
    public SystemConfig(int numberOfNodes, int replicationFactor, int transactionTimeoutSeconds) {
        this.numberOfNodes = numberOfNodes;
        this.replicationFactor = replicationFactor;
        this.transactionTimeoutSeconds = transactionTimeoutSeconds;
    }
    
    // Getters and Setters
    public int getNumberOfNodes() {
        return numberOfNodes;
    }
    
    public void setNumberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }
    
    public int getReplicationFactor() {
        return replicationFactor;
    }
    
    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }
    
    public int getTransactionTimeoutSeconds() {
        return transactionTimeoutSeconds;
    }
    
    public void setTransactionTimeoutSeconds(int transactionTimeoutSeconds) {
        this.transactionTimeoutSeconds = transactionTimeoutSeconds;
    }
    
    public int getMaxParallelTransactions() {
        return maxParallelTransactions;
    }
    
    public void setMaxParallelTransactions(int maxParallelTransactions) {
        this.maxParallelTransactions = maxParallelTransactions;
    }
    
    public long getNodeFailureProbability() {
        return nodeFailureProbability;
    }
    
    public void setNodeFailureProbability(long nodeFailureProbability) {
        this.nodeFailureProbability = nodeFailureProbability;
    }
    
    public long getNetworkDelayMs() {
        return networkDelayMs;
    }
    
    public void setNetworkDelayMs(long networkDelayMs) {
        this.networkDelayMs = networkDelayMs;
    }
    
    public long getLockTimeoutMs() {
        return lockTimeoutMs;
    }
    
    public void setLockTimeoutMs(long lockTimeoutMs) {
        this.lockTimeoutMs = lockTimeoutMs;
    }
    
    public boolean isEnableWriteAheadLog() {
        return enableWriteAheadLog;
    }
    
    public void setEnableWriteAheadLog(boolean enableWriteAheadLog) {
        this.enableWriteAheadLog = enableWriteAheadLog;
    }
    
    public boolean isEnableAutoRecovery() {
        return enableAutoRecovery;
    }
    
    public void setEnableAutoRecovery(boolean enableAutoRecovery) {
        this.enableAutoRecovery = enableAutoRecovery;
    }
    
    public int getHeartbeatIntervalSeconds() {
        return heartbeatIntervalSeconds;
    }
    
    public void setHeartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    }
    
    public int getFailureDetectionThreshold() {
        return failureDetectionThreshold;
    }
    
    public void setFailureDetectionThreshold(int failureDetectionThreshold) {
        this.failureDetectionThreshold = failureDetectionThreshold;
    }
    
    public long getTransactionTimeoutMs() {
        return TimeUnit.SECONDS.toMillis(transactionTimeoutSeconds);
    }
    
    @Override
    public String toString() {
        return String.format("SystemConfig{nodes=%d, replication=%d, timeout=%ds, " +
                           "parallel=%d, failureProb=%d%%, delay=%dms}",
                           numberOfNodes, replicationFactor, transactionTimeoutSeconds,
                           maxParallelTransactions, nodeFailureProbability, networkDelayMs);
    }
}