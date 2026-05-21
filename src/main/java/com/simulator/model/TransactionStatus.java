package com.simulator.model;

/**
 * Enum untuk status transaksi
 */
public enum TransactionStatus {
    INITIALIZED("Initialized"),
    PREPARING("Preparing"),
    PREPARED("Prepared"),
    COMMITTING("Committing"),
    COMMITTED("Committed"),
    ABORTING("Aborting"),
    ABORTED("Aborted"),
    TIMEOUT("Timeout"),
    FAILED("Failed");
    
    private final String description;
    
    TransactionStatus(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
    
    public boolean isCompleted() {
        return this == COMMITTED || this == ABORTED || this == FAILED;
    }
    
    public boolean isFinal() {
        return isCompleted() || this == TIMEOUT;
    }
    
    @Override
    public String toString() {
        return description;
    }
}