package com.simulator.log;

/**
 * Tipe-tipe log untuk Write-Ahead Log
 */
public enum LogType {
    BEGIN("Transaction begin"),
    PREPARE("Prepare phase"),
    COMMIT("Commit phase"),
    ABORT("Abort phase"),
    CHECKPOINT("Checkpoint"),
    HEARTBEAT("Node heartbeat");
    
    private final String description;
    
    LogType(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
    
    @Override
    public String toString() {
        return description;
    }
}