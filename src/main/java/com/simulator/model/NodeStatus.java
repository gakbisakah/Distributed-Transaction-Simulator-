package com.simulator.model;

/**
 * Enum untuk status node dalam sistem distributed
 */
public enum NodeStatus {
    ACTIVE("Active"),
    INACTIVE("Inactive"),
    FAILED("Failed"),
    RECOVERING("Recovering"),
    SUSPECTED("Suspected");
    
    private final String description;
    
    NodeStatus(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
    
    public boolean isAvailable() {
        return this == ACTIVE;
    }
    
    public boolean isFailed() {
        return this == FAILED;
    }
    
    @Override
    public String toString() {
        return description;
    }
}