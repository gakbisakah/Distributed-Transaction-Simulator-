package com.simulator.lock;

/**
 * Jenis-jenis lock dalam sistem distributed
 */
public enum LockType {
    SHARED("Shared Lock"),
    EXCLUSIVE("Exclusive Lock"),
    INTENT_SHARED("Intent Shared"),
    INTENT_EXCLUSIVE("Intent Exclusive");
    
    private final String description;
    
    LockType(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
    
    public boolean isShared() {
        return this == SHARED || this == INTENT_SHARED;
    }
    
    public boolean isExclusive() {
        return this == EXCLUSIVE || this == INTENT_EXCLUSIVE;
    }
    
    @Override
    public String toString() {
        return description;
    }
}