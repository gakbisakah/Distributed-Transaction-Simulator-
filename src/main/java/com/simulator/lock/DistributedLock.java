package com.simulator.lock;

import java.util.Set;
import java.util.UUID;

/**
 * Representasi distributed lock untuk resource coordination
 */
public class DistributedLock {
    private final String lockId;
    private final String transactionId;
    private final String resourceId;
    private final Set<String> resourceIds;
    private final long timestamp;
    private long timeout;
    private boolean acquired;
    
    public DistributedLock(String transactionId, String resourceId, Set<String> resourceIds) {
        this.lockId = UUID.randomUUID().toString();
        this.transactionId = transactionId;
        this.resourceId = resourceId;
        this.resourceIds = resourceIds;
        this.timestamp = System.currentTimeMillis();
        this.acquired = false;
    }
    
    public String getLockId() {
        return lockId;
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public String getResourceId() {
        return resourceId;
    }
    
    public Set<String> getResourceIds() {
        return resourceIds;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public long getTimeout() {
        return timeout;
    }
    
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
    
    public boolean isAcquired() {
        return acquired;
    }
    
    public void setAcquired(boolean acquired) {
        this.acquired = acquired;
    }
    
    public boolean isExpired() {
        return System.currentTimeMillis() - timestamp > timeout;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedLock that = (DistributedLock) o;
        return lockId.equals(that.lockId);
    }
    
    @Override
    public int hashCode() {
        return lockId.hashCode();
    }
    
    @Override
    public String toString() {
        return String.format("DistributedLock{id='%s', tx='%s', resource='%s', acquired=%s}",
                           lockId, transactionId, resourceId, acquired);
    }
}