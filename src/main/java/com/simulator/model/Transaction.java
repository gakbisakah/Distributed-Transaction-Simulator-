package com.simulator.model;

import com.simulator.util.IdGenerator;

import java.util.*;

/**
 * Model untuk merepresentasikan transaksi distributed
 */
public class Transaction {
    private final String id;
    private final String name;
    private final Map<String, Object> data;
    private final Set<String> resourceIds;
    private TransactionStatus status;
    private long startTime;
    private long endTime;
    private int priority;
    
    public Transaction(String id, String name) {
        this.id = id;
        this.name = name;
        this.data = new HashMap<>();
        this.resourceIds = new HashSet<>();
        this.status = TransactionStatus.INITIALIZED;
        this.priority = 1;
    }
    
    /**
     * Membuat transaksi random untuk simulasi
     */
    public static Transaction createRandomTransaction(int seed) {
        String id = IdGenerator.generateTransactionId();
        String name = "TX-" + seed;
        Transaction tx = new Transaction(id, name);
        
        // Tambahkan resource random
        Random random = new Random(seed);
        int numResources = random.nextInt(3) + 1; // 1-3 resources
        for (int i = 0; i < numResources; i++) {
            tx.addResource("resource-" + random.nextInt(10));
        }
        
        // Tambahkan data random
        tx.setData("amount", random.nextDouble() * 1000);
        tx.setData("type", random.nextBoolean() ? "DEBIT" : "CREDIT");
        tx.setData("timestamp", System.currentTimeMillis());
        
        // Priority berdasarkan seed
        tx.setPriority(random.nextInt(5) + 1);
        
        return tx;
    }
    
    // Getters and Setters
    public String getId() {
        return id;
    }
    
    public String getName() {
        return name;
    }
    
    public Map<String, Object> getData() {
        return Collections.unmodifiableMap(data);
    }
    
    public void setData(String key, Object value) {
        this.data.put(key, value);
    }
    
    public Set<String> getResourceIds() {
        return Collections.unmodifiableSet(resourceIds);
    }
    
    public void addResource(String resourceId) {
        this.resourceIds.add(resourceId);
    }
    
    public TransactionStatus getStatus() {
        return status;
    }
    
    public void setStatus(TransactionStatus status) {
        this.status = status;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    
    public long getEndTime() {
        return endTime;
    }
    
    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }
    
    public int getPriority() {
        return priority;
    }
    
    public void setPriority(int priority) {
        this.priority = priority;
    }
    
    public long getDuration() {
        if (endTime > 0 && startTime > 0) {
            return endTime - startTime;
        }
        return 0;
    }
    
    @Override
    public String toString() {
        return String.format("Transaction{id='%s', name='%s', status=%s, resources=%d, priority=%d}",
                           id, name, status, resourceIds.size(), priority);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Objects.equals(id, that.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}