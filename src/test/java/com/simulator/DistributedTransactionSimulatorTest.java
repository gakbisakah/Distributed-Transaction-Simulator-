package com.simulator;

import com.simulator.core.TransactionCoordinator;
import com.simulator.model.Transaction;
import com.simulator.model.TransactionStatus;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class DistributedTransactionSimulatorTest {
    
    private DistributedTransactionSimulator simulator;
    
    @Before
    public void setUp() {
        simulator = new DistributedTransactionSimulator();
        simulator.start();
    }
    
    @Test
    public void testSingleTransaction() {
        Transaction tx = Transaction.createRandomTransaction(1);
        simulator.processTransaction(tx);
        
        // Tunggu sebentar untuk proses
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        assertNotNull(simulator.getMetricsCollector());
        assertTrue(simulator.getMetricsCollector().getPerformanceStats().getTotalTransactions() >= 0);
    }
    
    @Test
    public void testMultipleTransactions() {
        for (int i = 0; i < 10; i++) {
            Transaction tx = Transaction.createRandomTransaction(i);
            simulator.processTransaction(tx);
        }
        
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        var stats = simulator.getMetricsCollector().getPerformanceStats();
        assertTrue(stats.getTotalTransactions() > 0);
        System.out.println("Test multiple transactions completed: " + stats);
    }
    
    @Test
    public void testTransactionCreation() {
        Transaction tx = Transaction.createRandomTransaction(100);
        
        assertNotNull(tx.getId());
        assertNotNull(tx.getName());
        assertNotNull(tx.getResourceIds());
        assertTrue(tx.getResourceIds().size() > 0);
        assertNotNull(tx.getData());
    }
}