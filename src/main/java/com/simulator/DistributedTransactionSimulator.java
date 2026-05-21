package com.simulator;

import com.simulator.config.SystemConfig;
import com.simulator.core.TransactionCoordinator;
import com.simulator.core.TransactionManager;
import com.simulator.fault.FaultInjector;
import com.simulator.metrics.MetricsCollector;
import com.simulator.model.Transaction;
import com.simulator.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Kelas utama untuk menjalankan simulator distributed transaction
 * dengan kemampuan fault tolerance dan parallel processing
 */
public class DistributedTransactionSimulator {
    private static final Logger logger = LoggerFactory.getLogger(DistributedTransactionSimulator.class);
    
    private final SystemConfig config;
    private final NodeManager nodeManager;
    private final TransactionManager transactionManager;
    private final TransactionCoordinator coordinator;
    private final FaultInjector faultInjector;
    private final MetricsCollector metricsCollector;
    private final ExecutorService executorService;
    
    public DistributedTransactionSimulator() {
        this.config = new SystemConfig();
        this.nodeManager = new NodeManager(config);
        this.transactionManager = new TransactionManager(config);
        this.coordinator = new TransactionCoordinator(config, nodeManager, transactionManager);
        this.faultInjector = new FaultInjector(config, nodeManager);
        this.metricsCollector = new MetricsCollector();
        this.executorService = Executors.newFixedThreadPool(config.getMaxParallelTransactions());
    }
    
    /**
     * Memulai simulator
     */
    public void start() {
        logger.info("Memulai Distributed Transaction Simulator...");
        logger.info("Konfigurasi: {}", config);
        
        // Inisialisasi semua node
        nodeManager.initializeAllNodes();
        
        // Memulai fault injector untuk simulasi kegagalan
        faultInjector.startRandomFaultInjection();
        
        // Memulai monitoring metrics
        metricsCollector.startCollection();
        
        logger.info("Simulator siap menerima transaksi");
    }
    
    /**
     * Memproses transaksi secara parallel
     */
    public void processTransaction(Transaction transaction) {
        executorService.submit(() -> {
            try {
                logger.info("Memproses transaksi: {}", transaction.getId());
                long startTime = System.currentTimeMillis();
                
                // Koordinasi transaksi distributed
                boolean success = coordinator.executeTransaction(transaction);
                
                long endTime = System.currentTimeMillis();
                long duration = endTime - startTime;
                
                // Kumpulkan metrics
                metricsCollector.recordTransaction(transaction, success, duration);
                
                if (success) {
                    logger.info("Transaksi {} berhasil dalam {} ms", transaction.getId(), duration);
                } else {
                    logger.warn("Transaksi {} gagal dalam {} ms", transaction.getId(), duration);
                }
            } catch (Exception e) {
                logger.error("Error memproses transaksi {}: {}", transaction.getId(), e.getMessage());
                metricsCollector.recordFailedTransaction(transaction);
            }
        });
    }
    
    /**
     * Menghentikan simulator dengan graceful shutdown
     */
    public void shutdown() {
        logger.info("Menghentikan simulator...");
        
        faultInjector.stop();
        metricsCollector.stopCollection();
        executorService.shutdown();
        
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("Simulator dihentikan");
        metricsCollector.printFinalReport();
    }
    
    /**
     * Mendapatkan metrics collector untuk monitoring
     */
    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }
    
    /**
     * Main method untuk menjalankan simulator
     */
    public static void main(String[] args) {
        DistributedTransactionSimulator simulator = new DistributedTransactionSimulator();
        
        // Tambahkan shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(simulator::shutdown));
        
        // Start simulator
        simulator.start();
        
        // Simulasi beban kerja
        simulateWorkload(simulator);
    }
    
    /**
     * Simulasi beban kerja dengan berbagai tipe transaksi
     */
    private static void simulateWorkload(DistributedTransactionSimulator simulator) {
        ExecutorService workloadExecutor = Executors.newFixedThreadPool(10);
        
        for (int i = 0; i < 100; i++) {
            final int transactionId = i;
            workloadExecutor.submit(() -> {
                Transaction tx = Transaction.createRandomTransaction(transactionId);
                simulator.processTransaction(tx);
                
                // Simulasi interval antar transaksi
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        
        workloadExecutor.shutdown();
        try {
            workloadExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}