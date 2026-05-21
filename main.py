#!/usr/bin/env python3
"""
Distributed Transaction Simulator - Main Entry Point
Simulator Transaksi Terdistribusi dengan Fault Tolerance dan Parallel Processing
"""

import asyncio
import sys
import logging
from pathlib import Path

# Tambahkan src ke path
sys.path.insert(0, str(Path(__file__).parent))

from src.config.system_config import SystemConfig
from src.core.transaction_manager import TransactionManager
from src.core.transaction_coordinator import TransactionCoordinator
from src.core.transaction_executor import TransactionExecutor
from src.node.node_manager import NodeManager
from src.fault.fault_injector import FaultInjector
from src.metrics.metrics_collector import MetricsCollector
from src.util.id_generator import IdGenerator

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DistributedTransactionSimulator:
    """
    Kelas utama simulator transaksi terdistribusi
    Mengelola seluruh komponen simulator
    """
    
    def __init__(self, config: SystemConfig = None):
        """
        Inisialisasi simulator dengan konfigurasi yang diberikan
        
        Args:
            config: Konfigurasi sistem, jika None akan menggunakan default
        """
        self.config = config or SystemConfig()
        self.node_manager = None
        self.transaction_manager = None
        self.transaction_coordinator = None
        self.transaction_executor = None
        self.fault_injector = None
        self.metrics_collector = None
        self.is_running = False
        
        logger.info(f"Simulator initialized dengan konfigurasi: {self.config.num_nodes} nodes")
    
    async def initialize(self):
        """Menginisialisasi semua komponen simulator"""
        logger.info("Menginisialisasi komponen simulator...")
        
        # Inisialisasi metrics collector
        self.metrics_collector = MetricsCollector(self.config)
        
        # Inisialisasi node manager
        self.node_manager = NodeManager(self.config, self.metrics_collector)
        await self.node_manager.initialize()
        
        # Inisialisasi fault injector
        self.fault_injector = FaultInjector(self.config, self.node_manager)
        
        # Inisialisasi transaction components
        self.transaction_coordinator = TransactionCoordinator(
            self.config, 
            self.node_manager,
            self.fault_injector,
            self.metrics_collector
        )
        
        self.transaction_executor = TransactionExecutor(
            self.config,
            self.node_manager,
            self.fault_injector,
            self.metrics_collector
        )
        
        self.transaction_manager = TransactionManager(
            self.config,
            self.transaction_coordinator,
            self.transaction_executor,
            self.metrics_collector
        )
        
        logger.info("Semua komponen berhasil diinisialisasi")
    
    async def start(self):
        """Memulai simulator"""
        if not self.node_manager:
            await self.initialize()
        
        self.is_running = True
        logger.info("Simulator dimulai...")
        
        # Start node manager
        await self.node_manager.start()
        
        # Start fault injector
        await self.fault_injector.start()

        # Start metrics collector
        if self.metrics_collector:
            await self.metrics_collector.start()

        # Start transaction manager
        if self.transaction_manager:
            await self.transaction_manager.start()

        logger.info("Simulator berjalan dan siap memproses transaksi")
    
    async def stop(self):
        """Menghentikan simulator"""
        self.is_running = False
        logger.info("Menghentikan simulator...")
        
        # Stop semua komponen
        if self.transaction_manager:
            await self.transaction_manager.stop()

        if self.fault_injector:
            await self.fault_injector.stop()

        if self.metrics_collector:
            await self.metrics_collector.stop()

        if self.node_manager:
            await self.node_manager.stop()
        
        # Cetak statistik akhir
        if self.metrics_collector:
            stats = self.metrics_collector.get_performance_stats()
            logger.info(f"Statistik Final:\n{stats.to_string()}")
        
        logger.info("Simulator berhenti")
    
    async def submit_transaction(self, transaction_data: dict) -> str:
        """
        Submit transaksi untuk diproses
        
        Args:
            transaction_data: Data transaksi
            
        Returns:
            Transaction ID
        """
        if not self.is_running:
            raise RuntimeError("Simulator tidak berjalan")
        
        transaction_id = IdGenerator.generate_transaction_id()
        
        # Submit ke transaction manager
        result = await self.transaction_manager.submit_transaction(
            transaction_id, 
            transaction_data
        )
        
        logger.info(f"Transaksi {transaction_id} disubmit: {result}")
        return transaction_id
    
    async def get_transaction_status(self, transaction_id: str) -> dict:
        """
        Mendapatkan status transaksi
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            Status transaksi
        """
        return await self.transaction_manager.get_transaction_status(transaction_id)
    
    async def run_benchmark(self, num_transactions: int = 100):
        """
        Menjalankan benchmark dengan jumlah transaksi tertentu
        
        Args:
            num_transactions: Jumlah transaksi yang akan dijalankan
        """
        logger.info(f"Memulai benchmark dengan {num_transactions} transaksi...")
        
        transactions = []
        for i in range(num_transactions):
            transaction_data = {
                'type': 'transfer',
                'amount': 100 + (i % 1000),
                'from_account': f'ACC_{i % 10}',
                'to_account': f'ACC_{(i + 1) % 10}',
                'timestamp': i
            }
            tx_id = await self.submit_transaction(transaction_data)
            transactions.append(tx_id)
        
        # Tunggu semua transaksi selesai
        await asyncio.sleep(5)
        
        # Kumpulkan hasil
        completed = 0
        failed = 0
        for tx_id in transactions:
            status = await self.get_transaction_status(tx_id)
            if status['status'] == 'COMMITTED':
                completed += 1
            else:
                failed += 1
        
        logger.info(f"Benchmark selesai: {completed} berhasil, {failed} gagal")
        
        return {
            'total': num_transactions,
            'completed': completed,
            'failed': failed,
            'success_rate': completed / num_transactions if num_transactions > 0 else 0
        }


async def main():
    """Fungsi utama"""
    logger.info("=" * 60)
    logger.info("Distributed Transaction Simulator v1.0")
    logger.info("Simulator Transaksi Terdistribusi dengan Fault Tolerance")
    logger.info("=" * 60)
    
    # Buat konfigurasi
    config = SystemConfig()
    config.num_nodes = 5
    config.num_partitions = 10
    config.fault_injection_enabled = True
    config.fault_probability = 0.1  # 10% chance of fault
    config.transaction_timeout_ms = 5000
    
    # Inisialisasi simulator
    simulator = DistributedTransactionSimulator(config)
    
    try:
        # Start simulator
        await simulator.start()
        
        # Jalankan test transaksi
        logger.info("\nMenjalankan test transaksi...")
        
        # Submit beberapa transaksi
        test_transactions = [
            {'type': 'deposit', 'amount': 500, 'account': 'ACC_001'},
            {'type': 'withdraw', 'amount': 200, 'account': 'ACC_001'},
            {'type': 'transfer', 'amount': 300, 'from': 'ACC_001', 'to': 'ACC_002'},
            {'type': 'deposit', 'amount': 1000, 'account': 'ACC_003'},
        ]
        
        for tx_data in test_transactions:
            tx_id = await simulator.submit_transaction(tx_data)
            logger.info(f"Transaksi {tx_id} disubmit: {tx_data}")
            await asyncio.sleep(0.5)
        
        # Tunggu sebentar untuk pemrosesan
        await asyncio.sleep(3)
        
        # Cek status transaksi
        for tx_data in test_transactions:
            # Note: Ini hanya demo, di implementasi nyata kita perlu track ID
            pass
        
        # Jalankan benchmark
        logger.info("\nMenjalankan benchmark...")
        results = await simulator.run_benchmark(20)
        
        logger.info(f"\nHasil Benchmark: {results}")
        
        # Tampilkan statistik
        stats = simulator.metrics_collector.get_performance_stats()
        logger.info(f"\nStatistik Performa:\n{stats.to_string()}")
        
        # Tampilkan failure summary
        fault_summary = simulator.fault_injector.get_fault_summary()
        logger.info(f"\nRingkasan Gagal Sistem:\n{fault_summary}")
        
        await asyncio.sleep(2)
        
    except KeyboardInterrupt:
        logger.info("Menerima interrupt signal...")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        await simulator.stop()
        logger.info("Simulator terminated")


if __name__ == "__main__":
    asyncio.run(main())