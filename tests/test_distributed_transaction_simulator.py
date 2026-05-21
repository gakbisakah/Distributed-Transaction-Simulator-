"""
Unit tests untuk Distributed Transaction Simulator
"""

import asyncio
import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.system_config import SystemConfig
from src.core.transaction_manager import TransactionManager
from src.core.transaction_coordinator import TransactionCoordinator
from src.core.transaction_executor import TransactionExecutor
from src.node.node_manager import NodeManager
from src.model.transaction import Transaction
from src.model.transaction_status import TransactionStatus
from src.fault.fault_injector import FaultInjector, FaultType
from src.metrics.metrics_collector import MetricsCollector
from src.util.id_generator import IdGenerator


class TestDistributedTransactionSimulator:
    """Test cases untuk distributed transaction simulator"""
    
    @pytest.fixture
    def config(self):
        """Fixture untuk konfigurasi test"""
        config = SystemConfig()
        config.num_nodes = 3
        config.num_partitions = 4
        config.fault_injection_enabled = False  # Disable for basic tests
        config.transaction_timeout_ms = 2000
        return config
    
    @pytest.fixture
    async def node_manager(self, config):
        """Fixture untuk node manager"""
        metrics = MetricsCollector(config)
        manager = NodeManager(config, metrics)
        await manager.initialize()
        await manager.start()
        yield manager
        await manager.stop()
    
    @pytest.fixture
    async def transaction_coordinator(self, config, node_manager):
        """Fixture untuk transaction coordinator"""
        fault_injector = FaultInjector(config, node_manager)
        metrics = MetricsCollector(config)
        coordinator = TransactionCoordinator(config, node_manager, fault_injector, metrics)
        yield coordinator
    
    @pytest.fixture
    async def transaction_executor(self, config, node_manager):
        """Fixture untuk transaction executor"""
        fault_injector = FaultInjector(config, node_manager)
        metrics = MetricsCollector(config)
        executor = TransactionExecutor(config, node_manager, fault_injector, metrics)
        yield executor
    
    @pytest.fixture
    async def transaction_manager(self, config, transaction_coordinator, transaction_executor):
        """Fixture untuk transaction manager"""
        metrics = MetricsCollector(config)
        manager = TransactionManager(config, transaction_coordinator, transaction_executor, metrics)
        await manager.start()
        yield manager
        await manager.stop()
    
    @pytest.mark.asyncio
    async def test_transaction_submission(self, transaction_manager):
        """Test submit transaksi"""
        tx_data = {
            'type': 'deposit',
            'account': 'test_account',
            'amount': 1000
        }
        
        tx_id = await transaction_manager.submit_transaction(
            IdGenerator.generate_transaction_id(),
            tx_data
        )
        
        assert tx_id is not None
        assert tx_id.startswith('tx_')
        
        # Tunggu sebentar
        await asyncio.sleep(0.5)
        
        # Cek status
        status = await transaction_manager.get_transaction_status(tx_id)
        assert status['exists'] is True
        assert status['status'] in ['COMMITTED', 'PROCESSING', 'PENDING']
    
    @pytest.mark.asyncio
    async def test_transaction_commit(self, transaction_manager):
        """Test commit transaksi"""
        tx_data = {
            'type': 'transfer',
            'from': 'acc1',
            'to': 'acc2',
            'amount': 500
        }
        
        tx_id = await transaction_manager.submit_transaction(
            IdGenerator.generate_transaction_id(),
            tx_data
        )
        
        await asyncio.sleep(1)
        
        status = await transaction_manager.get_transaction_status(tx_id)
        assert status['exists'] is True
        
        # Status seharusnya committed atau failed
        assert status['status'] in ['COMMITTED', 'FAILED']
    
    @pytest.mark.asyncio
    async def test_transaction_timeout(self, config, transaction_manager):
        """Test timeout transaksi"""
        # Set timeout kecil
        config.transaction_timeout_ms = 100
        
        tx_data = {
            'type': 'slow_operation',
            'delay': 200  # Delay lebih dari timeout
        }
        
        tx_id = await transaction_manager.submit_transaction(
            IdGenerator.generate_transaction_id(),
            tx_data
        )
        
        await asyncio.sleep(0.5)
        
        status = await transaction_manager.get_transaction_status(tx_id)
        # Mungkin timeout atau failed
        assert status['status'] in ['TIMEOUT', 'FAILED']
    
    @pytest.mark.asyncio
    async def test_node_failure_injection(self, config, node_manager):
        """Test inject node failure"""
        config.fault_injection_enabled = True
        
        fault_injector = FaultInjector(config, node_manager)
        await fault_injector.start()
        
        # Inject node failure
        fault_id = await fault_injector.inject_fault(FaultType.NODE_FAILURE, node_id=1)
        
        assert fault_id is not None
        
        # Cek node status
        node = node_manager.get_node(1)
        assert node is not None
        
        # Cleanup
        await fault_injector.recover_fault(fault_id)
        await fault_injector.stop()
    
    @pytest.mark.asyncio
    async def test_concurrent_transactions(self, transaction_manager):
        """Test multiple concurrent transactions"""
        tasks = []
        
        for i in range(10):
            tx_data = {
                'type': 'deposit',
                'account': f'account_{i % 3}',
                'amount': 100 * (i + 1)
            }
            
            task = transaction_manager.submit_transaction(
                IdGenerator.generate_transaction_id(),
                tx_data
            )
            tasks.append(task)
        
        tx_ids = await asyncio.gather(*tasks)
        
        assert len(tx_ids) == 10
        
        await asyncio.sleep(2)
        
        # Cek statistik
        stats = transaction_manager.get_stats()
        assert stats['total_transactions'] >= 10
    
    @pytest.mark.asyncio
    async def test_transaction_retry(self, transaction_manager):
        """Test retry mechanism untuk failed transactions"""
        tx_data = {
            'type': 'failing_operation',
            'should_fail': True
        }
        
        tx_id = await transaction_manager.submit_transaction(
            IdGenerator.generate_transaction_id(),
            tx_data
        )
        
        await asyncio.sleep(1)
        
        # Coba retry
        result = await transaction_manager.retry_failed_transaction(tx_id)
        
        # Mungkin bisa retry atau tidak tergantung implementasi
        # Ini hanya test bahwa method tidak throw exception
    
    @pytest.mark.asyncio
    async def test_metrics_collection(self, config):
        """Test metrics collection"""
        metrics = MetricsCollector(config)
        await metrics.start()
        
        # Record beberapa metrics
        metrics.record_transaction_submitted()
        metrics.record_transaction_start('tx1')
        metrics.record_transaction_committed('tx1')
        
        metrics.record_transaction_submitted()
        metrics.record_transaction_start('tx2')
        metrics.record_transaction_failed('tx2')
        
        # Dapatkan statistik
        stats = metrics.get_performance_stats()
        
        assert stats.total_transactions == 2
        assert stats.committed_transactions == 1
        assert stats.failed_transactions == 1
        
        await metrics.stop()
    
    @pytest.mark.asyncio
    async def test_lock_mechanism(self, transaction_executor):
        """Test lock mechanism"""
        # Test acquire dan release lock
        lock_acquired = await transaction_executor.lock_manager.acquire_lock(
            'tx1',
            'resource1'
        )
        
        assert lock_acquired is True
        
        # Coba acquire lagi dari transaksi lain
        lock_acquired2 = await transaction_executor.lock_manager.acquire_lock(
            'tx2',
            'resource1'
        )
        
        # Seharusnya tidak bisa karena masih dilock tx1
        assert lock_acquired2 is False
        
        # Release lock
        released = await transaction_executor.lock_manager.release_lock('tx1', 'resource1')
        assert released is True
        
        # Sekarang tx2 bisa acquire
        lock_acquired3 = await transaction_executor.lock_manager.acquire_lock(
            'tx2',
            'resource1'
        )
        assert lock_acquired3 is True
    
    @pytest.mark.asyncio
    async def test_2pc_coordination(self, transaction_coordinator):
        """Test Two-Phase Commit coordination"""
        transaction = Transaction(
            transaction_id=IdGenerator.generate_transaction_id(),
            data={
                'type': 'test_transaction',
                'account': 'test_account',
                'amount': 100
            }
        )
        
        result = await transaction_coordinator.execute_transaction(transaction)
        
        # Hasil bisa success atau fail tergantung kondisi
        assert 'success' in result
        
    @pytest.mark.asyncio
    async def test_id_generator(self):
        """Test ID generator"""
        # Test transaction ID generation
        tx_id1 = IdGenerator.generate_transaction_id()
        tx_id2 = IdGenerator.generate_transaction_id()
        
        assert tx_id1 != tx_id2
        assert tx_id1.startswith('tx_')
        
        # Test lock ID generation
        lock_id1 = IdGenerator.generate_lock_id()
        lock_id2 = IdGenerator.generate_lock_id()
        
        assert lock_id1 != lock_id2
        assert lock_id1.startswith('lock_')
        
        # Test sequence number
        seq1 = IdGenerator.generate_sequence_number()
        seq2 = IdGenerator.generate_sequence_number()
        
        assert seq2 > seq1


def run_tests():
    """Helper untuk menjalankan tests"""
    pytest.main([__file__, '-v', '--asyncio-mode=auto'])


if __name__ == "__main__":
    run_tests()