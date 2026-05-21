"""
Transaction Manager - Mengelola siklus hidup transaksi
"""

import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from collections import defaultdict

from src.model.transaction import Transaction
from src.model.transaction_status import TransactionStatus
from src.config.system_config import SystemConfig
from src.core.transaction_coordinator import TransactionCoordinator
from src.core.transaction_executor import TransactionExecutor
from src.metrics.metrics_collector import MetricsCollector
from src.util.id_generator import IdGenerator
from src.util.timeout_manager import TimeoutManager

logger = logging.getLogger(__name__)


class TransactionManager:
    """
    Manajer transaksi utama - mengelola semua aspek pemrosesan transaksi
    """
    
    def __init__(
        self,
        config: SystemConfig,
        coordinator: TransactionCoordinator,
        executor: TransactionExecutor,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        """
        Inisialisasi transaction manager
        
        Args:
            config: Konfigurasi sistem
            coordinator: Coordinator untuk distributed transactions
            executor: Executor untuk menjalankan transaksi
            metrics_collector: Kolektor metrics
        """
        self.config = config
        self.coordinator = coordinator
        self.executor = executor
        self.metrics = metrics_collector
        
        self.transactions: Dict[str, Transaction] = {}
        self.pending_queue: asyncio.Queue = asyncio.Queue()
        self.active_transactions: Dict[str, asyncio.Task] = {}
        
        self.timeout_manager = TimeoutManager()
        self.is_running = False
        self.processor_task: Optional[asyncio.Task] = None
        
        logger.info("TransactionManager initialized")
    
    async def start(self):
        """Memulai transaction manager"""
        self.is_running = True
        self.processor_task = asyncio.create_task(self._process_queue())
        logger.info("TransactionManager started")
    
    async def stop(self):
        """Menghentikan transaction manager"""
        self.is_running = False
        
        if self.processor_task:
            self.processor_task.cancel()
            try:
                await self.processor_task
            except asyncio.CancelledError:
                pass
        
        # Cancel semua active transactions
        for tx_id, task in self.active_transactions.items():
            task.cancel()
        
        # Tunggu semua transaksi selesai
        if self.active_transactions:
            await asyncio.gather(*self.active_transactions.values(), return_exceptions=True)
        
        logger.info("TransactionManager stopped")
    
    async def submit_transaction(self, transaction_id: str, data: dict) -> str:
        """
        Submit transaksi untuk diproses
        
        Args:
            transaction_id: ID transaksi
            data: Data transaksi
            
        Returns:
            Transaction ID
        """
        transaction = Transaction(
            transaction_id=transaction_id,
            data=data,
            status=TransactionStatus.PENDING,
            created_at=datetime.now()
        )
        
        self.transactions[transaction_id] = transaction
        
        # Masukkan ke queue untuk diproses
        await self.pending_queue.put(transaction)
        
        # Set timeout
        self.timeout_manager.set_timeout(
            transaction_id,
            self.config.transaction_timeout_ms / 1000,
            self._handle_transaction_timeout,
            args=(transaction_id,)
        )
        
        if self.metrics:
            self.metrics.record_transaction_submitted()
        
        logger.debug(f"Transaction {transaction_id} submitted and queued")
        return transaction_id
    
    async def _process_queue(self):
        """Memproses queue transaksi secara paralel"""
        workers = []
        
        for i in range(self.config.core_pool_size):
            worker = asyncio.create_task(self._worker(i))
            workers.append(worker)
        
        try:
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            for worker in workers:
                worker.cancel()
            raise
    
    async def _worker(self, worker_id: int):
        """
        Worker thread untuk memproses transaksi dari queue
        
        Args:
            worker_id: ID worker
        """
        logger.debug(f"Worker {worker_id} started")
        
        while self.is_running:
            try:
                # Ambil transaksi dari queue dengan timeout
                transaction = await asyncio.wait_for(
                    self.pending_queue.get(),
                    timeout=1.0
                )
                
                # Proses transaksi
                await self._process_transaction(transaction)
                
                # Tandai queue task selesai
                self.pending_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
        
        logger.debug(f"Worker {worker_id} stopped")
    
    async def _process_transaction(self, transaction: Transaction):
        """
        Memproses single transaction
        
        Args:
            transaction: Transaksi yang akan diproses
        """
        start_time = datetime.now()
        transaction.status = TransactionStatus.PROCESSING
        
        try:
            # Coba eksekusi dengan coordinator
            result = await self.coordinator.execute_transaction(transaction)
            
            if result['success']:
                transaction.status = TransactionStatus.COMMITTED
                transaction.committed_at = datetime.now()
                
                if self.metrics:
                    self.metrics.record_transaction_committed()
            else:
                transaction.status = TransactionStatus.FAILED
                transaction.error_message = result.get('error', 'Unknown error')
                
                if self.metrics:
                    self.metrics.record_transaction_failed()
            
        except Exception as e:
            transaction.status = TransactionStatus.FAILED
            transaction.error_message = str(e)
            
            if self.metrics:
                self.metrics.record_transaction_failed()
            
            logger.error(f"Transaction {transaction.transaction_id} failed: {e}")
        
        finally:
            transaction.completed_at = datetime.now()
            transaction.processing_time_ms = (
                (transaction.completed_at - start_time).total_seconds() * 1000
            )
            
            # Hapus timeout
            self.timeout_manager.cancel_timeout(transaction.transaction_id)
            
            # Hapus dari active transactions
            if transaction.transaction_id in self.active_transactions:
                del self.active_transactions[transaction.transaction_id]
            
            logger.info(
                f"Transaction {transaction.transaction_id} completed "
                f"with status {transaction.status.value} "
                f"in {transaction.processing_time_ms:.2f}ms"
            )
    
    def _handle_transaction_timeout(self, transaction_id: str):
        """
        Handle timeout transaksi
        
        Args:
            transaction_id: ID transaksi yang timeout
        """
        if transaction_id in self.transactions:
            transaction = self.transactions[transaction_id]
            
            if transaction.status == TransactionStatus.PROCESSING:
                transaction.status = TransactionStatus.TIMEOUT
                transaction.error_message = "Transaction timeout"
                
                logger.warning(f"Transaction {transaction_id} timed out")
                
                if self.metrics:
                    self.metrics.record_transaction_timeout()
    
    async def get_transaction_status(self, transaction_id: str) -> dict:
        """
        Mendapatkan status transaksi
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            Dictionary berisi status transaksi
        """
        if transaction_id not in self.transactions:
            return {
                'exists': False,
                'status': 'NOT_FOUND',
                'message': 'Transaction not found'
            }
        
        transaction = self.transactions[transaction_id]
        
        return {
            'exists': True,
            'transaction_id': transaction.transaction_id,
            'status': transaction.status.value,
            'created_at': transaction.created_at.isoformat() if transaction.created_at else None,
            'completed_at': transaction.completed_at.isoformat() if transaction.completed_at else None,
            'processing_time_ms': transaction.processing_time_ms,
            'error_message': transaction.error_message,
            'retry_count': transaction.retry_count
        }
    
    async def retry_failed_transaction(self, transaction_id: str) -> bool:
        """
        Mencoba ulang transaksi yang gagal
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            True jika berhasil di-retry, False jika tidak
        """
        if transaction_id not in self.transactions:
            logger.warning(f"Cannot retry: Transaction {transaction_id} not found")
            return False
        
        transaction = self.transactions[transaction_id]
        
        if transaction.retry_count >= self.config.max_retry_count:
            logger.warning(f"Transaction {transaction_id} exceeded max retry count")
            return False
        
        transaction.retry_count += 1
        transaction.status = TransactionStatus.PENDING
        transaction.error_message = None
        
        # Submit ulang ke queue
        await self.pending_queue.put(transaction)
        
        logger.info(f"Transaction {transaction_id} queued for retry (attempt {transaction.retry_count})")
        return True
    
    def get_stats(self) -> dict:
        """
        Mendapatkan statistik transaction manager
        
        Returns:
            Dictionary statistik
        """
        total = len(self.transactions)
        committed = sum(1 for tx in self.transactions.values() 
                       if tx.status == TransactionStatus.COMMITTED)
        failed = sum(1 for tx in self.transactions.values() 
                    if tx.status == TransactionStatus.FAILED)
        pending = sum(1 for tx in self.transactions.values() 
                     if tx.status == TransactionStatus.PENDING)
        processing = sum(1 for tx in self.transactions.values() 
                        if tx.status == TransactionStatus.PROCESSING)
        
        return {
            'total_transactions': total,
            'committed': committed,
            'failed': failed,
            'pending': pending,
            'processing': processing,
            'queue_size': self.pending_queue.qsize(),
            'active_workers': len(self.active_transactions)
        }