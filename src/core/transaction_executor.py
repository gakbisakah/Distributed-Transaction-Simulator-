"""
Transaction Executor - Eksekutor untuk operasi transaksi di node lokal
"""

from __future__ import annotations
import asyncio
import logging
from typing import Dict, Any, Optional, TYPE_CHECKING
from datetime import datetime

from src.model.transaction import Transaction
from src.model.transaction_status import TransactionStatus
from src.config.system_config import SystemConfig
from src.fault.fault_injector import FaultInjector
from src.metrics.metrics_collector import MetricsCollector
from src.lock.lock_manager import LockManager
from src.log.write_ahead_log import WriteAheadLog

if TYPE_CHECKING:
    from src.node.node_manager import NodeManager

logger = logging.getLogger(__name__)


class TransactionExecutor:
    """
    Eksekutor untuk menjalankan operasi transaksi secara lokal
    """
    
    def __init__(
        self,
        config: SystemConfig,
        node_manager: NodeManager,
        fault_injector: FaultInjector,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        """
        Inisialisasi transaction executor
        
        Args:
            config: Konfigurasi sistem
            node_manager: Manajer node
            fault_injector: Injector fault
            metrics_collector: Kolektor metrics
        """
        self.config = config
        self.node_manager = node_manager
        self.fault_injector = fault_injector
        self.metrics = metrics_collector
        
        self.lock_manager = LockManager(config)
        self.write_ahead_log = WriteAheadLog(config)
        
        # Simulasi data storage
        self.data_store: Dict[str, Dict[str, Any]] = {}
        self.prepared_transactions: Dict[str, Dict] = {}
        
        logger.info("TransactionExecutor initialized")
    
    async def execute_local_transaction(self, transaction: Transaction) -> bool:
        """
        Execute transaksi secara lokal pada node ini
        
        Args:
            transaction: Transaksi yang akan dieksekusi
            
        Returns:
            True jika berhasil, False jika gagal
        """
        transaction_id = transaction.transaction_id
        
        try:
            # Log start
            await self.write_ahead_log.log_local_execution_start(transaction_id)
            
            # Dapatkan locks
            locks_acquired = await self._acquire_locks(transaction)
            if not locks_acquired:
                logger.warning(f"Failed to acquire locks for transaction {transaction_id}")
                return False
            
            # Execute operasi
            result = await self._execute_operations(transaction)
            
            if result:
                await self.write_ahead_log.log_local_execution_commit(transaction_id)
            else:
                await self.write_ahead_log.log_local_execution_abort(transaction_id)
            
            # Release locks
            await self._release_locks(transaction)
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing transaction {transaction_id}: {e}")
            await self.write_ahead_log.log_local_execution_abort(transaction_id)
            await self._release_locks(transaction)
            return False
    
    async def _acquire_locks(self, transaction: Transaction) -> bool:
        """
        Acquire locks untuk resource yang diperlukan transaksi
        
        Args:
            transaction: Transaksi yang memerlukan locks
            
        Returns:
            True jika semua locks berhasil diacquire
        """
        resources = self._get_required_resources(transaction.data)
        
        for resource in resources:
            acquired = await self.lock_manager.acquire_lock(
                transaction.transaction_id,
                resource,
                transaction.data.get('lock_type', 'WRITE')
            )
            
            if not acquired:
                # Release locks yang sudah diacquire
                for r in resources[:resources.index(resource)]:
                    await self.lock_manager.release_lock(transaction.transaction_id, r)
                return False
        
        return True
    
    async def _release_locks(self, transaction: Transaction):
        """
        Release semua locks yang dipegang transaksi
        
        Args:
            transaction: Transaksi yang melepas locks
        """
        resources = self._get_required_resources(transaction.data)
        
        for resource in resources:
            await self.lock_manager.release_lock(transaction.transaction_id, resource)
    
    def _get_required_resources(self, data: dict) -> list:
        """
        Mendapatkan resource keys yang diperlukan untuk transaksi
        
        Args:
            data: Data transaksi
            
        Returns:
            List resource keys
        """
        resources = []
        
        if 'account' in data:
            resources.append(f"account:{data['account']}")
        
        if 'from' in data:
            resources.append(f"account:{data['from']}")
        
        if 'to' in data:
            resources.append(f"account:{data['to']}")
        
        if not resources:
            # Default resource berdasarkan transaction type
            resources.append(f"type:{data.get('type', 'default')}")
        
        return resources
    
    async def _execute_operations(self, transaction: Transaction) -> bool:
        """
        Execute operasi transaksi pada data store
        
        Args:
            transaction: Transaksi yang akan dieksekusi
            
        Returns:
            True jika berhasil
        """
        data = transaction.data
        tx_type = data.get('type', 'unknown')
        
        # Simulasi operasi database
        try:
            if tx_type == 'deposit':
                return await self._execute_deposit(data)
            elif tx_type == 'withdraw':
                return await self._execute_withdraw(data)
            elif tx_type == 'transfer':
                return await self._execute_transfer(data)
            else:
                return await self._execute_generic_operation(data)
                
        except Exception as e:
            logger.error(f"Operation execution error: {e}")
            return False
    
    async def _execute_deposit(self, data: dict) -> bool:
        """
        Execute deposit operation
        
        Args:
            data: Data deposit
            
        Returns:
            True jika berhasil
        """
        account = data.get('account')
        amount = data.get('amount', 0)
        
        if not account or amount <= 0:
            return False
        
        # Simulasi delay operasi
        await asyncio.sleep(0.01)
        
        # Update data store
        if account not in self.data_store:
            self.data_store[account] = {'balance': 0}
        
        self.data_store[account]['balance'] += amount
        self.data_store[account]['last_updated'] = datetime.now()
        
        logger.debug(f"Deposited {amount} to {account}. New balance: {self.data_store[account]['balance']}")
        return True
    
    async def _execute_withdraw(self, data: dict) -> bool:
        """
        Execute withdraw operation
        
        Args:
            data: Data withdraw
            
        Returns:
            True jika berhasil
        """
        account = data.get('account')
        amount = data.get('amount', 0)
        
        if not account or amount <= 0:
            return False
        
        # Simulasi delay
        await asyncio.sleep(0.01)
        
        # Cek saldo
        if account not in self.data_store:
            return False
        
        if self.data_store[account]['balance'] < amount:
            logger.warning(f"Insufficient balance in {account}")
            return False
        
        self.data_store[account]['balance'] -= amount
        self.data_store[account]['last_updated'] = datetime.now()
        
        logger.debug(f"Withdrew {amount} from {account}. New balance: {self.data_store[account]['balance']}")
        return True
    
    async def _execute_transfer(self, data: dict) -> bool:
        """
        Execute transfer operation between accounts
        
        Args:
            data: Data transfer
            
        Returns:
            True jika berhasil
        """
        from_account = data.get('from')
        to_account = data.get('to')
        amount = data.get('amount', 0)
        
        if not from_account or not to_account or amount <= 0:
            return False
        
        # Execute withdraw dari source
        withdraw_data = {'account': from_account, 'amount': amount, 'type': 'withdraw'}
        withdraw_success = await self._execute_withdraw(withdraw_data)
        
        if not withdraw_success:
            return False
        
        # Execute deposit ke destination
        deposit_data = {'account': to_account, 'amount': amount, 'type': 'deposit'}
        deposit_success = await self._execute_deposit(deposit_data)
        
        if not deposit_success:
            # Rollback withdraw
            rollback_data = {'account': from_account, 'amount': amount, 'type': 'deposit'}
            await self._execute_deposit(rollback_data)
            return False
        
        logger.debug(f"Transferred {amount} from {from_account} to {to_account}")
        return True
    
    async def _execute_generic_operation(self, data: dict) -> bool:
        """
        Execute generic operation
        
        Args:
            data: Data operasi
            
        Returns:
            True jika berhasil
        """
        # Simulasi generic operation
        await asyncio.sleep(0.005)
        
        key = data.get('key', 'default')
        value = data.get('value')
        
        if value is not None:
            self.data_store[key] = {'value': value, 'updated_at': datetime.now()}
            return True
        
        return key in self.data_store
    
    async def prepare_transaction(self, transaction_id: str, transaction_data: dict) -> bool:
        """
        Prepare transaction untuk 2PC
        
        Args:
            transaction_id: ID transaksi
            transaction_data: Data transaksi
            
        Returns:
            True jika prepare berhasil
        """
        # Cek apakah sudah diprepare
        if transaction_id in self.prepared_transactions:
            return True
        
        # Simulasi pengecekan resource
        resources = self._get_required_resources(transaction_data)
        
        for resource in resources:
            if resource in self.prepared_transactions:
                # Resource sedang digunakan
                return False
        
        # Prepare transaction
        self.prepared_transactions[transaction_id] = {
            'data': transaction_data,
            'resources': resources,
            'prepared_at': datetime.now()
        }
        
        # Log prepare
        await self.write_ahead_log.log_prepare(transaction_id, transaction_data)
        
        logger.debug(f"Transaction {transaction_id} prepared")
        return True
    
    async def commit_transaction(self, transaction_id: str) -> bool:
        """
        Commit transaction yang sudah diprepare
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            True jika commit berhasil
        """
        if transaction_id not in self.prepared_transactions:
            logger.warning(f"Transaction {transaction_id} not found in prepared state")
            return False
        
        prepared = self.prepared_transactions[transaction_id]
        
        # Execute operation
        # Buat transaction object sementara
        temp_tx = Transaction(
            transaction_id=transaction_id,
            data=prepared['data'],
            status=TransactionStatus.PROCESSING,
            created_at=datetime.now()
        )
        
        success = await self._execute_operations(temp_tx)
        
        if success:
            # Commit log
            await self.write_ahead_log.log_commit(transaction_id)
            
            # Hapus dari prepared
            del self.prepared_transactions[transaction_id]
            
            logger.debug(f"Transaction {transaction_id} committed")
            return True
        else:
            logger.error(f"Transaction {transaction_id} commit failed")
            return False
    
    async def abort_transaction(self, transaction_id: str) -> bool:
        """
        Abort transaction yang sudah diprepare
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            True jika abort berhasil
        """
        if transaction_id in self.prepared_transactions:
            # Log abort
            await self.write_ahead_log.log_abort(transaction_id)
            
            # Hapus dari prepared
            del self.prepared_transactions[transaction_id]
            
            logger.debug(f"Transaction {transaction_id} aborted")
        
        return True
    
    async def get_data(self, key: str) -> Optional[Any]:
        """
        Mendapatkan data dari data store
        
        Args:
            key: Key data
            
        Returns:
            Value data atau None jika tidak ditemukan
        """
        if key in self.data_store:
            return self.data_store[key].get('value')
        return None
    
    async def get_balance(self, account: str) -> int:
        """
        Mendapatkan balance akun
        
        Args:
            account: Account ID
            
        Returns:
            Balance akun
        """
        if account in self.data_store:
            return self.data_store[account].get('balance', 0)
        return 0
    
    def get_stats(self) -> dict:
        """
        Mendapatkan statistik executor
        
        Returns:
            Dictionary statistik
        """
        return {
            'data_store_size': len(self.data_store),
            'prepared_transactions': len(self.prepared_transactions),
            'active_locks': self.lock_manager.get_active_lock_count()
        }