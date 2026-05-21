"""
Transaction Coordinator - Koordinator untuk distributed transactions (2PC)
"""

from __future__ import annotations
import asyncio
import logging
from typing import Dict, List, Any, Optional, TYPE_CHECKING
from datetime import datetime
from enum import Enum

from src.model.transaction import Transaction
from src.config.system_config import SystemConfig
from src.metrics.metrics_collector import MetricsCollector
from src.log.write_ahead_log import WriteAheadLog
from src.util.id_generator import IdGenerator

if TYPE_CHECKING:
    from src.node.node_manager import NodeManager
    from src.fault.fault_injector import FaultInjector

logger = logging.getLogger(__name__)


class TwoPCPhase(Enum):
    """Fase-fase dalam Two-Phase Commit protocol"""
    INIT = "INIT"
    PREPARE = "PREPARE"
    COMMIT = "COMMIT"
    ABORT = "ABORT"


class Vote(Enum):
    """Vote dari participant dalam 2PC"""
    YES = "YES"
    NO = "NO"
    TIMEOUT = "TIMEOUT"


class TransactionCoordinator:
    """
    Koordinator untuk distributed transactions menggunakan Two-Phase Commit (2PC)
    """
    
    def __init__(
        self,
        config: SystemConfig,
        node_manager: NodeManager,
        fault_injector: FaultInjector,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        """
        Inisialisasi transaction coordinator
        
        Args:
            config: Konfigurasi sistem
            node_manager: Manajer node untuk komunikasi
            fault_injector: Injector untuk simulasi fault
            metrics_collector: Kolektor metrics
        """
        self.config = config
        self.node_manager = node_manager
        self.fault_injector = fault_injector
        self.metrics = metrics_collector
        
        self.active_transactions: Dict[str, Dict] = {}
        self.write_ahead_log = WriteAheadLog(config)
        
        # Locks untuk thread safety
        self._lock = asyncio.Lock()
        
        logger.info("TransactionCoordinator initialized")
    
    async def execute_transaction(self, transaction: Transaction) -> Dict[str, Any]:
        """
        Execute transaction menggunakan Two-Phase Commit
        
        Args:
            transaction: Transaksi yang akan dieksekusi
            
        Returns:
            Dictionary hasil eksekusi
        """
        transaction_id = transaction.transaction_id
        
        # Log awal transaksi
        await self.write_ahead_log.log_transaction_start(transaction_id, transaction.data)
        
        # Tentukan participants berdasarkan data transaksi
        participants = self._determine_participants(transaction.data)
        
        # Simpan informasi transaksi
        async with self._lock:
            self.active_transactions[transaction_id] = {
                'transaction': transaction,
                'participants': participants,
                'phase': TwoPCPhase.INIT,
                'votes': {},
                'start_time': datetime.now()
            }
        
        try:
            # Fase 1: Prepare
            prepare_result = await self._phase_prepare(transaction_id, participants)
            
            if not prepare_result['success']:
                # Fase 2: Abort (jika prepare gagal)
                await self._phase_abort(transaction_id, participants)
                return {
                    'success': False,
                    'error': prepare_result.get('error', 'Prepare phase failed')
                }
            
            # Fase 2: Commit
            commit_result = await self._phase_commit(transaction_id, participants)
            
            if commit_result['success']:
                await self.write_ahead_log.log_transaction_commit(transaction_id)
                return {
                    'success': True,
                    'message': 'Transaction committed successfully'
                }
            else:
                await self.write_ahead_log.log_transaction_abort(transaction_id)
                return {
                    'success': False,
                    'error': commit_result.get('error', 'Commit phase failed')
                }
                
        except Exception as e:
            logger.error(f"Transaction {transaction_id} execution error: {e}")
            await self._phase_abort(transaction_id, participants)
            await self.write_ahead_log.log_transaction_abort(transaction_id)
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            async with self._lock:
                if transaction_id in self.active_transactions:
                    del self.active_transactions[transaction_id]
    
    def _determine_participants(self, transaction_data: dict) -> List[int]:
        """
        Menentukan participant nodes untuk transaksi
        
        Args:
            transaction_data: Data transaksi
            
        Returns:
            List node IDs yang menjadi participant
        """
        participants = set()
        
        # Untuk demo, kita gunakan hash dari data untuk menentukan participant
        # Dalam implementasi nyata, ini akan berdasarkan data partitioning
        
        account = transaction_data.get('account', transaction_data.get('from', 'default'))
        account_hash = hash(account) % self.config.num_partitions
        
        # Dapatkan owner partition
        from src.config.system_config import SystemConfig
        owner_node = self.config.get_partition_owner(account_hash)
        participants.add(owner_node)
        
        # Tambahkan node untuk 'to' account jika ada
        to_account = transaction_data.get('to')
        if to_account:
            to_hash = hash(to_account) % self.config.num_partitions
            to_node = self.config.get_partition_owner(to_hash)
            participants.add(to_node)
        
        # Tambahkan coordinator node
        participants.add(0)
        
        # Tambahkan replica nodes berdasarkan replication factor
        all_nodes = set(range(self.config.num_nodes))
        additional_nodes = set()
        
        for participant in participants:
            for i in range(self.config.replication_factor - 1):
                replica = (participant + i + 1) % self.config.num_nodes
                if replica != participant:
                    additional_nodes.add(replica)
        
        participants.update(additional_nodes)
        
        return list(participants)
    
    async def _phase_prepare(self, transaction_id: str, participants: List[int]) -> Dict[str, Any]:
        """
        Fase PREPARE dari 2PC - meminta semua participant untuk mempersiapkan
        
        Args:
            transaction_id: ID transaksi
            participants: List participant nodes
            
        Returns:
            Dictionary hasil prepare phase
        """
        logger.debug(f"Transaction {transaction_id}: Starting PREPARE phase with {participants}")
        
        async with self._lock:
            self.active_transactions[transaction_id]['phase'] = TwoPCPhase.PREPARE
        
        # Kirim prepare request ke semua participant
        prepare_tasks = []
        for node_id in participants:
            task = self._send_prepare_request(transaction_id, node_id)
            prepare_tasks.append(task)
        
        # Tunggu semua response dengan timeout
        try:
            responses = await asyncio.wait_for(
                asyncio.gather(*prepare_tasks, return_exceptions=True),
                timeout=self.config.transaction_timeout_ms / 1000
            )
        except asyncio.TimeoutError:
            logger.warning(f"Transaction {transaction_id}: PREPARE phase timeout")
            return {'success': False, 'error': 'Prepare phase timeout'}
        
        # Evaluasi votes
        all_yes = True
        failed_nodes = []
        
        for i, response in enumerate(responses):
            node_id = participants[i]
            
            if isinstance(response, Exception):
                logger.error(f"Node {node_id} prepare failed: {response}")
                all_yes = False
                failed_nodes.append(node_id)
            elif response.get('vote') == Vote.YES.value:
                async with self._lock:
                    self.active_transactions[transaction_id]['votes'][node_id] = Vote.YES
            else:
                all_yes = False
                failed_nodes.append(node_id)
                async with self._lock:
                    self.active_transactions[transaction_id]['votes'][node_id] = Vote.NO
        
        # Log prepare result
        await self.write_ahead_log.log_prepare_phase(transaction_id, all_yes, failed_nodes)
        
        return {
            'success': all_yes,
            'failed_nodes': failed_nodes,
            'error': None if all_yes else f"Failed nodes: {failed_nodes}"
        }
    
    async def _send_prepare_request(self, transaction_id: str, node_id: int) -> Dict[str, Any]:
        """
        Kirim prepare request ke participant node
        
        Args:
            transaction_id: ID transaksi
            node_id: ID node participant
            
        Returns:
            Response dari participant
        """
        # Simulasi network delay
        await asyncio.sleep(self.config.network_latency_ms / 1000)

        # Ambil data transaksi untuk simulasi delay jika ada
        transaction_data = {}
        async with self._lock:
            if transaction_id in self.active_transactions:
                transaction_data = self.active_transactions[transaction_id]['transaction'].data

        # Simulasi processing delay jika dispesifikasikan di data transaksi
        if 'delay' in transaction_data:
            delay_ms = transaction_data['delay']
            logger.debug(f"Simulating processing delay of {delay_ms}ms on node {node_id}")
            await asyncio.sleep(delay_ms / 1000)

        # Cek apakah node masih aktif
        node = self.node_manager.get_node(node_id)
        if not node or not node.is_healthy:
            logger.warning(f"Node {node_id} is not healthy")
            
            # Inject fault jika enabled
            if self.fault_injector and self.fault_injector.should_inject_fault():
                await self.fault_injector.inject_network_failure(node_id, transaction_id)
            
            return {'vote': Vote.NO.value, 'error': 'Node unhealthy'}
        
        # Inject network failure jika diperlukan
        if self.fault_injector and self.fault_injector.should_inject_fault():
            if self.fault_injector.should_drop_message():
                logger.info(f"Dropping prepare message to node {node_id}")
                return {'vote': Vote.TIMEOUT.value, 'error': 'Message dropped'}
        
        # Simulasi proses prepare di node
        try:
            # Dalam implementasi nyata, ini akan mengakses resource lokal
            result = await node.prepare_transaction(transaction_id)
            
            if result:
                return {'vote': Vote.YES.value}
            else:
                return {'vote': Vote.NO.value, 'error': 'Prepare failed'}
                
        except Exception as e:
            logger.error(f"Error preparing on node {node_id}: {e}")
            return {'vote': Vote.NO.value, 'error': str(e)}
    
    async def _phase_commit(self, transaction_id: str, participants: List[int]) -> Dict[str, Any]:
        """
        Fase COMMIT dari 2PC - meminta semua participant untuk commit
        
        Args:
            transaction_id: ID transaksi
            participants: List participant nodes
            
        Returns:
            Dictionary hasil commit phase
        """
        logger.debug(f"Transaction {transaction_id}: Starting COMMIT phase")
        
        async with self._lock:
            self.active_transactions[transaction_id]['phase'] = TwoPCPhase.COMMIT
        
        # Kirim commit request ke semua participant
        commit_tasks = []
        for node_id in participants:
            task = self._send_commit_request(transaction_id, node_id)
            commit_tasks.append(task)
        
        # Tunggu semua response
        responses = await asyncio.gather(*commit_tasks, return_exceptions=True)
        
        # Evaluasi hasil commit
        all_success = True
        failed_nodes = []
        
        for i, response in enumerate(responses):
            node_id = participants[i]
            
            if isinstance(response, Exception) or not response.get('success', False):
                all_success = False
                failed_nodes.append(node_id)
                logger.error(f"Node {node_id} commit failed: {response}")
        
        # Log commit result
        await self.write_ahead_log.log_commit_phase(transaction_id, all_success, failed_nodes)
        
        if not all_success:
            # Jika commit gagal, perlu recovery
            logger.error(f"Transaction {transaction_id}: Commit failed on nodes {failed_nodes}")
            return {
                'success': False,
                'error': f'Commit failed on nodes: {failed_nodes}'
            }
        
        return {'success': True}
    
    async def _send_commit_request(self, transaction_id: str, node_id: int) -> Dict[str, Any]:
        """
        Kirim commit request ke participant node
        
        Args:
            transaction_id: ID transaksi
            node_id: ID node participant
            
        Returns:
            Response dari participant
        """
        # Simulasi network delay
        await asyncio.sleep(self.config.network_latency_ms / 1000)
        
        # Cek node health
        node = self.node_manager.get_node(node_id)
        if not node or not node.is_healthy:
            logger.warning(f"Node {node_id} unavailable for commit")
            return {'success': False, 'error': 'Node unavailable'}
        
        # Commit transaksi di node
        try:
            success = await node.commit_transaction(transaction_id)
            return {'success': success}
        except Exception as e:
            logger.error(f"Error committing on node {node_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _phase_abort(self, transaction_id: str, participants: List[int]):
        """
        Fase ABORT dari 2PC - meminta semua participant untuk abort
        
        Args:
            transaction_id: ID transaksi
            participants: List participant nodes
        """
        logger.debug(f"Transaction {transaction_id}: Starting ABORT phase")
        
        async with self._lock:
            self.active_transactions[transaction_id]['phase'] = TwoPCPhase.ABORT
        
        # Kirim abort request ke semua participant
        abort_tasks = []
        for node_id in participants:
            task = self._send_abort_request(transaction_id, node_id)
            abort_tasks.append(task)
        
        # Tunggu semua response (ignore failures)
        await asyncio.gather(*abort_tasks, return_exceptions=True)
        
        # Log abort
        await self.write_ahead_log.log_abort_phase(transaction_id)
        
        logger.debug(f"Transaction {transaction_id}: ABORT phase completed")
    
    async def _send_abort_request(self, transaction_id: str, node_id: int) -> Dict[str, Any]:
        """
        Kirim abort request ke participant node
        
        Args:
            transaction_id: ID transaksi
            node_id: ID node participant
            
        Returns:
            Response dari participant
        """
        # Simulasi network delay
        await asyncio.sleep(self.config.network_latency_ms / 1000)
        
        node = self.node_manager.get_node(node_id)
        if node and node.is_healthy:
            try:
                await node.abort_transaction(transaction_id)
            except Exception as e:
                logger.error(f"Error aborting on node {node_id}: {e}")
        
        return {'success': True}  # Selalu return success untuk abort
    
    async def recover_transaction(self, transaction_id: str):
        """
        Recovery untuk transaksi yang terputus
        
        Args:
            transaction_id: ID transaksi yang akan direcovery
        """
        # Load log terakhir untuk transaksi
        last_phase = await self.write_ahead_log.get_last_phase(transaction_id)
        
        if last_phase is None:
            logger.warning(f"No log found for transaction {transaction_id}")
            return
        
        if last_phase == TwoPCPhase.COMMIT.value:
            # Transaksi sudah di-commit, pastikan semua participant commit
            if transaction_id in self.active_transactions:
                participants = self.active_transactions[transaction_id]['participants']
                await self._phase_commit(transaction_id, participants)
        elif last_phase == TwoPCPhase.PREPARE.value:
            # Transaksi dalam prepare phase, cek votes
            if transaction_id in self.active_transactions:
                votes = self.active_transactions[transaction_id]['votes']
                if all(v == Vote.YES for v in votes.values()):
                    await self._phase_commit(transaction_id, 
                                           self.active_transactions[transaction_id]['participants'])
                else:
                    await self._phase_abort(transaction_id,
                                          self.active_transactions[transaction_id]['participants'])
        else:
            # Default: abort
            if transaction_id in self.active_transactions:
                await self._phase_abort(transaction_id,
                                      self.active_transactions[transaction_id]['participants'])
        
        logger.info(f"Transaction {transaction_id} recovery completed")