"""
Recovery Manager - Mengelola recovery dari kegagalan sistem
"""

from __future__ import annotations
import asyncio
import logging
from typing import Dict, List, Optional, Set, TYPE_CHECKING
from datetime import datetime

from src.config.system_config import SystemConfig
from src.model.node_status import NodeStatus
from src.log.write_ahead_log import WriteAheadLog

if TYPE_CHECKING:
    from src.node.node_manager import NodeManager

logger = logging.getLogger(__name__)


class RecoveryManager:
    """
    Manajer recovery untuk memulihkan sistem setelah kegagalan
    """
    
    def __init__(self, config: SystemConfig, node_manager: NodeManager):
        """
        Inisialisasi recovery manager
        
        Args:
            config: Konfigurasi sistem
            node_manager: Manajer node
        """
        self.config = config
        self.node_manager = node_manager
        self.write_ahead_log = WriteAheadLog(config)
        
        self.recovering_nodes: Set[int] = set()
        self.recovery_tasks: Dict[int, asyncio.Task] = {}
        
        # Statistik
        self.recovery_count = 0
        self.successful_recoveries = 0
        self.failed_recoveries = 0
        
        logger.info("RecoveryManager initialized")
    
    async def recover_node(self, node_id: int) -> bool:
        """
        Recovery node yang gagal
        
        Args:
            node_id: ID node yang akan direcovery
            
        Returns:
            True jika recovery berhasil
        """
        if node_id in self.recovering_nodes:
            logger.warning(f"Node {node_id} is already being recovered")
            return False
        
        logger.info(f"Starting recovery for node {node_id}")
        self.recovering_nodes.add(node_id)
        
        try:
            # Create recovery task
            recovery_task = asyncio.create_task(self._perform_node_recovery(node_id))
            self.recovery_tasks[node_id] = recovery_task
            
            # Wait for recovery with timeout
            result = await asyncio.wait_for(
                recovery_task,
                timeout=self.config.recovery_timeout_ms / 1000
            )
            
            if result:
                self.successful_recoveries += 1
                logger.info(f"Node {node_id} recovered successfully")
            else:
                self.failed_recoveries += 1
                logger.error(f"Node {node_id} recovery failed")
            
            return result
            
        except asyncio.TimeoutError:
            logger.error(f"Node {node_id} recovery timed out")
            self.failed_recoveries += 1
            return False
            
        except Exception as e:
            logger.error(f"Error during node {node_id} recovery: {e}")
            self.failed_recoveries += 1
            return False
            
        finally:
            self.recovering_nodes.discard(node_id)
            self.recovery_tasks.pop(node_id, None)
            self.recovery_count += 1
    
    async def _perform_node_recovery(self, node_id: int) -> bool:
        """
        Perform actual node recovery process
        
        Args:
            node_id: ID node yang direcovery
            
        Returns:
            True jika recovery berhasil
        """
        # Step 1: Restart node
        node = self.node_manager.get_node(node_id)
        if not node:
            logger.error(f"Node {node_id} not found")
            return False
        
        await node.restart()
        
        # Step 2: Load WAL for pending transactions
        pending_transactions = await self.write_ahead_log.get_pending_transactions(node_id)
        
        # Step 3: Replay transactions
        for tx_id, tx_data in pending_transactions:
            try:
                await self._replay_transaction(node_id, tx_id, tx_data)
            except Exception as e:
                logger.error(f"Failed to replay transaction {tx_id}: {e}")
                # Continue with other transactions
        
        # Step 4: Sync with coordinator
        await self._sync_with_coordinator(node_id)
        
        # Step 5: Mark node as healthy
        node.status = NodeStatus.ACTIVE
        node.is_healthy = True
        node.failure_count = 0
        
        return True
    
    async def _replay_transaction(self, node_id: int, transaction_id: str, transaction_data: dict):
        """
        Replay transaction dari WAL
        
        Args:
            node_id: ID node
            transaction_id: ID transaksi
            transaction_data: Data transaksi
        """
        logger.debug(f"Replaying transaction {transaction_id} on node {node_id}")
        
        # Replay berdasarkan tipe log
        log_entries = await self.write_ahead_log.get_transaction_logs(transaction_id)
        
        for entry in log_entries:
            if entry.log_type == 'PREPARE':
                # Prepare ulang
                await self.node_manager.prepare_transaction_on_node(node_id, transaction_id, transaction_data)
            elif entry.log_type == 'COMMIT':
                # Commit ulang
                await self.node_manager.commit_transaction_on_node(node_id, transaction_id)
    
    async def _sync_with_coordinator(self, node_id: int):
        """
        Sinkronisasi node dengan coordinator
        
        Args:
            node_id: ID node yang disinkronisasi
        """
        coordinator_id = self.config.coordinator_nodes[0] if self.config.coordinator_nodes else 0
        
        # Request current cluster state dari coordinator
        cluster_state = await self.node_manager.request_cluster_state(coordinator_id)
        
        # Update node's state berdasarkan cluster state
        await self.node_manager.update_cluster_state(node_id, cluster_state)
        
        logger.debug(f"Node {node_id} synced with coordinator")
    
    async def recover_cluster(self) -> bool:
        """
        Recovery seluruh cluster setelah major failure
        
        Returns:
            True jika recovery berhasil
        """
        logger.info("Starting full cluster recovery")
        
        failed_nodes = []
        for node_id in range(self.config.num_nodes):
            node = self.node_manager.get_node(node_id)
            if node and node.status == NodeStatus.FAILED:
                failed_nodes.append(node_id)
        
        if not failed_nodes:
            logger.info("No failed nodes to recover")
            return True
        
        # Recover nodes in parallel (with limit)
        semaphore = asyncio.Semaphore(3)  # Max 3 concurrent recoveries
        
        async def recover_with_limit(node_id: int):
            async with semaphore:
                return await self.recover_node(node_id)
        
        tasks = [recover_with_limit(node_id) for node_id in failed_nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if r is True)
        
        logger.info(f"Cluster recovery completed: {success_count}/{len(failed_nodes)} nodes recovered")
        
        return success_count == len(failed_nodes)
    
    async def recover_transaction(self, transaction_id: str) -> bool:
        """
        Recovery single transaction yang terputus
        
        Args:
            transaction_id: ID transaksi yang direcovery
            
        Returns:
            True jika recovery berhasil
        """
        logger.info(f"Recovering transaction {transaction_id}")
        
        # Load transaction log
        log_entries = await self.write_ahead_log.get_transaction_logs(transaction_id)
        
        if not log_entries:
            logger.warning(f"No logs found for transaction {transaction_id}")
            return False
        
        # Determine last phase
        last_phase = None
        for entry in log_entries:
            if entry.log_type in ['COMMIT', 'ABORT']:
                last_phase = entry.log_type
        
        if last_phase == 'COMMIT':
            # Transaction was committed, ensure all participants committed
            await self._ensure_transaction_committed(transaction_id, log_entries)
            return True
        elif last_phase == 'ABORT':
            # Transaction was aborted, ensure all participants aborted
            await self._ensure_transaction_aborted(transaction_id, log_entries)
            return True
        else:
            # Transaction in intermediate state, abort it
            logger.warning(f"Transaction {transaction_id} in incomplete state, aborting")
            await self._ensure_transaction_aborted(transaction_id, log_entries)
            return False
    
    async def _ensure_transaction_committed(self, transaction_id: str, log_entries: list):
        """
        Pastikan semua participant commit transaksi
        
        Args:
            transaction_id: ID transaksi
            log_entries: Log entries untuk transaksi
        """
        # Extract participants from logs
        participants = set()
        for entry in log_entries:
            if 'participants' in entry.data:
                participants.update(entry.data['participants'])
        
        # Send commit to all participants
        for node_id in participants:
            try:
                await self.node_manager.commit_transaction_on_node(node_id, transaction_id)
            except Exception as e:
                logger.error(f"Failed to commit on node {node_id}: {e}")
    
    async def _ensure_transaction_aborted(self, transaction_id: str, log_entries: list):
        """
        Pastikan semua participant abort transaksi
        
        Args:
            transaction_id: ID transaksi
            log_entries: Log entries untuk transaksi
        """
        # Extract participants from logs
        participants = set()
        for entry in log_entries:
            if 'participants' in entry.data:
                participants.update(entry.data['participants'])
        
        # Send abort to all participants
        for node_id in participants:
            try:
                await self.node_manager.abort_transaction_on_node(node_id, transaction_id)
            except Exception as e:
                logger.error(f"Failed to abort on node {node_id}: {e}")
    
    def get_recovery_status(self, node_id: int = None) -> dict:
        """
        Mendapatkan status recovery
        
        Args:
            node_id: Optional node ID spesifik
            
        Returns:
            Dictionary status recovery
        """
        if node_id is not None:
            return {
                'is_recovering': node_id in self.recovering_nodes,
                'recovery_task_exists': node_id in self.recovery_tasks
            }
        
        return {
            'total_recoveries': self.recovery_count,
            'successful_recoveries': self.successful_recoveries,
            'failed_recoveries': self.failed_recoveries,
            'currently_recovering': list(self.recovering_nodes),
            'success_rate': (self.successful_recoveries / self.recovery_count 
                           if self.recovery_count > 0 else 0)
        }
    
    async def cancel_recovery(self, node_id: int) -> bool:
        """
        Membatalkan recovery yang sedang berjalan
        
        Args:
            node_id: ID node yang recovery-nya dibatalkan
            
        Returns:
            True jika berhasil dibatalkan
        """
        if node_id not in self.recovery_tasks:
            logger.warning(f"No ongoing recovery for node {node_id}")
            return False
        
        task = self.recovery_tasks[node_id]
        task.cancel()
        
        try:
            await task
        except asyncio.CancelledError:
            pass
        
        self.recovering_nodes.discard(node_id)
        self.recovery_tasks.pop(node_id, None)
        
        logger.info(f"Recovery for node {node_id} cancelled")
        return True