"""
Distributed Node - Representasi node dalam sistem terdistribusi
"""

from __future__ import annotations
import asyncio
import logging
import uuid
from typing import Dict, Any, Optional, List, TYPE_CHECKING
from datetime import datetime
from enum import Enum

from src.model.node_status import NodeStatus
from src.config.system_config import SystemConfig
from src.lock.lock_manager import LockManager
from src.log.write_ahead_log import WriteAheadLog
from src.core.transaction_executor import TransactionExecutor

if TYPE_CHECKING:
    from src.fault.failure_detector import FailureDetector

logger = logging.getLogger(__name__)


class DistributedNode:
    """
    Kelas yang merepresentasikan node dalam cluster terdistribusi
    """
    
    def __init__(
        self,
        node_id: int,
        config: SystemConfig,
        failure_detector: FailureDetector = None
    ):
        """
        Inisialisasi distributed node
        
        Args:
            node_id: ID unik node
            config: Konfigurasi sistem
            failure_detector: Failure detector untuk monitoring
        """
        self.node_id = node_id
        self.config = config
        self.failure_detector = failure_detector
        
        self.status = NodeStatus.ACTIVE
        self.is_healthy = True
        self.failure_count = 0
        
        self.transaction_executor = TransactionExecutor(config, None, None, None)
        self.lock_manager = LockManager(config)
        self.write_ahead_log = WriteAheadLog(config)
        
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # Partitions owned by this node
        self.owned_partitions = config.get_node_partitions(node_id)
        
        # Metrics
        self.transactions_processed = 0
        self.transactions_succeeded = 0
        self.transactions_failed = 0
        self.start_time = None
        
        logger.info(f"DistributedNode {node_id} initialized with {len(self.owned_partitions)} partitions")
    
    async def start(self):
        """Memulai node"""
        self.is_running = True
        self.start_time = datetime.now()
        
        # Start heartbeat sender
        self.heartbeat_task = asyncio.create_task(self._send_heartbeats())
        
        logger.info(f"Node {self.node_id} started")
    
    async def stop(self):
        """Menghentikan node"""
        self.is_running = False
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Node {self.node_id} stopped")
    
    async def _send_heartbeats(self):
        """Mengirim heartbeat ke failure detector secara periodik"""
        while self.is_running and self.status == NodeStatus.ACTIVE:
            try:
                await asyncio.sleep(self.config.heartbeat_interval_ms / 1000)
                
                if self.failure_detector:
                    self.failure_detector.record_heartbeat(self.node_id)
                
                # Update metrics
                self.is_healthy = True
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error sending heartbeat from node {self.node_id}: {e}")
    
    async def prepare_transaction(self, transaction_id: str) -> bool:
        """
        Prepare transaction pada node ini
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            True jika prepare berhasil
        """
        if not self.is_healthy:
            logger.warning(f"Node {self.node_id} is not healthy, cannot prepare")
            return False
        
        # Implementation will be connected to transaction executor
        # For demo, return True
        return True
    
    async def commit_transaction(self, transaction_id: str) -> bool:
        """
        Commit transaction pada node ini
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            True jika commit berhasil
        """
        if not self.is_healthy:
            logger.warning(f"Node {self.node_id} is not healthy, cannot commit")
            return False
        
        self.transactions_processed += 1
        self.transactions_succeeded += 1
        
        return True
    
    async def abort_transaction(self, transaction_id: str) -> bool:
        """
        Abort transaction pada node ini
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            True jika abort berhasil
        """
        if not self.is_healthy:
            logger.warning(f"Node {self.node_id} is not healthy, cannot abort")
            return False
        
        self.transactions_processed += 1
        self.transactions_failed += 1
        
        return True
    
    async def fail(self):
        """Simulate node failure"""
        if self.status == NodeStatus.ACTIVE:
            self.status = NodeStatus.FAILED
            self.is_healthy = False
            self.failure_count += 1
            
            logger.error(f"Node {self.node_id} failed (failure count: {self.failure_count})")
            
            # Stop heartbeat
            if self.heartbeat_task:
                self.heartbeat_task.cancel()
    
    async def restart(self):
        """Restart node after failure"""
        self.status = NodeStatus.RECOVERING
        
        logger.info(f"Node {self.node_id} restarting...")
        
        # Simulate restart delay
        await asyncio.sleep(1)
        
        self.status = NodeStatus.ACTIVE
        self.is_healthy = True
        
        # Restart heartbeat
        if self.heartbeat_task:
            self.heartbeat_task = asyncio.create_task(self._send_heartbeats())
        
        logger.info(f"Node {self.node_id} restarted successfully")
    
    def get_stats(self) -> dict:
        """
        Mendapatkan statistik node
        
        Returns:
            Dictionary statistik
        """
        uptime_ms = 0
        if self.start_time:
            uptime_ms = (datetime.now() - self.start_time).total_seconds() * 1000
        
        return {
            'node_id': self.node_id,
            'status': self.status.value,
            'is_healthy': self.is_healthy,
            'failure_count': self.failure_count,
            'uptime_ms': uptime_ms,
            'transactions_processed': self.transactions_processed,
            'transactions_succeeded': self.transactions_succeeded,
            'transactions_failed': self.transactions_failed,
            'owned_partitions': self.owned_partitions,
            'success_rate': (self.transactions_succeeded / self.transactions_processed 
                           if self.transactions_processed > 0 else 0)
        }