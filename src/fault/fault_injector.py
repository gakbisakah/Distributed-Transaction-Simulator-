"""
Fault Injector - Injector untuk simulasi kegagalan sistem
"""

from __future__ import annotations
import asyncio
import logging
import random
from typing import Dict, Any, Optional, List, TYPE_CHECKING
from datetime import datetime
from enum import Enum

from src.config.system_config import SystemConfig

if TYPE_CHECKING:
    from src.node.node_manager import NodeManager

logger = logging.getLogger(__name__)


class FaultType(Enum):
    """Tipe-tipe fault yang dapat di-inject"""
    
    NODE_FAILURE = "NODE_FAILURE"
    NETWORK_PARTITION = "NETWORK_PARTITION"
    MESSAGE_LOSS = "MESSAGE_LOSS"
    HIGH_LATENCY = "HIGH_LATENCY"
    TRANSACTION_TIMEOUT = "TRANSACTION_TIMEOUT"
    CORRUPTED_MESSAGE = "CORRUPTED_MESSAGE"
    RESOURCE_EXHAUSTION = "RESOURCE_EXHAUSTION"


class FaultInjector:
    """
    Injector untuk mensimulasikan berbagai jenis fault dalam sistem
    """
    
    def __init__(self, config: SystemConfig, node_manager: NodeManager):
        """
        Inisialisasi fault injector
        
        Args:
            config: Konfigurasi sistem
            node_manager: Manajer node untuk mengontrol node
        """
        self.config = config
        self.node_manager = node_manager
        
        self.is_running = False
        self.injected_faults: List[Dict] = []
        self.active_faults: Dict[str, Any] = {}
        
        self.fault_task: Optional[asyncio.Task] = None
        
        # Statistik
        self.fault_count = 0
        self.fault_types: Dict[str, int] = {}
        
        logger.info("FaultInjector initialized")
    
    async def start(self):
        """Memulai fault injector"""
        if not self.config.fault_injection_enabled:
            logger.info("Fault injection is disabled")
            return
        
        self.is_running = True
        self.fault_task = asyncio.create_task(self._scheduled_fault_injection())
        
        logger.info("FaultInjector started")
    
    async def stop(self):
        """Menghentikan fault injector"""
        self.is_running = False
        
        if self.fault_task:
            self.fault_task.cancel()
            try:
                await self.fault_task
            except asyncio.CancelledError:
                pass
        
        # Recover all active faults
        for fault_id in list(self.active_faults.keys()):
            await self.recover_fault(fault_id)
        
        logger.info("FaultInjector stopped")
    
    async def _scheduled_fault_injection(self):
        """Menjadwalkan injection fault secara periodik"""
        logger.info("Scheduled fault injection started")
        
        while self.is_running:
            try:
                # Tunggu interval random
                await asyncio.sleep(random.uniform(2.0, 5.0))
                
                # Cek apakah perlu inject fault
                if random.random() < self.config.fault_probability:
                    await self._inject_random_fault()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduled fault injection: {e}")
    
    async def _inject_random_fault(self):
        """Inject random fault berdasarkan konfigurasi"""
        fault_types = list(FaultType)
        fault_type = random.choice(fault_types)
        
        # Bobot untuk tipe fault
        weights = {
            FaultType.NODE_FAILURE: self.config.node_failure_probability,
            FaultType.NETWORK_PARTITION: self.config.network_partition_probability,
            FaultType.MESSAGE_LOSS: self.config.message_loss_probability,
            FaultType.HIGH_LATENCY: 0.05,
            FaultType.TRANSACTION_TIMEOUT: 0.03,
            FaultType.CORRUPTED_MESSAGE: 0.02,
            FaultType.RESOURCE_EXHAUSTION: 0.01
        }
        
        if random.random() < weights.get(fault_type, 0.02):
            await self.inject_fault(fault_type)
    
    async def inject_fault(self, fault_type: FaultType, **kwargs) -> str:
        """
        Inject fault ke sistem
        
        Args:
            fault_type: Tipe fault yang akan di-inject
            **kwargs: Parameter tambahan untuk fault
            
        Returns:
            Fault ID
        """
        fault_id = f"{fault_type.value}_{datetime.now().timestamp()}"
        
        logger.warning(f"Injecting fault: {fault_type.value} with id {fault_id}")
        
        try:
            if fault_type == FaultType.NODE_FAILURE:
                await self._inject_node_failure(fault_id, **kwargs)
            elif fault_type == FaultType.NETWORK_PARTITION:
                await self._inject_network_partition(fault_id, **kwargs)
            elif fault_type == FaultType.MESSAGE_LOSS:
                await self._inject_message_loss(fault_id, **kwargs)
            elif fault_type == FaultType.HIGH_LATENCY:
                await self._inject_high_latency(fault_id, **kwargs)
            elif fault_type == FaultType.TRANSACTION_TIMEOUT:
                await self._inject_transaction_timeout(fault_id, **kwargs)
            elif fault_type == FaultType.CORRUPTED_MESSAGE:
                await self._inject_corrupted_message(fault_id, **kwargs)
            elif fault_type == FaultType.RESOURCE_EXHAUSTION:
                await self._inject_resource_exhaustion(fault_id, **kwargs)
            
            # Catat statistik
            self.fault_count += 1
            fault_type_str = fault_type.value
            self.fault_types[fault_type_str] = self.fault_types.get(fault_type_str, 0) + 1
            
            self.injected_faults.append({
                'id': fault_id,
                'type': fault_type.value,
                'timestamp': datetime.now(),
                'kwargs': kwargs
            })
            
            return fault_id
            
        except Exception as e:
            logger.error(f"Failed to inject fault {fault_type.value}: {e}")
            raise
    
    async def _inject_node_failure(self, fault_id: str, **kwargs):
        """
        Inject node failure
        
        Args:
            fault_id: ID fault
            **kwargs: node_id dapat dispesifikasi
        """
        node_id = kwargs.get('node_id', random.randint(0, self.config.num_nodes - 1))
        
        # Simpan state sebelum failure
        self.active_faults[fault_id] = {
            'type': FaultType.NODE_FAILURE,
            'node_id': node_id,
            'timestamp': datetime.now()
        }
        
        # Failure node
        node = self.node_manager.get_node(node_id)
        if node:
            await node.fail()
            logger.info(f"Node {node_id} failed due to fault injection")
    
    async def _inject_network_partition(self, fault_id: str, **kwargs):
        """
        Inject network partition
        
        Args:
            fault_id: ID fault
            **kwargs: nodes_a dan nodes_b untuk partition
        """
        all_nodes = list(range(self.config.num_nodes))
        split_point = random.randint(1, len(all_nodes) - 1)
        
        nodes_a = all_nodes[:split_point]
        nodes_b = all_nodes[split_point:]
        
        self.active_faults[fault_id] = {
            'type': FaultType.NETWORK_PARTITION,
            'nodes_a': nodes_a,
            'nodes_b': nodes_b,
            'timestamp': datetime.now()
        }
        
        # Aktifkan partition di node manager
        await self.node_manager.set_network_partition(nodes_a, nodes_b)
        
        logger.info(f"Network partition created between {nodes_a} and {nodes_b}")
    
    async def _inject_message_loss(self, fault_id: str, **kwargs):
        """
        Inject message loss probability
        
        Args:
            fault_id: ID fault
            **kwargs: loss_probability (default 0.3)
        """
        loss_probability = kwargs.get('loss_probability', 0.3)
        
        self.active_faults[fault_id] = {
            'type': FaultType.MESSAGE_LOSS,
            'loss_probability': loss_probability,
            'timestamp': datetime.now()
        }
        
        await self.node_manager.set_message_loss_probability(loss_probability)
        
        logger.info(f"Message loss probability set to {loss_probability}")
    
    async def _inject_high_latency(self, fault_id: str, **kwargs):
        """
        Inject high network latency
        
        Args:
            fault_id: ID fault
            **kwargs: latency_ms (default 500)
        """
        latency_ms = kwargs.get('latency_ms', 500)
        
        self.active_faults[fault_id] = {
            'type': FaultType.HIGH_LATENCY,
            'latency_ms': latency_ms,
            'timestamp': datetime.now()
        }
        
        await self.node_manager.set_network_latency(latency_ms)
        
        logger.info(f"Network latency increased to {latency_ms}ms")
    
    async def _inject_transaction_timeout(self, fault_id: str, **kwargs):
        """
        Inject transaction timeout
        
        Args:
            fault_id: ID fault
            **kwargs: transaction_id optional
        """
        transaction_id = kwargs.get('transaction_id')
        
        self.active_faults[fault_id] = {
            'type': FaultType.TRANSACTION_TIMEOUT,
            'transaction_id': transaction_id,
            'timestamp': datetime.now()
        }
        
        # Trigger timeout untuk transaksi tertentu
        # Implementasi akan dihubungkan dengan transaction manager
        
        logger.info(f"Transaction timeout injected for {transaction_id or 'random transaction'}")
    
    async def _inject_corrupted_message(self, fault_id: str, **kwargs):
        """
        Inject corrupted message
        
        Args:
            fault_id: ID fault
            **kwargs: corruption_probability
        """
        corruption_probability = kwargs.get('corruption_probability', 0.2)
        
        self.active_faults[fault_id] = {
            'type': FaultType.CORRUPTED_MESSAGE,
            'corruption_probability': corruption_probability,
            'timestamp': datetime.now()
        }
        
        await self.node_manager.set_corruption_probability(corruption_probability)
        
        logger.info(f"Message corruption probability set to {corruption_probability}")
    
    async def _inject_resource_exhaustion(self, fault_id: str, **kwargs):
        """
        Inject resource exhaustion
        
        Args:
            fault_id: ID fault
            **kwargs: resource_type
        """
        resource_type = kwargs.get('resource_type', 'memory')
        
        self.active_faults[fault_id] = {
            'type': FaultType.RESOURCE_EXHAUSTION,
            'resource_type': resource_type,
            'timestamp': datetime.now()
        }
        
        logger.info(f"Resource exhaustion injected for {resource_type}")
    
    async def recover_fault(self, fault_id: str) -> bool:
        """
        Recover dari fault yang telah di-inject
        
        Args:
            fault_id: ID fault yang akan direcover
            
        Returns:
            True jika recover berhasil
        """
        if fault_id not in self.active_faults:
            logger.warning(f"Fault {fault_id} not found or already recovered")
            return False
        
        fault_info = self.active_faults[fault_id]
        fault_type = fault_info['type']
        
        logger.info(f"Recovering from fault {fault_id} of type {fault_type.value}")
        
        try:
            if fault_type == FaultType.NODE_FAILURE:
                node_id = fault_info['node_id']
                await self.node_manager.recover_node(node_id)
                
            elif fault_type == FaultType.NETWORK_PARTITION:
                await self.node_manager.clear_network_partition()
                
            elif fault_type == FaultType.MESSAGE_LOSS:
                await self.node_manager.clear_message_loss()
                
            elif fault_type == FaultType.HIGH_LATENCY:
                await self.node_manager.clear_network_latency()
                
            elif fault_type == FaultType.CORRUPTED_MESSAGE:
                await self.node_manager.clear_corruption()
            
            # Hapus dari active faults
            del self.active_faults[fault_id]
            
            logger.info(f"Successfully recovered from fault {fault_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to recover from fault {fault_id}: {e}")
            return False
    
    async def recover_all_faults(self):
        """Recover semua active faults"""
        fault_ids = list(self.active_faults.keys())
        
        for fault_id in fault_ids:
            await self.recover_fault(fault_id)
        
        logger.info(f"Recovered {len(fault_ids)} faults")
    
    def should_inject_fault(self) -> bool:
        """
        Cek apakah fault perlu di-inject berdasarkan probabilitas
        
        Returns:
            True jika fault harus di-inject
        """
        if not self.config.fault_injection_enabled:
            return False
        
        return random.random() < self.config.fault_probability
    
    def should_drop_message(self) -> bool:
        """
        Cek apakah message harus di-drop
        
        Returns:
            True jika message harus di-drop
        """
        # Cek active message loss fault
        for fault_info in self.active_faults.values():
            if (fault_info['type'] == FaultType.MESSAGE_LOSS and 
                random.random() < fault_info.get('loss_probability', 0)):
                return True
        
        return False
    
    def get_fault_summary(self) -> dict:
        """
        Mendapatkan ringkasan fault yang telah terjadi
        
        Returns:
            Dictionary ringkasan fault
        """
        return {
            'total_faults': self.fault_count,
            'active_faults': len(self.active_faults),
            'faults_by_type': self.fault_types,
            'recent_faults': self.injected_faults[-10:]  # 10 fault terakhir
        }
    
    def inject_network_failure(self, node_id: int, transaction_id: str):
        """
        Helper untuk inject network failure pada komunikasi tertentu
        
        Args:
            node_id: ID node target
            transaction_id: ID transaksi
        """
        return self.should_drop_message()