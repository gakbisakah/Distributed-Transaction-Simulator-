"""
Node Manager - Mengelola semua node dalam cluster
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

from src.config.system_config import SystemConfig
from src.node.distributed_node import DistributedNode
from src.model.node_status import NodeStatus
from src.fault.failure_detector import FailureDetector
from src.fault.recovery_manager import RecoveryManager
from src.metrics.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)


class NodeManager:
    """
    Manajer untuk mengelola semua node dalam cluster terdistribusi
    """
    
    def __init__(self, config: SystemConfig, metrics_collector: MetricsCollector = None):
        """
        Inisialisasi node manager
        
        Args:
            config: Konfigurasi sistem
            metrics_collector: Kolektor metrics
        """
        self.config = config
        self.metrics = metrics_collector
        
        self.nodes: Dict[int, DistributedNode] = {}
        self.local_node_id = 0  # Node ini adalah coordinator
        
        self.failure_detector = FailureDetector(config, self)
        self.recovery_manager = RecoveryManager(config, self)
        
        # Fault simulation flags
        self.network_partition_active = False
        self.partition_groups: tuple = None
        self.message_loss_probability = 0.0
        self.network_latency_ms = config.network_latency_ms
        self.corruption_probability = 0.0
        
        logger.info(f"NodeManager initialized with {config.num_nodes} nodes")
    
    async def initialize(self):
        """Menginisialisasi semua node"""
        for node_id in range(self.config.num_nodes):
            node = DistributedNode(node_id, self.config, self.failure_detector)
            self.nodes[node_id] = node
        
        logger.info(f"Initialized {len(self.nodes)} nodes")
    
    async def start(self):
        """Memulai semua node"""
        # Start failure detector
        await self.failure_detector.start()
        
        # Start all nodes
        for node in self.nodes.values():
            await node.start()
        
        logger.info("All nodes started")
    
    async def stop(self):
        """Menghentikan semua node"""
        # Stop failure detector
        await self.failure_detector.stop()
        
        # Stop all nodes
        for node in self.nodes.values():
            await node.stop()
        
        logger.info("All nodes stopped")
    
    def get_node(self, node_id: int) -> Optional[DistributedNode]:
        """
        Mendapatkan node berdasarkan ID
        
        Args:
            node_id: ID node
            
        Returns:
            Node object atau None jika tidak ditemukan
        """
        return self.nodes.get(node_id)
    
    def get_active_nodes(self) -> List[DistributedNode]:
        """
        Mendapatkan semua node yang aktif
        
        Returns:
            List node yang aktif
        """
        return [node for node in self.nodes.values() if node.is_healthy]
    
    async def on_node_suspected(self, node_id: int):
        """
        Callback ketika node dicurigai gagal
        
        Args:
            node_id: ID node yang dicurigai
        """
        logger.warning(f"Node {node_id} is suspected failed")
        
        if self.metrics:
            self.metrics.record_node_suspected()
    
    async def on_node_failure(self, node_id: int):
        """
        Callback ketika node dikonfirmasi gagal
        
        Args:
            node_id: ID node yang gagal
        """
        logger.error(f"Node {node_id} confirmed failed")
        
        # Start recovery
        asyncio.create_task(self.recovery_manager.recover_node(node_id))
        
        if self.metrics:
            self.metrics.record_node_failure()
    
    async def on_node_recovery(self, node_id: int):
        """
        Callback ketika node berhasil recovery
        
        Args:
            node_id: ID node yang recovery
        """
        logger.info(f"Node {node_id} recovered")
        
        if self.metrics:
            self.metrics.record_node_recovery()
    
    async def set_network_partition(self, nodes_a: List[int], nodes_b: List[int]):
        """
        Mengaktifkan network partition simulation
        
        Args:
            nodes_a: Kelompok node A
            nodes_b: Kelompok node B
        """
        self.network_partition_active = True
        self.partition_groups = (nodes_a, nodes_b)
        
        logger.warning(f"Network partition activated: {nodes_a} <-> {nodes_b}")
    
    async def clear_network_partition(self):
        """Menonaktifkan network partition"""
        self.network_partition_active = False
        self.partition_groups = None
        
        logger.info("Network partition cleared")
    
    async def set_message_loss_probability(self, probability: float):
        """
        Mengatur probabilitas message loss
        
        Args:
            probability: Probabilitas loss (0-1)
        """
        self.message_loss_probability = max(0.0, min(1.0, probability))
        logger.info(f"Message loss probability set to {self.message_loss_probability}")
    
    async def clear_message_loss(self):
        """Menonaktifkan message loss"""
        self.message_loss_probability = 0.0
        logger.info("Message loss cleared")
    
    async def set_network_latency(self, latency_ms: int):
        """
        Mengatur network latency simulasi
        
        Args:
            latency_ms: Latency dalam milliseconds
        """
        self.network_latency_ms = latency_ms
        logger.info(f"Network latency set to {latency_ms}ms")
    
    async def clear_network_latency(self):
        """Mengembalikan network latency ke default"""
        self.network_latency_ms = self.config.network_latency_ms
        logger.info(f"Network latency restored to {self.network_latency_ms}ms")
    
    async def set_corruption_probability(self, probability: float):
        """
        Mengatur probabilitas corrupt message
        
        Args:
            probability: Probabilitas corruption
        """
        self.corruption_probability = max(0.0, min(1.0, probability))
        logger.info(f"Message corruption probability set to {self.corruption_probability}")
    
    async def clear_corruption(self):
        """Menonaktifkan message corruption"""
        self.corruption_probability = 0.0
        logger.info("Message corruption cleared")
    
    async def recover_node(self, node_id: int) -> bool:
        """
        Recovery node yang gagal
        
        Args:
            node_id: ID node yang direcovery
            
        Returns:
            True jika berhasil
        """
        return await self.recovery_manager.recover_node(node_id)
    
    async def prepare_transaction_on_node(self, node_id: int, transaction_id: str, data: dict) -> bool:
        """
        Prepare transaction pada node tertentu
        
        Args:
            node_id: ID node target
            transaction_id: ID transaksi
            data: Data transaksi
            
        Returns:
            True jika prepare berhasil
        """
        node = self.get_node(node_id)
        if not node or not node.is_healthy:
            return False
        
        return await node.prepare_transaction(transaction_id)
    
    async def commit_transaction_on_node(self, node_id: int, transaction_id: str) -> bool:
        """
        Commit transaction pada node tertentu
        
        Args:
            node_id: ID node target
            transaction_id: ID transaksi
            
        Returns:
            True jika commit berhasil
        """
        node = self.get_node(node_id)
        if not node or not node.is_healthy:
            return False
        
        return await node.commit_transaction(transaction_id)
    
    async def abort_transaction_on_node(self, node_id: int, transaction_id: str) -> bool:
        """
        Abort transaction pada node tertentu
        
        Args:
            node_id: ID node target
            transaction_id: ID transaksi
            
        Returns:
            True jika abort berhasil
        """
        node = self.get_node(node_id)
        if not node:
            return False
        
        return await node.abort_transaction(transaction_id)
    
    async def request_cluster_state(self, coordinator_id: int) -> dict:
        """
        Request cluster state dari coordinator
        
        Args:
            coordinator_id: ID coordinator
            
        Returns:
            Dictionary cluster state
        """
        # Simulate request to coordinator
        return {
            'cluster_id': 'distributed-tx-simulator',
            'timestamp': datetime.now().isoformat(),
            'nodes': [
                node.get_stats() for node in self.nodes.values()
            ],
            'active_nodes': len(self.get_active_nodes()),
            'total_nodes': len(self.nodes)
        }
    
    async def update_cluster_state(self, node_id: int, cluster_state: dict):
        """
        Update cluster state pada node
        
        Args:
            node_id: ID node yang diupdate
            cluster_state: Cluster state dari coordinator
        """
        node = self.get_node(node_id)
        if node:
            # Update node state based on cluster state
            logger.debug(f"Node {node_id} state updated from cluster")
    
    def get_stats(self) -> dict:
        """
        Mendapatkan statistik node manager
        
        Returns:
            Dictionary statistik
        """
        node_stats = [node.get_stats() for node in self.nodes.values()]
        
        return {
            'total_nodes': len(self.nodes),
            'active_nodes': len(self.get_active_nodes()),
            'healthy_nodes': sum(1 for node in self.nodes.values() if node.is_healthy),
            'failed_nodes': sum(1 for node in self.nodes.values() if node.status == NodeStatus.FAILED),
            'recovering_nodes': sum(1 for node in self.nodes.values() if node.status == NodeStatus.RECOVERING),
            'nodes': node_stats,
            'network_partition_active': self.network_partition_active,
            'message_loss_probability': self.message_loss_probability,
            'corruption_probability': self.corruption_probability,
            'current_latency_ms': self.network_latency_ms
        }