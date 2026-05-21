"""
System Configuration - Konfigurasi sistem simulator
"""

from dataclasses import dataclass, field
from typing import List, Optional
import os


@dataclass
class SystemConfig:
    """
    Konfigurasi sistem untuk simulator transaksi terdistribusi
    Menyimpan semua parameter konfigurasi yang diperlukan
    """
    
    # Node configuration
    num_nodes: int = 3
    num_partitions: int = 6
    replication_factor: int = 2
    
    # Network configuration
    network_latency_ms: int = 10
    network_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 1000
    heartbeat_timeout_ms: int = 5000
    
    # Transaction configuration
    transaction_timeout_ms: int = 10000
    max_retry_count: int = 3
    retry_delay_ms: int = 1000
    transaction_batch_size: int = 100
    
    # Fault tolerance configuration
    fault_injection_enabled: bool = True
    fault_probability: float = 0.05
    node_failure_probability: float = 0.02
    network_partition_probability: float = 0.01
    message_loss_probability: float = 0.03
    
    # Recovery configuration
    recovery_timeout_ms: int = 15000
    max_recovery_attempts: int = 3
    
    # Lock configuration
    lock_timeout_ms: int = 5000
    deadlock_detection_interval_ms: int = 2000
    
    # Logging configuration
    log_level: str = "INFO"
    enable_audit_log: bool = True
    wal_directory: str = "wal_logs"
    
    # Metrics configuration
    metrics_collection_interval_ms: int = 5000
    enable_performance_metrics: bool = True
    metrics_retention_minutes: int = 60
    
    # Performance tuning
    core_pool_size: int = 4
    max_pool_size: int = 8
    queue_capacity: int = 1000
    keep_alive_seconds: int = 60
    
    # Partition configuration
    partition_keys: List[str] = field(default_factory=lambda: [
        "account_id", "user_id", "transaction_id"
    ])
    
    # Node roles
    coordinator_nodes: List[int] = field(default_factory=list)
    worker_nodes: List[int] = field(default_factory=list)
    
    def __post_init__(self):
        """Validasi dan inisialisasi post-processing"""
        
        # Validasi parameter
        assert self.num_nodes >= 1, "Minimal 1 node diperlukan"
        assert self.replication_factor <= self.num_nodes, \
            "Replication factor tidak boleh melebihi jumlah node"
        assert 0 <= self.fault_probability <= 1, \
            "Fault probability harus antara 0 dan 1"
        assert self.transaction_timeout_ms > 0, \
            "Transaction timeout harus positif"
        
        # Inisialisasi node roles jika kosong
        if not self.coordinator_nodes:
            # Node 0 sebagai coordinator utama
            self.coordinator_nodes = [0]
        
        if not self.worker_nodes:
            self.worker_nodes = list(range(1, self.num_nodes))
        
        # Buat direktori WAL jika diperlukan
        if self.enable_audit_log:
            os.makedirs(self.wal_directory, exist_ok=True)
    
    def to_dict(self) -> dict:
        """Konversi konfigurasi ke dictionary"""
        return {
            'num_nodes': self.num_nodes,
            'num_partitions': self.num_partitions,
            'replication_factor': self.replication_factor,
            'network_latency_ms': self.network_latency_ms,
            'transaction_timeout_ms': self.transaction_timeout_ms,
            'fault_injection_enabled': self.fault_injection_enabled,
            'fault_probability': self.fault_probability,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'SystemConfig':
        """Buat konfigurasi dari dictionary"""
        return cls(**data)
    
    def get_node_partitions(self, node_id: int) -> List[int]:
        """
        Mendapatkan partition yang menjadi tanggung jawab node tertentu
        
        Args:
            node_id: ID node
            
        Returns:
            List partition ID
        """
        partitions_per_node = self.num_partitions // self.num_nodes
        start_partition = node_id * partitions_per_node
        end_partition = start_partition + partitions_per_node
        
        # Node terakhir mendapat sisa partition
        if node_id == self.num_nodes - 1:
            end_partition = self.num_partitions
        
        return list(range(start_partition, end_partition))
    
    def get_partition_owner(self, partition_id: int) -> int:
        """
        Mendapatkan node owner untuk partition tertentu
        
        Args:
            partition_id: ID partition
            
        Returns:
            Node ID yang memiliki partition
        """
        partitions_per_node = self.num_partitions // self.num_nodes
        node_id = partition_id // partitions_per_node
        return min(node_id, self.num_nodes - 1)