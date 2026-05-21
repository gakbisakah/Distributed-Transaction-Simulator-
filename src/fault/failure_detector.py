"""
Failure Detector - Mendeteksi kegagalan node dalam cluster
"""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Dict, Optional, Set, List, TYPE_CHECKING
from datetime import datetime
from collections import defaultdict

from src.config.system_config import SystemConfig
from src.model.node_status import NodeStatus

if TYPE_CHECKING:
    from src.node.node_manager import NodeManager

logger = logging.getLogger(__name__)


class FailureDetector:
    """
    Failure detector untuk mendeteksi node failure dalam cluster
    Menggunakan protocol heartbeat dan phi-accrual failure detector
    """
    
    def __init__(self, config: SystemConfig, node_manager: NodeManager):
        """
        Inisialisasi failure detector
        
        Args:
            config: Konfigurasi sistem
            node_manager: Manajer node
        """
        self.config = config
        self.node_manager = node_manager
        
        self.heartbeat_history: Dict[int, List[float]] = defaultdict(list)
        self.last_heartbeat: Dict[int, float] = {}
        self.suspected_nodes: Set[int] = set()
        
        self.is_running = False
        self.detector_task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        
        # Konfigurasi phi-accrual
        self.history_size = 100
        self.min_std_deviation = 100  # ms
        self.phi_threshold = 8.0
        
        logger.info("FailureDetector initialized")
    
    async def start(self):
        """Memulai failure detector"""
        self.is_running = True
        self.detector_task = asyncio.create_task(self._detection_loop())
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        logger.info("FailureDetector started")
    
    async def stop(self):
        """Menghentikan failure detector"""
        self.is_running = False
        
        if self.detector_task:
            self.detector_task.cancel()
            try:
                await self.detector_task
            except asyncio.CancelledError:
                pass
        
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        logger.info("FailureDetector stopped")
    
    async def _detection_loop(self):
        """Loop utama untuk deteksi failure"""
        logger.info("Failure detection loop started")
        
        while self.is_running:
            try:
                await asyncio.sleep(self.config.heartbeat_interval_ms / 1000)
                
                for node_id in range(self.config.num_nodes):
                    await self._check_node_health(node_id)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in detection loop: {e}")
    
    async def _check_node_health(self, node_id: int):
        """
        Cek kesehatan node
        
        Args:
            node_id: ID node yang akan dicek
        """
        # Skip self
        if node_id == self.node_manager.local_node_id:
            return
        
        current_time = time.time() * 1000  # Convert to ms
        last_heartbeat = self.last_heartbeat.get(node_id, 0)
        
        if last_heartbeat == 0:
            # Belum pernah menerima heartbeat
            return
        
        time_since_last = current_time - last_heartbeat
        
        # Hitung phi value
        phi = self._compute_phi(node_id, time_since_last)
        
        if phi > self.phi_threshold:
            if node_id not in self.suspected_nodes:
                logger.warning(f"Node {node_id} suspected failed (phi={phi:.2f})")
                self.suspected_nodes.add(node_id)
                await self._handle_suspected_failure(node_id)
        else:
            if node_id in self.suspected_nodes:
                logger.info(f"Node {node_id} recovered (phi={phi:.2f})")
                self.suspected_nodes.discard(node_id)
                await self._handle_recovery(node_id)
    
    def _compute_phi(self, node_id: int, time_since_last: float) -> float:
        """
        Compute phi value untuk phi-accrual failure detector
        
        Args:
            node_id: ID node
            time_since_last: Waktu sejak heartbeat terakhir (ms)
            
        Returns:
            Phi value
        """
        history = self.heartbeat_history[node_id]
        
        if len(history) < 10:
            # Belum cukup data, gunakan default
            return 0.0
        
        # Hitung mean dan standard deviation
        mean = sum(history) / len(history)
        
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std_dev = max(variance ** 0.5, self.min_std_deviation)
        
        # Hitung phi menggunakan cumulative distribution function
        import math
        
        # Normalized time since last heartbeat
        y = (time_since_last - mean) / std_dev
        
        # CDF of normal distribution
        cdf = 0.5 * (1 + math.erf(y / math.sqrt(2)))
        
        # Phi = -log10(1 - CDF)
        if cdf >= 0.999999:
            return 10.0
        
        phi = -math.log10(1 - cdf)
        
        return min(phi, 10.0)  # Cap at 10
    
    async def _handle_suspected_failure(self, node_id: int):
        """
        Handle suspected node failure
        
        Args:
            node_id: ID node yang dicurigai
        """
        node = self.node_manager.get_node(node_id)
        
        if node and node.status != NodeStatus.SUSPECT:
            node.status = NodeStatus.SUSPECT
            
            # Notify node manager
            await self.node_manager.on_node_suspected(node_id)
            
            # Schedule fail detection
            asyncio.create_task(self._confirm_failure(node_id))
    
    async def _confirm_failure(self, node_id: int):
        """
        Konfirmasi apakah node benar-benar gagal
        
        Args:
            node_id: ID node yang dicurigai
        """
        # Tunggu beberapa saat untuk konfirmasi
        await asyncio.sleep(self.config.heartbeat_timeout_ms / 1000)
        
        # Cek ulang
        current_time = time.time() * 1000
        last_heartbeat = self.last_heartbeat.get(node_id, 0)
        
        if current_time - last_heartbeat > self.config.heartbeat_timeout_ms:
            # Node benar-benar gagal
            logger.error(f"Node {node_id} confirmed failed")
            
            node = self.node_manager.get_node(node_id)
            if node and node.status != NodeStatus.FAILED:
                node.status = NodeStatus.FAILED
                await self.node_manager.on_node_failure(node_id)
    
    async def _handle_recovery(self, node_id: int):
        """
        Handle node recovery
        
        Args:
            node_id: ID node yang recover
        """
        node = self.node_manager.get_node(node_id)
        
        if node and node.status in [NodeStatus.SUSPECT, NodeStatus.FAILED]:
            node.status = NodeStatus.ACTIVE
            await self.node_manager.on_node_recovery(node_id)
    
    async def _cleanup_loop(self):
        """Loop untuk cleanup history heartbeat"""
        while self.is_running:
            try:
                await asyncio.sleep(60)  # Cleanup setiap menit
                
                # Batasi history size
                for node_id in list(self.heartbeat_history.keys()):
                    if len(self.heartbeat_history[node_id]) > self.history_size:
                        self.heartbeat_history[node_id] = (
                            self.heartbeat_history[node_id][-self.history_size:]
                        )
                
                # Hapus node yang sudah tidak ada
                active_nodes = set(range(self.config.num_nodes))
                for node_id in list(self.heartbeat_history.keys()):
                    if node_id not in active_nodes:
                        del self.heartbeat_history[node_id]
                        self.last_heartbeat.pop(node_id, None)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
    
    def record_heartbeat(self, node_id: int):
        """
        Record heartbeat dari node
        
        Args:
            node_id: ID node pengirim heartbeat
        """
        current_time = time.time() * 1000
        
        if node_id in self.last_heartbeat:
            interval = current_time - self.last_heartbeat[node_id]
            self.heartbeat_history[node_id].append(interval)
            
            # Keep history size limited
            if len(self.heartbeat_history[node_id]) > self.history_size:
                self.heartbeat_history[node_id] = self.heartbeat_history[node_id][-self.history_size:]
        
        self.last_heartbeat[node_id] = current_time
        
        # Jika node sebelumnya suspected/recovering, update status
        if node_id in self.suspected_nodes:
            self.suspected_nodes.discard(node_id)
    
    def is_node_available(self, node_id: int) -> bool:
        """
        Cek apakah node tersedia untuk digunakan
        
        Args:
            node_id: ID node
            
        Returns:
            True jika node tersedia
        """
        if node_id in self.suspected_nodes:
            return False
        
        node = self.node_manager.get_node(node_id)
        if not node:
            return False
        
        return node.status.is_available()
    
    def get_failed_nodes(self) -> List[int]:
        """
        Mendapatkan daftar node yang gagal
        
        Returns:
            List node IDs yang gagal
        """
        failed = []
        
        for node_id in range(self.config.num_nodes):
            node = self.node_manager.get_node(node_id)
            if node and node.status == NodeStatus.FAILED:
                failed.append(node_id)
        
        return failed
    
    def get_stats(self) -> dict:
        """
        Mendapatkan statistik failure detector
        
        Returns:
            Dictionary statistik
        """
        return {
            'suspected_nodes': list(self.suspected_nodes),
            'failed_nodes': self.get_failed_nodes(),
            'heartbeat_count': len(self.last_heartbeat),
            'heartbeat_history_sizes': {
                node_id: len(history) 
                for node_id, history in self.heartbeat_history.items()
            }
        }