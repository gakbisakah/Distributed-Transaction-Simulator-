"""
Metrics Collector - Mengumpulkan metrics performa sistem
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict

from src.config.system_config import SystemConfig
from src.metrics.performance_stats import PerformanceStats

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Kolektor metrics untuk monitoring performa sistem
    """
    
    def __init__(self, config: SystemConfig):
        """
        Inisialisasi metrics collector
        
        Args:
            config: Konfigurasi sistem
        """
        self.config = config
        
        # Transaction metrics
        self.transaction_submitted = 0
        self.transaction_committed = 0
        self.transaction_failed = 0
        self.transaction_timeout = 0
        
        self.transaction_latencies: List[float] = []
        self.transaction_start_times: Dict[str, float] = {}
        
        # Node metrics
        self.node_failures = 0
        self.node_recoveries = 0
        self.node_suspected = 0
        
        # System metrics
        self.start_time = datetime.now()
        self.last_collection_time = datetime.now()
        
        # Throughput tracking
        self.throughput_samples: List[int] = []
        self.throughput_sample_interval_ms = 1000
        
        self.collection_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        logger.info("MetricsCollector initialized")
    
    async def start(self):
        """Memulai metrics collection"""
        self.is_running = True
        self.start_time = datetime.now()
        self.collection_task = asyncio.create_task(self._periodic_collection())
        
        logger.info("MetricsCollector started")
    
    async def stop(self):
        """Menghentikan metrics collection"""
        self.is_running = False
        
        if self.collection_task:
            self.collection_task.cancel()
            try:
                await self.collection_task
            except asyncio.CancelledError:
                pass
        
        logger.info("MetricsCollector stopped")
    
    async def _periodic_collection(self):
        """Periodic collection of metrics"""
        while self.is_running:
            try:
                await asyncio.sleep(self.config.metrics_collection_interval_ms / 1000)
                
                # Collect throughput sample
                self.throughput_samples.append(self.transaction_committed)
                
                # Keep only last hour of samples
                max_samples = (60 * 60 * 1000) // self.config.metrics_collection_interval_ms
                if len(self.throughput_samples) > max_samples:
                    self.throughput_samples = self.throughput_samples[-max_samples:]
                
                self.last_collection_time = datetime.now()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic collection: {e}")
    
    def record_transaction_submitted(self):
        """Record submitted transaction"""
        self.transaction_submitted += 1
    
    def record_transaction_start(self, transaction_id: str):
        """
        Record transaction start time
        
        Args:
            transaction_id: ID transaksi
        """
        self.transaction_start_times[transaction_id] = time.time() * 1000
    
    def record_transaction_committed(self, transaction_id: str = None):
        """
        Record committed transaction
        
        Args:
            transaction_id: Optional transaction ID for latency calculation
        """
        self.transaction_committed += 1
        
        if transaction_id and transaction_id in self.transaction_start_times:
            start_time = self.transaction_start_times.pop(transaction_id)
            latency = (time.time() * 1000) - start_time
            self.transaction_latencies.append(latency)
            
            # Keep only last 10000 latencies
            if len(self.transaction_latencies) > 10000:
                self.transaction_latencies = self.transaction_latencies[-10000:]
    
    def record_transaction_failed(self, transaction_id: str = None):
        """
        Record failed transaction
        
        Args:
            transaction_id: Optional transaction ID
        """
        self.transaction_failed += 1
        
        if transaction_id and transaction_id in self.transaction_start_times:
            del self.transaction_start_times[transaction_id]
    
    def record_transaction_timeout(self, transaction_id: str = None):
        """
        Record transaction timeout
        
        Args:
            transaction_id: Optional transaction ID
        """
        self.transaction_timeout += 1
        
        if transaction_id and transaction_id in self.transaction_start_times:
            del self.transaction_start_times[transaction_id]
    
    def record_node_failure(self):
        """Record node failure"""
        self.node_failures += 1
    
    def record_node_recovery(self):
        """Record node recovery"""
        self.node_recoveries += 1
    
    def record_node_suspected(self):
        """Record node suspected"""
        self.node_suspected += 1
    
    def get_performance_stats(self) -> PerformanceStats:
        """
        Mendapatkan statistik performa
        
        Returns:
            PerformanceStats object
        """
        # Calculate throughput
        throughput = 0.0
        if self.throughput_samples and len(self.throughput_samples) >= 2:
            # Calculate transactions per second
            samples = self.throughput_samples[-10:]  # Last 10 samples
            if samples:
                throughput = sum(samples) / len(samples) * (
                    1000 / self.config.metrics_collection_interval_ms
                )
        
        # Calculate average latency
        avg_latency = 0.0
        if self.transaction_latencies:
            avg_latency = sum(self.transaction_latencies) / len(self.transaction_latencies)
        
        # Calculate p95 and p99 latency
        p95_latency = 0.0
        p99_latency = 0.0
        
        if self.transaction_latencies:
            sorted_latencies = sorted(self.transaction_latencies)
            p95_index = int(len(sorted_latencies) * 0.95)
            p99_index = int(len(sorted_latencies) * 0.99)
            
            p95_latency = sorted_latencies[p95_index] if p95_index < len(sorted_latencies) else sorted_latencies[-1]
            p99_latency = sorted_latencies[p99_index] if p99_index < len(sorted_latencies) else sorted_latencies[-1]
        
        # Calculate success rate
        total = self.transaction_committed + self.transaction_failed
        success_rate = self.transaction_committed / total if total > 0 else 0
        
        # Calculate uptime
        uptime_seconds = (datetime.now() - self.start_time).total_seconds()
        
        return PerformanceStats(
            total_transactions=self.transaction_submitted,
            committed_transactions=self.transaction_committed,
            failed_transactions=self.transaction_failed,
            timeout_transactions=self.transaction_timeout,
            success_rate=success_rate,
            average_latency_ms=avg_latency,
            p95_latency_ms=p95_latency,
            p99_latency_ms=p99_latency,
            throughput_tps=throughput,
            node_failures=self.node_failures,
            node_recoveries=self.node_recoveries,
            uptime_seconds=uptime_seconds,
            timestamp=datetime.now()
        )
    
    def get_latency_distribution(self) -> dict:
        """
        Mendapatkan distribusi latency
        
        Returns:
            Dictionary distribusi latency
        """
        if not self.transaction_latencies:
            return {}
        
        distribution = {
            '<10ms': 0,
            '10-50ms': 0,
            '50-100ms': 0,
            '100-500ms': 0,
            '500-1000ms': 0,
            '>1000ms': 0
        }
        
        for latency in self.transaction_latencies:
            if latency < 10:
                distribution['<10ms'] += 1
            elif latency < 50:
                distribution['10-50ms'] += 1
            elif latency < 100:
                distribution['50-100ms'] += 1
            elif latency < 500:
                distribution['100-500ms'] += 1
            elif latency < 1000:
                distribution['500-1000ms'] += 1
            else:
                distribution['>1000ms'] += 1
        
        total = len(self.transaction_latencies)
        for key in distribution:
            distribution[key] = (distribution[key] / total) * 100
        
        return distribution
    
    def reset(self):
        """Reset all metrics"""
        self.transaction_submitted = 0
        self.transaction_committed = 0
        self.transaction_failed = 0
        self.transaction_timeout = 0
        self.transaction_latencies = []
        self.transaction_start_times = {}
        self.node_failures = 0
        self.node_recoveries = 0
        self.node_suspected = 0
        self.throughput_samples = []
        self.start_time = datetime.now()
    
    def get_metrics_summary(self) -> dict:
        """
        Mendapatkan ringkasan metrics
        
        Returns:
            Dictionary ringkasan metrics
        """
        stats = self.get_performance_stats()
        
        return {
            'transactions': {
                'submitted': self.transaction_submitted,
                'committed': self.transaction_committed,
                'failed': self.transaction_failed,
                'timeout': self.transaction_timeout,
                'success_rate': f"{stats.success_rate * 100:.2f}%"
            },
            'latency': {
                'avg_ms': f"{stats.average_latency_ms:.2f}",
                'p95_ms': f"{stats.p95_latency_ms:.2f}",
                'p99_ms': f"{stats.p99_latency_ms:.2f}"
            },
            'throughput': {
                'tps': f"{stats.throughput_tps:.2f}"
            },
            'nodes': {
                'failures': self.node_failures,
                'recoveries': self.node_recoveries,
                'suspected': self.node_suspected
            },
            'uptime': {
                'seconds': stats.uptime_seconds,
                'formatted': self._format_uptime(stats.uptime_seconds)
            }
        }
    
    def _format_uptime(self, seconds: float) -> str:
        """
        Format uptime menjadi string readable
        
        Args:
            seconds: Uptime dalam seconds
            
        Returns:
            Formatted uptime string
        """
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"