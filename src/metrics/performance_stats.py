"""
Performance Stats - Objek untuk menyimpan statistik performa
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class PerformanceStats:
    """
    Objek untuk menyimpan statistik performa sistem
    """
    
    total_transactions: int
    committed_transactions: int
    failed_transactions: int
    timeout_transactions: int
    success_rate: float
    average_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    throughput_tps: float
    node_failures: int
    node_recoveries: int
    uptime_seconds: float
    timestamp: datetime
    
    def to_dict(self) -> dict:
        """Konversi ke dictionary"""
        return {
            'total_transactions': self.total_transactions,
            'committed_transactions': self.committed_transactions,
            'failed_transactions': self.failed_transactions,
            'timeout_transactions': self.timeout_transactions,
            'success_rate': self.success_rate,
            'average_latency_ms': self.average_latency_ms,
            'p95_latency_ms': self.p95_latency_ms,
            'p99_latency_ms': self.p99_latency_ms,
            'throughput_tps': self.throughput_tps,
            'node_failures': self.node_failures,
            'node_recoveries': self.node_recoveries,
            'uptime_seconds': self.uptime_seconds,
            'timestamp': self.timestamp.isoformat()
        }
    
    def to_string(self) -> str:
        """Konversi ke string readable"""
        return f"""
        ========================================
        PERFORMANCE STATISTICS
        ========================================
        Total Transactions:    {self.total_transactions}
        Committed:            {self.committed_transactions}
        Failed:               {self.failed_transactions}
        Timeout:              {self.timeout_transactions}
        Success Rate:         {self.success_rate * 100:.2f}%
        
        Latency (ms):
          Average:            {self.average_latency_ms:.2f}
          P95:                {self.p95_latency_ms:.2f}
          P99:                {self.p99_latency_ms:.2f}
        
        Throughput:           {self.throughput_tps:.2f} tps
        
        Fault Tolerance:
          Node Failures:      {self.node_failures}
          Node Recoveries:    {self.node_recoveries}
        
        Uptime:               {self.uptime_seconds:.0f} seconds
        ========================================
        """