"""
Node Status Enum - Status node dalam sistem terdistribusi
"""

from enum import Enum


class NodeStatus(Enum):
    """
    Enum untuk status node dalam cluster
    """
    
    ACTIVE = "ACTIVE"
    """Node berfungsi normal"""
    
    DEGRADED = "DEGRADED"
    """Node mengalami penurunan performa"""
    
    FAILED = "FAILED"
    """Node gagal/tidak berfungsi"""
    
    RECOVERING = "RECOVERING"
    """Node sedang dalam proses recovery"""
    
    SUSPECT = "SUSPECT"
    """Node dicurigai gagal, sedang dalam observasi"""
    
    def is_available(self) -> bool:
        """Cek apakah node tersedia untuk memproses request"""
        return self in [NodeStatus.ACTIVE, NodeStatus.DEGRADED]
    
    def is_failed(self) -> bool:
        """Cek apakah node dalam keadaan gagal"""
        return self == NodeStatus.FAILED
    
    def is_recovering(self) -> bool:
        """Cek apakah node sedang recovery"""
        return self == NodeStatus.RECOVERING