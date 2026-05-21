"""
Transaction Status Enum - Status transaksi dalam sistem
"""

from enum import Enum


class TransactionStatus(Enum):
    """
    Enum untuk status transaksi
    """
    
    PENDING = "PENDING"
    """Transaksi menunggu untuk diproses"""
    
    PROCESSING = "PROCESSING"
    """Transaksi sedang diproses"""
    
    PREPARED = "PREPARED"
    """Transaksi sudah diprepare (2PC phase 1 selesai)"""
    
    COMMITTED = "COMMITTED"
    """Transaksi berhasil di-commit"""
    
    ABORTED = "ABORTED"
    """Transaksi di-abort"""
    
    FAILED = "FAILED"
    """Transaksi gagal"""
    
    TIMEOUT = "TIMEOUT"
    """Transaksi timeout"""
    
    def is_active(self) -> bool:
        """Cek apakah transaksi masih aktif"""
        return self in [TransactionStatus.PENDING, 
                       TransactionStatus.PROCESSING,
                       TransactionStatus.PREPARED]
    
    def is_final(self) -> bool:
        """Cek apakah status adalah final state"""
        return self in [TransactionStatus.COMMITTED,
                       TransactionStatus.ABORTED,
                       TransactionStatus.FAILED,
                       TransactionStatus.TIMEOUT]