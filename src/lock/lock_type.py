"""
Lock Type Enum - Tipe-tipe lock dalam sistem
"""

from enum import Enum


class LockType(Enum):
    """
    Enum untuk tipe lock dalam distributed locking
    """
    
    READ = "READ"
    """Read lock (shared) - multiple readers allowed"""
    
    WRITE = "WRITE"
    """Write lock (exclusive) - only one writer allowed"""
    
    def is_compatible(self, other: 'LockType') -> bool:
        """
        Cek apakah lock type ini kompatibel dengan lock type lain
        
        Args:
            other: Lock type lain
            
        Returns:
            True jika kompatibel
        """
        if self == LockType.READ and other == LockType.READ:
            return True
        return False