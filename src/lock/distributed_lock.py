"""
Distributed Lock - Implementasi distributed lock untuk koordinasi
"""

import asyncio
import logging
import uuid
from typing import Optional
from datetime import datetime

from src.config.system_config import SystemConfig

logger = logging.getLogger(__name__)


class DistributedLock:
    """
    Distributed lock implementation menggunakan lease-based locking
    """
    
    def __init__(self, resource_key: str, config: SystemConfig):
        """
        Inisialisasi distributed lock
        
        Args:
            resource_key: Key resource yang dilock
            config: Konfigurasi sistem
        """
        self.resource_key = resource_key
        self.config = config
        
        self.owner_id: Optional[str] = None
        self.lease_expiry: Optional[datetime] = None
        self.lock_holder: Optional[str] = None
        
        self._lock = asyncio.Lock()
        self._waiters: asyncio.Queue = asyncio.Queue()
        
        logger.debug(f"DistributedLock created for resource: {resource_key}")
    
    async def acquire(
        self,
        owner: str,
        timeout_ms: int = None,
        lease_duration_ms: int = None
    ) -> bool:
        """
        Acquire lock dengan timeout dan lease duration
        
        Args:
            owner: Owner identifier (biasanya transaction ID)
            timeout_ms: Timeout untuk acquire lock
            lease_duration_ms: Durasi lease lock
            
        Returns:
            True jika lock berhasil diacquire
        """
        timeout = timeout_ms or self.config.lock_timeout_ms
        lease = lease_duration_ms or self.config.lock_timeout_ms
        
        start_time = datetime.now()
        
        async with self._lock:
            while self.owner_id is not None and self._is_lease_valid():
                # Lock sedang dipegang, cek timeout
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                if elapsed >= timeout:
                    logger.debug(f"Timeout acquiring lock for {self.resource_key}")
                    return False
                
                # Tunggu sebentar sebelum cek ulang
                await asyncio.sleep(0.01)
            
            # Acquire lock
            self.owner_id = owner
            self.lease_expiry = datetime.now()
            self.lease_expiry = self.lease_expiry.replace(
                microsecond=0
            )  # Normalize
            
            # Add lease duration
            from datetime import timedelta
            self.lease_expiry += timedelta(milliseconds=lease)
            
            self.lock_holder = owner
            
            logger.debug(f"Lock acquired for {self.resource_key} by {owner}")
            return True
    
    async def release(self, owner: str) -> bool:
        """
        Release lock
        
        Args:
            owner: Owner yang melepas lock
            
        Returns:
            True jika lock berhasil direlease
        """
        async with self._lock:
            if self.owner_id != owner:
                logger.warning(f"Lock owner mismatch: {owner} vs {self.owner_id}")
                return False
            
            self.owner_id = None
            self.lease_expiry = None
            self.lock_holder = None
            
            logger.debug(f"Lock released for {self.resource_key} by {owner}")
            return True
    
    def _is_lease_valid(self) -> bool:
        """
        Cek apakah lease masih valid
        
        Returns:
            True jika lease masih valid
        """
        if self.lease_expiry is None:
            return False
        
        return datetime.now() < self.lease_expiry
    
    async def renew_lease(self, owner: str, duration_ms: int) -> bool:
        """
        Perpanjang lease lock
        
        Args:
            owner: Owner lock
            duration_ms: Durasi perpanjangan
            
        Returns:
            True jika berhasil diperpanjang
        """
        async with self._lock:
            if self.owner_id != owner:
                logger.warning(f"Cannot renew: lock owned by {self.owner_id}")
                return False
            
            if not self._is_lease_valid():
                logger.warning(f"Lease already expired for {self.resource_key}")
                return False
            
            from datetime import timedelta
            self.lease_expiry += timedelta(milliseconds=duration_ms)
            
            logger.debug(f"Lease renewed for {self.resource_key} by {owner}")
            return True
    
    def get_owner(self) -> Optional[str]:
        """
        Mendapatkan owner lock saat ini
        
        Returns:
            Owner ID atau None jika tidak ada lock
        """
        if self._is_lease_valid():
            return self.owner_id
        return None
    
    def is_locked(self) -> bool:
        """
        Cek apakah resource sedang dilock
        
        Returns:
            True jika sedang dilock
        """
        return self.owner_id is not None and self._is_lease_valid()
    
    def get_expiry_time_ms(self) -> Optional[float]:
        """
        Mendapatkan waktu expiry lease dalam milliseconds
        
        Returns:
            Milliseconds until expiry atau None
        """
        if self.lease_expiry is None:
            return None
        
        remaining = (self.lease_expiry - datetime.now()).total_seconds() * 1000
        return max(0, remaining)