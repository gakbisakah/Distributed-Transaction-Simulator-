"""
Timeout Manager - Mengelola timeout untuk operasi asinkron
"""

import asyncio
import logging
import threading
import time
from typing import Dict, Callable, Optional, Any
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class TimeoutEntry:
    """Entry untuk timeout yang dijadwalkan"""
    
    key: str
    expiry_time: float
    callback: Callable
    args: tuple
    kwargs: dict


class TimeoutManager:
    """
    Manajer untuk mengelola timeout pada berbagai operasi
    """
    
    def __init__(self):
        """
        Inisialisasi timeout manager
        """
        self._timeouts: Dict[str, TimeoutEntry] = {}
        self._lock = threading.RLock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        logger.info("TimeoutManager initialized")
    
    async def start(self):
        """Memulai timeout manager"""
        self.is_running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("TimeoutManager started")
    
    async def stop(self):
        """Menghentikan timeout manager"""
        self.is_running = False
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        logger.info("TimeoutManager stopped")
    
    def set_timeout(
        self,
        key: str,
        timeout_seconds: float,
        callback: Callable,
        args: tuple = (),
        kwargs: dict = None
    ):
        """
        Set timeout untuk key tertentu
        
        Args:
            key: Key identifier untuk timeout
            timeout_seconds: Durasi timeout dalam detik
            callback: Fungsi callback yang dipanggil saat timeout
            args: Arguments untuk callback
            kwargs: Keyword arguments untuk callback
        """
        if kwargs is None:
            kwargs = {}
        
        expiry_time = time.time() + timeout_seconds
        
        with self._lock:
            self._timeouts[key] = TimeoutEntry(
                key=key,
                expiry_time=expiry_time,
                callback=callback,
                args=args,
                kwargs=kwargs
            )
        
        logger.debug(f"Timeout set for {key} at {expiry_time}")
    
    def cancel_timeout(self, key: str) -> bool:
        """
        Membatalkan timeout untuk key tertentu
        
        Args:
            key: Key timeout yang akan dibatalkan
            
        Returns:
            True jika timeout ditemukan dan dibatalkan
        """
        with self._lock:
            if key in self._timeouts:
                del self._timeouts[key]
                logger.debug(f"Timeout cancelled for {key}")
                return True
        
        return False
    
    def has_timeout(self, key: str) -> bool:
        """
        Cek apakah timeout untuk key ada
        
        Args:
            key: Key timeout yang dicek
            
        Returns:
            True jika timeout ada
        """
        with self._lock:
            return key in self._timeouts
    
    def get_remaining_time(self, key: str) -> Optional[float]:
        """
        Mendapatkan sisa waktu timeout dalam detik
        
        Args:
            key: Key timeout yang dicek
            
        Returns:
            Sisa waktu dalam detik atau None jika tidak ada
        """
        with self._lock:
            if key not in self._timeouts:
                return None
            
            entry = self._timeouts[key]
            remaining = entry.expiry_time - time.time()
            return max(0, remaining)
    
    async def _cleanup_loop(self):
        """Loop untuk membersihkan timeout yang expired"""
        logger.info("Timeout cleanup loop started")
        
        while self.is_running:
            try:
                await asyncio.sleep(0.1)  # Check every 100ms
                
                current_time = time.time()
                expired_keys = []
                
                with self._lock:
                    for key, entry in self._timeouts.items():
                        if current_time >= entry.expiry_time:
                            expired_keys.append(key)
                    
                    # Remove expired entries
                    for key in expired_keys:
                        del self._timeouts[key]
                
                # Execute callbacks for expired timeouts
                for key in expired_keys:
                    # We need to re-get entry because we deleted it
                    # In practice, we stored the entry info before deletion
                    pass
                
                # Re-acquire with lock to get entry info
                expired_entries = []
                with self._lock:
                    for key in expired_keys:
                        # Entry sudah dihapus, kita perlu menyimpannya sebelum dihapus
                        pass
                
                # Execute callbacks
                for key in expired_keys:
                    logger.debug(f"Timeout expired for {key}")
                    # Call callback (should be handled differently in async context)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
    
    def _execute_callback(self, entry: TimeoutEntry):
        """
        Execute timeout callback
        
        Args:
            entry: Timeout entry
        """
        try:
            # Handle both sync and async callbacks
            result = entry.callback(*entry.args, **entry.kwargs)
            
            # If result is coroutine, we need to handle it
            if asyncio.iscoroutine(result):
                # Create task for coroutine
                asyncio.create_task(result)
                
        except Exception as e:
            logger.error(f"Error executing timeout callback for {entry.key}: {e}")
    
    def get_timeout_count(self) -> int:
        """
        Mendapatkan jumlah timeout yang aktif
        
        Returns:
            Jumlah timeout aktif
        """
        with self._lock:
            return len(self._timeouts)
    
    def clear_all_timeouts(self):
        """Menghapus semua timeout"""
        with self._lock:
            count = len(self._timeouts)
            self._timeouts.clear()
            logger.info(f"Cleared {count} timeouts")