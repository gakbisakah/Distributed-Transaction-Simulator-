"""
Lock Manager - Mengelola distributed locks untuk resource
"""

import asyncio
import logging
from typing import Dict, Optional, Set
from datetime import datetime

from src.config.system_config import SystemConfig
from src.lock.distributed_lock import DistributedLock
from src.lock.lock_type import LockType

logger = logging.getLogger(__name__)


class LockManager:
    """
    Manajer untuk mengelola semua distributed locks dalam sistem
    """
    
    def __init__(self, config: SystemConfig):
        """
        Inisialisasi lock manager
        
        Args:
            config: Konfigurasi sistem
        """
        self.config = config
        self.locks: Dict[str, DistributedLock] = {}
        self.lock_owners: Dict[str, Set[str]] = {}  # owner -> set of resources
        
        self.deadlock_detection_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # Lock wait graph untuk deadlock detection
        self.wait_graph: Dict[str, Set[str]] = {}  # transaction -> waiting for resources
        
        logger.info("LockManager initialized")
    
    async def start(self):
        """Memulai lock manager"""
        self.is_running = True
        self.deadlock_detection_task = asyncio.create_task(self._deadlock_detection_loop())
        logger.info("LockManager started")
    
    async def stop(self):
        """Menghentikan lock manager"""
        self.is_running = False
        
        if self.deadlock_detection_task:
            self.deadlock_detection_task.cancel()
            try:
                await self.deadlock_detection_task
            except asyncio.CancelledError:
                pass
        
        # Release all locks
        for resource_key in list(self.locks.keys()):
            lock = self.locks[resource_key]
            if lock.is_locked():
                owner = lock.get_owner()
                if owner:
                    await self.release_lock(owner, resource_key)
        
        logger.info("LockManager stopped")
    
    async def acquire_lock(
        self,
        owner: str,
        resource_key: str,
        lock_type: LockType = LockType.WRITE,
        timeout_ms: Optional[int] = None
    ) -> bool:
        """
        Acquire lock untuk resource
        
        Args:
            owner: Owner identifier (transaction ID)
            resource_key: Key resource yang akan dilock
            lock_type: Tipe lock (READ/WRITE)
            timeout_ms: Timeout untuk acquire lock

        Returns:
            True jika lock berhasil diacquire
        """
        # Dapatkan atau buat lock untuk resource
        if resource_key not in self.locks:
            self.locks[resource_key] = DistributedLock(resource_key, self.config)
        
        lock = self.locks[resource_key]
        
        # Untuk saat ini, implementasi sederhana (hanya exclusive locks)
        # TODO: Implement read-write locks
        
        # Cek deadlock
        if await self._would_deadlock(owner, resource_key):
            logger.warning(f"Deadlock detected for {owner} on {resource_key}")
            return False
        
        # Record wait if lock is held
        if lock.is_locked() and lock.get_owner() != owner:
            self._record_wait(owner, resource_key)
        
        # Try to acquire
        acquired = await lock.acquire(owner, timeout_ms=timeout_ms)
        
        if acquired:
            # Record ownership
            if owner not in self.lock_owners:
                self.lock_owners[owner] = set()
            self.lock_owners[owner].add(resource_key)
            
            # Clear wait record
            self._clear_wait(owner, resource_key)
            
            logger.debug(f"Lock acquired: {owner} on {resource_key}")
        
        return acquired
    
    async def release_lock(self, owner: str, resource_key: str) -> bool:
        """
        Release lock untuk resource
        
        Args:
            owner: Owner yang melepas lock
            resource_key: Key resource yang akan direlease
            
        Returns:
            True jika lock berhasil direlease
        """
        if resource_key not in self.locks:
            logger.warning(f"Lock not found for {resource_key}")
            return False
        
        lock = self.locks[resource_key]
        
        if lock.get_owner() != owner:
            logger.warning(f"Lock owner mismatch for {resource_key}")
            return False
        
        released = await lock.release(owner)
        
        if released:
            # Remove from ownership
            if owner in self.lock_owners:
                self.lock_owners[owner].discard(resource_key)
                if not self.lock_owners[owner]:
                    del self.lock_owners[owner]
            
            logger.debug(f"Lock released: {owner} from {resource_key}")
        
        return released
    
    async def release_all_locks(self, owner: str) -> int:
        """
        Release semua locks yang dipegang oleh owner
        
        Args:
            owner: Owner yang melepas semua locks
            
        Returns:
            Jumlah locks yang direlease
        """
        if owner not in self.lock_owners:
            return 0
        
        released_count = 0
        resources = list(self.lock_owners[owner])
        
        for resource_key in resources:
            if await self.release_lock(owner, resource_key):
                released_count += 1
        
        logger.debug(f"Released {released_count} locks for {owner}")
        return released_count
    
    def _record_wait(self, owner: str, resource_key: str):
        """
        Record bahwa transaction sedang menunggu lock
        
        Args:
            owner: Owner yang menunggu
            resource_key: Resource yang ditunggu
        """
        if owner not in self.wait_graph:
            self.wait_graph[owner] = set()
        
        self.wait_graph[owner].add(resource_key)
    
    def _clear_wait(self, owner: str, resource_key: str):
        """
        Clear wait record
        
        Args:
            owner: Owner yang tidak lagi menunggu
            resource_key: Resource yang tidak lagi ditunggu
        """
        if owner in self.wait_graph:
            self.wait_graph[owner].discard(resource_key)
            if not self.wait_graph[owner]:
                del self.wait_graph[owner]
    
    async def _would_deadlock(self, owner: str, resource_key: str) -> bool:
        """
        Cek apakah acquire lock akan menyebabkan deadlock
        
        Args:
            owner: Owner yang akan acquire lock
            resource_key: Resource yang akan dilock
            
        Returns:
            True jika akan terjadi deadlock
        """
        # Cek apakah resource sedang dilock oleh transaction lain
        if resource_key not in self.locks:
            return False
        
        lock = self.locks[resource_key]
        current_owner = lock.get_owner()
        
        if current_owner is None or current_owner == owner:
            return False
        
        # Cek apakah current_owner menunggu owner (cyclic dependency)
        return self._has_cycle(owner, current_owner, set())
    
    def _has_cycle(self, start: str, current: str, visited: Set[str]) -> bool:
        """
        DFS untuk deteksi cycle dalam wait graph
        
        Args:
            start: Starting transaction
            current: Current transaction being checked
            visited: Visited transactions
            
        Returns:
            True jika ditemukan cycle
        """
        if current in visited:
            return current == start
        
        if current not in self.wait_graph:
            return False
        
        visited.add(current)
        
        for waiting_for in self.wait_graph[current]:
            # Dapatkan transaction yang memegang lock pada resource yang ditunggu
            if waiting_for in self.locks:
                lock_owner = self.locks[waiting_for].get_owner()
                if lock_owner and self._has_cycle(start, lock_owner, visited):
                    return True
        
        visited.remove(current)
        return False
    
    async def _deadlock_detection_loop(self):
        """Loop untuk periodic deadlock detection"""
        while self.is_running:
            try:
                await asyncio.sleep(self.config.deadlock_detection_interval_ms / 1000)
                
                # Detect dan resolve deadlocks
                deadlocks = await self._detect_deadlocks()
                
                for deadlock_cycle in deadlocks:
                    logger.warning(f"Deadlock detected: {deadlock_cycle}")
                    await self._resolve_deadlock(deadlock_cycle)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in deadlock detection: {e}")
    
    async def _detect_deadlocks(self) -> list:
        """
        Deteksi deadlock dalam wait graph
        
        Returns:
            List cycle deadlock yang ditemukan
        """
        deadlocks = []
        visited_global = set()
        
        for owner in self.wait_graph:
            if owner not in visited_global:
                cycle = []
                if self._find_cycle(owner, owner, set(), cycle):
                    deadlocks.append(cycle)
                    visited_global.update(cycle)
        
        return deadlocks
    
    def _find_cycle(self, start: str, current: str, visited: Set[str], cycle: list) -> bool:
        """
        Find cycle in wait graph
        
        Args:
            start: Starting node
            current: Current node
            visited: Visited nodes
            cycle: List untuk menyimpan cycle
            
        Returns:
            True jika ditemukan cycle
        """
        if current in visited:
            if current == start:
                cycle.extend(visited)
                return True
            return False
        
        if current not in self.wait_graph:
            return False
        
        visited.add(current)
        
        for waiting_for in self.wait_graph[current]:
            if waiting_for in self.locks:
                lock_owner = self.locks[waiting_for].get_owner()
                if lock_owner and self._find_cycle(start, lock_owner, visited, cycle):
                    return True
        
        visited.remove(current)
        return False
    
    async def _resolve_deadlock(self, deadlock_cycle: list):
        """
        Resolve deadlock dengan memilih victim transaction
        
        Args:
            deadlock_cycle: Cycle deadlock yang ditemukan
        """
        if not deadlock_cycle:
            return
        
        # Pilih victim (yang termuda atau random)
        victim = deadlock_cycle[0]  # Simplifikasi: pilih yang pertama
        
        logger.info(f"Resolving deadlock by aborting transaction {victim}")
        
        # Abort victim transaction (release all its locks)
        await self.release_all_locks(victim)
    
    def get_active_lock_count(self) -> int:
        """
        Mendapatkan jumlah active locks
        
        Returns:
            Jumlah locks yang sedang aktif
        """
        return sum(1 for lock in self.locks.values() if lock.is_locked())
    
    def get_stats(self) -> dict:
        """
        Mendapatkan statistik lock manager
        
        Returns:
            Dictionary statistik
        """
        return {
            'total_locks': len(self.locks),
            'active_locks': self.get_active_lock_count(),
            'active_owners': len(self.lock_owners),
            'waiting_transactions': len(self.wait_graph),
            'locks_by_resource': {
                resource: lock.get_owner() 
                for resource, lock in self.locks.items() 
                if lock.is_locked()
            }
        }