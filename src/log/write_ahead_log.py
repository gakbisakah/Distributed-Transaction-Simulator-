"""
Write Ahead Log - Implementasi WAL untuk durability
"""

import json
import os
import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path

from src.config.system_config import SystemConfig
from src.log.log_entry import LogEntry
from src.log.log_type import LogType

logger = logging.getLogger(__name__)


class WriteAheadLog:
    """
    Write-Ahead Log untuk durability dan recovery
    """
    
    def __init__(self, config: SystemConfig):
        """
        Inisialisasi write-ahead log
        
        Args:
            config: Konfigurasi sistem
        """
        self.config = config
        self.log_dir = Path(config.wal_directory)
        
        # Buat direktori log jika belum ada
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Log file untuk setiap node
        self.log_files: Dict[int, asyncio.File] = {}
        
        # In-memory buffer
        self.buffer: List[LogEntry] = []
        self.buffer_size = 0
        self.max_buffer_size = 1000
        
        self.flush_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        logger.info(f"WriteAheadLog initialized at {self.log_dir}")
    
    async def start(self):
        """Memulai WAL"""
        self.is_running = True
        self.flush_task = asyncio.create_task(self._periodic_flush())
        
        # Load existing logs
        await self._load_existing_logs()
        
        logger.info("WriteAheadLog started")
    
    async def stop(self):
        """Menghentikan WAL dan flush semua buffer"""
        self.is_running = False
        
        # Flush remaining buffer
        await self._flush_buffer()
        
        if self.flush_task:
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass
        
        # Close all log files
        for file in self.log_files.values():
            file.close()
        
        logger.info("WriteAheadLog stopped")
    
    async def _load_existing_logs(self):
        """Load existing logs dari disk"""
        for log_file in self.log_dir.glob("wal_*.log"):
            try:
                # Extract node_id from filename
                node_id = int(log_file.stem.split('_')[1])
                
                # Open file for reading
                with open(log_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            entry_dict = json.loads(line)
                            entry = LogEntry.from_dict(entry_dict)
                            # Add to appropriate structure (for recovery)
                            
                logger.debug(f"Loaded logs from {log_file}")
                
            except Exception as e:
                logger.error(f"Error loading log {log_file}: {e}")
    
    async def _get_log_file(self, node_id: int):
        """
        Dapatkan file handle untuk node log
        
        Args:
            node_id: ID node
            
        Returns:
            File handle
        """
        if node_id not in self.log_files:
            log_path = self.log_dir / f"wal_{node_id}.log"
            self.log_files[node_id] = open(log_path, 'a')
        
        return self.log_files[node_id]
    
    async def log_entry(self, entry: LogEntry):
        """
        Log entry ke WAL
        
        Args:
            entry: Log entry yang akan ditulis
        """
        self.buffer.append(entry)
        self.buffer_size += 1
        
        # Flush if buffer is full
        if self.buffer_size >= self.max_buffer_size:
            await self._flush_buffer()
    
    async def _flush_buffer(self):
        """Flush buffer ke disk"""
        if not self.buffer:
            return
        
        entries_to_flush = self.buffer.copy()
        self.buffer.clear()
        self.buffer_size = 0
        
        # Group by node_id
        entries_by_node: Dict[int, List[LogEntry]] = {}
        
        for entry in entries_to_flush:
            if entry.node_id not in entries_by_node:
                entries_by_node[entry.node_id] = []
            entries_by_node[entry.node_id].append(entry)
        
        # Write to each node's log file
        for node_id, entries in entries_by_node.items():
            try:
                log_file = await self._get_log_file(node_id)
                
                for entry in entries:
                    log_line = json.dumps(entry.to_dict()) + '\n'
                    log_file.write(log_line)
                
                log_file.flush()
                
            except Exception as e:
                logger.error(f"Error flushing logs for node {node_id}: {e}")
    
    async def _periodic_flush(self):
        """Periodic flush buffer ke disk"""
        while self.is_running:
            try:
                await asyncio.sleep(1)  # Flush every second
                await self._flush_buffer()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")
    
    async def log_transaction_start(self, transaction_id: str, data: dict):
        """
        Log transaction start
        
        Args:
            transaction_id: ID transaksi
            data: Data transaksi
        """
        entry = LogEntry(
            log_type=LogType.TRANSACTION_START,
            transaction_id=transaction_id,
            data={'transaction_data': data},
            node_id=0  # Coordinator node
        )
        await self.log_entry(entry)
    
    async def log_transaction_commit(self, transaction_id: str):
        """
        Log transaction commit
        
        Args:
            transaction_id: ID transaksi
        """
        entry = LogEntry(
            log_type=LogType.TRANSACTION_COMMIT,
            transaction_id=transaction_id,
            data={},
            node_id=0
        )
        await self.log_entry(entry)
    
    async def log_transaction_abort(self, transaction_id: str):
        """
        Log transaction abort
        
        Args:
            transaction_id: ID transaksi
        """
        entry = LogEntry(
            log_type=LogType.TRANSACTION_ABORT,
            transaction_id=transaction_id,
            data={},
            node_id=0
        )
        await self.log_entry(entry)
    
    async def log_prepare_phase(self, transaction_id: str, success: bool, failed_nodes: list):
        """
        Log prepare phase result
        
        Args:
            transaction_id: ID transaksi
            success: Apakah prepare berhasil
            failed_nodes: Node yang gagal
        """
        entry = LogEntry(
            log_type=LogType.PREPARE_PHASE,
            transaction_id=transaction_id,
            data={
                'success': success,
                'failed_nodes': failed_nodes
            },
            node_id=0
        )
        await self.log_entry(entry)
    
    async def log_commit_phase(self, transaction_id: str, success: bool, failed_nodes: list):
        """
        Log commit phase result
        
        Args:
            transaction_id: ID transaksi
            success: Apakah commit berhasil
            failed_nodes: Node yang gagal
        """
        entry = LogEntry(
            log_type=LogType.COMMIT_PHASE,
            transaction_id=transaction_id,
            data={
                'success': success,
                'failed_nodes': failed_nodes
            },
            node_id=0
        )
        await self.log_entry(entry)
    
    async def log_abort_phase(self, transaction_id: str):
        """
        Log abort phase
        
        Args:
            transaction_id: ID transaksi
        """
        entry = LogEntry(
            log_type=LogType.ABORT_PHASE,
            transaction_id=transaction_id,
            data={},
            node_id=0
        )
        await self.log_entry(entry)
    
    async def log_prepare(self, transaction_id: str, data: dict):
        """
        Log prepare pada node
        
        Args:
            transaction_id: ID transaksi
            data: Data transaksi
        """
        entry = LogEntry(
            log_type=LogType.PREPARE,
            transaction_id=transaction_id,
            data=data,
            node_id=self.config.coordinator_nodes[0] if self.config.coordinator_nodes else 0
        )
        await self.log_entry(entry)
    
    async def log_commit(self, transaction_id: str):
        """
        Log commit pada node
        
        Args:
            transaction_id: ID transaksi
        """
        entry = LogEntry(
            log_type=LogType.COMMIT,
            transaction_id=transaction_id,
            data={},
            node_id=self.config.coordinator_nodes[0] if self.config.coordinator_nodes else 0
        )
        await self.log_entry(entry)
    
    async def log_abort(self, transaction_id: str):
        """
        Log abort pada node
        
        Args:
            transaction_id: ID transaksi
        """
        entry = LogEntry(
            log_type=LogType.ABORT,
            transaction_id=transaction_id,
            data={},
            node_id=self.config.coordinator_nodes[0] if self.config.coordinator_nodes else 0
        )
        await self.log_entry(entry)
    
    async def log_local_execution_start(self, transaction_id: str):
        """
        Log local execution start
        
        Args:
            transaction_id: ID transaksi
        """
        entry = LogEntry(
            log_type=LogType.LOCAL_EXECUTION,
            transaction_id=transaction_id,
            data={'phase': 'start'},
            node_id=self.config.coordinator_nodes[0] if self.config.coordinator_nodes else 0
        )
        await self.log_entry(entry)
    
    async def log_local_execution_commit(self, transaction_id: str):
        """
        Log local execution commit
        
        Args:
            transaction_id: ID transaksi
        """
        entry = LogEntry(
            log_type=LogType.LOCAL_EXECUTION,
            transaction_id=transaction_id,
            data={'phase': 'commit'},
            node_id=self.config.coordinator_nodes[0] if self.config.coordinator_nodes else 0
        )
        await self.log_entry(entry)
    
    async def log_local_execution_abort(self, transaction_id: str):
        """
        Log local execution abort
        
        Args:
            transaction_id: ID transaksi
        """
        entry = LogEntry(
            log_type=LogType.LOCAL_EXECUTION,
            transaction_id=transaction_id,
            data={'phase': 'abort'},
            node_id=self.config.coordinator_nodes[0] if self.config.coordinator_nodes else 0
        )
        await self.log_entry(entry)
    
    async def get_transaction_logs(self, transaction_id: str) -> List[LogEntry]:
        """
        Mendapatkan semua log entry untuk transaksi tertentu
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            List log entries
        """
        entries = []
        
        for log_file in self.log_dir.glob("wal_*.log"):
            try:
                with open(log_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            entry_dict = json.loads(line)
                            if entry_dict.get('transaction_id') == transaction_id:
                                entries.append(LogEntry.from_dict(entry_dict))
            except Exception as e:
                logger.error(f"Error reading {log_file}: {e}")
        
        return entries
    
    async def get_pending_transactions(self, node_id: int) -> List[tuple]:
        """
        Mendapatkan transaksi yang pending untuk node tertentu
        
        Args:
            node_id: ID node
            
        Returns:
            List of (transaction_id, data) untuk pending transactions
        """
        pending = []
        
        log_path = self.log_dir / f"wal_{node_id}.log"
        if not log_path.exists():
            return pending
        
        try:
            with open(log_path, 'r') as f:
                for line in f:
                    if line.strip():
                        entry_dict = json.loads(line)
                        entry = LogEntry.from_dict(entry_dict)
                        
                        if entry.log_type == LogType.PREPARE:
                            pending.append((entry.transaction_id, entry.data))
                        elif entry.log_type == LogType.COMMIT:
                            # Remove from pending if committed
                            pending = [(tid, data) for tid, data in pending if tid != entry.transaction_id]
                        elif entry.log_type == LogType.ABORT:
                            # Remove from pending if aborted
                            pending = [(tid, data) for tid, data in pending if tid != entry.transaction_id]
                            
        except Exception as e:
            logger.error(f"Error reading {log_path}: {e}")
        
        return pending
    
    async def get_last_phase(self, transaction_id: str) -> Optional[str]:
        """
        Mendapatkan phase terakhir untuk transaksi
        
        Args:
            transaction_id: ID transaksi
            
        Returns:
            Nama phase terakhir atau None
        """
        logs = await self.get_transaction_logs(transaction_id)
        
        if not logs:
            return None
        
        # Sort by timestamp
        logs.sort(key=lambda x: x.timestamp)
        
        # Get last log
        last_log = logs[-1]
        
        phase_map = {
            LogType.TRANSACTION_START: 'START',
            LogType.PREPARE_PHASE: 'PREPARE',
            LogType.COMMIT_PHASE: 'COMMIT',
            LogType.ABORT_PHASE: 'ABORT',
            LogType.TRANSACTION_COMMIT: 'COMMIT',
            LogType.TRANSACTION_ABORT: 'ABORT'
        }
        
        return phase_map.get(last_log.log_type)