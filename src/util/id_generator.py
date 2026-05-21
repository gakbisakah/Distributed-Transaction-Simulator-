"""
ID Generator - Generator untuk ID unik dalam sistem
"""

import uuid
import time
import random
from typing import Optional


class IdGenerator:
    """
    Generator untuk menghasilkan berbagai jenis ID dalam sistem
    """
    
    # Counter untuk sequence numbers
    _sequence_counter = 0
    _last_timestamp = 0
    
    @classmethod
    def generate_transaction_id(cls) -> str:
        """
        Menghasilkan ID transaksi unik
        
        Returns:
            Transaction ID format: tx_{timestamp}_{uuid}
        """
        timestamp = int(time.time() * 1000)
        unique_part = str(uuid.uuid4()).replace('-', '')[:8]
        
        return f"tx_{timestamp}_{unique_part}"
    
    @classmethod
    def generate_node_id(cls) -> int:
        """
        Menghasilkan ID node (simulasi untuk node baru)
        
        Returns:
            Node ID
        """
        return random.randint(1000, 9999)
    
    @classmethod
    def generate_lock_id(cls) -> str:
        """
        Menghasilkan ID lock unik
        
        Returns:
            Lock ID format: lock_{timestamp}_{random}
        """
        timestamp = int(time.time() * 1000)
        random_part = random.randint(0, 999999)
        
        return f"lock_{timestamp}_{random_part}"
    
    @classmethod
    def generate_session_id(cls) -> str:
        """
        Menghasilkan ID session unik
        
        Returns:
            Session ID format: sess_{uuid}
        """
        return f"sess_{uuid.uuid4().hex[:16]}"
    
    @classmethod
    def generate_sequence_number(cls) -> int:
        """
        Menghasilkan sequence number untuk log entries
        
        Returns:
            Sequence number yang increment
        """
        cls._sequence_counter += 1
        return cls._sequence_counter
    
    @classmethod
    def generate_request_id(cls) -> str:
        """
        Menghasilkan ID request unik untuk tracing
        
        Returns:
            Request ID format: req_{timestamp}_{sequence}
        """
        timestamp = int(time.time() * 1000)
        cls._sequence_counter += 1
        
        return f"req_{timestamp}_{cls._sequence_counter}"
    
    @classmethod
    def reset_sequence(cls):
        """Reset sequence counter (untuk testing)"""
        cls._sequence_counter = 0
        cls._last_timestamp = 0