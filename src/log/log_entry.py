"""
Log Entry - Representasi entry dalam write-ahead log
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional

from src.log.log_type import LogType


@dataclass
class LogEntry:
    """
    Entry dalam write-ahead log
    """
    
    log_type: LogType
    transaction_id: str
    data: Dict[str, Any]
    node_id: int
    timestamp: datetime = field(default_factory=datetime.now)
    sequence_number: int = 0
    
    def to_dict(self) -> dict:
        """Konversi log entry ke dictionary"""
        return {
            'log_type': self.log_type.value,
            'transaction_id': self.transaction_id,
            'data': self.data,
            'node_id': self.node_id,
            'timestamp': self.timestamp.isoformat(),
            'sequence_number': self.sequence_number
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'LogEntry':
        """Buat log entry dari dictionary"""
        return cls(
            log_type=LogType(data['log_type']),
            transaction_id=data['transaction_id'],
            data=data['data'],
            node_id=data['node_id'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            sequence_number=data.get('sequence_number', 0)
        )