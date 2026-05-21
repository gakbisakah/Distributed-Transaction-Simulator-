"""
Transaction Model - Model data untuk transaksi
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum

from src.model.transaction_status import TransactionStatus


@dataclass
class Transaction:
    """
    Kelas model untuk merepresentasikan transaksi dalam sistem
    """
    
    transaction_id: str
    data: Dict[str, Any]
    status: TransactionStatus = TransactionStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    committed_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    processing_time_ms: float = 0.0
    coordinator_id: Optional[int] = None
    participants: list = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Konversi transaksi ke dictionary"""
        return {
            'transaction_id': self.transaction_id,
            'data': self.data,
            'status': self.status.value,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'committed_at': self.committed_at.isoformat() if self.committed_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'error_message': self.error_message,
            'retry_count': self.retry_count,
            'processing_time_ms': self.processing_time_ms,
            'coordinator_id': self.coordinator_id,
            'participants': self.participants
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Transaction':
        """Buat transaksi dari dictionary"""
        return cls(
            transaction_id=data['transaction_id'],
            data=data['data'],
            status=TransactionStatus(data['status']),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None,
            updated_at=datetime.fromisoformat(data['updated_at']) if data.get('updated_at') else None,
            committed_at=datetime.fromisoformat(data['committed_at']) if data.get('committed_at') else None,
            completed_at=datetime.fromisoformat(data['completed_at']) if data.get('completed_at') else None,
            error_message=data.get('error_message'),
            retry_count=data.get('retry_count', 0),
            processing_time_ms=data.get('processing_time_ms', 0.0),
            coordinator_id=data.get('coordinator_id'),
            participants=data.get('participants', [])
        )
    
    def is_completed(self) -> bool:
        """Cek apakah transaksi sudah selesai"""
        return self.status in [
            TransactionStatus.COMMITTED,
            TransactionStatus.FAILED,
            TransactionStatus.TIMEOUT
        ]
    
    def is_success(self) -> bool:
        """Cek apakah transaksi berhasil"""
        return self.status == TransactionStatus.COMMITTED
    
    def can_retry(self, max_retries: int = 3) -> bool:
        """Cek apakah transaksi bisa di-retry"""
        return (self.status in [TransactionStatus.FAILED, TransactionStatus.TIMEOUT] 
                and self.retry_count < max_retries)