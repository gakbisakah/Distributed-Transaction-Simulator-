"""
Log Type Enum - Tipe-tipe log entry dalam WAL
"""

from enum import Enum


class LogType(Enum):
    """
    Enum untuk tipe log entry dalam Write-Ahead Log
    """
    
    # Transaction lifecycle logs
    TRANSACTION_START = "TRANSACTION_START"
    TRANSACTION_COMMIT = "TRANSACTION_COMMIT"
    TRANSACTION_ABORT = "TRANSACTION_ABORT"
    
    # 2PC logs
    PREPARE = "PREPARE"
    COMMIT = "COMMIT"
    ABORT = "ABORT"
    
    # Phase logs
    PREPARE_PHASE = "PREPARE_PHASE"
    COMMIT_PHASE = "COMMIT_PHASE"
    ABORT_PHASE = "ABORT_PHASE"
    
    # Execution logs
    LOCAL_EXECUTION = "LOCAL_EXECUTION"
    
    # Recovery logs
    RECOVERY_START = "RECOVERY_START"
    RECOVERY_COMPLETE = "RECOVERY_COMPLETE"
    
    # Checkpoint
    CHECKPOINT = "CHECKPOINT"