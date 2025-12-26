"""常量定义"""
from enum import Enum

class MigrationStatus(Enum):
    """迁移状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"

class TableStatus(Enum):
    """表状态枚举"""
    NOT_STARTED = "not_started"
    MIGRATING = "migrating"
    COMPLETED = "completed"
    FAILED = "failed"