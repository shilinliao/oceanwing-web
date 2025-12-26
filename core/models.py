"""数据模型"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum

@dataclass
class MigrationTask:
    """迁移任务"""
    source_table: str
    target_table: str
    date_str: str
    day_offset: int
    priority: int = 0
    task_id: str = ""
    status: str = "pending"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    records_processed: int = 0
    error_message: str = ""

@dataclass
class MigrationResult:
    """迁移结果"""
    success: bool
    table_name: str
    records_migrated: int
    start_time: datetime
    end_time: datetime
    error_message: Optional[str] = None
    execution_time: float = 0.0

    def __post_init__(self):
        if self.end_time and self.start_time:
            self.execution_time = (self.end_time - self.start_time).total_seconds()

@dataclass
class MigrationStats:
    """迁移统计"""
    total_tables: int = 0
    completed_tables: int = 0
    failed_tables: int = 0
    total_records: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    execution_time: float = 0.0

    @property
    def progress_percentage(self) -> float:
        if self.total_tables == 0:
            return 0.0
        return (self.completed_tables + self.failed_tables) / self.total_tables * 100

    @property
    def success_rate(self) -> float:
        if self.total_tables == 0:
            return 0.0
        return self.completed_tables / self.total_tables * 100

@dataclass
class TableInfo:
    """表信息"""
    name: str
    source_name: str
    description: str
    migration_days: int
    status: str = "not_started"
    last_migration: Optional[datetime] = None
    records_migrated: int = 0
    avg_migration_time: float = 0.0