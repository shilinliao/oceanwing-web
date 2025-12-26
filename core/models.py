"""数据模型定义"""
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime

@dataclass(order=True)
class MigrationTask:
    """迁移任务数据类"""
    priority: int = field(compare=True)
    task_id: int = field(compare=False)
    source_table: str = field(compare=False)
    target_table: str = field(compare=False)
    day: int = field(compare=False)
    date_str: str = field(compare=False)
    columns: List['ColumnDefinition'] = field(compare=False)
    table_index: int = field(compare=False)

    def __init__(self, source_table: str, target_table: str, day: int, date_str: str,
                 columns: List['ColumnDefinition'], task_id: int, priority: int = 0, table_index: int = 0):
        self.priority = priority
        self.task_id = task_id
        self.source_table = source_table
        self.target_table = target_table
        self.day = day
        self.date_str = date_str
        self.columns = columns
        self.table_index = table_index

    def __repr__(self):
        return f"MigrationTask(id={self.task_id}, priority={self.priority}, date={self.date_str}, table={self.target_table})"


class ColumnDefinition:
    """列定义类"""
    def __init__(self, name: str, data_type: str):
        self.name = name
        self.type = data_type

    def get_name(self) -> str:
        return self.name

    def get_type(self) -> str:
        return self.type

    def __repr__(self):
        return f"ColumnDefinition(name={self.name}, type={self.type})"


@dataclass
class MigrationResult:
    """迁移结果类"""
    success: bool
    table_name: str
    records_migrated: int
    start_time: datetime
    end_time: datetime
    error_message: Optional[str] = None

    @property
    def duration(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    def __repr__(self):
        status = "SUCCESS" if self.success else "FAILED"
        return f"MigrationResult({status}, {self.table_name}, records={self.records_migrated}, duration={self.duration:.2f}s)"