"""数据迁移应用核心"""
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.settings import Config
from core.models import MigrationResult, MigrationStats, TableInfo
from database.clickhouse_client import ClickHouseClient
from database.mysql_client import MySQLClient

logger = logging.getLogger(__name__)

class DataMigrationApp:
    """数据迁移应用"""

    def __init__(self, max_workers: int = None):
        self.max_workers = max_workers or Config.MAX_WORKERS_PER_TABLE
        self.is_running = False
        self.current_task = None
        self.stats = MigrationStats()

        # 数据库客户端
        self.clickhouse_client = ClickHouseClient()
        self.mysql_client = MySQLClient()

        # 线程控制
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # 表信息
        self.tables_info = self._initialize_tables_info()

    def _initialize_tables_info(self) -> Dict[str, TableInfo]:
        """初始化表信息"""
        tables = {}
        for source, target in zip(Config.SOURCE_TABLES, Config.TARGET_TABLES):
            tables[target] = TableInfo(
                name=target,
                source_name=source,
                description=f"{source} -> {target} 数据迁移",
                migration_days=Config.MIGRATION_DAYS.get(target, 30)
            )
        return tables

    def test_connections(self) -> Dict[str, bool]:
        """测试数据库连接"""
        results = {
            'clickhouse': self.clickhouse_client.test_connection(),
            'mysql': self.mysql_client.test_connection()
        }
        return results

    def migrate_all_tables(self, days_override: Dict[str, int] = None) -> bool:
        """迁移所有表"""
        if self.is_running:
            logger.warning("迁移任务已在运行中")
            return False

        self.is_running = True
        self._stop_event.clear()
        self.stats = MigrationStats(
            total_tables=len(self.tables_info),
            start_time=datetime.now()
        )

        try:
            # 使用线程池并行迁移表
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有表迁移任务
                future_to_table = {}
                for table_info in self.tables_info.values():
                    if self._stop_event.is_set():
                        break

                    days = days_override.get(table_info.name, table_info.migration_days) if days_override else table_info.migration_days
                    future = executor.submit(self._migrate_single_table, table_info, days)
                    future_to_table[future] = table_info.name

                # 等待所有任务完成
                for future in as_completed(future_to_table):
                    if self._stop_event.is_set():
                        break

                    table_name = future_to_table[future]
                    try:
                        result = future.result()
                        if result.success:
                            self.stats.completed_tables += 1
                            self.stats.total_records += result.records_migrated
                            self.tables_info[table_name].status = "completed"
                            self.tables_info[table_name].last_migration = datetime.now()
                            self.tables_info[table_name].records_migrated = result.records_migrated
                        else:
                            self.stats.failed_tables += 1
                            self.tables_info[table_name].status = "failed"
                    except Exception as e:
                        logger.error(f"表 {table_name} 迁移失败: {str(e)}")
                        self.stats.failed_tables += 1
                        self.tables_info[table_name].status = "failed"

            self.stats.end_time = datetime.now()
            self.stats.execution_time = (self.stats.end_time - self.stats.start_time).total_seconds()

            success = self.stats.failed_tables == 0
            logger.info(f"所有表迁移完成: 成功{self.stats.completed_tables}/失败{self.stats.failed_tables}")

            return success

        except Exception as e:
            logger.error(f"迁移过程发生错误: {str(e)}")
            return False
        finally:
            self.is_running = False

    def migrate_single_table(self, table_name: str, days: int = None) -> bool:
        """迁移单个表"""
        if table_name not in self.tables_info:
            logger.error(f"表 {table_name} 不存在")
            return False

        if self.is_running:
            logger.warning("迁移任务已在运行中")
            return False

        self.is_running = True
        table_info = self.tables_info[table_name]
        migration_days = days or table_info.migration_days

        try:
            result = self._migrate_single_table(table_info, migration_days)

            if result.success:
                table_info.status = "completed"
                table_info.last_migration = datetime.now()
                table_info.records_migrated = result.records_migrated
            else:
                table_info.status = "failed"

            return result.success

        except Exception as e:
            logger.error(f"表 {table_name} 迁移失败: {str(e)}")
            table_info.status = "failed"
            return False
        finally:
            self.is_running = False

    def _migrate_single_table(self, table_info: TableInfo, days: int) -> MigrationResult:
        """迁移单个表的具体实现"""
        start_time = datetime.now()
        result = MigrationResult(
            success=False,
            table_name=table_info.name,
            records_migrated=0,
            start_time=start_time,
            end_time=start_time
        )

        try:
            logger.info(f"开始迁移表: {table_info.source_name} -> {table_info.name}")

            # 连接数据库
            if not self.clickhouse_client.connect():
                result.error_message = "ClickHouse连接失败"
                return result

            if not self.mysql_client.connect():
                result.error_message = "MySQL连接失败"
                return result

            # 获取表结构
            schema = self.clickhouse_client.get_table_schema(table_info.source_name)
            if not schema:
                result.error_message = "获取表结构失败"
                return result

            # 确保目标表存在
            if not self.mysql_client.table_exists(table_info.name):
                if not self.mysql_client.create_table(table_info.name, schema):
                    result.error_message = "创建目标表失败"
                    return result

            # 迁移数据
            records_migrated = self._migrate_table_data(table_info, schema, days)
            result.records_migrated = records_migrated
            result.success = True

            logger.info(f"表 {table_info.name} 迁移完成: {records_migrated} 条记录")

        except Exception as e:
            result.error_message = str(e)
            logger.error(f"表 {table_info.name} 迁移失败: {str(e)}")

        finally:
            result.end_time = datetime.now()
            return result

    def _migrate_table_data(self, table_info: TableInfo, schema: List[Dict], days: int) -> int:
        """迁移表数据"""
        total_records = 0

        # 获取时间字段
        time_field = Config.get_table_time_field(table_info.name)

        # 按天迁移数据
        for day_offset in range(days, 0, -1):
            if self._stop_event.is_set():
                break

            # 计算日期范围
            end_date = datetime.now() - timedelta(days=day_offset-1)
            start_date = end_date - timedelta(days=1)

            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            try:
                # 获取源数据
                select_fields = ", ".join([f"`{col['name']}`" for col in schema])
                where_conditions = [
                    f"`{time_field}` >= '{start_date_str}'",
                    f"`{time_field}` < '{end_date_str}'"
                ]

                # 添加过滤条件
                filter_condition = Config.get_table_filter_condition(table_info.name)
                if filter_condition:
                    where_conditions.append(filter_condition.strip().lstrip('AND').strip())

                where_clause = " AND ".join(where_conditions)
                select_sql = f"SELECT {select_fields} FROM {table_info.source_name} WHERE {where_clause}"

                # 执行查询
                result = self.clickhouse_client.execute_query(select_sql)
                if not result or not hasattr(result, 'result_rows'):
                    continue

                records = result.result_rows
                if not records:
                    continue

                # 准备插入数据
                column_mapping = Config.TABLE_COLUMNS_MAPPING.get(table_info.name, {})
                column_names = []
                for col in schema:
                    target_name = column_mapping.get(col['name'], col['name'].lower())
                    column_names.append(target_name)

                placeholders = ", ".join(["%s"] * len(column_names))
                insert_sql = f"INSERT INTO {table_info.name} ({', '.join(column_names)}) VALUES ({placeholders})"
                # 批量插入数据
                batch_size = Config.BATCH_SIZE
                for i in range(0, len(records), batch_size):
                    if self._stop_event.is_set():
                        break

                    batch = records[i:i + batch_size]
                    processed_batch = []

                    for record in batch:
                        processed_record = []
                        for value in record:
                            if isinstance(value, (list, dict)):
                                processed_record.append(str(value))
                            else:
                                processed_record.append(value)
                        processed_batch.append(tuple(processed_record))

                    # 执行批量插入
                    try:
                        row_count = self.mysql_client.execute_many(insert_sql, processed_batch)
                        total_records += row_count
                    except Exception as e:
                        logger.error(f"批量插入失败: {str(e)}")
                        # 尝试逐条插入
                        for record in processed_batch:
                            try:
                                self.mysql_client.execute_query(insert_sql, record, fetch=False)
                                total_records += 1
                            except Exception as e2:
                                logger.warning(f"单条记录插入失败: {str(e2)}")

                logger.info(f"日期 {start_date_str} 迁移完成: {len(records)} 条记录")

            except Exception as e:
                logger.error(f"日期 {start_date_str} 迁移失败: {str(e)}")
                continue

            return total_records

            def stop_migration(self):
                """停止迁移"""
                if self.is_running:
                    logger.info("正在停止迁移任务...")
                    self._stop_event.set()
                    self.is_running = False
                    return True
                return False

            def get_status(self) -> Dict[str, Any]:
                """获取应用状态"""
                return {
                    'is_running': self.is_running,
                    'stats': self.stats,
                    'tables_info': self.tables_info,
                    'stop_requested': self._stop_event.is_set()
                }

            def get_table_progress(self, table_name: str) -> Dict[str, Any]:
                """获取表迁移进度"""
                if table_name not in self.tables_info:
                    return {}

                table_info = self.tables_info[table_name]
                return {
                    'name': table_info.name,
                    'status': table_info.status,
                    'last_migration': table_info.last_migration,
                    'records_migrated': table_info.records_migrated,
                    'description': table_info.description
                }

            def get_overall_progress(self) -> Dict[str, Any]:
                """获取总体进度"""
                completed = self.stats.completed_tables
                failed = self.stats.failed_tables
                total = self.stats.total_tables
                progress = (completed + failed) / total * 100 if total > 0 else 0

                return {
                    'progress_percentage': progress,
                    'completed_tables': completed,
                    'failed_tables': failed,
                    'total_tables': total,
                    'total_records': self.stats.total_records,
                    'execution_time': self.stats.execution_time,
                    'is_running': self.is_running
                }