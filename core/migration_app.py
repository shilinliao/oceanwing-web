"""主应用类"""
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from threading import Event

from config.settings import Config
from core.models import MigrationTask, ColumnDefinition
from core.counters import ThreadSafeCounter
from database.clickhouse_client import ClickHouseClientManager
from database.mysql_client import MySQLClientManager
from workers.task_processor import TaskProcessor
from workers.table_worker import TableWorkerManager

logger = logging.getLogger('DataMigrationApp')


class DataMigrationApp:
    """数据迁移主应用"""

    def __init__(self, max_workers_per_table: int = None, schedule_enabled: bool = True):
        # 配置
        self.max_workers_per_table = max_workers_per_table or Config.MAX_WORKERS_PER_TABLE
        self.schedule_enabled = schedule_enabled

        # 数据库管理器
        self.clickhouse_manager = ClickHouseClientManager()
        self.mysql_manager = MySQLClientManager()

        # 任务处理器
        self.task_processor = TaskProcessor(self.clickhouse_manager, self.mysql_manager)

        # 工作线程管理器
        self.worker_manager = TableWorkerManager(self.task_processor, self.max_workers_per_table)

        # 状态控制
        self.is_running = False
        self.shutdown_event = Event()
        self.task_counter = ThreadSafeCounter()

        # 表配置
        self.source_tables = Config.SOURCE_TABLES
        self.target_tables = Config.TARGET_TABLES
        self.table_columns_mapping = Config.get_table_columns_mapping()

    def migrate_single_table(self, source_table: str, target_table: str, days: int = 30) -> bool:
        """迁移单个表"""
        if source_table not in self.source_tables or target_table not in self.target_tables:
            logger.error(f"Invalid table configuration: {source_table} -> {target_table}")
            return False

        table_index = self.source_tables.index(source_table)
        table_key = f"{source_table}_{target_table}"

        logger.info(f"Starting single table migration: {source_table} -> {target_table} (last {days} days)")

        try:
            # 生成迁移任务
            tasks = self._generate_migration_tasks(source_table, target_table, table_index, days)
            if not tasks:
                logger.error("No tasks generated for migration")
                return False

            # 启动工作线程
            self.worker_manager.start_table_workers(table_key, table_index)

            # 添加任务到队列
            for task in tasks:
                self.worker_manager.add_task(table_key, task)

            # 等待任务完成
            success = self.worker_manager.wait_for_completion(table_key, timeout=3600)  # 1小时超时

            # 停止工作线程
            self.worker_manager.stop_table_workers(table_key)

            # 获取统计信息
            stats = self.worker_manager.get_statistics()
            logger.info(f"Single table migration completed: {stats}")

            return success

        except Exception as e:
            logger.error(f"Single table migration failed: {str(e)}", exc_info=True)
            self.worker_manager.stop_table_workers(table_key)
            return False

    def migrate_all_tables_parallel(self, days_override: Dict[str, int] = None) -> bool:
        """并行迁移所有表"""
        if self.is_running:
            logger.warning("Migration is already running")
            return False

        self.is_running = True
        start_time = time.time()

        logger.info("=" * 60)
        logger.info("Starting parallel migration for all tables")
        logger.info(f"Workers per table: {self.max_workers_per_table}")
        logger.info(f"Total tables: {len(self.source_tables)}")
        logger.info("=" * 60)

        # 重置统计
        self.task_counter.reset()
        self.worker_manager.total_records.reset()
        self.worker_manager.completed_tasks.reset()
        self.worker_manager.failed_tasks.reset()

        table_threads = []
        table_results = {}

        try:
            # 为每个表创建迁移线程
            for i, (source_table, target_table) in enumerate(zip(self.source_tables, self.target_tables)):
                # 使用配置的迁移天数
                migration_days_config = {
                    "ods_query": 30,
                    "ods_campain": 30,
                    "ods_campaign_dsp": 30,
                    "ods_aws_asin_philips": 30
                }
                days = migration_days_config.get(target_table, 30)

                # 应用覆盖配置
                if days_override and target_table in days_override:
                    days = days_override[target_table]

                # 创建并启动表迁移线程
                thread = threading.Thread(
                    target=self._migrate_table_thread,
                    args=(source_table, target_table, i, days, table_results),
                    name=f"Table{i}-Migration",
                    daemon=True
                )
                thread.start()
                table_threads.append(thread)

            # 等待所有表迁移完成
            completed_count = 0
            total_tables = len(table_threads)

            while completed_count < total_tables and not self.shutdown_event.is_set():
                time.sleep(2)

                # 检查线程状态
                for thread in table_threads[:]:
                    if not thread.is_alive():
                        table_threads.remove(thread)
                        completed_count += 1

                # 更新进度
                if completed_count > 0:
                    progress = (completed_count / total_tables) * 100
                    elapsed_time = time.time() - start_time
                    logger.info(f"Overall progress: {completed_count}/{total_tables} tables ({progress:.1f}%), "
                                f"Elapsed: {elapsed_time:.0f}s")

            # 如果收到关闭信号，停止所有迁移
            if self.shutdown_event.is_set():
                logger.info("Shutdown requested, stopping all migrations")
                self._stop_all_migrations(table_threads)
                return False

            # 等待所有线程完全结束
            for thread in table_threads:
                thread.join(timeout=10)

            # 统计结果
            successful_tables = sum(1 for result in table_results.values() if result)
            total_time = time.time() - start_time
            stats = self.worker_manager.get_statistics()

            logger.info("=" * 60)
            logger.info(f"All tables migration completed in {total_time:.2f}s")
            logger.info(f"Successful tables: {successful_tables}/{total_tables}")
            logger.info(f"Total records migrated: {stats['total_records']}")
            logger.info(f"Completed tasks: {stats['completed_tasks']}")
            logger.info(f"Failed tasks: {stats['failed_tasks']}")
            if total_time > 0:
                logger.info(f"Average speed: {stats['total_records'] / total_time:.1f} records/s")
            logger.info("=" * 60)

            return successful_tables == total_tables

        except Exception as e:
            logger.error(f"Parallel migration failed: {str(e)}", exc_info=True)
            self._stop_all_migrations(table_threads)
            return False
        finally:
            self.is_running = False

    def _migrate_table_thread(self, source_table: str, target_table: str, table_index: int,
                              days: int, results_dict: Dict):
        """表迁移线程函数"""
        table_key = f"{source_table}_{target_table}"
        thread_id = threading.get_ident()

        try:
            logger.info(f"Table-{table_index}: Starting migration for {source_table} -> {target_table}")

            # 获取表结构
            columns = self.clickhouse_manager.get_table_schema(thread_id, source_table, table_index)
            if not columns:
                logger.error(f"Table-{table_index}: Failed to get schema for {source_table}")
                results_dict[table_key] = False
                return

            # 创建目标表
            self.mysql_manager.create_table_if_not_exists(thread_id, target_table, columns, table_index)

            # 生成迁移任务
            tasks = self._generate_migration_tasks(source_table, target_table, table_index, days, columns)
            if not tasks:
                logger.error(f"Table-{table_index}: No tasks generated")
                results_dict[table_key] = False
                return

            # 启动工作线程
            self.worker_manager.start_table_workers(table_key, table_index)

            # 添加任务到队列
            for task in tasks:
                if self.shutdown_event.is_set():
                    break
                self.worker_manager.add_task(table_key, task)

            # 等待任务完成
            success = self.worker_manager.wait_for_completion(table_key, timeout=3600)

            # 停止工作线程
            self.worker_manager.stop_table_workers(table_key)

            results_dict[table_key] = success
            logger.info(f"Table-{table_index}: Migration {'succeeded' if success else 'failed'}")

        except Exception as e:
            logger.error(f"Table-{table_index}: Migration error: {str(e)}", exc_info=True)
            results_dict[table_key] = False
            self.worker_manager.stop_table_workers(table_key)

    def _generate_migration_tasks(self, source_table: str, target_table: str, table_index: int,
                                  days: int, columns: List[ColumnDefinition] = None) -> List[MigrationTask]:
        """生成迁移任务"""
        if columns is None:
            thread_id = threading.get_ident()
            columns = self.clickhouse_manager.get_table_schema(thread_id, source_table, table_index)

        tasks = []
        current_time = datetime.now()

        # 生成过去days天的任务
        for i in range(days, 0, -1):
            if self.shutdown_event.is_set():
                break

            date = current_time + timedelta(days=-i)
            date_str = self._format_date(date)
            task_id = self.task_counter.increment()

            task = MigrationTask(
                source_table=source_table,
                target_table=target_table,
                day=-i,
                date_str=date_str,
                columns=columns,
                task_id=task_id,
                priority=i,  # 越旧的数据优先级越高
                table_index=table_index
            )
            tasks.append(task)

        # 按优先级排序（优先级数字越小越先执行）
        tasks.sort(key=lambda x: x.priority)
        logger.info(f"Generated {len(tasks)} tasks for {target_table}")

        return tasks

    def _stop_all_migrations(self, table_threads: List[threading.Thread]):
        """停止所有迁移"""
        self.shutdown_event.set()

        # 停止所有工作线程
        self.worker_manager.stop_all_workers()

        # 等待表线程结束
        for thread in table_threads:
            thread.join(timeout=2)

    def _format_date(self, date_obj: datetime) -> str:
        """格式化日期"""
        return date_obj.strftime('%Y-%m-%d')

    def get_status(self) -> Dict[str, Any]:
        """获取应用状态"""
        stats = self.worker_manager.get_statistics()
        return {
            'is_running': self.is_running,
            'total_tasks': self.task_counter.get(),
            'total_records': stats['total_records'],
            'completed_tasks': stats['completed_tasks'],
            'failed_tasks': stats['failed_tasks'],
            'shutdown_requested': self.shutdown_event.is_set()
        }

    def shutdown(self):
        """优雅关闭应用"""
        logger.info("Shutting down migration app...")
        self.shutdown_event.set()
        self.is_running = False

        # 停止所有工作线程
        self.worker_manager.stop_all_workers()

        # 关闭数据库连接
        self.clickhouse_manager.close_all()
        self.mysql_manager.close_all()

        logger.info("Migration app shutdown completed")