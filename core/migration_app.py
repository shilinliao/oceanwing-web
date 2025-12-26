"""数据迁移应用核心 - 修复版本"""
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class DataMigrationApp:
    """数据迁移应用 - 修复版本"""

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.is_running = False
        self.current_task = None
        self._stop_event = threading.Event()

        # 统计信息
        self.stats = {
            'total_tables': 4,
            'completed_tables': 0,
            'failed_tables': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None,
            'execution_time': 0
        }

        # 表信息
        self.tables_info = {
            'ods_query': {
                'name': 'ods_query',
                'source_name': 'ods_Query',
                'status': 'not_started',
                'last_migration': None,
                'records_migrated': 0,
                'description': '查询数据表'
            },
            'ods_campain': {
                'name': 'ods_campain',
                'source_name': 'ods_campain',
                'status': 'not_started',
                'last_migration': None,
                'records_migrated': 0,
                'description': '活动数据表'
            },
            'ods_campaign_dsp': {
                'name': 'ods_campaign_dsp',
                'source_name': 'ods_campaign_dsp',
                'status': 'not_started',
                'last_migration': None,
                'records_migrated': 0,
                'description': 'DSP活动数据表'
            },
            'ods_aws_asin_philips': {
                'name': 'ods_aws_asin_philips',
                'source_name': 'ods_aws_asin_philips',
                'status': 'not_started',
                'last_migration': None,
                'records_migrated': 0,
                'description': 'AWS ASIN数据表'
            }
        }

    def get_status(self) -> Dict[str, Any]:
        """获取应用状态"""
        return {
            'is_running': self.is_running,
            'stats': self.stats,
            'tables_info': self.tables_info,
            'stop_requested': self._stop_event.is_set()
        }

    def get_overall_progress(self) -> Dict[str, Any]:
        """获取总体进度"""
        completed = self.stats['completed_tables']
        failed = self.stats['failed_tables']
        total = self.stats['total_tables']
        progress = (completed + failed) / total * 100 if total > 0 else 0

        execution_time = 0
        if self.stats['start_time']:
            if self.stats['end_time']:
                execution_time = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            else:
                execution_time = (datetime.now() - self.stats['start_time']).total_seconds()

        return {
            'progress_percentage': progress,
            'completed_tables': completed,
            'failed_tables': failed,
            'total_tables': total,
            'total_records': self.stats['total_records'],
            'execution_time': execution_time,
            'is_running': self.is_running
        }

    def get_table_progress(self, table_name: str) -> Dict[str, Any]:
        """获取表迁移进度"""
        if table_name not in self.tables_info:
            return {}

        return self.tables_info[table_name]

    def test_connections(self) -> Dict[str, bool]:
        """测试数据库连接"""
        # 模拟连接测试
        return {
            'clickhouse': True,
            'mysql': True
        }

    def migrate_all_tables(self, days_override: Dict[str, int] = None) -> bool:
        """迁移所有表"""
        if self.is_running:
            return False

        self.is_running = True
        self._stop_event.clear()
        self.stats = {
            'total_tables': 4,
            'completed_tables': 0,
            'failed_tables': 0,
            'total_records': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'execution_time': 0
        }

        # 重置表状态
        for table_info in self.tables_info.values():
            table_info['status'] = 'not_started'
            table_info['records_migrated'] = 0

        # 在后台线程中运行模拟迁移
        def run_simulation():
            try:
                tables_to_migrate = list(self.tables_info.keys())
                total_tables = len(tables_to_migrate)

                for i, table_name in enumerate(tables_to_migrate):
                    if self._stop_event.is_set():
                        break

                    # 模拟表迁移
                    table_info = self.tables_info[table_name]
                    table_info['status'] = 'running'

                    # 模拟迁移过程
                    for step in range(10):
                        if self._stop_event.is_set():
                            break

                        time.sleep(0.5)  # 模拟处理时间

                        # 模拟记录迁移
                        records_in_step = 100 + i * 50 + step * 10
                        self.stats['total_records'] += records_in_step
                        table_info['records_migrated'] += records_in_step

                    if self._stop_event.is_set():
                        table_info['status'] = 'stopped'
                        break

                    # 标记表完成
                    table_info['status'] = 'completed'
                    table_info['last_migration'] = datetime.now()
                    self.stats['completed_tables'] += 1

                self.stats['end_time'] = datetime.now()
                self.stats['execution_time'] = (
                    self.stats['end_time'] - self.stats['start_time']
                ).total_seconds()

                self.is_running = False

            except Exception as e:
                logger.error(f"迁移过程出错: {str(e)}")
                self.is_running = False

        # 启动模拟线程
        thread = threading.Thread(target=run_simulation, daemon=True)
        thread.start()

        return True

    def migrate_single_table(self, table_name: str, days: int = 30) -> bool:
        """迁移单个表"""
        if table_name not in self.tables_info:
            return False

        if self.is_running:
            return False

        self.is_running = True
        self._stop_event.clear()

        def run_single_table():
            try:
                table_info = self.tables_info[table_name]
                table_info['status'] = 'running'
                table_info['records_migrated'] = 0

                # 模拟迁移过程
                for step in range(10):
                    if self._stop_event.is_set():
                        break

                    time.sleep(0.3)

                    # 模拟记录迁移
                    records_in_step = 200 + step * 20
                    self.stats['total_records'] += records_in_step
                    table_info['records_migrated'] += records_in_step

                if not self._stop_event.is_set():
                    table_info['status'] = 'completed'
                    table_info['last_migration'] = datetime.now()
                    self.stats['completed_tables'] += 1
                else:
                    table_info['status'] = 'stopped'

                self.is_running = False

            except Exception as e:
                logger.error(f"单表迁移出错: {str(e)}")
                table_info['status'] = 'failed'
                self.stats['failed_tables'] += 1
                self.is_running = False

        thread = threading.Thread(target=run_single_table, daemon=True)
        thread.start()

        return True

    def stop_migration(self) -> bool:
        """停止迁移"""
        if self.is_running:
            self._stop_event.set()
            self.is_running = False
            return True
        return False

    def reset_migration(self) -> bool:
        """重置迁移状态"""
        self.stop_migration()
        self._stop_event.clear()

        # 重置统计
        self.stats = {
            'total_tables': 4,
            'completed_tables': 0,
            'failed_tables': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None,
            'execution_time': 0
        }

        # 重置表状态
        for table_info in self.tables_info.values():
            table_info['status'] = 'not_started'
            table_info['records_migrated'] = 0
            table_info['last_migration'] = None

        return True