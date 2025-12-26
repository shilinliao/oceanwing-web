"""表工作线程管理"""
import logging
import threading
from queue import Queue, Empty
from typing import Dict, List
from threading import Event

from core.models import MigrationTask
from core.counters import ThreadSafeCounter
from workers.task_processor import TaskProcessor

logger = logging.getLogger('DataMigrationApp')


class TableWorkerManager:
    """表工作线程管理器"""

    def __init__(self, task_processor: TaskProcessor, max_workers_per_table: int = 4):
        self.task_processor = task_processor
        self.max_workers_per_table = max_workers_per_table

        # 工作线程管理
        self.worker_threads = {}
        self.worker_stop_events = {}
        self.table_queues = {}

        # 统计计数器
        self.total_records = ThreadSafeCounter()
        self.completed_tasks = ThreadSafeCounter()
        self.failed_tasks = ThreadSafeCounter()

    def start_table_workers(self, table_key: str, table_index: int):
        """启动表工作线程"""
        if table_key in self.worker_threads:
            logger.warning(f"Workers for table {table_key} are already running")
            return

        # 创建任务队列
        if table_key not in self.table_queues:
            self.table_queues[table_key] = Queue()

        # 创建停止事件
        stop_events = []
        worker_threads = []

        for worker_id in range(self.max_workers_per_table):
            stop_event = Event()
            worker_thread = threading.Thread(
                target=self._worker_thread_func,
                args=(table_key, table_index, worker_id, stop_event),
                name=f"Table{table_index}-Worker{worker_id}",
                daemon=True
            )
            worker_thread.start()

            stop_events.append(stop_event)
            worker_threads.append(worker_thread)

        self.worker_threads[table_key] = worker_threads
        self.worker_stop_events[table_key] = stop_events

        logger.info(f"Started {self.max_workers_per_table} workers for table {table_key}")

    def stop_table_workers(self, table_key: str, timeout: float = 5.0):
        """停止表工作线程"""
        if table_key not in self.worker_threads:
            return

        # 发送停止信号
        for stop_event in self.worker_stop_events[table_key]:
            stop_event.set()

        # 发送终止信号到队列
        queue = self.table_queues.get(table_key)
        if queue:
            for _ in range(self.max_workers_per_table):
                queue.put(None)

        # 等待线程结束
        for thread in self.worker_threads[table_key]:
            thread.join(timeout=timeout)

        # 清理资源
        del self.worker_threads[table_key]
        del self.worker_stop_events[table_key]
        if table_key in self.table_queues:
            del self.table_queues[table_key]

        logger.info(f"Stopped workers for table {table_key}")

    def add_task(self, table_key: str, task: MigrationTask):
        """添加任务到队列"""
        if table_key not in self.table_queues:
            logger.error(f"No queue found for table {table_key}")
            return False

        self.table_queues[table_key].put(task)
        return True

    def wait_for_completion(self, table_key: str, timeout: float = None):
        """等待表任务完成"""
        if table_key not in self.table_queues:
            return True

        queue = self.table_queues[table_key]
        try:
            queue.join()
            return True
        except Exception as e:
            logger.error(f"Error waiting for queue completion: {str(e)}")
            return False

    def get_queue_size(self, table_key: str) -> int:
        """获取队列大小"""
        if table_key in self.table_queues:
            return self.table_queues[table_key].qsize()
        return 0

    def _worker_thread_func(self, table_key: str, table_index: int, worker_id: int, stop_event: Event):
        """工作线程函数"""
        logger.info(f"Table-{table_index}-Worker-{worker_id}: Started")

        while not stop_event.is_set():
            try:
                # 从队列获取任务
                task = self.table_queues[table_key].get(timeout=1)
                if task is None:  # 终止信号
                    break

                # 处理任务
                try:
                    insert_count = self.task_processor.process_task(task, worker_id)

                    if insert_count > 0:
                        self.total_records.increment(insert_count)
                        self.completed_tasks.increment()
                    else:
                        self.failed_tasks.increment()

                except Exception as e:
                    logger.error(f"Table-{table_index}-Worker-{worker_id}: Task processing error: {str(e)}")
                    self.failed_tasks.increment()
                finally:
                    self.table_queues[table_key].task_done()

            except Empty:
                continue
            except Exception as e:
                if not stop_event.is_set():
                    logger.error(f"Table-{table_index}-Worker-{worker_id}: Queue error: {str(e)}")
                continue

        logger.info(f"Table-{table_index}-Worker-{worker_id}: Stopped")

    def get_statistics(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            'total_records': self.total_records.get(),
            'completed_tasks': self.completed_tasks.get(),
            'failed_tasks': self.failed_tasks.get()
        }

    def stop_all_workers(self, timeout: float = 5.0):
        """停止所有工作线程"""
        for table_key in list(self.worker_threads.keys()):
            self.stop_table_workers(table_key, timeout)

    def is_table_running(self, table_key: str) -> bool:
        """检查表是否在运行"""
        return table_key in self.worker_threads