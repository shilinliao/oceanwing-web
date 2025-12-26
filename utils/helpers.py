"""工具函数"""
import time
import random
from datetime import datetime, timedelta
from typing import Any, Callable, Optional
import functools


def retry_with_backoff(max_retries: int = 3, delay_base: float = 1.0,
                       exceptions: tuple = (Exception,)):
    """带退避的重试装饰器"""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        raise

                    wait_time = delay_base * (2 ** attempt) + random.uniform(0, 1)
                    print(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {wait_time:.2f}s...")
                    time.sleep(wait_time)
            return None

        return wrapper

    return decorator


def format_duration(seconds: float) -> str:
    """格式化时间间隔"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def human_readable_size(size_bytes: int) -> str:
    """将字节数转换为人类可读的大小"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def get_memory_usage() -> dict:
    """获取内存使用情况"""
    process = psutil.Process()
    memory_info = process.memory_info()

    return {
        'rss': human_readable_size(memory_info.rss),  # 常驻内存
        'vms': human_readable_size(memory_info.vms),  # 虚拟内存
        'percent': process.memory_percent()
    }


class Timer:
    """计时器上下文管理器"""

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.duration = self.end - self.start

    def get_duration(self) -> float:
        return getattr(self, 'duration', 0)