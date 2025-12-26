"""线程安全计数器"""
from threading import Lock

class ThreadSafeCounter:
    """线程安全计数器"""
    def __init__(self, initial_value: int = 0):
        self.value = initial_value
        self.lock = Lock()

    def increment(self, amount: int = 1) -> int:
        """增加计数值"""
        with self.lock:
            self.value += amount
            return self.value

    def decrement(self, amount: int = 1) -> int:
        """减少计数值"""
        with self.lock:
            self.value -= amount
            return self.value

    def get(self) -> int:
        """获取当前值"""
        with self.lock:
            return self.value

    def set(self, value: int) -> None:
        """设置值"""
        with self.lock:
            self.value = value

    def reset(self) -> None:
        """重置计数器"""
        with self.lock:
            self.value = 0