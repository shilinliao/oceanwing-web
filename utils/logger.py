"""日志配置"""
import logging
import sys
from pathlib import Path

def setup_logging(level=logging.INFO, log_file=None):
    """设置日志配置"""
    # 创建日志格式
    formatter = logging.Formatter(
        '%(asctime)s [%(process)d:%(threadName)s] [%(name)s] %(levelname)s - %(message)s'
    )

    # 获取根日志记录器
    logger = logging.getLogger()
    logger.setLevel(level)

    # 清除现有处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件处理器
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger