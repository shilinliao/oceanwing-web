"""日志配置"""
import logging
import sys
from pathlib import Path


def setup_logging(log_level: str = "INFO", log_file: str = "data_migration.log"):
    """设置日志配置"""

    # 创建日志目录
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # 配置日志格式
    log_format = '%(asctime)s [%(process)d:%(threadName)s] [%(name)s] %(levelname)s - %(message)s'

    # 创建根日志记录器
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))

    # 清除现有处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format))

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(log_format))

    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger