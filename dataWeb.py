"""程序入口点"""
import argparse
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.logger import setup_logging
from core.migration_app import DataMigrationApp
from scheduler.migration_scheduler import MigrationScheduler
from config.settings import Config


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='数据迁移管理系统')

    parser.add_argument('--once', action='store_true',
                        help='运行一次迁移后退出')
    parser.add_argument('--workers', type=int, default=Config.MAX_WORKERS_PER_TABLE,
                        help=f'每表工作线程数 (默认: {Config.MAX_WORKERS_PER_TABLE})')
    parser.add_argument('--days', type=int,
                        help='迁移天数 (覆盖默认配置)')
    parser.add_argument('--table', type=str,
                        help='只迁移指定表')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO', help='日志级别')
    parser.add_argument('--log-file', default='data_migration.log',
                        help='日志文件路径')

    return parser.parse_args()


def main():
    """主函数"""
    args = parse_arguments()

    # 设置日志
    setup_logging(args.log_level, args.log_file)

    # 创建应用实例
    schedule_enabled = not args.once
    app = None

    try:
        # 创建迁移应用
        app = DataMigrationApp(
            max_workers_per_table=args.workers,
            schedule_enabled=sche