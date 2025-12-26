"""程序入口点"""
import argparse
import sys
import os
import logging
import signal
import time

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.logger import setup_logging
from core.migration_app import DataMigrationApp
from scheduler.migration_scheduler import MigrationScheduler
from config.settings import Config
from config.validator import validate_all_configurations

logger = logging.getLogger('DataMigrationApp')

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='数据迁移管理系统 - ClickHouse到MySQL数据迁移',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 运行一次迁移后退出
  python main.py --once
  
  # 指定工作线程数和日志级别
  python main.py --once --workers 8 --log-level DEBUG
  
  # 只迁移特定表
  python main.py --once --table ods_query
  
  # 自定义迁移天数
  python main.py --once --days 7
  
  # 启动定时调度器
  python main.py
        """
    )

    parser.add_argument('--once', action='store_true',
                       help='运行一次迁移后退出（不启动调度器）')
    parser.add_argument('--workers', type=int, default=Config.MAX_WORKERS_PER_TABLE,
                       help=f'每表工作线程数 (默认: {Config.MAX_WORKERS_PER_TABLE})')
    parser.add_argument('--days', type=int,
                       help='迁移天数（覆盖所有表的默认配置）')
    parser.add_argument('--table', type=str, choices=['ods_query', 'ods_campain', 'ods_campaign_dsp', 'ods_aws_asin_philips'],
                       help='只迁移指定表（需要配合--once使用）')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='日志级别')
    parser.add_argument('--log-file', default='data_migration.log',
                       help='日志文件路径')
    parser.add_argument('--validate', action='store_true',
                       help='验证配置后退出')
    parser.add_argument('--test-connections', action='store_true',
                       help='测试数据库连接后退出')
    parser.add_argument('--simple-scheduler', action='store_true',
                       help='使用简单调度器（不处理信号）')

    return parser.parse_args()

def test_database_connections():
    """测试数据库连接"""
    logger.info("开始测试数据库连接...")

    try:
        # 测试ClickHouse连接
        import clickhouse_connect
        client = clickhouse_connect.get_client(**Config.CLICKHOUSE_CONFIG)
        result = client.query("SELECT 1 as test")
        client.close()
        logger.info("✅ ClickHouse连接测试成功")
    except Exception as e:
        logger.error(f"❌ ClickHouse连接测试失败: {str(e)}")
        return False

    try:
        # 测试MySQL连接
        import pymysql
        conn = pymysql.connect(**Config.MYSQL_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        conn.close()
        logger.info("✅ MySQL连接测试成功")
    except Exception as e:
        logger.error(f"❌ MySQL连接测试失败: {str(e)}")
        return False

    logger.info("🎉 所有数据库连接测试成功！")
    return True

def setup_signal_handlers(app: DataMigrationApp):
    """设置信号处理器"""
    def signal_handler(signum, frame):
        logger.info(f"收到信号 {signum}，开始优雅关闭...")
        app.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler) # kill命令

def print_startup_info(args, app: DataMigrationApp):
    """打印启动信息"""
    logger.info("=" * 70)
    logger.info("数据迁移管理系统 - 启动")
    logger.info("=" * 70)
    logger.info(f"运行模式: {'单次执行' if args.once else '定时调度'}")
    logger.info(f"工作线程: {app.max_workers_per_table} 线程/表")
    logger.info(f"目标表数: {len(Config.SOURCE_TABLES)}")
    logger.info(f"总线程数: {app.max_workers_per_table * len(Config.SOURCE_TABLES)}")
    logger.info(f"日志级别: {args.log_level}")
    logger.info(f"日志文件: {args.log_file}")

    if args.table:
        logger.info(f"指定表: {args.table}")
    if args.days:
        logger.info(f"迁移天数: {args.days}天")

    logger.info("配置表:")
    for i, (source, target) in enumerate(zip(Config.SOURCE_TABLES, Config.TARGET_TABLES)):
        days_config = Config.get_table_migration_days()
        days = days_config.get(target, 30)
        if args.days:
            days = args.days
        logger.info(f"  {i+1}. {source} -> {target} ({days}天)")

    logger.info("=" * 70)

def run_single_migration(args, app: DataMigrationApp) -> bool:
    """运行单次迁移"""
    try:
        if args.table:
            # 迁移单个表
            source_table = None
            target_table = args.table

            # 查找对应的源表
            for src, tgt in zip(Config.SOURCE_TABLES, Config.TARGET_TABLES):
                if tgt == target_table:
                    source_table = src
                    break

            if not source_table:
                logger.error(f"未找到表 {target_table} 的配置")
                return False

            days = args.days or Config.get_table_migration_days().get(target_table, 30)
            logger.info(f"开始迁移单个表: {source_table} -> {target_table} ({days}天)")

            return app.migrate_single_table(source_table, target_table, days)

        else:
            # 迁移所有表
            days_override = None
            if args.days:
                days_override = {table: args.days for table in Config.TARGET_TABLES}
                logger.info(f"所有表迁移天数设置为: {args.days}天")

            return app.migrate_all_tables_parallel(days_override)

    except KeyboardInterrupt:
        logger.info("用户中断迁移过程")
        app.shutdown()
        return False
    except Exception as e:
        logger.error(f"迁移过程发生错误: {str(e)}", exc_info=True)
        return False

def run_scheduler(args, app: DataMigrationApp):
    """运行调度器"""
    try:
        # 设置信号处理器（如果不使用简单调度器）
        if not args.simple_scheduler:
            setup_signal_handlers(app)

        # 创建调度器
        scheduler = MigrationScheduler(app)

        # 设置调度
        if not scheduler.setup_schedule():
            logger.error("调度器设置失败")
            return 1

        # 运行调度器
        if args.simple_scheduler:
            scheduler.run_scheduler()
        else:
            # 标准调度器运行
            logger.info("启动标准调度器...")
            scheduler.run_scheduler()

    except KeyboardInterrupt:
        logger.info("调度器被用户中断")
    except Exception as e:
        logger.error(f"调度器运行错误: {str(e)}", exc_info=True)
        return 1

    return 0

def main():
    """主函数"""
    args = parse_arguments()

    # 设置日志
    setup_logging(args.log_level, args.log_file)

    # 验证配置
    if args.validate:
        if validate_all_configurations():
            return 0
        else:
            return 1

    # 测试连接
    if args.test_connections:
        if test_database_connections():
            return 0
        else:
            return 1

    app = None
    exit_code = 0

    try:
        # 创建迁移应用
        schedule_enabled = not args.once
        app = DataMigrationApp(
            max_workers_per_table=args.workers,
            schedule_enabled=schedule_enabled
        )

        # 打印启动信息
        print_startup_info(args, app)

        # 测试数据库连接
        logger.info("测试数据库连接...")
        if not test_database_connections():
            logger.error("数据库连接测试失败，程序退出")
            return 1

        # 根据模式运行
        if args.once:
            # 单次执行模式
            success = run_single_migration(args, app)
            exit_code = 0 if success else 1

            if success:
                logger.info("🎉 数据迁移任务执行成功！")
            else:
                logger.error("❌ 数据迁移任务执行失败")

        else:
            # 调度器模式
            exit_code = run_scheduler(args, app)

    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        exit_code = 0
    except Exception as e:
        logger.error(f"程序运行错误: {str(e)}", exc_info=True)
        exit_code = 1
    finally:
        if app:
            app.shutdown()
        logger.info("数据迁移管理系统已关闭")

    return exit_code

if __name__ == "__main__":
    exit(main())