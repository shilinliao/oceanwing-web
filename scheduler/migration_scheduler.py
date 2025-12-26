"""迁移调度器"""
import logging
import time
import schedule
import select
import sys
from datetime import datetime
from pytz import timezone

from core.migration_app import DataMigrationApp
from config.settings import Config

logger = logging.getLogger('DataMigrationApp')


class MigrationScheduler:
    """迁移任务调度器"""

    def __init__(self, migration_app: DataMigrationApp):
        self.migration_app = migration_app
        self.schedule_enabled = migration_app.schedule_enabled
        self.schedule_time = Config.SCHEDULE_TIME
        self.timezone = timezone(Config.TIMEZONE)

    def setup_schedule(self) -> bool:
        """设置定时任务"""
        if not self.schedule_enabled:
            logger.info("Schedule is disabled, running migration once")
            return self._run_migration_once()

        # 清除现有任务
        schedule.clear()

        # 设置定时任务
        schedule.every().day.at(self.schedule_time).do(self._scheduled_migration_job)

        logger.info(f"Schedule set up: daily at {self.schedule_time} {Config.TIMEZONE} time")

        # 立即执行一次
        logger.info("Running initial migration...")
        initial_success = self._run_migration_once()

        if initial_success:
            logger.info("Initial migration completed successfully")
        else:
            logger.warning("Initial migration had issues, but scheduler will continue")

        return initial_success

    def run_scheduler(self):
        """运行调度器"""
        if not self.schedule_enabled:
            logger.info("Schedule is disabled, exiting")
            return

        logger.info("Starting migration scheduler...")
        logger.info(f"Next run: {schedule.next_run()}")
        logger.info("Press Enter in console to stop the scheduler")

        try:
            while not self.migration_app.shutdown_event.is_set():
                # 运行待执行的任务
                schedule.run_pending()

                # 检查控制台输入
                if self._check_user_input():
                    logger.info("User requested shutdown via console")
                    break

                # 计算下次执行时间
                next_run = schedule.next_run()
                if next_run:
                    time_until_next = (next_run - datetime.now(self.timezone)).total_seconds()

                    if time_until_next > 60:
                        # 每分钟检查一次
                        self._wait_with_input_check(60)
                    else:
                        # 每秒检查一次
                        time.sleep(1)
                else:
                    time.sleep(60)

        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user (KeyboardInterrupt)")
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}", exc_info=True)
        finally:
            self.migration_app.shutdown()
            logger.info("Scheduler stopped")

    def _scheduled_migration_job(self):
        """定时迁移任务"""
        logger.info("=" * 60)
        logger.info(f"Starting scheduled migration at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        try:
            success = self.migration_app.migrate_all_tables_parallel()

            if success:
                logger.info("Scheduled migration completed successfully")
            else:
                logger.error("Scheduled migration completed with errors")

            return success

        except Exception as e:
            logger.error(f"Error in scheduled migration: {str(e)}", exc_info=True)
            return False

    def _run_migration_once(self) -> bool:
        """运行一次迁移"""
        try:
            return self.migration_app.migrate_all_tables_parallel()
        except Exception as e:
            logger.error(f"Error in one-time migration: {str(e)}", exc_info=True)
            return False

    def _check_user_input(self) -> bool:
        """检查用户输入（非阻塞）"""
        if select.select([sys.stdin], [], [], 0)[0]:
            line = sys.stdin.readline().strip().lower()
            return line in ['quit', 'exit', 'q', 'stop']
        return False

    def _wait_with_input_check(self, seconds: int):
        """等待并检查输入"""
        for _ in range(seconds):
            if self.migration_app.shutdown_event.is_set():
                return
            if self._check_user_input():
                self.migration_app.shutdown_event.set()
                return
            time.sleep(1)