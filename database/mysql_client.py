"""MySQL客户端管理"""
import logging
import pymysql
from typing import List, Any, Dict, Tuple
from threading import Lock

from config.settings import Config

logger = logging.getLogger('DataMigrationApp')


class MySQLClientManager:
    """MySQL客户端管理器"""

    def __init__(self):
        self.connections = {}
        self.lock = Lock()
        self.config = Config.MYSQL_CONFIG

    def get_connection(self, thread_id: int, table_index: int = 0):
        """获取MySQL连接"""
        key = f"{thread_id}_{table_index}"
        with self.lock:
            if key not in self.connections:
                logger.info(f"Creating MySQL connection for thread {thread_id}, table {table_index}")
                try:
                    mysql_config = self.config.copy()
                    conn = pymysql.connect(**mysql_config)

                    # 优化连接设置
                    with conn.cursor() as cursor:
                        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        cursor.execute(f"SET SESSION innodb_lock_wait_timeout = {Config.LOCK_TIMEOUT}")
                        cursor.execute("SET SESSION wait_timeout = 28800")
                        conn.commit()

                    self.connections[key] = conn

                except Exception as e:
                    logger.error(f"Failed to create MySQL connection: {str(e)}")
                    raise
            return self.connections[key]

    def close_all(self):
        """关闭所有连接"""
        with self.lock:
            for key, conn in self.connections.items():
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"Error closing MySQL connection {key}: {str(e)}")
            self.connections.clear()

    def create_table_if_not_exists(self, thread_id: int, table_name: str, columns: List, table_index: int = 0) -> bool:
        """创建表（如果不存在）"""
        try:
            conn = self.get_connection(thread_id, table_index)
            with conn.cursor() as cursor:
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    logger.info(f"Table {table_name} does not exist, creating...")

                    create_sql = self._build_create_table_sql(table_name, columns)
                    cursor.execute(create_sql)
                    conn.commit()
                    logger.info(f"Created target table: {table_name}")
                    return True
            return False

        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise

    def _build_create_table_sql(self, table_name: str, columns: List) -> str:
        """构建创建表SQL"""
        type_mapping = Config.get_type_mapping()
        column_definitions = []

        for col in columns:
            mysql_type = type_mapping.get(col.get_type().lower(), 'TEXT')
            column_definitions.append(f"`{col.get_name().lower()}` {mysql_type}")

        create_sql = f"CREATE TABLE {table_name} ({', '.join(column_definitions)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        return create_sql