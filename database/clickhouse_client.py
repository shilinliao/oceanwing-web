"""ClickHouse客户端管理"""
import logging
import clickhouse_connect
from typing import List, Any, Dict
from threading import Lock

from core.models import ColumnDefinition
from config.settings import Config

logger = logging.getLogger('DataMigrationApp')


class ClickHouseClientManager:
    """ClickHouse客户端管理器"""

    def __init__(self):
        self.clients = {}
        self.lock = Lock()
        self.config = Config.CLICKHOUSE_CONFIG

    def get_client(self, thread_id: int, table_index: int = 0):
        """获取ClickHouse客户端"""
        key = f"{thread_id}_{table_index}"
        with self.lock:
            if key not in self.clients:
                logger.info(f"Creating ClickHouse connection for thread {thread_id}, table {table_index}")
                try:
                    self.clients[key] = clickhouse_connect.get_client(**self.config)
                except Exception as e:
                    logger.error(f"Failed to create ClickHouse client: {str(e)}")
                    raise
            return self.clients[key]

    def close_all(self):
        """关闭所有连接"""
        with self.lock:
            for key, client in self.clients.items():
                try:
                    client.close()
                except Exception as e:
                    logger.warning(f"Error closing ClickHouse client {key}: {str(e)}")
            self.clients.clear()

    def get_table_schema(self, thread_id: int, table_name: str, table_index: int = 0) -> List[ColumnDefinition]:
        """获取表结构"""
        columns = []
        try:
            client = self.get_client(thread_id, table_index)
            query = f"DESCRIBE TABLE {table_name}"
            result = client.query(query)

            for row in result.result_rows:
                name = row[0]
                data_type = row[1]
                columns.append(ColumnDefinition(name, data_type))

            logger.info(f"Retrieved schema for {table_name}: {len(columns)} columns")
            return columns

        except Exception as e:
            logger.error(f"Error getting table schema for {table_name}: {str(e)}")
            raise

    def execute_query(self, thread_id: int, query: str, table_index: int = 0):
        """执行查询"""
        client = self.get_client(thread_id, table_index)
        return client.query(query)