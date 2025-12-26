"""MySQL客户端"""
import pymysql
import logging
from typing import List, Dict, Any, Optional
from config.settings import Config

logger = logging.getLogger(__name__)

class MySQLClient:
    """MySQL客户端"""

    def __init__(self):
        self.config = Config.MYSQL_CONFIG
        self.connection = None

    def connect(self):
        """连接数据库"""
        try:
            self.connection = pymysql.connect(**self.config)
            logger.info("MySQL连接成功")
            return True
        except Exception as e:
            logger.error(f"MySQL连接失败: {str(e)}")
            return False

    def disconnect(self):
        """断开连接"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("MySQL连接已关闭")
            except Exception as e:
                logger.error(f"关闭MySQL连接失败: {str(e)}")

    def execute_query(self, query: str, params: tuple = None,
                     fetch: bool = True) -> Optional[Any]:
        """执行查询"""
        try:
            if not self.connection or not self.connection.open:
                self.connect()

            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                if fetch:
                    result = cursor.fetchall()
                    self.connection.commit()
                    return result
                else:
                    self.connection.commit()
                    return cursor.rowcount
        except Exception as e:
            logger.error(f"MySQL查询失败: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return None

    def execute_many(self, query: str, params: List[tuple]) -> int:
        """批量执行"""
        try:
            if not self.connection or not self.connection.open:
                self.connect()

            with self.connection.cursor() as cursor:
                cursor.executemany(query, params)
                self.connection.commit()
                return cursor.rowcount
        except Exception as e:
            logger.error(f"MySQL批量执行失败: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return 0

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        query = "SHOW TABLES LIKE %s"
        result = self.execute_query(query, (table_name,))
        return bool(result)

    def create_table(self, table_name: str, columns: List[Dict]) -> bool:
        """创建表"""
        try:
            column_definitions = []
            for col in columns:
                mysql_type = self._map_column_type(col['type'])
                column_definitions.append(f"`{col['name']}` {mysql_type}")

            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            create_sql += ", ".join(column_definitions)
            create_sql += ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

            self.execute_query(create_sql, fetch=False)
            return True
        except Exception as e:
            logger.error(f"创建表失败: {str(e)}")
            return False

    def _map_column_type(self, clickhouse_type: str) -> str:
        """映射ClickHouse类型到MySQL类型"""
        type_mapping = {
            'UInt8': 'TINYINT UNSIGNED',
            'Int8': 'TINYINT',
            'UInt16': 'SMALLINT UNSIGNED',
            'Int16': 'SMALLINT',
            'UInt32': 'INT UNSIGNED',
            'Int32': 'INT',
            'UInt64': 'BIGINT UNSIGNED',
            'Int64': 'BIGINT',
            'Float32': 'FLOAT',
            'Float64': 'DOUBLE',
            'String': 'VARCHAR(1000)',
            'Date': 'DATE',
            'DateTime': 'DATETIME',
            'Decimal': 'DECIMAL(20,6)'
        }

        return type_mapping.get(clickhouse_type, 'TEXT')

    def test_connection(self) -> bool:
        """测试连接"""
        try:
            result = self.execute_query("SELECT 1")
            return result is not None
        except Exception:
            return False