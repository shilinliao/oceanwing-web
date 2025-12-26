"""ClickHouse客户端"""
import clickhouse_connect
import logging
from typing import List, Dict, Any, Optional
from config.settings import Config

logger = logging.getLogger(__name__)

class ClickHouseClient:
    """ClickHouse客户端"""

    def __init__(self):
        self.config = Config.CLICKHOUSE_CONFIG
        self.client = None

    def connect(self):
        """连接数据库"""
        try:
            self.client = clickhouse_connect.get_client(**self.config)
            logger.info("ClickHouse连接成功")
            return True
        except Exception as e:
            logger.error(f"ClickHouse连接失败: {str(e)}")
            return False

    def disconnect(self):
        """断开连接"""
        if self.client:
            try:
                self.client.close()
                logger.info("ClickHouse连接已关闭")
            except Exception as e:
                logger.error(f"关闭ClickHouse连接失败: {str(e)}")

    def execute_query(self, query: str, params: Dict = None) -> Optional[Any]:
        """执行查询"""
        try:
            if not self.client:
                self.connect()

            result = self.client.query(query, parameters=params)
            return result
        except Exception as e:
            logger.error(f"ClickHouse查询失败: {str(e)}")
            return None

    def get_table_schema(self, table_name: str) -> List[Dict]:
        """获取表结构"""
        query = f"DESCRIBE TABLE {table_name}"
        result = self.execute_query(query)

        if result and hasattr(result, 'result_rows'):
            schema = []
            for row in result.result_rows:
                schema.append({
                    'name': row[0],
                    'type': row[1],
                    'default_type': row[2],
                    'default_expression': row[3],
                    'comment': row[4],
                    'codec_expression': row[5],
                    'ttl_expression': row[6]
                })
            return schema
        return []

    def get_table_row_count(self, table_name: str, date_field: str,
                           start_date: str, end_date: str) -> int:
        """获取表记录数"""
        query = f"""
        SELECT COUNT(*) 
        FROM {table_name} 
        WHERE {date_field} >= '{start_date}' 
        AND {date_field} < '{end_date}'
        """

        result = self.execute_query(query)
        if result and hasattr(result, 'result_rows'):
            return result.result_rows[0][0] if result.result_rows else 0
        return 0

    def test_connection(self) -> bool:
        """测试连接"""
        try:
            result = self.execute_query("SELECT 1")
            return result is not None
        except Exception:
            return False