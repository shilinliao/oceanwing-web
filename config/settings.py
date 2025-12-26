"""配置管理"""
import os
from typing import Dict, Any, List

class Config:
    """应用配置类"""

    # ClickHouse连接配置
    CLICKHOUSE_CONFIG = {
        'host': '47.109.55.96',
        'port': 8124,
        'database': 'semanticdb_haiyi',
        'username': 'haiyi',
        'password': 'G7f@2eBw',
        'secure': False,
        'verify': False,
        'connect_timeout': 30,
        'send_receive_timeout': 300
    }

    # MySQL连接配置
    MYSQL_CONFIG = {
        'host': 'ow-masterdata-1.cavkqwqmyvuw.us-west-2.rds.amazonaws.com',
        'port': 3306,
        'database': 'ow_base',
        'user': 'ow_base_user',
        'password': '3we@5y_+05iu',
        'charset': 'utf8mb4',
        'autocommit': False,
        'connect_timeout': 30,
        'read_timeout': 60,
        'write_timeout': 60
    }

    # 表名映射
    SOURCE_TABLES = ["ods_Query", "ods_campain", "ods_campaign_dsp", "ods_aws_asin_philips"]
    TARGET_TABLES = ["ods_query", "ods_campain", "ods_campaign_dsp", "ods_aws_asin_philips"]

    # 列名映射
    TABLE_COLUMNS_MAPPING = {
        "ods_campain": {
            "Time": "time", "Campaign Name": "campaign_name", "Profile Name": "profile_name",
            "Portfolio Name": "portfolio_name", "Status": "status", "Impression": "impression",
            "Click": "click", "Spend": "spend", "CTR": "ctr", "CPC": "cpc", "CVR 14d": "cvr14d",
            "ACOS 14d": "acos14d", "ROAS 14d": "roas14d", "Order 14d": "order14d",
            "Sale Units 14d": "sale_units14d", "Sales 14d": "sales14d", "Campaign Type": "campaign_type",
            "Campaign Tag": "campaign_tag", "DPV 14天": "dpv14d", "活动Id": "campaign_id"
        },
        "ods_campaign_dsp": {
            "TimeColumn": "time_column", "IntervalStart": "interval_start", "IntervalEnd": "interval_end",
            "EntityName": "entity_name", "EntityId": "entity_id", "AdvertiserName": "advertiser_name",
            "AdvertiserId": "advertiser_id", "CountryCode": "country_code", "OrderName": "order_name",
            "OrderId": "order_id", "LineItemName": "line_item_name", "LineItemId": "line_item_id",
            "LineItemType": "line_item_type", "CreativeName": "creative_name", "CreativeId": "creative_id",
            "CreativeSize": "creative_size", "Creative Tag": "creative_tag", "Lineitem Tag": "line_item_tag",
            "Order Tag": "order_tag", "TotalCost": "total_cost", "Impressions": "impressions",
            "ClickThroughs": "click_throughs", "CTR": "ctr", "ATC": "atc", "Purchases": "purchases"
        },
        "ods_aws_asin_philips": {
            "Time": "time", "ASIN": "ASIN", "Profile_Name": "profile_name", "SKU": "sku",
            "Title": "title", "Brand": "brand", "Image_Url": "image_url", "Impression": "impression",
            "Click": "click", "Spend": "spend", "CTR": "ctr", "CPC": "cpc", "CVR_14d": "cvr14d",
            "ACOS_14d": "acos14d", "ROAS_14d": "roas14d", "Order_14d": "order14d",
            "Sale_Units_14d": "sale_units14d", "Sales_14d": "sales14d", "ASIN_Tag": "asin_tag",
            "Status": "status", "Profile_Id": "profile_id", "Philips_ALL": "philips_all"
        },
        "ods_query": {
            "Time": "time", "Query": "query", "Keyword Text": "keyword_text", "Profile Name": "profile_name",
            "Current Bid": "current_bid", "Campaign Name": "campaign_name", "Adgroup": "adgroup",
            "Impression": "impression", "Impression.rank": "impression_rank", "Impression.share": "impression_share",
            "Click": "click", "Spend": "spend", "CTR": "ctr", "CPC": "cpc", "CVR 14d": "cvr14d",
            "ACOS 14d": "acos14d", "ROAS 14d": "roas14d", "Order 14d": "order14d",
            "Sale Units 14d": "sale_units14d", "Sales 14d": "sales14d", "DPV 14天": "dpv14d",
            "广告活动标签": "campaign_tag", "活动类型": "activity_type"
        }
    }

    # 迁移配置
    MIGRATION_DAYS = {
        "ods_query": 30,
        "ods_campain": 30,
        "ods_campaign_dsp": 30,
        "ods_aws_asin_philips": 30
    }

    # 性能配置
    MAX_WORKERS_PER_TABLE = 4
    MAX_RETRIES = 3
    RETRY_DELAY_BASE = 1
    BATCH_SIZE = 1000
    LOCK_TIMEOUT = 30

    # 调度配置
    SCHEDULE_TIME = "09:00"
    TIMEZONE = "Asia/Shanghai"

    @classmethod
    def get_table_time_field(cls, table_name: str) -> str:
        """获取表的时间字段"""
        time_fields = {
            "ods_campaign_dsp": "TimeColumn",
            "ods_campain": "Time",
            "ods_aws_asin_philips": "Time",
            "ods_query": "Time"
        }
        return time_fields.get(table_name, "Time")

    @classmethod
    def get_table_filter_condition(cls, table_name: str) -> str:
        """获取表的过滤条件"""
        filter_conditions = {
            "ods_campain": " AND `Profile Name` LIKE 'Philips%'",
            "ods_aws_asin_philips": " AND `Profile_Name` LIKE 'Philips%'",
            "ods_query": " AND `Profile Name` LIKE 'Philips%'",
            "ods_campaign_dsp": ""  # DSP表可能不需要过滤
        }
        return filter_conditions.get(table_name, "")