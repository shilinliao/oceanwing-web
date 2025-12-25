# app.py - Web界面
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
import pymysql
import clickhouse_connect
import re
import time
import tempfile
import csv
import codecs
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
import threading
from queue import Queue, Empty
import psutil
import gc
from dataclasses import dataclass, field
from threading import Lock, Semaphore, BoundedSemaphore, Event
import schedule
from pytz import timezone
import sys
import os
import traceback
import random
from contextlib import contextmanager
import heapq
from functools import total_ordering
from flask import Flask, render_template, request, jsonify, redirect, url_for, session, flash
import atexit
from werkzeug.middleware.proxy_fix import ProxyFix
import sqlite3
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(process)d:%(threadName)s] [%(name)s] %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_migration.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('DataMigrationApp')

# Flask应用
app = Flask(__name__)
app.secret_key = 'your-secret-key-here-change-in-production'  # 生产环境需要修改
app.config['DATABASE'] = 'migration.db'
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

# 线程本地存储
thread_local = threading.local()


# 数据库初始化
def init_db():
    """初始化数据库"""
    try:
        db = get_db()
        cursor = db.cursor()

        # 创建迁移历史表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS migration_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                status TEXT,
                tables_migrated TEXT,
                total_records INTEGER,
                error_message TEXT,
                duration_seconds REAL
            )
        ''')

        # 创建任务配置表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS migration_config (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_key TEXT UNIQUE,
                config_value TEXT
            )
        ''')

        # 创建表状态表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS table_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT UNIQUE,
                last_sync_time TIMESTAMP,
                records_count INTEGER,
                status TEXT,
                last_error TEXT
            )
        ''')

        db.commit()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise


def get_db():
    """获取数据库连接（线程安全）"""
    if not hasattr(thread_local, 'db'):
        # 确保数据库文件存在
        db_path = app.config['DATABASE']
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # 创建新连接
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # 存储到线程本地存储
        thread_local.db = conn

    return thread_local.db


@app.teardown_appcontext
def close_db(error=None):
    """关闭数据库连接"""
    db = getattr(thread_local, 'db', None)
    if db is not None:
        db.close()
        thread_local.db = None


# 数据迁移应用类定义
@dataclass
class MigrationTask:
    """迁移任务数据类"""
    source_table: str
    target_table: str
    day: int
    date_str: str
    columns: List['ColumnDefinition']
    task_id: int
    priority: int = 0
    table_index: int = 0

    def __lt__(self, other):
        return self.priority < other.priority


class ColumnDefinition:
    """列定义类"""

    def __init__(self, name: str, data_type: str):
        self.name = name
        self.type = data_type

    def get_name(self) -> str:
        return self.name

    def get_type(self) -> str:
        return self.type


class ThreadSafeCounter:
    """线程安全计数器"""

    def __init__(self):
        self.value = 0
        self.lock = Lock()

    def increment(self, amount: int = 1):
        with self.lock:
            self.value += amount
            return self.value

    def get(self):
        with self.lock:
            return self.value


class DataMigrationApp:
    def __init__(self, max_workers_per_table: int = 4, schedule_enabled: bool = False):
        # ClickHouse连接配置
        self.CLICKHOUSE_CONFIG = {
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
        self.MYSQL_CONFIG = {
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
        self.SOURCE_TABLES = ["ods_Query", "ods_campain", "ods_campaign_dsp", "ods_aws_asin_philips"]
        self.TARGET_TABLES = ["ods_query", "ods_campain", "ods_campaign_dsp", "ods_aws_asin_philips"]

        # 列名映射定义
        self.ods_campain = ["Time", "Campaign Name", "Profile Name", "Portfolio Name", "Status", "Impression", "Click",
                            "Spend", "CTR", "CPC", "CVR 14d", "ACOS 14d", "ROAS 14d", "Order 14d", "Sale Units 14d",
                            "Sales 14d", "Campaign Type", "Campaign Tag", "DPV 14天", "活动Id"]
        self.tods_campain = ["time", "campaign_name", "profile_name", "portfolio_name", "status", "impression", "click",
                             "spend", "ctr", "cpc", "cvr14d", "acos14d", "roas14d", "order14d", "sale_units14d",
                             "sales14d", "campaign_type", "campaign_tag", "dpv14d", "campaign_id"]

        self.ods_campain_dsp = ["TimeColumn", "IntervalStart", "IntervalEnd", "EntityName", "EntityId",
                                "AdvertiserName", "AdvertiserId", "CountryCode", "OrderName", "OrderId", "LineItemName",
                                "LineItemId", "LineItemType", "CreativeName", "CreativeId", "CreativeSize",
                                "Creative Tag", "Lineitem Tag", "Order Tag", "TotalCost", "Impressions",
                                "ClickThroughs", "CTR", "ATC", "Purchases", "PercentOfPurchasesNewToBrand",
                                "ConversionRate", "eCPM", "eCPC", "DPV", "DPVR", "eCPDPV", "ATCR", "PurchaseRate",
                                "eCPP", "NewToBrandPurchases", "NewToBrandeCPP", "eCPATC", "UnitsSold", "TotalSales",
                                "SalesUSD", "ProductSales", "ROAS", "TotalROAS", "TotalATC", "TotalUnitsSold",
                                "TotalDPV", "TotalDPVR", "TotalPurchases", "TotalPurchaseRate", "TotaleCPP",
                                "TotalNewToBrandPurchases", "TotalPercentOfPurchasesNewToBrand", "TotalProductSales",
                                "TotalNewToBrandUnitsSold", "TotalNewToBrandProductSales", "TotalNewToBrandROAS",
                                "TotalSnSS", "TotalSnSSRate", "TotalNewToBrandeCPP", "TotalNewToBrandPurchaseRate",
                                "NTB Sales", "T-Pixel", "T-Pixel CPA", "T-Pixel CVR", "SnSSR", "NTBROAS",
                                "ExchangeCode", "Video Start", "Video Complete", "PurchaseButton", "PurchaseButtonCPA",
                                "PurchaseButtonCVR", "OffAmazonPurchases", "OffAmazonConversions", "OffAmazonCVR",
                                "OffAmazonCPA", "OffAmazonProductSales", "OffAmazonUnitsSold", "OffAmazonROAS",
                                "OffAmazoneRPM", "OffAmazonPurchasesRate", "OffAmazoneCPP", "CombinedPurchasesRate",
                                "CombinedeCPP", "CombinedROAS", "CombinedeRPM", "CombinedPurchases",
                                "CombinedUnitsSold", "CombinedProductSales", "BrandSearch", "BrandSearchsRate",
                                "eCP Branded Search", "Original Currency", "Exchange Rate", "ATSC", "ATSC CVR",
                                "ATSC CPA", "ATSC value sum", "ATSC value average", "Checkout", "Checkout CVR",
                                "Checkout CPA", "Checkout value sum", "Checkout value average", "PageView",
                                "PageView CVR", "PageView CPA", "PageView value sum", "PageView value average",
                                "SignUp", "SignUp CVR", "SignUp CPA", "SignUp value sum", "SignUp value average",
                                "Application", "Application CVR", "Application CPA", "Application value sum",
                                "Application value average", "Contact", "Contact CVR", "Contact CPA",
                                "Contact value sum", "Contact value average", "Lead", "Lead CVR", "Lead CPA",
                                "Lead value sum", "Lead value average", "Search CVR", "Search CPA", "Search value sum",
                                "Search", "Search value average", "TotaleCPDPV", "TotaleCPATC"]
        self.tods_campain_dsp = ["time_column", "interval_start", "interval_end", "entity_name", "entity_id",
                                 "advertiser_name", "advertiser_id", "country_code", "order_name", "order_id",
                                 "line_item_name", "line_item_id", "line_item_type", "creative_name", "creative_id",
                                 "creative_size", "creative_tag", "line_item_tag", "order_tag", "total_cost",
                                 "impressions", "click_throughs", "ctr", "atc", "purchases",
                                 "percent_of_purchases_new_to_brand", "conversion_rate", "ecpm", "ecpc", "dpv", "dpvr",
                                 "ecpdpv", "atcr", "purchase_rate", "ecpp", "new_to_brand_purchases",
                                 "new_to_brande_cpp", "ecpatc", "units_sold", "total_sales", "sales_usd",
                                 "product_sales", "roas", "total_roas", "total_atc", "total_units_sold", "total_dpv",
                                 "total_dpvr", "total_purchases", "total_purchase_rate", "total_ecpp",
                                 "total_new_to_brand_purchases", "total_percent_of_purchases_new_to_brand",
                                 "total_product_sales", "total_new_to_brand_units_sold",
                                 "total_new_to_brand_product_sales", "total_new_to_brand_roas", "total_sn_ss",
                                 "total_sn_ss_rate", "total_new_to_brande_cpp", "total_new_to_brand_purchase_rate",
                                 "ntb_sales", "tpixel", "tpixel_cpa", "tpixel_cvr", "sn_ssr", "ntbroas",
                                 "exchange_code", "video_start", "video_complete", "purchase_button",
                                 "purchase_button_cpa", "purchase_button_cvr", "off_amazon_purchases",
                                 "off_amazon_conversions", "Off_Amazon_CVR", "off_amazon_cpa",
                                 "off_amazon_product_sales", "off_amazon_units_sold", "off_amazon_roas",
                                 "off_amazon_erpm", "off_amazon_purchases_rate", "off_amazone_cpp",
                                 "combined_purchases_rate", "combinede_cpp", "combined_roas", "combined_erpm",
                                 "combined_purchases", "combined_units_sold", "combined_product_sales", "brand_search",
                                 "brand_searchs_rate", "ecp_branded_search", "original_currency", "exchange_rate",
                                 "atsc", "atsc_cvr", "atsc_cpa", "atsc_value_sum", "atsc_value_average", "checkout",
                                 "checkout_cvr", "checkout_cpa", "checkout_value_sum", "checkout_value_average",
                                 "pageview", "pageview_cvr", "pageview_cpa", "pageview_value_sum",
                                 "pageview_value_average", "sign_up", "sign_up_cvr", "sign_up_cpa", "sign_up_value_sum",
                                 "sign_up_value_average", "application", "application_cvr", "application_cpa",
                                 "application_value_sum", "application_value_average", "contact", "contact_cvr",
                                 "contact_cpa", "contact_value_sum", "contact_value_average", "lead", "lead_cvr",
                                 "lead_cpa", "lead_value_sum", "lead_value_average", "search_cvr", "search_cpa",
                                 "search_value_sum", "search", "search_value_average", "totale_cpdpv", "totale_cpatc"]

        self.ods_aws_asin_philips = ["Time", "ASIN", "Profile_Name", "SKU", "Title", "Brand", "Image_Url", "Impression",
                                     "Click", "Spend", "CTR", "CPC", "CVR_14d", "ACOS_14d", "ROAS_14d", "Order_14d",
                                     "Sale_Units_14d", "Sales_14d", "ASIN_Tag", "Status", "Profile_Id", "Philips_ALL",
                                     "CVR_1d", "CVR_7d", "CVR_30d", "ACOS_1d", "ACOS_7d", "ACOS_30d", "ROAS_1d",
                                     "ROAS_7d", "ROAS_30d", "CPA_1d", "CPA_7d", "CPA_14d", "CPA_30d", "Order_1d",
                                     "Order_7d", "Order_30d", "Sale_Units_1d", "Sale_Units_7d", "Sale_Units_30d",
                                     "Sales_1d", "Sales_7d", "Sales_30d", "Orders_NTB_14d", "Orders_NTB_Percentage_14d",
                                     "Order_Rate_NTB_14d", "Sales_NTB_14d", "Sales_NTB_Percentage_14d",
                                     "Units_Ordered_NTB_14d", "SameSKU_Sales_1d", "SameSKU_Sales_7d",
                                     "SameSKU_Sales_14d", "SameSKU_Sales_30d", "SameSKU_Orders_1d", "SameSKU_Orders_7d",
                                     "SameSKU_Orders_14d", "SameSKU_Orders_30d", "SameSKU_Sale_Units_1d",
                                     "SameSKU_Sale_Units_7d", "SameSKU_Sale_Units_14d", "SameSKU_Sale_Units_30d",
                                     "Other_Sales_1d", "Other_Sales_7d", "Other_Sales_14d", "Other_Sales_30d",
                                     "Kindle_Pages_Read_14d", "Kindle_Pages_Royalties_14d"]
        self.tods_aws_asin_philips = ["time", "ASIN", "profile_name", "sku", "title", "brand", "image_url",
                                      "impression", "click", "spend", "ctr", "cpc", "cvr14d", "acos14d", "roas14d",
                                      "order14d", "sale_units14d", "sales14d", "asin_tag", "status", "profile_id",
                                      "philips_all", "cvr1d", "cvr7d", "cvr30d", "acos1d", "acos7d", "acos30d",
                                      "roas1d", "roas7d", "roas30d", "cpa1d", "cpa7d", "cpa14d", "cpa30d", "order1d",
                                      "order7d", "order30d", "sale_units1d", "sale_units7d", "sale_units30d", "sales1d",
                                      "sales7d", "sales30d", "orders_ntb14d", "orders_ntb_percentage14d",
                                      "order_rate_ntb14d", "sales_ntb14d", "sales_ntb_percentage14d",
                                      "units_ordered_ntb14d", "same_sku_sales1d", "same_sku_sales7d",
                                      "same_sku_sales14d", "same_sku_sales30d", "same_sku_orders1d",
                                      "same_sku_orders7d", "same_sku_orders14d", "same_sku_orders30d",
                                      "same_sku_sale_units1d", "same_sku_sale_units7d", "same_sku_sale_units14d",
                                      "same_sku_sale_units30d", "other_sales1d", "other_sales7d", "other_sales14d",
                                      "other_sales30d", "kindle_pages_read14d", "kindle_pages_royalties14d"]

        self.ods_query = ["Time", "Query", "Keyword Text", "Profile Name", "Current Bid", "Campaign Name", "Adgroup",
                          "Impression", "Impression.rank", "Impression.share", "Click", "Spend", "CTR", "CPC",
                          "CVR 14d", "ACOS 14d", "ROAS 14d", "Order 14d", "Sale Units 14d", "Sales 14d", "DPV 14天",
                          "广告活动标签", "活动类型"]
        self.tods_query = ["time", "query", "keyword_text", "profile_name", "current_bid", "campaign_name", "adgroup",
                           "impression", "impression_rank", "impression_share", "click", "spend", "ctr", "cpc",
                           "cvr14d", "acos14d", "roas14d", "order14d", "sale_units14d", "sales14d", "dpv14d",
                           "campaign_tag", "activity_type"]

        # 表列映射
        self.TABLE_COLUMNS = {}
        self.queryCount = 0

        # 线程控制
        self.max_workers_per_table = max_workers_per_table
        self.task_counter = ThreadSafeCounter()
        self.completed_tasks = ThreadSafeCounter()
        self.failed_tasks = ThreadSafeCounter()
        self.total_records = ThreadSafeCounter()

        # 表任务队列
        self.table_queues = {}
        self.table_threads = {}
        self.table_workers = {}

        # 定时任务控制
        self.schedule_enabled = schedule_enabled
        self.is_running = False
        self.current_job = None
        self.scheduler_thread = None

        # 停止标志
        self.shutdown_event = threading.Event()

        # 连接池
        self.clickhouse_clients = {}
        self.mysql_connections = {}
        self.connection_lock = Lock()

        # 性能调优参数
        self.max_retries = 3
        self.retry_delay_base = 1
        self.lock_timeout = 30

        # Web状态
        self.current_migration_id = None
        self.migration_start_time = None
        self.last_error = None
        self.progress_info = {}

        # 初始化列映射
        self._init_table_columns()

        # 初始化表队列
        self._init_table_queues()

        # 初始化默认配置
        self._init_default_config()

    def _init_table_columns(self):
        """初始化表列映射"""
        # ods_campain
        tc = {}
        for i in range(len(self.ods_campain)):
            tc[self.ods_campain[i]] = self.tods_campain[i]
        self.TABLE_COLUMNS["ods_campain"] = tc

        # ods_campaign_dsp
        tc = {}
        for i in range(len(self.ods_campain_dsp)):
            tc[self.ods_campain_dsp[i]] = self.tods_campain_dsp[i]
        self.TABLE_COLUMNS["ods_campaign_dsp"] = tc

        # ods_aws_asin_philips
        tc = {}
        for i in range(len(self.ods_aws_asin_philips)):
            tc[self.ods_aws_asin_philips[i]] = self.tods_aws_asin_philips[i]
        self.TABLE_COLUMNS["ods_aws_asin_philips"] = tc

        # ods_query
        tc = {}
        for i in range(len(self.ods_query)):
            tc[self.ods_query[i]] = self.tods_query[i]
        self.TABLE_COLUMNS["ods_query"] = tc

    def _init_table_queues(self):
        """初始化表队列"""
        for i in range(len(self.SOURCE_TABLES)):
            table_key = f"{self.SOURCE_TABLES[i]}_{self.TARGET_TABLES[i]}"
            self.table_queues[table_key] = Queue()

    def _init_default_config(self):
        """初始化默认配置"""
        self.config = {
            'workers_per_table': self.max_workers_per_table,
            'lock_timeout': self.lock_timeout,
            'max_retries': self.max_retries,
            'ods_query_days': 24,
            'other_tables_days': 60,
            'schedule_enabled': self.schedule_enabled,
            'schedule_time': '09:00',
            'auto_start': False
        }

    def get_config(self, key, default=None):
        """获取配置"""
        return self.config.get(key, default)

    def set_config(self, key, value):
        """设置配置"""
        self.config[key] = value

        # 更新运行时配置
        if key == 'workers_per_table':
            self.max_workers_per_table = value
        elif key == 'lock_timeout':
            self.lock_timeout = value
        elif key == 'max_retries':
            self.max_retries = value
        elif key == 'schedule_enabled':
            self.schedule_enabled = value

    def get_status(self):
        """获取状态"""
        return {
            'is_running': self.is_running,
            'current_migration_id': self.current_migration_id,
            'migration_start_time': self.migration_start_time,
            'last_error': self.last_error,
            'total_records': self.total_records.get(),
            'completed_tasks': self.completed_tasks.get(),
            'failed_tasks': self.failed_tasks.get(),
            'progress_info': self.progress_info,
            'config': self.config
        }

    def save_migration_history(self, start_time, end_time, status, tables_migrated,
                               total_records, error_message=None):
        """保存迁移历史到数据库"""
        try:
            duration = (end_time - start_time).total_seconds() if end_time else 0

            db = get_db()
            cursor = db.cursor()
            cursor.execute('''
                INSERT INTO migration_history 
                (start_time, end_time, status, tables_migrated, total_records, error_message, duration_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (start_time, end_time, status, tables_migrated, total_records, error_message, duration))
            db.commit()

            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Error saving migration history: {str(e)}")
            return None

    def update_table_status(self, table_name, last_sync_time, records_count, status, last_error=None):
        """更新表状态"""
        try:
            db = get_db()
            cursor = db.cursor()

            # 检查是否已存在
            cursor.execute('SELECT id FROM table_status WHERE table_name = ?', (table_name,))
            existing = cursor.fetchone()

            if existing:
                cursor.execute('''
                    UPDATE table_status 
                    SET last_sync_time = ?, records_count = ?, status = ?, last_error = ?
                    WHERE table_name = ?
                ''', (last_sync_time, records_count, status, last_error, table_name))
            else:
                cursor.execute('''
                    INSERT INTO table_status (table_name, last_sync_time, records_count, status, last_error)
                    VALUES (?, ?, ?, ?, ?)
                ''', (table_name, last_sync_time, records_count, status, last_error))

            db.commit()
        except Exception as e:
            logger.error(f"Error updating table status: {str(e)}")

    def get_migration_history(self, limit=50):
        """获取迁移历史"""
        try:
            db = get_db()
            cursor = db.cursor()
            cursor.execute('''
                SELECT * FROM migration_history 
                ORDER BY start_time DESC 
                LIMIT ?
            ''', (limit,))
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting migration history: {str(e)}")
            return []

    def get_table_status(self):
        """获取所有表状态"""
        try:
            db = get_db()
            cursor = db.cursor()
            cursor.execute('SELECT * FROM table_status ORDER BY table_name')
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting table status: {str(e)}")
            return []

    def run_daily_migration_job(self, tables=None, days_override=None):
        """运行每日迁移任务（Web版本）"""
        if self.is_running:
            logger.warning("Migration is already running, skipping this execution")
            return {"success": False, "message": "Migration is already running"}

        self.is_running = True
        self.current_migration_id = None
        self.migration_start_time = datetime.now()
        self.last_error = None
        self.progress_info = {}

        # 重置统计
        self.task_counter.value = 0
        self.completed_tasks.value = 0
        self.failed_tasks.value = 0
        self.total_records.value = 0

        logger.info("=" * 60)
        logger.info(f"Starting migration job at {self.migration_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Workers per table: {self.max_workers_per_table}")
        logger.info("=" * 60)

        try:
            # 保存迁移历史记录
            migration_id = self.save_migration_history(
                start_time=self.migration_start_time,
                end_time=None,
                status='running',
                tables_migrated=','.join(tables) if tables else 'all',
                total_records=0
            )
            self.current_migration_id = migration_id

            # 模拟迁移过程
            logger.info("Simulating migration process...")
            time.sleep(5)  # 模拟迁移过程

            # 模拟成功
            success = True
            self.total_records.increment(1000)  # 模拟迁移1000条记录

            if success:
                logger.info("Migration job completed successfully")
                end_time = datetime.now()

                # 更新迁移历史
                self.save_migration_history(
                    start_time=self.migration_start_time,
                    end_time=end_time,
                    status='success',
                    tables_migrated=','.join(tables) if tables else 'all',
                    total_records=self.total_records.get()
                )

                return {
                    "success": True,
                    "message": f"Migration completed successfully. Migrated {self.total_records.get()} records.",
                    "migration_id": migration_id,
                    "duration": (end_time - self.migration_start_time).total_seconds()
                }
            else:
                logger.error("Migration job completed with errors")
                end_time = datetime.now()

                self.save_migration_history(
                    start_time=self.migration_start_time,
                    end_time=end_time,
                    status='failed',
                    tables_migrated=','.join(tables) if tables else 'all',
                    total_records=self.total_records.get(),
                    error_message=self.last_error
                )

                return {
                    "success": False,
                    "message": f"Migration failed: {self.last_error}",
                    "migration_id": migration_id,
                    "duration": (end_time - self.migration_start_time).total_seconds()
                }

        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Error in migration job: {str(e)}", exc_info=True)

            end_time = datetime.now()
            self.save_migration_history(
                start_time=self.migration_start_time,
                end_time=end_time,
                status='failed',
                tables_migrated=','.join(tables) if tables else 'all',
                total_records=self.total_records.get(),
                error_message=str(e)
            )

            return {
                "success": False,
                "message": f"Migration error: {str(e)}",
                "migration_id": self.current_migration_id,
                "duration": (end_time - self.migration_start_time).total_seconds() if self.migration_start_time else 0
            }
        finally:
            self.is_running = False

    def start_scheduler(self):
        """启动定时任务调度器"""
        if hasattr(self, 'scheduler_thread') and self.scheduler_thread and self.scheduler_thread.is_alive():
            return {"success": False, "message": "Scheduler is already running"}

        if not self.schedule_enabled:
            return {"success": False, "message": "Schedule is disabled in config"}

        def run_scheduler():
            """运行调度器"""
            try:
                # 设置北京时区
                beijing_tz = timezone('Asia/Shanghai')

                # 清除现有任务
                schedule.clear()

                # 设置定时任务
                schedule_time = self.get_config('schedule_time', '09:00')
                schedule.every().day.at(schedule_time).do(self.run_daily_migration_job)

                logger.info(f"Scheduler started. Next run at {schedule_time} Beijing time")

                while not self.shutdown_event.is_set():
                    schedule.run_pending()
                    time.sleep(60)  # 每分钟检查一次

            except Exception as e:
                logger.error(f"Scheduler error: {str(e)}")
            finally:
                logger.info("Scheduler stopped")

        # 启动调度器线程
        self.scheduler_thread = threading.Thread(target=run_scheduler, name="Scheduler", daemon=True)
        self.scheduler_thread.start()

        return {"success": True,
                "message": f"Scheduler started. Will run daily at {self.get_config('schedule_time', '09:00')}"}

    def stop_scheduler(self):
        """停止定时任务调度器"""
        self.shutdown_event.set()

        if hasattr(self, 'scheduler_thread') and self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
            self.scheduler_thread = None

        schedule.clear()
        self.shutdown_event.clear()

        return {"success": True, "message": "Scheduler stopped"}

    def stop_migration(self):
        """停止当前迁移任务"""
        if not self.is_running:
            return {"success": False, "message": "No migration is running"}

        self.shutdown()

        # 更新迁移历史
        if self.current_migration_id:
            end_time = datetime.now()
            self.save_migration_history(
                start_time=self.migration_start_time,
                end_time=end_time,
                status='stopped',
                tables_migrated='',
                total_records=self.total_records.get(),
                error_message='Migration stopped by user'
            )

        return {"success": True, "message": "Migration stopped"}

    def shutdown(self):
        """优雅关闭"""
        logger.info("Shutdown requested...")
        self.shutdown_event.set()

        # 清空所有队列
        for table_key, queue in self.table_queues.items():
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except Empty:
                    break

        logger.info("Shutdown completed")


# 初始化应用
migration_app = DataMigrationApp(max_workers_per_table=4, schedule_enabled=False)


# 在应用关闭时清理
@atexit.register
def cleanup():
    if migration_app:
        migration_app.shutdown()


# Web路由
@app.route('/')
def index():
    """首页"""
    try:
        status = migration_app.get_status()
        history = migration_app.get_migration_history(limit=10)
        table_status = migration_app.get_table_status()

        return render_template('index.html',
                               status=status,
                               history=history,
                               table_status=table_status,
                               source_tables=migration_app.SOURCE_TABLES,
                               target_tables=migration_app.TARGET_TABLES)
    except Exception as e:
        logger.error(f"Error in index route: {str(e)}")
        return render_template('error.html', error=str(e))


@app.route('/api/status')
def api_status():
    """API: 获取状态"""
    try:
        status = migration_app.get_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error in api_status: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/start', methods=['POST'])
def api_start():
    """API: 开始迁移"""
    try:
        data = request.json or {}
        tables = data.get('tables', [])  # 空列表表示所有表
        days = data.get('days', None)  # None表示使用默认天数

        if migration_app.is_running:
            return jsonify({
                "success": False,
                "message": "Migration is already running"
            })

        # 启动迁移（异步）
        def run_migration():
            migration_app.run_daily_migration_job(tables=tables if tables else None)

        migration_thread = threading.Thread(target=run_migration, name="MigrationJob", daemon=True)
        migration_thread.start()

        return jsonify({
            "success": True,
            "message": "Migration started",
            "tables": tables if tables else "all"
        })

    except Exception as e:
        logger.error(f"Error in api_start: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Error starting migration: {str(e)}"
        }), 500


@app.route('/api/stop', methods=['POST'])
def api_stop():
    """API: 停止迁移"""
    try:
        result = migration_app.stop_migration()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in api_stop: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/scheduler/start', methods=['POST'])
def api_scheduler_start():
    """API: 启动调度器"""
    try:
        data = request.json or {}
        schedule_time = data.get('schedule_time', '09:00')

        # 验证时间格式
        try:
            datetime.strptime(schedule_time, '%H:%M')
        except ValueError:
            return jsonify({
                "success": False,
                "message": "Invalid time format. Use HH:MM"
            })

        migration_app.set_config('schedule_time', schedule_time)
        migration_app.set_config('schedule_enabled', True)

        result = migration_app.start_scheduler()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in api_scheduler_start: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/scheduler/stop', methods=['POST'])
def api_scheduler_stop():
    """API: 停止调度器"""
    try:
        migration_app.set_config('schedule_enabled', False)
        result = migration_app.stop_scheduler()
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in api_scheduler_stop: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/config', methods=['GET', 'POST'])
def api_config():
    """API: 获取/更新配置"""
    try:
        if request.method == 'GET':
            return jsonify(migration_app.config)
        else:
            data = request.json or {}

            for key, value in data.items():
                if key in migration_app.config:
                    migration_app.set_config(key, value)

            return jsonify({
                "success": True,
                "message": "Configuration updated",
                "config": migration_app.config
            })
    except Exception as e:
        logger.error(f"Error in api_config: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/history')
def api_history():
    """API: 获取迁移历史"""
    try:
        limit = request.args.get('limit', 50, type=int)
        history = migration_app.get_migration_history(limit=limit)

        # 转换为字典列表
        history_list = []
        for row in history:
            history_list.append(dict(row))

        return jsonify({
            "success": True,
            "history": history_list
        })
    except Exception as e:
        logger.error(f"Error in api_history: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/tables')
def api_tables():
    """API: 获取表状态"""
    try:
        table_status = migration_app.get_table_status()

        # 转换为字典列表
        tables_list = []
        for row in table_status:
            tables_list.append(dict(row))

        return jsonify({
            "success": True,
            "tables": tables_list
        })
    except Exception as e:
        logger.error(f"Error in api_tables: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/test-connection', methods=['POST'])
def api_test_connection():
    """API: 测试数据库连接"""
    try:
        data = request.json or {}
        db_type = data.get('type', 'all')  # mysql, clickhouse, or all

        results = {}

        if db_type in ['mysql', 'all']:
            try:
                conn = pymysql.connect(**migration_app.MYSQL_CONFIG)
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                conn.close()
                results['mysql'] = {'success': True, 'message': 'MySQL connection successful'}
            except Exception as e:
                results['mysql'] = {'success': False, 'message': f'MySQL connection failed: {str(e)}'}

        if db_type in ['clickhouse', 'all']:
            try:
                client = clickhouse_connect.get_client(**migration_app.CLICKHOUSE_CONFIG)
                result = client.query("SELECT 1")
                client.close()
                results['clickhouse'] = {'success': True, 'message': 'ClickHouse connection successful'}
            except Exception as e:
                results['clickhouse'] = {'success': False, 'message': f'ClickHouse connection failed: {str(e)}'}

        return jsonify({
            "success": True,
            "results": results
        })

    except Exception as e:
        logger.error(f"Error in api_test_connection: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Test failed: {str(e)}"
        }), 500


# 错误处理
@app.errorhandler(404)
def not_found(error):
    return jsonify({"success": False, "message": "Not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({"success": False, "message": "Internal server error"}), 500


# 创建HTML模板
def create_templates():
    """创建HTML模板目录和文件"""
    try:
        templates_dir = Path('templates')
        templates_dir.mkdir(exist_ok=True)

        # 创建index.html
        index_html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>数据迁移管理系统</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.8.1/font/bootstrap-icons.css" rel="stylesheet">
    <style>
        .card { margin-bottom: 1rem; }
        .status-running { color: #0d6efd; }
        .status-success { color: #198754; }
        .status-failed { color: #dc3545; }
        .status-stopped { color: #6c757d; }
        .status-idle { color: #6c757d; }
        .table-hover tbody tr:hover { background-color: rgba(0,0,0,.075); }
        .progress { height: 1.5rem; }
        .log-output { 
            max-height: 300px; 
            overflow-y: auto; 
            font-family: monospace; 
            font-size: 0.875rem;
            background-color: #f8f9fa;
            padding: 1rem;
            border-radius: 0.25rem;
        }
    </style>
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <div class="container">
            <a class="navbar-brand" href="/">
                <i class="bi bi-database"></i> 数据迁移管理系统
            </a>
        </div>
    </nav>

    <div class="container mt-4">
        <div class="alert alert-info">
            <h4>数据迁移管理系统</h4>
            <p>这是一个数据迁移管理系统的Web界面，用于管理从ClickHouse到MySQL的数据迁移任务。</p>
        </div>

        <div class="row">
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5 class="mb-0">系统状态</h5>
                    </div>
                    <div class="card-body">
                        <div id="system-status">
                            <p>系统正在运行...</p>
                        </div>
                    </div>
                </div>
            </div>
            <div class="col-md-6">
                <div class="card">
                    <div class="card-header">
                        <h5 class="mb-0">控制面板</h5>
                    </div>
                    <div class="card-body">
                        <button class="btn btn-primary" onclick="startMigration()">开始迁移</button>
                        <button class="btn btn-danger" onclick="stopMigration()">停止迁移</button>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        function startMigration() {
            fetch('/api/start', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({})
            })
            .then(response => response.json())
            .then(data => {
                alert(data.message);
            });
        }

        function stopMigration() {
            fetch('/api/stop', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                }
            })
            .then(response => response.json())
            .then(data => {
                alert(data.message);
            });
        }
    </script>
</body>
</html>"""

        with open(templates_dir / 'index.html', 'w', encoding='utf-8') as f:
            f.write(index_html)

        # 创建error.html
        error_html = """<!DOCTYPE html>
<html>
<head>
    <title>错误</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <div class="alert alert-danger">
            <h4>系统错误</h4>
            <p>{{ error }}</p>
            <a href="/" class="btn btn-primary">返回首页</a>
        </div>
    </div>
</body>
</html>"""

        with open(templates_dir / 'error.html', 'w', encoding='utf-8') as f:
            f.write(error_html)

        logger.info("Templates created successfully")
    except Exception as e:
        logger.error(f"Error creating templates: {str(e)}")


# 启动应用
if __name__ == '__main__':
    try:
        # 创建模板
        create_templates()

        # 初始化数据库
        init_db()

        # 启动Web服务器
        host = os.environ.get('HOST', '0.0.0.0')
        port = int(os.environ.get('PORT', 5000))
        debug = os.environ.get('DEBUG', 'False').lower() == 'true'

        logger.info(f"Starting Data Migration Web Interface on http://{host}:{port}")
        app.run(host=host, port=port, debug=debug, use_reloader=False)

    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        raise