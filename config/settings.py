"""配置管理模块"""
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

    # 性能调优参数
    MAX_WORKERS_PER_TABLE = 4
    MAX_RETRIES = 3
    RETRY_DELAY_BASE = 1
    LOCK_TIMEOUT = 30
    BATCH_SIZE_SMALL = 500
    BATCH_SIZE_MEDIUM = 1000
    BATCH_SIZE_LARGE = 2000

    # 调度配置
    SCHEDULE_TIME = "09:00"
    TIMEZONE = "Asia/Shanghai"

    @classmethod
    def get_table_columns_mapping(cls) -> Dict[str, Dict[str, str]]:
        """获取完整的表列映射配置"""
        return {
            "ods_campain": cls._create_column_mapping(
                # 源表列名
                ["Time", "Campaign Name", "Profile Name", "Portfolio Name", "Status", "Impression", "Click",
                 "Spend", "CTR", "CPC", "CVR 14d", "ACOS 14d", "ROAS 14d", "Order 14d", "Sale Units 14d",
                 "Sales 14d", "Campaign Type", "Campaign Tag", "DPV 14天", "活动Id"],
                # 目标表列名
                ["time", "campaign_name", "profile_name", "portfolio_name", "status", "impression", "click",
                 "spend", "ctr", "cpc", "cvr14d", "acos14d", "roas14d", "order14d", "sale_units14d",
                 "sales14d", "campaign_type", "campaign_tag", "dpv14d", "campaign_id"]
            ),
            "ods_campaign_dsp": cls._create_column_mapping(
                # 源表列名 (完整列表)
                ["TimeColumn", "IntervalStart", "IntervalEnd", "EntityName", "EntityId",
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
                 "Search", "Search value average", "TotaleCPDPV", "TotaleCPATC"],
                # 目标表列名 (完整列表)
                ["time_column", "interval_start", "interval_end", "entity_name", "entity_id",
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
            ),
            "ods_aws_asin_philips": cls._create_column_mapping(
                # 源表列名 (完整列表)
                ["Time", "ASIN", "Profile_Name", "SKU", "Title", "Brand", "Image_Url", "Impression",
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
                 "Kindle_Pages_Read_14d", "Kindle_Pages_Royalties_14d"],
                # 目标表列名 (完整列表)
                ["time", "ASIN", "profile_name", "sku", "title", "brand", "image_url",
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
            ),
            "ods_query": cls._create_column_mapping(
                # 源表列名
                ["Time", "Query", "Keyword Text", "Profile Name", "Current Bid", "Campaign Name", "Adgroup",
                 "Impression", "Impression.rank", "Impression.share", "Click", "Spend", "CTR", "CPC",
                 "CVR 14d", "ACOS 14d", "ROAS 14d", "Order 14d", "Sale Units 14d", "Sales 14d", "DPV 14天",
                 "广告活动标签", "活动类型"],
                # 目标表列名
                ["time", "query", "keyword_text", "profile_name", "current_bid", "campaign_name", "adgroup",
                 "impression", "impression_rank", "impression_share", "click", "spend", "ctr", "cpc",
                 "cvr14d", "acos14d", "roas14d", "order14d", "sale_units14d", "sales14d", "dpv14d",
                 "campaign_tag", "activity_type"]
            )
        }

    @classmethod
    def get_table_migration_days(cls) -> Dict[str, int]:
        """获取各表的迁移天数配置"""
        return {
            "ods_query": 30,
            "ods_campain": 60,
            "ods_campaign_dsp": 60,
            "ods_aws_asin_philips": 60
        }

    @classmethod
    def get_table_time_fields(cls) -> Dict[str, str]:
        """获取各表的时间字段配置"""
        return {
            "ods_campain": "Time",
            "ods_campaign_dsp": "TimeColumn",
            "ods_aws_asin_philips": "Time",
            "ods_query": "Time"
        }

    @classmethod
    def get_table_filter_conditions(cls) -> Dict[str, str]:
        """获取各表的过滤条件"""
        return {
            "ods_campain": "`Profile Name` LIKE 'Philips%'",
            "ods_campaign_dsp": "",  # DSP表可能不需要Philips过滤
            "ods_aws_asin_philips": "`Profile_Name` LIKE 'Philips%'",
            "ods_query": "`Profile Name` LIKE 'Philips%'"
        }

    @staticmethod
    def _create_column_mapping(source_columns: list, target_columns: list) -> Dict[str, str]:
        """创建列映射字典"""
        if len(source_columns) != len(target_columns):
            raise ValueError(f"Source and target columns count mismatch: {len(source_columns)} vs {len(target_columns)}")

        return {source: target for source, target in zip(source_columns, target_columns)}

    @classmethod
    def get_type_mapping(cls) -> Dict[str, str]:
        """获取类型映射配置"""
        return {
            'uint8': 'TINYINT',
            'int8': 'TINYINT',
            'uint16': 'SMALLINT',
            'int16': 'SMALLINT',
            'uint32': 'INT',
            'int32': 'INT',
            'uint64': 'BIGINT',
            'int64': 'BIGINT',
            'float32': 'FLOAT',
            'float64': 'DOUBLE',
            'string': 'VARCHAR(1000)',
            'date': 'DATE',
            'datetime': 'DATETIME',
            'decimal': 'DECIMAL(20,6)',
            'text': 'TEXT',
            'array': 'TEXT',
            'map': 'TEXT'
        }

    @classmethod
    def validate_config(cls) -> bool:
        """验证配置完整性"""
        try:
            # 验证表配置一致性
            if len(cls.SOURCE_TABLES) != len(cls.TARGET_TABLES):
                raise ValueError("Source and target tables count mismatch")

            # 验证列映射配置
            column_mapping = cls.get_table_columns_mapping()
            for target_table in cls.TARGET_TABLES:
                if target_table not in column_mapping:
                    raise ValueError(f"Missing column mapping for table: {target_table}")

            # 验证迁移天数配置
            days_config = cls.get_table_migration_days()
            for target_table in cls.TARGET_TABLES:
                if target_table not in days_config:
                    raise ValueError(f"Missing migration days for table: {target_table}")

            return True

        except Exception as e:
            print(f"Configuration validation failed: {e}")
            return False