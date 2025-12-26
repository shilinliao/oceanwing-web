# streamlit_app.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import threading
import sqlite3
import json
import logging
from pathlib import Path
import pymysql
import clickhouse_connect
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DataMigrationApp')

# 页面配置
st.set_page_config(
    page_title="数据迁移管理系统",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)


# 数据库配置
class DatabaseConfig:
    CLICKHOUSE_CONFIG = {
        'host': '47.109.55.96',
        'port': 8124,
        'database': 'semanticdb_haiyi',
        'username': 'haiyi',
        'password': 'G7f@2eBw',
        'secure': False,
        'verify': False
    }

    MYSQL_CONFIG = {
        'host': 'ow-masterdata-1.cavkqwqmyvuw.us-west-2.rds.amazonaws.com',
        'port': 3306,
        'database': 'ow_base',
        'user': 'ow_base_user',
        'password': '3we@5y_+05iu',
        'charset': 'utf8mb4'
    }


# 数据迁移管理器
class DataMigrationManager:
    def __init__(self):
        self.is_running = False
        self.current_task = None
        self.migration_thread = None
        self.progress = 0
        self.status_message = ""
        self.total_records = 0
        self.start_time = None
        self.end_time = None

        # 表配置
        self.tables_config = {
            "ods_Query": {
                "target": "ods_query",
                "days": 24,
                "description": "查询数据表"
            },
            "ods_campain": {
                "target": "ods_campain",
                "days": 60,
                "description": "活动数据表"
            },
            "ods_campaign_dsp": {
                "target": "ods_campaign_dsp",
                "days": 60,
                "description": "DSP活动数据表"
            },
            "ods_aws_asin_philips": {
                "target": "ods_aws_asin_philips",
                "days": 60,
                "description": "AWS ASIN数据表"
            }
        }

        # 初始化本地数据库
        self.init_local_database()

    def init_local_database(self):
        """初始化本地SQLite数据库"""
        try:
            conn = sqlite3.connect('migration.db')
            cursor = conn.cursor()

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

            conn.commit()
            conn.close()
            logger.info("Local database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing local database: {str(e)}")

    def test_database_connections(self):
        """测试数据库连接"""
        results = {}

        # 测试MySQL连接
        try:
            conn = pymysql.connect(**DatabaseConfig.MYSQL_CONFIG)
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            conn.close()
            results['mysql'] = {'status': 'success', 'message': 'MySQL连接正常'}
        except Exception as e:
            results['mysql'] = {'status': 'error', 'message': f'MySQL连接失败: {str(e)}'}

        # 测试ClickHouse连接
        try:
            client = clickhouse_connect.get_client(**DatabaseConfig.CLICKHOUSE_CONFIG)
            result = client.query("SELECT 1")
            client.close()
            results['clickhouse'] = {'status': 'success', 'message': 'ClickHouse连接正常'}
        except Exception as e:
            results['clickhouse'] = {'status': 'error', 'message': f'ClickHouse连接失败: {str(e)}'}

        return results

    def simulate_migration(self, selected_tables, days_override=None):
        """模拟数据迁移过程"""
        self.is_running = True
        self.start_time = datetime.now()
        self.progress = 0
        self.total_records = 0
        self.status_message = "开始数据迁移..."

        try:
            # 保存迁移记录
            conn = sqlite3.connect('migration.db')
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO migration_history 
                (start_time, end_time, status, tables_migrated, total_records, error_message, duration_seconds)
                VALUES (?, NULL, 'running', ?, 0, NULL, 0)
            ''', (self.start_time, ','.join(selected_tables)))
            migration_id = cursor.lastrowid
            conn.commit()
            conn.close()

            # 模拟迁移过程
            total_steps = len(selected_tables) * 10  # 每个表10个步骤
            current_step = 0

            for table in selected_tables:
                table_config = self.tables_config[table]
                days = days_override if days_override else table_config['days']

                self.status_message = f"正在迁移表: {table} (最近{days}天数据)"

                # 模拟表迁移的10个步骤
                for step in range(10):
                    if not self.is_running:
                        break

                    time.sleep(0.5)  # 模拟处理时间
                    current_step += 1
                    self.progress = int((current_step / total_steps) * 100)

                    # 模拟记录迁移
                    records_in_step = np.random.randint(100, 500)
                    self.total_records += records_in_step

                    # 更新表状态
                    self.update_table_status(table, f"迁移中... ({step + 1}/10)")

                if not self.is_running:
                    break

                # 标记表完成
                self.update_table_status(table, "完成", self.total_records)

            if self.is_running:
                self.status_message = "数据迁移完成!"
                self.end_time = datetime.now()
                status = 'success'
            else:
                self.status_message = "迁移已停止"
                self.end_time = datetime.now()
                status = 'stopped'

            # 更新迁移记录
            duration = (self.end_time - self.start_time).total_seconds()
            conn = sqlite3.connect('migration.db')
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE migration_history 
                SET end_time = ?, status = ?, total_records = ?, duration_seconds = ?
                WHERE id = ?
            ''', (self.end_time, status, self.total_records, duration, migration_id))
            conn.commit()
            conn.close()

        except Exception as e:
            self.status_message = f"迁移出错: {str(e)}"
            self.end_time = datetime.now()
            logger.error(f"Migration error: {str(e)}")

        finally:
            self.is_running = False

    def update_table_status(self, table_name, status, records_count=0):
        """更新表状态"""
        try:
            conn = sqlite3.connect('migration.db')
            cursor = conn.cursor()

            cursor.execute('SELECT id FROM table_status WHERE table_name = ?', (table_name,))
            existing = cursor.fetchone()

            if existing:
                cursor.execute('''
                    UPDATE table_status 
                    SET last_sync_time = ?, records_count = ?, status = ?
                    WHERE table_name = ?
                ''', (datetime.now(), records_count, status, table_name))
            else:
                cursor.execute('''
                    INSERT INTO table_status (table_name, last_sync_time, records_count, status)
                    VALUES (?, ?, ?, ?)
                ''', (table_name, datetime.now(), records_count, status))

            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error updating table status: {str(e)}")

    def get_migration_history(self, limit=10):
        """获取迁移历史"""
        try:
            conn = sqlite3.connect('migration.db')
            df = pd.read_sql_query(f'''
                SELECT * FROM migration_history 
                ORDER BY start_time DESC 
                LIMIT {limit}
            ''', conn)
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Error getting migration history: {str(e)}")
            return pd.DataFrame()

    def get_table_status(self):
        """获取表状态"""
        try:
            conn = sqlite3.connect('migration.db')
            df = pd.read_sql_query('SELECT * FROM table_status ORDER BY table_name', conn)
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Error getting table status: {str(e)}")
            return pd.DataFrame()

    def stop_migration(self):
        """停止迁移"""
        self.is_running = False
        return "迁移已停止"


# 初始化管理器
if 'migration_manager' not in st.session_state:
    st.session_state.migration_manager = DataMigrationManager()


def main():
    # 页面标题
    st.title("🚀 数据迁移管理系统")
    st.markdown("---")

    # 侧边栏
    with st.sidebar:
        st.header("控制面板")

        # 数据库连接测试
        if st.button("🔌 测试数据库连接"):
            with st.spinner("测试连接中..."):
                results = st.session_state.migration_manager.test_database_connections()

                for db, result in results.items():
                    if result['status'] == 'success':
                        st.success(f"✅ {db.upper()}: {result['message']}")
                    else:
                        st.error(f"❌ {db.upper()}: {result['message']}")

        st.markdown("---")

        # 快速操作
        st.subheader("快速操作")
        col1, col2 = st.columns(2)

        with col1:
            if st.button("🔄 刷新状态", use_container_width=True):
                st.rerun()

        with col2:
            if st.session_state.migration_manager.is_running:
                if st.button("⏹️ 停止迁移", use_container_width=True):
                    st.session_state.migration_manager.stop_migration()
                    st.rerun()

    # 主内容区域
    tab1, tab2, tab3, tab4 = st.tabs(["📊 仪表板", "🚀 迁移控制", "📋 迁移历史", "⚙️ 系统设置"])

    with tab1:
        display_dashboard()

    with tab2:
        display_migration_control()

    with tab3:
        display_migration_history()

    with tab4:
        display_system_settings()


def display_dashboard():
    """显示仪表板"""
    col1, col2, col3, col4 = st.columns(4)

    manager = st.session_state.migration_manager

    with col1:
        status = "运行中" if manager.is_running else "空闲"
        color = "🟢" if manager.is_running else "⚪"
        st.metric("系统状态", f"{color} {status}")

    with col2:
        st.metric("总迁移记录", f"{manager.total_records:,}")

    with col3:
        if manager.start_time:
            duration = (datetime.now() - manager.start_time).total_seconds() if manager.is_running else 0
            st.metric("运行时间", f"{int(duration)}秒")
        else:
            st.metric("运行时间", "0秒")

    with col4:
        success_count = len(manager.get_migration_history()[manager.get_migration_history()['status'] == 'success'])
        st.metric("成功迁移", f"{success_count}次")

    # 进度显示
    if manager.is_running:
        st.subheader("迁移进度")
        st.progress(manager.progress / 100)
        st.info(f"**状态:** {manager.status_message}")
        st.write(f"**进度:** {manager.progress}%")
        st.write(f"**已迁移记录:** {manager.total_records:,}")

        # 自动刷新
        time.sleep(1)
        st.rerun()

    # 表状态
    st.subheader("📋 表状态监控")
    table_status = manager.get_table_status()

    if not table_status.empty:
        # 美化显示
        display_df = table_status[['table_name', 'last_sync_time', 'records_count', 'status']].copy()
        display_df.columns = ['表名', '最后同步时间', '记录数', '状态']

        # 状态颜色映射
        def color_status(val):
            if val == '完成':
                return 'color: green; font-weight: bold;'
            elif '迁移中' in val:
                return 'color: orange; font-weight: bold;'
            elif '错误' in val:
                return 'color: red; font-weight: bold;'
            else:
                return ''

        styled_df = display_df.style.map(lambda x: color_status(x), subset=['状态'])
        st.dataframe(styled_df, use_container_width=True)
    else:
        st.info("暂无表状态信息")


def display_migration_control():
    """显示迁移控制界面"""
    manager = st.session_state.migration_manager

    st.header("数据迁移控制")

    # 迁移配置
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("选择迁移表")
        selected_tables = []
        for table, config in manager.tables_config.items():
            if st.checkbox(f"{table} → {config['target']}", value=True,
                           help=f"{config['description']} (默认{config['days']}天)"):
                selected_tables.append(table)

    with col2:
        st.subheader("迁移设置")
        days_override = st.number_input("迁移天数覆盖", min_value=1, max_value=365, value=30,
                                        help="留空使用表默认天数")
        if days_override == 30:  # 默认值
            days_override = None

        st.info("""
        **迁移说明:**
        - ods_query: 默认迁移24天数据
        - 其他表: 默认迁移60天数据
        - 可自定义覆盖天数
        """)

    # 控制按钮
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🚀 开始迁移", type="primary", use_container_width=True):
            if not selected_tables:
                st.error("请至少选择一个表进行迁移")
            else:
                # 在新线程中运行迁移
                def run_migration():
                    manager.simulate_migration(selected_tables, days_override)

                migration_thread = threading.Thread(target=run_migration)
                migration_thread.daemon = True
                migration_thread.start()
                st.rerun()

    with col2:
        if manager.is_running:
            if st.button("⏹️ 停止迁移", type="secondary", use_container_width=True):
                manager.stop_migration()
                st.rerun()

    with col3:
        if st.button("🔄 重置状态", use_container_width=True):
            st.rerun()

    # 实时日志
    st.subheader("📝 实时日志")
    log_placeholder = st.empty()

    if manager.is_running:
        with log_placeholder.container():
            st.code(f"""
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO: {manager.status_message}
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO: 进度: {manager.progress}%
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - INFO: 已迁移记录: {manager.total_records}
            """)

        # 自动刷新日志
        time.sleep(2)
        st.rerun()


def display_migration_history():
    """显示迁移历史"""
    st.header("迁移历史记录")

    manager = st.session_state.migration_manager
    history_df = manager.get_migration_history(limit=20)

    if not history_df.empty:
        # 美化显示
        display_df = history_df[
            ['id', 'start_time', 'end_time', 'status', 'tables_migrated', 'total_records', 'duration_seconds']].copy()
        display_df.columns = ['ID', '开始时间', '结束时间', '状态', '迁移表', '记录数', '耗时(秒)']

        # 状态颜色
        def color_status(val):
            if val == 'success':
                return 'background-color: #d4edda; color: #155724;'
            elif val == 'failed':
                return 'background-color: #f8d7da; color: #721c24;'
            elif val == 'stopped':
                return 'background-color: #fff3cd; color: #856404;'
            else:
                return ''

        styled_df = display_df.style.map(lambda x: color_status(x), subset=['状态'])
        st.dataframe(styled_df, use_container_width=True)

        # 统计信息
        col1, col2, col3 = st.columns(3)
        total_migrations = len(history_df)
        success_rate = (len(
            history_df[history_df['status'] == 'success']) / total_migrations * 100) if total_migrations > 0 else 0
        total_records = history_df['total_records'].sum()

        with col1:
            st.metric("总迁移次数", total_migrations)
        with col2:
            st.metric("成功率", f"{success_rate:.1f}%")
        with col3:
            st.metric("总迁移记录", f"{total_records:,}")
    else:
        st.info("暂无迁移历史记录")


def display_system_settings():
    """显示系统设置"""
    st.header("系统设置")

    manager = st.session_state.migration_manager

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("数据库配置")
        st.json(DatabaseConfig.MYSQL_CONFIG, expanded=False)
        st.json(DatabaseConfig.CLICKHOUSE_CONFIG, expanded=False)

        if st.button("验证配置"):
            with st.spinner("验证中..."):
                results = manager.test_database_connections()
                for db, result in results.items():
                    if result['status'] == 'success':
                        st.success(f"✅ {db.upper()}配置正确")
                    else:
                        st.error(f"❌ {db.upper()}配置错误: {result['message']}")

    with col2:
        st.subheader("表配置信息")
        tables_info = []
        for table, config in manager.tables_config.items():
            tables_info.append({
                '源表': table,
                '目标表': config['target'],
                '默认天数': config['days'],
                '描述': config['description']
            })

        st.table(pd.DataFrame(tables_info))

    st.subheader("系统信息")
    col1, col2 = st.columns(2)

    with col1:
        st.info(f"""
        **Python版本:** {sys.version.split()[0]}
        **Streamlit版本:** {st.__version__}
        **当前时间:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """)

    with col2:
        st.info(f"""
        **运行状态:** {'运行中' if manager.is_running else '空闲'}
        **最后操作:** {manager.status_message}
        **数据库文件:** migration.db
        """)


if __name__ == "__main__":
    main()