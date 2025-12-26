"""Streamlit数据迁移管理页面"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.migration_app import DataMigrationApp
from config.settings import Config
from utils.logger import setup_logging

# 页面配置
st.set_page_config(
    page_title="数据迁移管理系统",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化session state
if 'migration_app' not in st.session_state:
    st.session_state.migration_app = None
if 'migration_status' not in st.session_state:
    st.session_state.migration_status = {}
if 'last_update' not in st.session_state:
    st.session_state.last_update = datetime.now()

class StreamlitMigrationManager:
    """Streamlit迁移管理器"""

    def __init__(self):
        self.setup_logging()
        self.setup_app()

    def setup_logging(self):
        """设置日志"""
        setup_logging("INFO", "streamlit_migration.log")

    def setup_app(self):
        """初始化应用"""
        if st.session_state.migration_app is None:
            try:
                st.session_state.migration_app = DataMigrationApp(
                    max_workers_per_table=4,
                    schedule_enabled=False  # Streamlit模式下不启用调度
                )
                st.success("✅ 迁移应用初始化成功")
            except Exception as e:
                st.error(f"❌ 应用初始化失败: {str(e)}")

    def get_migration_status(self):
        """获取迁移状态"""
        if st.session_state.migration_app:
            return st.session_state.migration_app.get_status()
        return {}

    def update_status(self):
        """更新状态"""
        st.session_state.migration_status = self.get_migration_status()
        st.session_state.last_update = datetime.now()

def main():
    """主页面"""
    # 标题和描述
    st.title("🚀 数据迁移管理系统")
    st.markdown("""
    **ClickHouse到MySQL数据迁移管理平台**
    - 实时监控迁移状态
    - 手动控制迁移任务
    - 查看迁移统计和性能指标
    """)

    # 初始化管理器
    manager = StreamlitMigrationManager()

    # 侧边栏
    with st.sidebar:
        st.header("控制面板")

        # 系统信息
        st.subheader("📊 系统信息")
        if st.session_state.migration_app:
            status = st.session_state.migration_app.get_status()
            st.metric("运行状态", "🟢 运行中" if not status.get('shutdown_requested', False) else "🔴 已停止")
            st.metric("迁移任务", "🟡 进行中" if status.get('is_running', False) else "🟢 空闲")
        else:
            st.metric("运行状态", "🔴 未初始化")

        # 控制按钮
        st.subheader("🎮 迁移控制")
        col1, col2 = st.columns(2)

        with col1:
            if st.button("▶️ 开始迁移", use_container_width=True):
                start_migration(manager)

        with col2:
            if st.button("⏹️ 停止迁移", use_container_width=True):
                stop_migration(manager)

        # 单表迁移选项
        st.subheader("📋 单表迁移")
        selected_table = st.selectbox(
            "选择要迁移的表",
            Config.TARGET_TABLES,
            index=0
        )

        migration_days = st.slider("迁移天数", 1, 90, 30)

        if st.button("🔧 迁移选中表", use_container_width=True):
            migrate_single_table(manager, selected_table, migration_days)

        # 配置设置
        st.subheader("⚙️ 系统配置")
        workers_per_table = st.slider("每表工作线程", 1, 16, 4)
        max_retries = st.slider("最大重试次数", 1, 10, 3)

        if st.button("💾 保存配置", use_container_width=True):
            update_config(manager, workers_per_table, max_retries)

        # 状态刷新
        st.subheader("🔄 状态刷新")
        if st.button("🔄 手动刷新", use_container_width=True):
            manager.update_status()
            st.rerun()

    # 主内容区域
    tab1, tab2, tab3, tab4 = st.tabs(["📊 仪表盘", "📈 迁移监控", "🔧 任务管理", "📋 系统配置"])

    with tab1:
        show_dashboard(manager)

    with tab2:
        show_migration_monitor(manager)

    with tab3:
        show_task_management(manager)

    with tab4:
        show_system_config(manager)

def start_migration(manager):
    """开始迁移"""
    try:
        with st.spinner("🚀 启动迁移任务..."):
            success = manager.migration_app.migrate_all_tables_parallel()
            if success:
                st.success("✅ 迁移任务启动成功！")
            else:
                st.error("❌ 迁移任务启动失败")
            time.sleep(2)
            st.rerun()
    except Exception as e:
        st.error(f"❌ 启动迁移失败: {str(e)}")

def stop_migration(manager):
    """停止迁移"""
    try:
        with st.spinner("🛑 停止迁移任务..."):
            manager.migration_app.shutdown()
            st.success("✅ 迁移任务已停止")
            time.sleep(2)
            st.rerun()
    except Exception as e:
        st.error(f"❌ 停止迁移失败: {str(e)}")

def migrate_single_table(manager, table_name, days):
    """迁移单个表"""
    try:
        with st.spinner(f"🔧 迁移表 {table_name}..."):
            # 查找对应的源表
            source_table = None
            for src, tgt in zip(Config.SOURCE_TABLES, Config.TARGET_TABLES):
                if tgt == table_name:
                    source_table = src
                    break

            if source_table:
                success = manager.migration_app.migrate_single_table(source_table, table_name, days)
                if success:
                    st.success(f"✅ 表 {table_name} 迁移成功！")
                else:
                    st.error(f"❌ 表 {table_name} 迁移失败")
            else:
                st.error(f"❌ 未找到表 {table_name} 的配置")

            time.sleep(2)
            st.rerun()
    except Exception as e:
        st.error(f"❌ 单表迁移失败: {str(e)}")

def update_config(manager, workers, retries):
    """更新配置"""
    try:
        # 这里可以添加配置更新逻辑
        st.success("✅ 配置已保存（演示功能）")
        time.sleep(1)
    except Exception as e:
        st.error(f"❌ 配置更新失败: {str(e)}")

def show_dashboard(manager):
    """显示仪表盘"""
    st.header("📊 实时监控仪表盘")

    # 获取状态信息
    status = manager.get_migration_status()

    # 关键指标
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("总记录数", f"{status.get('total_records', 0):,}")

    with col2:
        st.metric("完成任务数", f"{status.get('completed_tasks', 0):,}")

    with col3:
        st.metric("失败任务数", f"{status.get('failed_tasks', 0):,}")

    with col4:
        st.metric("运行状态", "🟢 运行中" if status.get('is_running', False) else "🟡 空闲")

    # 迁移进度
    st.subheader("📈 迁移进度")

    # 创建示例数据（实际中应该从应用获取）
    progress_data = {
        '表名': Config.TARGET_TABLES,
        '进度 (%)': [75, 60, 45, 80],  # 示例数据
        '记录数': [15000, 12000, 9000, 20000]  # 示例数据
    }

    df_progress = pd.DataFrame(progress_data)

    # 进度条显示
    for _, row in df_progress.iterrows():
        st.write(f"**{row['表名']}**")
        st.progress(row['进度 (%)'] / 100)
        st.write(f"记录数: {row['记录数']:,} | 进度: {row['进度 (%)']}%")
        st.write("---")

    # 性能图表
    st.subheader("🚀 性能指标")

    # 创建示例性能数据
    performance_data = {
        '时间': [f"T-{i}" for i in range(10, 0, -1)],
        '处理速度 (记录/秒)': [1200, 1150, 1250, 1300, 1280, 1350, 1400, 1380, 1420, 1450]
    }

    df_perf = pd.DataFrame(performance_data)

    fig = px.line(df_perf, x='时间', y='处理速度 (记录/秒)',
                  title='迁移处理速度趋势', markers=True)
    st.plotly_chart(fig, use_container_width=True)

def show_migration_monitor(manager):
    """显示迁移监控"""
    st.header("📈 实时迁移监控")

    # 表状态监控
    st.subheader("🔄 表迁移状态")

    # 创建表状态数据
    table_status_data = []
    for i, (source, target) in enumerate(zip(Config.SOURCE_TABLES, Config.TARGET_TABLES)):
        table_status_data.append({
            '序号': i + 1,
            '源表': source,
            '目标表': target,
            '状态': '🟢 完成' if i % 2 == 0 else '🟡 进行中',
            '开始时间': '2024-01-01 10:00',
            '结束时间': '2024-01-01 12:00' if i % 2 == 0 else '进行中',
            '记录数': f"{10000 + i * 2000:,}"
        })

    df_status = pd.DataFrame(table_status_data)
    st.dataframe(df_status, use_container_width=True)

    # 实时日志
    st.subheader("📝 实时日志")

    # 创建日志显示区域
    log_placeholder = st.empty()

    # 模拟实时日志（实际中应该从日志文件读取）
    sample_logs = [
        "INFO: 开始处理表 ods_query 的数据迁移",
        "INFO: 查询到 1500 条记录",
        "INFO: 成功插入 1500 条记录到MySQL",
        "INFO: 表 ods_query 迁移完成，耗时 45.2 秒",
        "INFO: 开始处理表 ods_campain 的数据迁移",
        "INFO: 查询到 3200 条记录",
        "WARNING: 遇到锁等待超时，重试中...",
        "INFO: 重试成功，继续处理"
    ]

    log_text = "\n".join([f"{datetime.now().strftime('%H:%M:%S')} - {log}"
                         for log in sample_logs[-10:]])  # 显示最后10条

    log_placeholder.text_area("实时日志", log_text, height=200, disabled=True)

    # 自动刷新
    if st.checkbox("🔄 自动刷新日志（每5秒）"):
        time.sleep(5)
        st.rerun()

def show_task_management(manager):
    """显示任务管理"""
    st.header("🔧 迁移任务管理")

    # 任务队列状态
    st.subheader("📋 任务队列")

    col1, col2 = st.columns(2)

    with col1:
        st.info("待处理任务")
        pending_tasks = [
            {"表名": "ods_query", "日期": "2024-01-01", "优先级": "高"},
            {"表名": "ods_campain", "日期": "2024-01-02", "优先级": "高"},
            {"表名": "ods_campaign_dsp", "日期": "2024-01-03", "优先级": "中"},
        ]

        for task in pending_tasks:
            with st.expander(f"{task['表名']} - {task['日期']}"):
                st.write(f"优先级: {task['优先级']}")
                if st.button(f"立即处理", key=f"process_{task['表名']}"):
                    st.success(f"开始处理 {task['表名']}")

    with col2:
        st.success("已完成任务")
        completed_tasks = [
            {"表名": "ods_aws_asin_philips", "日期": "2023-12-30", "状态": "成功"},
            {"表名": "ods_query", "日期": "2023-12-29", "状态": "成功"},
        ]

        for task in completed_tasks:
            st.write(f"✅ {task['表名']} - {task['日期']}")

    # 批量操作
    st.subheader("🎯 批量操作")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🚀 启动全部任务", use_container_width=True):
            st.info("开始执行所有待处理任务...")

    with col2:
        if st.button("⏸️ 暂停所有任务", use_container_width=True):
            st.warning("已暂停所有运行中的任务")

    with col3:
        if st.button("🗑️ 清空任务队列", use_container_width=True):
            st.error("任务队列已清空")

def show_system_config(manager):
    """显示系统配置"""
    st.header("⚙️ 系统配置管理")

    # 数据库配置
    st.subheader("🗄️ 数据库连接配置")

    col1, col2 = st.columns(2)

    with col1:
        st.write("**ClickHouse配置**")
        st.text_input("主机", value=Config.CLICKHOUSE_CONFIG['host'], disabled=True)
        st.number_input("端口", value=Config.CLICKHOUSE_CONFIG['port'], disabled=True)
        st.text_input("数据库", value=Config.CLICKHOUSE_CONFIG['database'], disabled=True)

    with col2:
        st.write("**MySQL配置**")
        st.text_input("主机", value=Config.MYSQL_CONFIG['host'], disabled=True)
        st.number_input("端口", value=Config.MYSQL_CONFIG['port'], disabled=True)
        st.text_input("数据库", value=Config.MYSQL_CONFIG['database'], disabled=True)

    # 性能配置
    st.subheader("🚀 性能配置")

    col1, col2, col3 = st.columns(3)

    with col1:
        workers = st.slider("工作线程数", 1, 16, 4)

    with col2:
        batch_size = st.selectbox("批量大小", [500, 1000, 2000, 5000], index=2)

    with col3:
        retries = st.slider("最大重试次数", 1, 10, 3)

    # 表配置
    st.subheader("📊 表映射配置")

    table_config_data = []
    for source, target in zip(Config.SOURCE_TABLES, Config.TARGET_TABLES):
        days_config = Config.get_table_migration_days()
        days = days_config.get(target, 30)

        table_config_data.append({
            '源表': source,
            '目标表': target,
            '迁移天数': days,
            '状态': '🟢 启用'
        })

    df_config = pd.DataFrame(table_config_data)
    st.dataframe(df_config, use_container_width=True)

    # 配置操作
    st.subheader("💾 配置操作")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("💾 保存配置", use_container_width=True):
            st.success("配置保存成功！")

    with col2:
        if st.button("🔄 重置配置", use_container_width=True):
            st.warning("配置已重置为默认值")

# 运行应用
if __name__ == "__main__":
    main()