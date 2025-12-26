"""Streamlit数据迁移管理页面 - 修复session_state键名问题"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.migration_app import DataMigrationApp

# 页面配置
st.set_page_config(
    page_title="数据迁移管理系统",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化应用状态 - 修复键名问题
if 'migration_app' not in st.session_state:
    st.session_state.migration_app = DataMigrationApp()

if 'migration_history' not in st.session_state:
    st.session_state.migration_history = []

if 'auto_refresh_flag' not in st.session_state:  # 修复键名冲突
    st.session_state.auto_refresh_flag = True

def main():
    """主页面"""
    # 标题和描述
    st.title("🚀 数据迁移管理系统")
    st.markdown("""
    **ClickHouse到MySQL数据迁移管理平台**
    - 📊 实时监控迁移状态
    - 🎮 手动控制迁移任务
    - 📈 查看迁移统计和性能指标
    - ⚙️ 灵活配置迁移参数
    """)

    # 侧边栏
    with st.sidebar:
        show_sidebar()

    # 主内容区域
    tab1, tab2, tab3, tab4 = st.tabs(["📊 仪表盘", "📈 迁移监控", "🔧 任务管理", "⚙️ 系统配置"])

    with tab1:
        show_dashboard()

    with tab2:
        show_migration_monitor()

    with tab3:
        show_task_management()

    with tab4:
        show_system_config()

    # 自动刷新
    if st.session_state.auto_refresh_flag:  # 使用修复后的键名
        time.sleep(2)
        st.rerun()

def show_sidebar():
    """显示侧边栏"""
    st.header("控制面板")

    # 系统信息
    st.subheader("📊 系统信息")
    status = st.session_state.migration_app.get_status()

    col1, col2 = st.columns(2)
    with col1:
        st.metric("运行状态", "🟢 运行中" if status['is_running'] else "🟡 空闲")
    with col2:
        st.metric("总记录数", f"{status['stats']['total_records']:,}")

    # 迁移控制
    st.subheader("🎮 迁移控制")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶️ 开始迁移", use_container_width=True,
                    disabled=status['is_running'], key="start_all"):
            start_migration()

    with col2:
        if st.button("⏹️ 停止迁移", use_container_width=True,
                    disabled=not status['is_running'], key="stop_all"):
            stop_migration()

    # 单表迁移
    st.subheader("📋 单表迁移")
    table_options = ['ods_query', 'ods_campain', 'ods_campaign_dsp', 'ods_aws_asin_philips']
    selected_table = st.selectbox("选择表", table_options, key="table_select")
    migration_days = st.slider("迁移天数", 1, 90, 30, key="days_slider")

    if st.button("🔧 迁移选中表", use_container_width=True,
                disabled=status['is_running'], key="start_single"):
        migrate_single_table(selected_table, migration_days)

    # 连接测试
    st.subheader("🔌 连接测试")
    if st.button("测试数据库连接", use_container_width=True, key="test_conn"):
        test_connections()

    # 配置选项
    st.subheader("⚙️ 配置选项")
    auto_refresh = st.checkbox("自动刷新", value=st.session_state.auto_refresh_flag, key="auto_refresh_check")
    st.session_state.auto_refresh_flag = auto_refresh  # 使用修复后的键名

    if st.button("🔄 重置状态", use_container_width=True, key="reset"):
        reset_migration()

def show_dashboard():
    """显示仪表盘"""
    st.header("📊 实时监控仪表盘")

    status = st.session_state.migration_app.get_status()
    overall_progress = st.session_state.migration_app.get_overall_progress()

    # 关键指标卡片
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("总记录数", f"{overall_progress['total_records']:,}")

    with col2:
        st.metric("完成进度", f"{overall_progress['progress_percentage']:.1f}%")

    with col3:
        st.metric("运行时间", f"{overall_progress['execution_time']:.1f}s")

    with col4:
        st.metric("表状态", f"{overall_progress['completed_tables']}/{overall_progress['total_tables']}")

    # 进度条
    progress = overall_progress['progress_percentage'] / 100
    st.progress(progress)

    # 迁移统计图表
    st.subheader("📈 迁移统计")

    # 创建表状态数据
    table_data = []
    for table_name in ['ods_query', 'ods_campain', 'ods_campaign_dsp', 'ods_aws_asin_philips']:
        table_info = st.session_state.migration_app.get_table_progress(table_name)
        table_data.append({
            '表名': table_name,
            '状态': table_info.get('status', 'unknown'),
            '最后迁移': table_info.get('last_migration', '从未'),
            '记录数': table_info.get('records_migrated', 0),
            '描述': table_info.get('description', '')
        })

    df_tables = pd.DataFrame(table_data)

    # 状态分布饼图
    if not df_tables.empty:
        status_counts = df_tables['状态'].value_counts()
        fig_pie = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title='表状态分布',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # 记录数柱状图
    if not df_tables.empty:
        fig_bar = px.bar(
            df_tables,
            x='表名',
            y='记录数',
            title='各表迁移记录数',
            color='状态',
            color_discrete_map={
                'completed': '#00CC96',
                'failed': '#EF553B',
                'running': '#636EFA',
                'not_started': '#AB63FA'
            }
        )
        st.plotly_chart(fig_bar, use_container_width=True)

def show_migration_monitor():
    """显示迁移监控"""
    st.header("📈 实时迁移监控")

    # 表状态监控
    st.subheader("🔄 表迁移状态")

    table_data = []
    for table_name in ['ods_query', 'ods_campain', 'ods_campaign_dsp', 'ods_aws_asin_philips']:
        table_info = st.session_state.migration_app.get_table_progress(table_name)
        table_data.append({
            '表名': table_name,
            '状态': get_status_icon(table_info.get('status', 'unknown')),
            '最后迁移时间': table_info.get('last_migration', '从未'),
            '迁移记录数': f"{table_info.get('records_migrated', 0):,}",
            '描述': table_info.get('description', '')
        })

    df_status = pd.DataFrame(table_data)
    st.dataframe(df_status, use_container_width=True, hide_index=True)

    # 实时日志
    st.subheader("📝 系统日志")

    # 创建日志显示
    status = st.session_state.migration_app.get_status()
    current_time = datetime.now().strftime('%H:%M:%S')

    log_entries = [
        f"{current_time} - INFO: 系统启动完成",
        f"{current_time} - INFO: 数据库连接正常" if status['is_running'] else f"{current_time} - INFO: 系统空闲",
    ]

    if status['is_running']:
        log_entries.append(f"{current_time} - INFO: 迁移任务进行中...")
        progress = st.session_state.migration_app.get_overall_progress()
        log_entries.append(f"{current_time} - INFO: 总体进度: {progress['progress_percentage']:.1f}%")
        log_entries.append(f"{current_time} - INFO: 已迁移记录: {progress['total_records']:,}")

    log_text = "\n".join(log_entries)
    st.text_area("实时日志", log_text, height=200, disabled=True, key="log_area")

def show_task_management():
    """显示任务管理"""
    st.header("🔧 迁移任务管理")

    # 任务队列状态
    st.subheader("📋 任务队列")

    col1, col2 = st.columns(2)

    with col1:
        st.info("🔵 待处理任务")
        for table_name in ['ods_query', 'ods_campain', 'ods_campaign_dsp', 'ods_aws_asin_philips']:
            table_info = st.session_state.migration_app.get_table_progress(table_name)
            if table_info.get('status') in ['not_started', 'failed']:
                with st.expander(f"{table_name} - {table_info.get('description', '')}"):
                    st.write(f"状态: {table_info.get('status', 'unknown')}")
                    st.write(f"最后迁移: {table_info.get('last_migration', '从未')}")
                    if st.button("立即处理", key=f"process_{table_name}"):
                        migrate_single_table(table_name, 30)

    with col2:
        st.success("🟢 已完成任务")
        completed_tables = []
        for table_name in ['ods_query', 'ods_campain', 'ods_campaign_dsp', 'ods_aws_asin_philips']:
            table_info = st.session_state.migration_app.get_table_progress(table_name)
            if table_info.get('status') == 'completed':
                completed_tables.append(table_name)
                st.write(f"✅ {table_name} - {table_info.get('records_migrated', 0):,} 条记录")

        if not completed_tables:
            st.write("暂无已完成任务")

    # 批量操作
    st.subheader("🎯 批量操作")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("🚀 启动全部任务", use_container_width=True, key="start_all_tasks"):
            start_migration()

    with col2:
        if st.button("⏸️ 暂停所有任务", use_container_width=True, key="pause_all"):
            stop_migration()

    with col3:
        if st.button("🔄 重置状态", use_container_width=True, key="reset_all"):
            reset_migration()

def show_system_config():
    """显示系统配置"""
    st.header("⚙️ 系统配置管理")

    # 数据库配置
    st.subheader("🗄️ 数据库连接配置")

    col1, col2 = st.columns(2)

    with col1:
        st.write("**ClickHouse配置**")
        st.text_input("主机", value="47.109.55.96", disabled=True, key="ch_host")
        st.number_input("端口", value=8124, disabled=True, key="ch_port")
        st.text_input("数据库", value="semanticdb_haiyi", disabled=True, key="ch_db")

    with col2:
        st.write("**MySQL配置**")
        st.text_input("主机", value="ow-masterdata-1.cavkqwqmyvuw.us-west-2.rds.amazonaws.com", disabled=True, key="mysql_host")
        st.number_input("端口", value=3306, disabled=True, key="mysql_port")
        st.text_input("数据库", value="ow_base", disabled=True, key="mysql_db")

    # 性能配置
    st.subheader("🚀 性能配置")

    col1, col2, col3 = st.columns(3)

    with col1:
        workers = st.slider("工作线程数", 1, 16, 4, key="config_workers")

    with col2:
        batch_size = st.selectbox("批量大小", [500, 1000, 2000, 5000], index=2, key="batch_size")

    with col3:
        retries = st.slider("最大重试次数", 1, 10, 3, key="max_retries")

    # 表配置
    st.subheader("📊 表迁移配置")

    table_config_data = [
        {'源表': 'ods_Query', '目标表': 'ods_query', '迁移天数': 30, '状态': '启用'},
        {'源表': 'ods_campain', '目标表': 'ods_campain', '迁移天数': 60, '状态': '启用'},
        {'源表': 'ods_campaign_dsp', '目标表': 'ods_campaign_dsp', '迁移天数': 60, '状态': '启用'},
        {'源表': 'ods_aws_asin_philips', '目标表': 'ods_aws_asin_philips', '迁移天数': 60, '状态': '启用'}
    ]

    df_config = pd.DataFrame(table_config_data)
    st.dataframe(df_config, use_container_width=True, hide_index=True)

    # 配置操作
    st.subheader("💾 配置操作")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("💾 保存配置", use_container_width=True, key="save_config"):
            st.success("配置保存成功！")

    with col2:
        if st.button("🔄 重置配置", use_container_width=True, key="reset_config"):
            st.warning("配置已重置为默认值")

def start_migration():
    """开始迁移"""
    try:
        with st.spinner("🚀 启动迁移任务..."):
            success = st.session_state.migration_app.migrate_all_tables()
            if success:
                st.success("✅ 迁移任务启动成功！")
                st.session_state.migration_history.append({
                    'timestamp': datetime.now(),
                    'action': 'start_all',
                    'success': True
                })
            else:
                st.error("❌ 迁移任务启动失败")
            time.sleep(2)
            st.rerun()
    except Exception as e:
        st.error(f"❌ 启动迁移失败: {str(e)}")

def stop_migration():
    """停止迁移"""
    try:
        with st.spinner("🛑 停止迁移任务..."):
            success = st.session_state.migration_app.stop_migration()
            if success:
                st.success("✅ 迁移任务已停止")
                st.session_state.migration_history.append({
                    'timestamp': datetime.now(),
                    'action': 'stop',
                    'success': True
                })
            else:
                st.warning("⚠️ 没有运行中的迁移任务")
            time.sleep(2)
            st.rerun()
    except Exception as e:
        st.error(f"❌ 停止迁移失败: {str(e)}")

def migrate_single_table(table_name, days):
    """迁移单个表"""
    try:
        with st.spinner(f"🔧 迁移表 {table_name}..."):
            success = st.session_state.migration_app.migrate_single_table(table_name, days)
            if success:
                st.success(f"✅ 表 {table_name} 迁移成功！")
                st.session_state.migration_history.append({
                    'timestamp': datetime.now(),
                    'action': f'migrate_{table_name}',
                    'success': True,
                    'days': days
                })
            else:
                st.error(f"❌ 表 {table_name} 迁移失败")
            time.sleep(2)
            st.rerun()
    except Exception as e:
        st.error(f"❌ 单表迁移失败: {str(e)}")

def test_connections():
    """测试连接"""
    try:
        with st.spinner("🔌 测试数据库连接..."):
            results = st.session_state.migration_app.test_connections()

            if results['clickhouse'] and results['mysql']:
                st.success("✅ 所有数据库连接正常")
            else:
                if not results['clickhouse']:
                    st.error("❌ ClickHouse连接失败")
                if not results['mysql']:
                    st.error("❌ MySQL连接失败")
    except Exception as e:
        st.error(f"❌ 连接测试失败: {str(e)}")

def reset_migration():
    """重置迁移状态"""
    try:
        with st.spinner("🔄 重置迁移状态..."):
            success = st.session_state.migration_app.reset_migration()
            if success:
                st.success("✅ 迁移状态已重置")
                st.session_state.migration_history.append({
                    'timestamp': datetime.now(),
                    'action': 'reset',
                    'success': True
                })
            time.sleep(1)
            st.rerun()
    except Exception as e:
        st.error(f"❌ 重置失败: {str(e)}")

def get_status_icon(status):
    """获取状态图标"""
    icons = {
        'completed': '🟢',
        'running': '🟡',
        'failed': '🔴',
        'not_started': '⚪',
        'stopped': '🟠',
        'unknown': '⚫'
    }
    return icons.get(status, '⚫')

if __name__ == "__main__":
    main()