# app.py
"""
🌊 OceanWing - 数据平台
精简稳定版
"""

import os
import sys
import warnings
warnings.filterwarnings('ignore')

# ========== 修复 Streamlit Cloud 线程问题 ==========
if sys.platform != 'win32':
    import signal
    signal.signal = lambda *args, **kwargs: None
# =================================================

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
from io import BytesIO
import base64

# 设置页面
st.set_page_config(
    page_title="OceanWing 数据平台",
    page_icon="🌊",
    layout="wide"
)

# 自定义CSS
st.markdown("""
<style>
.main {padding: 1rem;}
.stMetric {background-color: #f0f2f6; padding: 1rem; border-radius: 10px;}
</style>
""", unsafe_allow_html=True)

# 初始化会话状态
if 'data' not in st.session_state:
    st.session_state.data = None

# 侧边栏
with st.sidebar:
    st.title("🌊 OceanWing")
    st.divider()

    menu = st.radio(
        "选择功能",
        ["🏠 仪表板", "📁 数据管理", "📊 可视化", "🔧 工具", "⚙️ 设置"]
    )

    st.divider()
    st.caption(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# 主内容
if menu == "🏠 仪表板":
    st.title("📈 系统仪表板")

    # 指标卡片
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("总数据量", "1.2M", "+12%")
    with col2:
        st.metric("活跃用户", "856", "+8%")
    with col3:
        st.metric("响应时间", "145ms", "-5%")
    with col4:
        st.metric("成功率", "99.8%", "+0.2%")

    st.divider()

    # 图表
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("访问趋势")
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        visits = np.random.randint(100, 1000, size=30)
        df_visits = pd.DataFrame({'日期': dates, '访问量': visits})
        fig1 = px.line(df_visits, x='日期', y='访问量')
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("用户分布")
        df_dist = pd.DataFrame({
            '设备': ['Web', 'Mobile', 'Tablet'],
            '占比': [45, 35, 20]
        })
        fig2 = px.pie(df_dist, values='占比', names='设备')
        st.plotly_chart(fig2, use_container_width=True)

elif menu == "📁 数据管理":
    st.title("📁 数据管理")

    # 文件上传
    uploaded_file = st.file_uploader(
        "上传数据文件",
        type=['csv', 'xlsx', 'xls']
    )

    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            st.session_state.data = df
            st.success(f"✅ 文件加载成功: {uploaded_file.name}")

            # 显示信息
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("行数", len(df))
            with col2:
                st.metric("列数", len(df.columns))
            with col3:
                st.metric("文件大小", f"{uploaded_file.size/1024:.1f} KB")

            # 预览数据
            with st.expander("数据预览"):
                st.dataframe(df.head(10), use_container_width=True)

        except Exception as e:
            st.error(f"❌ 错误: {str(e)}")

    # 如果有数据，显示处理选项
    if st.session_state.data is not None:
        st.divider()
        st.subheader("数据处理")

        df = st.session_state.data

        if st.button("查看完整数据"):
            st.dataframe(df, use_container_width=True, height=400)

        if st.button("导出为CSV"):
            csv = df.to_csv(index=False)
            st.download_button(
                "📥 下载CSV",
                csv,
                "data_export.csv",
                "text/csv"
            )

elif menu == "📊 可视化":
    st.title("📊 数据可视化")

    if st.session_state.data is None:
        st.info("请先上传数据")
    else:
        df = st.session_state.data

        col1, col2 = st.columns(2)

        with col1:
            chart_type = st.selectbox(
                "图表类型",
                ["柱状图", "折线图", "散点图", "饼图"]
            )

            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if numeric_cols:
                x_col = st.selectbox("X轴", df.columns)
                y_col = st.selectbox("Y轴", numeric_cols,
                                   index=min(0, len(numeric_cols)-1))

        with col2:
            st.subheader("可视化结果")

            try:
                if chart_type == "柱状图":
                    fig = px.bar(df, x=x_col, y=y_col)
                elif chart_type == "折线图":
                    fig = px.line(df, x=x_col, y=y_col)
                elif chart_type == "散点图":
                    fig = px.scatter(df, x=x_col, y=y_col)
                elif chart_type == "饼图":
                    # 对饼图做聚合
                    pie_data = df[x_col].value_counts().reset_index()
                    pie_data.columns = [x_col, 'count']
                    fig = px.pie(pie_data, names=x_col, values='count')

                st.plotly_chart(fig, use_container_width=True)

            except Exception as e:
                st.error(f"图表生成失败: {str(e)}")

elif menu == "🔧 工具":
    st.title("🔧 实用工具")

    tab1, tab2, tab3 = st.tabs(["格式转换", "数据清洗", "批量处理"])

    with tab1:
        st.subheader("文件格式转换")
        st.info("上传文件后选择目标格式")

    with tab2:
        st.subheader("数据清洗工具")
        if st.session_state.data is not None:
            df = st.session_state.data

            if st.button("删除重复行"):
                original_len = len(df)
                df = df.drop_duplicates()
                st.session_state.data = df
                st.success(f"删除了 {original_len - len(df)} 个重复行")
                st.rerun()

            if st.button("重置索引"):
                df = df.reset_index(drop=True)
                st.session_state.data = df
                st.success("索引已重置")
                st.rerun()

    with tab3:
        st.subheader("批量处理")
        st.write("批量处理功能开发中...")

elif menu == "⚙️ 设置":
    st.title("⚙️ 系统设置")

    st.subheader("应用信息")
    st.write(f"Streamlit 版本: {st.__version__}")
    st.write(f"Pandas 版本: {pd.__version__}")
    st.write(f"Python 版本: {sys.version.split()[0]}")

    st.divider()

    st.subheader("系统操作")
    if st.button("🔄 重新加载", use_container_width=True):
        st.rerun()

    if st.button("🧹 清除缓存", use_container_width=True):
        st.session_state.clear()
        st.cache_data.clear()
        st.success("缓存已清除")
        st.rerun()

# 页脚
st.divider()
st.caption(f"🌊 OceanWing | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")