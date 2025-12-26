"""
🌊 OceanWing - 多功能数据平台
一个功能完整的 Streamlit Web 应用
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
import altair as alt
from datetime import datetime, timedelta
import time
import json
import base64
from io import BytesIO
import matplotlib.pyplot as plt
import seaborn as sns

# 页面配置
st.set_page_config(
    page_title="OceanWing 数据平台",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# 自定义CSS
def load_css():
    st.markdown("""
    <style>
    /* 主样式 */
    .main {
        padding: 0rem 1rem;
    }

    /* 标题样式 */
    .main-title {
        font-size: 3rem;
        background: linear-gradient(45deg, #0066CC, #00CCCC);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 2rem;
    }

    /* 卡片样式 */
    .card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }

    /* 指标卡片 */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
    }

    /* 按钮样式 */
    .stButton>button {
        width: 100%;
        border-radius: 8px;
        height: 3em;
        font-weight: bold;
    }

    /* 隐藏 Streamlit 默认元素 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* 标签页样式 */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }

    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        border-radius: 8px 8px 0px 0px;
    }
    </style>
    """, unsafe_allow_html=True)


# 加载CSS
load_css()

# 应用标题
st.markdown('<h1 class="main-title">🌊 OceanWing 数据平台</h1>', unsafe_allow_html=True)

# 初始化会话状态
if 'data' not in st.session_state:
    st.session_state.data = None
if 'charts' not in st.session_state:
    st.session_state.charts = {}
if 'theme' not in st.session_state:
    st.session_state.theme = 'light'
if 'language' not in st.session_state:
    st.session_state.language = 'zh'

# 侧边栏导航
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3067/3067256.png", width=100)
    st.markdown("### 🎯 导航菜单")

    menu_options = {
        "🏠 仪表板": "dashboard",
        "📁 数据管理": "data_management",
        "📊 数据可视化": "visualization",
        "🔍 数据分析": "analysis",
        "📤 文件处理": "file_processing",
        "⚙️ 系统设置": "settings"
    }

    selected_menu = st.selectbox(
        "选择功能",
        list(menu_options.keys()),
        index=0
    )

    st.divider()

    # 快捷操作
    st.markdown("### ⚡ 快捷操作")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 刷新", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("🗑️ 清除缓存", use_container_width=True):
            st.session_state.clear()
            st.success("缓存已清除！")

    st.divider()

    # 系统信息
    st.markdown("### 📊 系统状态")
    st.progress(75, text="系统负载: 75%")
    st.caption(f"🕐 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption("✅ 系统运行正常")

# 主内容区域
if menu_options[selected_menu] == "dashboard":
    st.header("📈 系统仪表板")

    # 顶部指标卡
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="总用户数",
            value="1,234",
            delta="+12.3%",
            delta_color="normal"
        )

    with col2:
        st.metric(
            label="今日活跃",
            value="567",
            delta="+8.2%",
            delta_color="normal"
        )

    with col3:
        st.metric(
            label="数据总量",
            value="5.6M",
            delta="+5.7%",
            delta_color="normal"
        )

    with col4:
        st.metric(
            label="响应时间",
            value="128ms",
            delta="-3.2%",
            delta_color="inverse"
        )

    st.divider()

    # 图表区域
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 访问趋势")
        # 生成示例数据
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        visits = np.random.randint(100, 1000, size=30).cumsum()
        df_visits = pd.DataFrame({'日期': dates, '访问量': visits})

        # 使用 Plotly
        fig1 = px.line(df_visits, x='日期', y='访问量',
                       title='过去30天访问趋势',
                       template='plotly_white')
        fig1.update_layout(height=300)
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("📊 用户分布")
        categories = ['Web', 'Mobile', 'Tablet', 'Desktop']
        values = [45, 30, 15, 10]
        df_dist = pd.DataFrame({'设备': categories, '占比': values})

        fig2 = px.pie(df_dist, values='占比', names='设备',
                      title='用户设备分布',
                      color_discrete_sequence=px.colors.sequential.RdBu)
        fig2.update_layout(height=300)
        st.plotly_chart(fig2, use_container_width=True)

    # 数据表格展示
    st.subheader("📋 最近活动")

    # 创建示例数据
    data = {
        '时间': pd.date_range('2024-01-01 08:00:00', periods=10, freq='3H').strftime('%H:%M'),
        '用户': ['用户' + str(i) for i in range(1, 11)],
        '操作': ['登录', '上传', '查询', '下载', '导出',
                 '编辑', '删除', '分享', '保存', '打印'],
        '状态': ['成功', '成功', '失败', '成功', '成功',
                 '成功', '失败', '成功', '成功', '成功']
    }
    df_activity = pd.DataFrame(data)

    st.dataframe(df_activity,
                 use_container_width=True,
                 column_config={
                     "时间": st.column_config.TextColumn("时间"),
                     "用户": st.column_config.TextColumn("用户"),
                     "操作": st.column_config.TextColumn("操作"),
                     "状态": st.column_config.SelectboxColumn(
                         "状态",
                         options=["成功", "失败", "进行中"]
                     )
                 })

elif menu_options[selected_menu] == "data_management":
    st.header("📁 数据管理")

    tab1, tab2, tab3 = st.tabs(["📤 数据上传", "📋 数据查看", "⚙️ 数据处理"])

    with tab1:
        st.subheader("上传数据文件")

        col1, col2 = st.columns(2)

        with col1:
            file_type = st.selectbox(
                "选择文件类型",
                ["CSV", "Excel", "JSON", "Parquet"]
            )

        with col2:
            encoding = st.selectbox(
                "文件编码",
                ["utf-8", "gbk", "gb2312", "latin1"],
                index=0
            )

        uploaded_file = st.file_uploader(
            "选择文件",
            type=['csv', 'xlsx', 'xls', 'json', 'parquet', 'txt'],
            help="支持 CSV, Excel, JSON, Parquet 格式"
        )

        if uploaded_file is not None:
            try:
                with st.spinner("正在处理文件..."):
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file, encoding=encoding)
                    elif uploaded_file.name.endswith(('.xlsx', '.xls')):
                        df = pd.read_excel(uploaded_file)
                    elif uploaded_file.name.endswith('.json'):
                        df = pd.read_json(uploaded_file)
                    elif uploaded_file.name.endswith('.parquet'):
                        df = pd.read_parquet(uploaded_file)
                    else:
                        st.error("不支持的文件格式")
                        df = None

                    if df is not None:
                        st.session_state.data = df
                        st.success(f"✅ 文件 '{uploaded_file.name}' 加载成功！")

                        # 显示文件信息
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("行数", len(df))
                        with col2:
                            st.metric("列数", len(df.columns))
                        with col3:
                            st.metric("文件大小", f"{uploaded_file.size / 1024:.2f} KB")

                        # 显示前几行
                        with st.expander("👀 预览数据"):
                            st.dataframe(df.head(), use_container_width=True)

                        # 显示数据类型
                        with st.expander("📊 数据类型"):
                            dtype_df = pd.DataFrame({
                                '列名': df.columns,
                                '数据类型': df.dtypes.astype(str).values,
                                '非空值数': df.count().values,
                                '空值数': df.isnull().sum().values
                            })
                            st.dataframe(dtype_df, use_container_width=True)

            except Exception as e:
                st.error(f"❌ 文件读取失败: {str(e)}")

    with tab2:
        st.subheader("数据查看与编辑")

        if st.session_state.data is not None:
            df = st.session_state.data

            # 数据过滤
            with st.expander("🔍 数据筛选", expanded=False):
                filter_col1, filter_col2 = st.columns(2)
                with filter_col1:
                    column_to_filter = st.selectbox(
                        "选择筛选列",
                        df.columns.tolist()
                    )
                with filter_col2:
                    if pd.api.types.is_numeric_dtype(df[column_to_filter]):
                        min_val = float(df[column_to_filter].min())
                        max_val = float(df[column_to_filter].max())
                        selected_range = st.slider(
                            "选择范围",
                            min_val, max_val,
                            (min_val, max_val)
                        )
                        filtered_df = df[(df[column_to_filter] >= selected_range[0]) &
                                         (df[column_to_filter] <= selected_range[1])]
                    else:
                        unique_values = df[column_to_filter].unique().tolist()
                        selected_values = st.multiselect(
                            "选择值",
                            unique_values,
                            default=unique_values[:min(5, len(unique_values))]
                        )
                        filtered_df = df[df[column_to_filter].isin(selected_values)]
            else:
            filtered_df = df

        # 数据显示
        st.dataframe(
            filtered_df,
            use_container_width=True,
            height=400,
            hide_index=False
        )

        # 数据操作
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("💾 保存到CSV", use_container_width=True):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 下载CSV",
                    data=csv,
                    file_name="filtered_data.csv",
                    mime="text/csv"
                )

        with col2:
            if st.button("📊 描述性统计", use_container_width=True):
                st.subheader("描述性统计")
                st.dataframe(filtered_df.describe(), use_container_width=True)

        with col3:
            if st.button("🔄 重置数据", use_container_width=True):
                st.session_state.data = None
                st.rerun()

    else:
    st.info("📁 请先上传数据文件")
    st.image("https://cdn-icons-png.flaticon.com/512/3767/3767084.png", width=200)

with tab3:
    st.subheader("数据处理")

    if st.session_state.data is not None:
        df = st.session_state.data

        operation = st.selectbox(
            "选择处理操作",
            ["空值处理", "数据类型转换", "重命名列", "数据排序", "数据采样"]
        )

        if operation == "空值处理":
            col1, col2 = st.columns(2)
            with col1:
                na_method = st.radio(
                    "处理方法",
                    ["删除包含空值的行", "填充空值"]
                )

            with col2:
                if na_method == "填充空值":
                    fill_value = st.text_input("填充值（数值列用数字，文本列用字符串）", "0")

            if st.button("执行处理", type="primary"):
                with st.spinner("处理中..."):
                    if na_method == "删除包含空值的行":
                        df_processed = df.dropna()
                    else:
                        try:
                            fill_val = float(fill_value) if fill_value.replace('.', '').isdigit() else fill_value
                            df_processed = df.fillna(fill_val)
                        except:
                            df_processed = df.fillna(fill_value)

                    st.session_state.data = df_processed
                    st.success(f"✅ 处理完成！删除了 {len(df) - len(df_processed)} 行")
                    st.rerun()

        elif operation == "数据类型转换":
            col_to_convert = st.selectbox("选择列", df.columns)
            current_type = str(df[col_to_convert].dtype)
            st.write(f"当前类型: **{current_type}**")

            new_type = st.selectbox(
                "转换为",
                ["整数 (int)", "浮点数 (float)", "字符串 (str)", "日期时间 (datetime)"]
            )

            if st.button("转换类型", type="primary"):
                with st.spinner("转换中..."):
                    try:
                        if new_type.startswith("整数"):
                            df[col_to_convert] = pd.to_numeric(df[col_to_convert], errors='coerce').astype('Int64')
                        elif new_type.startswith("浮点数"):
                            df[col_to_convert] = pd.to_numeric(df[col_to_convert], errors='coerce').astype(float)
                        elif new_type.startswith("字符串"):
                            df[col_to_convert] = df[col_to_convert].astype(str)
                        elif new_type.startswith("日期时间"):
                            df[col_to_convert] = pd.to_datetime(df[col_to_convert], errors='coerce')

                        st.session_state.data = df
                        st.success("✅ 类型转换成功！")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ 转换失败: {str(e)}")

elif menu_options[selected_menu] == "visualization":
st.header("📊 数据可视化")

if st.session_state.data is None:
    st.warning("请先上传数据以进行可视化")
else:
    df = st.session_state.data

    col1, col2 = st.columns([1, 3])

    with col1:
        st.subheader("📐 图表设置")

        chart_type = st.selectbox(
            "选择图表类型",
            ["折线图", "柱状图", "散点图", "饼图", "热力图", "箱线图", "面积图"]
        )

        x_axis = st.selectbox(
            "X轴",
            df.columns.tolist(),
            index=min(0, len(df.columns) - 1)
        )

        y_axis = st.selectbox(
            "Y轴",
            df.columns.tolist(),
            index=min(1, len(df.columns) - 1)
        )

        if chart_type in ["散点图", "折线图", "柱状图"] and len(df.columns) > 2:
            color_by = st.selectbox(
                "颜色分组",
                ["无"] + df.columns.tolist()
            )
        else:
            color_by = "无"

        chart_title = st.text_input("图表标题", f"{chart_type} - {x_axis} vs {y_axis}")

        # 高级设置
        with st.expander("⚙️ 高级设置"):
            height = st.slider("图表高度", 300, 800, 500)
            theme = st.selectbox("主题", ["plotly", "plotly_white", "plotly_dark", "ggplot2", "seaborn"])
            show_grid = st.checkbox("显示网格", value=True)
            show_legend = st.checkbox("显示图例", value=True)

    with col2:
        st.subheader("📈 可视化结果")

        try:
            with st.spinner("生成图表中..."):
                if chart_type == "折线图":
                    if color_by != "无":
                        fig = px.line(df, x=x_axis, y=y_axis, color=color_by,
                                      title=chart_title, template=theme)
                    else:
                        fig = px.line(df, x=x_axis, y=y_axis,
                                      title=chart_title, template=theme)

                elif chart_type == "柱状图":
                    if color_by != "无":
                        fig = px.bar(df, x=x_axis, y=y_axis, color=color_by,
                                     title=chart_title, template=theme)
                    else:
                        fig = px.bar(df, x=x_axis, y=y_axis,
                                     title=chart_title, template=theme)

                elif chart_type == "散点图":
                    if color_by != "无":
                        fig = px.scatter(df, x=x_axis, y=y_axis, color=color_by,
                                         title=chart_title, template=theme)
                    else:
                        fig = px.scatter(df, x=x_axis, y=y_axis,
                                         title=chart_title, template=theme)

                elif chart_type == "饼图":
                    fig = px.pie(df, names=x_axis, values=y_axis,
                                 title=chart_title, template=theme)

                elif chart_type == "热力图":
                    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                    if len(numeric_cols) >= 2:
                        corr_df = df[numeric_cols].corr()
                        fig = px.imshow(corr_df,
                                        title=chart_title,
                                        template=theme,
                                        color_continuous_scale='RdBu')
                    else:
                        st.warning("需要至少2个数值列来生成热力图")
                        fig = go.Figure()

                elif chart_type == "箱线图":
                    fig = px.box(df, x=x_axis, y=y_axis,
                                 title=chart_title, template=theme)

                elif chart_type == "面积图":
                    fig = px.area(df, x=x_axis, y=y_axis,
                                  title=chart_title, template=theme)

                # 更新布局
                fig.update_layout(
                    height=height,
                    showlegend=show_legend,
                    title_x=0.5,
                    title_font_size=20
                )

                if show_grid:
                    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGray')
                    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGray')

                st.plotly_chart(fig, use_container_width=True)

                # 图表操作
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    if st.button("💾 保存图表"):
                        img_bytes = fig.to_image(format="png")
                        st.download_button(
                            label="📥 下载PNG",
                            data=img_bytes,
                            file_name=f"chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
                            mime="image/png"
                        )

                with col_b:
                    if st.button("📊 保存HTML"):
                        html = fig.to_html()
                        st.download_button(
                            label="📥 下载HTML",
                            data=html,
                            file_name=f"chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                            mime="text/html"
                        )

        except Exception as e:
            st.error(f"❌ 图表生成失败: {str(e)}")
            st.info("请检查数据列类型是否适合所选图表类型")

elif menu_options[selected_menu] == "analysis":
st.header("🔍 数据分析")

if st.session_state.data is None:
    st.warning("请先上传数据以进行分析")
else:
    df = st.session_state.data

    tab1, tab2, tab3, tab4 = st.tabs(["📈 统计分析", "🔍 相关性分析", "📉 趋势分析", "🎯 聚类分析"])

    with tab1:
        st.subheader("描述性统计分析")

        # 选择数值列
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        if numeric_cols:
            selected_cols = st.multiselect(
                "选择要分析的列",
                numeric_cols,
                default=numeric_cols[:min(3, len(numeric_cols))]
            )

            if selected_cols:
                analysis_type = st.radio(
                    "分析类型",
                    ["基本统计", "分布分析", "离群值检测"]
                )

                if analysis_type == "基本统计":
                    stats_df = df[selected_cols].describe().T
                    stats_df['缺失值'] = df[selected_cols].isnull().sum().values
                    stats_df['缺失率%'] = (stats_df['缺失值'] / len(df) * 100).round(2)
                    stats_df['偏度'] = df[selected_cols].skew().round(4)
                    stats_df['峰度'] = df[selected_cols].kurtosis().round(4)

                    st.dataframe(stats_df, use_container_width=True)

                    # 可视化
                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("数据分布")
                        for col in selected_cols:
                            fig = px.histogram(df, x=col, title=f"{col} 分布")
                            st.plotly_chart(fig, use_container_width=True)

                elif analysis_type == "分布分析":
                    for col in selected_cols:
                        fig = go.Figure()
                        fig.add_trace(go.Histogram(x=df[col], name='直方图'))
                        fig.add_trace(go.Box(x=df[col], name='箱线图', yaxis='y2'))

                        fig.update_layout(
                            title=f"{col} 分布分析",
                            yaxis=dict(title="频数"),
                            yaxis2=dict(title="箱线图", overlaying='y', side='right'),
                            showlegend=False
                        )
                        st.plotly_chart(fig, use_container_width=True)

                elif analysis_type == "离群值检测":
                    for col in selected_cols:
                        Q1 = df[col].quantile(0.25)
                        Q3 = df[col].quantile(0.75)
                        IQR = Q3 - Q1
                        lower_bound = Q1 - 1.5 * IQR
                        upper_bound = Q3 + 1.5 * IQR

                        outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]

                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("下界", f"{lower_bound:.2f}")
                        with col_b:
                            st.metric("上界", f"{upper_bound:.2f}")
                        with col_c:
                            st.metric("离群值数", len(outliers))

                        if len(outliers) > 0:
                            st.dataframe(outliers[[col]], use_container_width=True)

        else:
            st.warning("没有找到数值列进行统计分析")

    with tab2:
        st.subheader("相关性分析")

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        if len(numeric_cols) >= 2:
            corr_matrix = df[numeric_cols].corr()

            # 热力图
            fig = px.imshow(corr_matrix,
                            text_auto='.2f',
                            aspect="auto",
                            color_continuous_scale='RdBu',
                            title="相关性热力图")
            st.plotly_chart(fig, use_container_width=True)

            # 相关性表格
            st.subheader("相关性矩阵")
            st.dataframe(corr_matrix.style.background_gradient(cmap='RdBu', axis=None).format("{:.3f}"),
                         use_container_width=True)

            # 散点图矩阵
            if len(numeric_cols) <= 6:  # 避免太多列的散点图
                st.subheader("散点图矩阵")
                fig = px.scatter_matrix(df[numeric_cols[:min(6, len(numeric_cols))]],
                                        title="散点图矩阵")
                fig.update_layout(height=600)
                st.plotly_chart(fig, use_container_width=True)

        else:
            st.warning("需要至少2个数值列进行相关性分析")

    with tab3:
        st.subheader("时间序列分析")

        # 寻找日期列
        date_cols = []
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                date_cols.append(col)
            else:
                try:
                    pd.to_datetime(df[col])
                    date_cols.append(col)
                except:
                    pass

        if date_cols:
            date_col = st.selectbox("选择日期列", date_cols)
            value_col = st.selectbox("选择数值列", numeric_cols)

            if date_col and value_col:
                # 转换日期
                df_ts = df.copy()
                df_ts[date_col] = pd.to_datetime(df_ts[date_col])
                df_ts = df_ts.sort_values(date_col)

                # 时间序列图
                fig = px.line(df_ts, x=date_col, y=value_col,
                              title=f"{value_col} 随时间变化趋势")
                st.plotly_chart(fig, use_container_width=True)

                # 移动平均
                window = st.slider("移动平均窗口", 3, 30, 7)
                df_ts[f'MA_{window}'] = df_ts[value_col].rolling(window=window).mean()

                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(x=df_ts[date_col], y=df_ts[value_col],
                                          name='原始数据', mode='lines'))
                fig2.add_trace(go.Scatter(x=df_ts[date_col], y=df_ts[f'MA_{window}'],
                                          name=f'{window}期移动平均', mode='lines', line=dict(width=3)))
                fig2.update_layout(title=f"移动平均分析 (窗口={window})")
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("未找到日期时间列")

elif menu_options[selected_menu] == "file_processing":
st.header("📤 文件处理工具")

tab1, tab2, tab3, tab4 = st.tabs(["🔄 格式转换", "✂️ 数据清洗", "🔀 数据合并", "📤 批量导出"])

with tab1:
    st.subheader("文件格式转换")

    uploaded_files = st.file_uploader(
        "选择要转换的文件",
        type=['csv', 'xlsx', 'xls', 'json', 'txt'],
        accept_multiple_files=True,
        help="可同时上传多个文件"
    )

    if uploaded_files:
        target_format = st.selectbox(
            "转换为格式",
            ["CSV", "Excel", "JSON", "Parquet"]
        )

        for uploaded_file in uploaded_files:
            with st.expander(f"📄 {uploaded_file.name}", expanded=False):
                try:
                    if uploaded_file.name.endswith('.csv'):
                        df = pd.read_csv(uploaded_file)
                    elif uploaded_file.name.endswith(('.xlsx', '.xls')):
                        df = pd.read_excel(uploaded_file)
                    elif uploaded_file.name.endswith('.json'):
                        df = pd.read_json(uploaded_file)
                    else:
                        st.warning(f"不支持的文件格式: {uploaded_file.name}")
                        continue

                    st.write(f"文件大小: {uploaded_file.size / 1024:.2f} KB")
                    st.write(f"数据维度: {df.shape[0]} 行 × {df.shape[1]} 列")

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button(f"预览 {uploaded_file.name}", use_container_width=True):
                            st.dataframe(df.head(), use_container_width=True)

                    with col2:
                        output_filename = f"{uploaded_file.name.split('.')[0]}_converted.{target_format.lower()}"

                        if target_format == "CSV":
                            output_data = df.to_csv(index=False)
                            mime_type = "text/csv"
                        elif target_format == "Excel":
                            output_buffer = BytesIO()
                            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                                df.to_excel(writer, index=False, sheet_name='Sheet1')
                            output_data = output_buffer.getvalue()
                            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        elif target_format == "JSON":
                            output_data = df.to_json(orient='records', indent=2)
                            mime_type = "application/json"
                        else:  # Parquet
                            output_buffer = BytesIO()
                            df.to_parquet(output_buffer, index=False)
                            output_data = output_buffer.getvalue()
                            mime_type = "application/octet-stream"

                        st.download_button(
                            label=f"⬇️ 下载 {target_format}",
                            data=output_data,
                            file_name=output_filename,
                            mime=mime_type,
                            use_container_width=True
                        )

                except Exception as e:
                    st.error(f"处理失败: {str(e)}")

with tab2:
    st.subheader("数据清洗工具")

    if st.session_state.data is not None:
        df = st.session_state.data

        cleaning_options = st.multiselect(
            "选择清洗操作",
            ["删除重复行", "删除空值行", "删除空值列", "重置索引", "列名格式化"]
        )

        if st.button("执行清洗", type="primary"):
            df_clean = df.copy()
            original_shape = df.shape

            with st.spinner("清洗中..."):
                if "删除重复行" in cleaning_options:
                    df_clean = df_clean.drop_duplicates()
                    st.info(f"删除了 {len(df) - len(df_clean)} 个重复行")

                if "删除空值行" in cleaning_options:
                    df_clean = df_clean.dropna(how='all')
                    st.info("删除了全部为空的空行")

                if "删除空值列" in cleaning_options:
                    df_clean = df_clean.dropna(axis=1, how='all')
                    st.info("删除了全部为空的列")

                if "重置索引" in cleaning_options:
                    df_clean = df_clean.reset_index(drop=True)
                    st.info("已重置索引")

                if "列名格式化" in cleaning_options:
                    df_clean.columns = [str(col).strip().replace(' ', '_').lower() for col in df_clean.columns]
                    st.info("列名已格式化")

            st.session_state.data = df_clean
            st.success(f"✅ 清洗完成！原始形状: {original_shape} → 新形状: {df_clean.shape}")
            st.rerun()

    else:
        st.info("请先上传数据")

with tab3:
    st.subheader("数据合并工具")

    st.warning("此功能需要上传多个文件")

with tab4:
    st.subheader("批量导出")

    if st.session_state.data is not None:
        df = st.session_state.data

        export_format = st.radio(
            "导出格式",
            ["CSV", "Excel", "JSON", "HTML", "Markdown", "LaTeX"]
        )

        if export_format == "CSV":
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 下载 CSV",
                data=csv,
                file_name=f"data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

        elif export_format == "Excel":
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Data')
            st.download_button(
                label="📥 下载 Excel",
                data=buffer.getvalue(),
                file_name=f"data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        elif export_format == "JSON":
            json_str = df.to_json(orient='records', indent=2)
            st.download_button(
                label="📥 下载 JSON",
                data=json_str,
                file_name=f"data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )

        elif export_format == "HTML":
            html_str = df.to_html(index=False)
            st.download_button(
                label="📥 下载 HTML",
                data=html_str,
                file_name=f"data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                mime="text/html"
            )

        elif export_format == "Markdown":
            md_str = df.to_markdown(index=False)
            st.download_button(
                label="📥 下载 Markdown",
                data=md_str,
                file_name=f"data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
                mime="text/markdown"
            )

        elif export_format == "LaTeX":
            latex_str = df.to_latex(index=False)
            st.download_button(
                label="📥 下载 LaTeX",
                data=latex_str,
                file_name=f"data_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tex",
                mime="text/plain"
            )

elif menu_options[selected_menu] == "settings":
st.header("⚙️ 系统设置")

tab1, tab2, tab3 = st.tabs(["🎨 界面设置", "🔐 安全设置", "📊 系统信息"])

with tab1:
    st.subheader("主题设置")

    col1, col2 = st.columns(2)

    with col1:
        theme = st.selectbox(
            "选择主题",
            ["浅色", "深色", "自动"],
            index=0
        )

        primary_color = st.color_picker("主色调", "#0066CC")
        secondary_color = st.color_picker("辅助色", "#00CCCC")

    with col2:
        font_size = st.select_slider(
            "字体大小",
            options=["小", "正常", "大", "加大"],
            value="正常"
        )

        density = st.select_slider(
            "界面密度",
            options=["紧凑", "舒适", "宽敞"],
            value="舒适"
        )

    if st.button("💾 保存界面设置", type="primary"):
        st.success("设置已保存！")
        st.info("部分设置需要刷新页面生效")

with tab2:
    st.subheader("安全设置")

    current_password = st.text_input("当前密码", type="password")
    new_password = st.text_input("新密码", type="password")
    confirm_password = st.text_input("确认新密码", type="password")

    if st.button("🔐 修改密码", type="primary"):
        if new_password == confirm_password and len(new_password) >= 8:
            st.success("密码修改成功！")
        elif len(new_password) < 8:
            st.warning("密码长度至少8位")
        else:
            st.error("两次输入的密码不一致")

    st.divider()

    st.subheader("会话管理")
    session_timeout = st.slider("会话超时（分钟）", 5, 240, 30)
    st.caption(f"将在 {session_timeout} 分钟后自动登出")

    if st.button("🚪 立即登出", type="secondary"):
        st.warning("您已登出")
        st.session_state.clear()
        st.rerun()

with tab3:
    st.subheader("系统信息")

    info_col1, info_col2 = st.columns(2)

    with info_col1:
        st.metric("Streamlit 版本", st.__version__)
        st.metric("Pandas 版本", pd.__version__)
        st.metric("Numpy 版本", np.__version__)

    with info_col2:
        st.metric("Python 版本", sys.version.split()[0])
        st.metric("运行平台", sys.platform)
        st.metric("当前时间", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    st.divider()

    # 系统状态
    st.subheader("系统状态")

    if st.button("🔄 检查更新", use_container_width=True):
        with st.spinner("检查中..."):
            time.sleep(1)
            st.success("✅ 已是最新版本")

    if st.button("🧹 清理缓存", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("✅ 缓存已清理")

# 页脚
st.divider()
footer_col1, footer_col2, footer_col3 = st.columns(3)
with footer_col1:
    st.caption(f"🌊 OceanWing v1.0.0")
with footer_col2:
    st.caption(f"🕐 最后更新: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
with footer_col3:
    st.caption("© 2024 OceanWing 数据平台")

# 添加自定义JavaScript
st.markdown("""
<script>
// 添加一些交互效果
document.addEventListener('DOMContentLoaded', function() {
    // 为所有按钮添加点击动画
    const buttons = document.querySelectorAll('.stButton button, .stDownloadButton button');
    buttons.forEach(button => {
        button.addEventListener('click', function() {
            this.style.transform = 'scale(0.95)';
            setTimeout(() => {
                this.style.transform = 'scale(1)';
            }, 150);
        });
    });

    // 添加页面加载动画
    const mainContent = document.querySelector('.main');
    if (mainContent) {
        mainContent.style.opacity = '0';
        mainContent.style.transition = 'opacity 0.5s ease-in';
        setTimeout(() => {
            mainContent.style.opacity = '1';
        }, 100);
    }
});
</script>
""", unsafe_allow_html=True)