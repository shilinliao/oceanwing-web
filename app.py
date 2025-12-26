import streamlit as st
from datetime import datetime

# 设置页面标题和图标
st.set_page_config(
    page_title="我的 Streamlit 应用",
    page_icon=":rocket:",
    layout="wide"
)

# 应用标题
st.title("欢迎来到我的 Streamlit 应用")
st.write(f"今天是 {datetime.now().strftime('%Y年%m月%d日')}")

# 侧边栏
with st.sidebar:
    st.header("导航")
    menu_option = st.radio(
        "选择功能",
        ["主页", "数据分析", "可视化", "设置"]
    )

    st.divider()
    st.write("关于")
    st.caption("这是一个使用 Streamlit 创建的示例应用")

# 主内容区
if menu_option == "主页":
    st.subheader("主页")
    st.write("这里是应用的主页内容。")

    # 示例按钮
    if st.button("点击我"):
        st.success("按钮被点击了！")

    # 示例滑块
    value = st.slider("选择一个数值", 0, 100, 50)
    st.write(f"你选择了: {value}")

elif menu_option == "数据分析":
    st.subheader("数据分析")
    st.write("这里可以展示数据分析功能。")

    # 上传文件示例
    uploaded_file = st.file_uploader("上传CSV文件", type=["csv"])
    if uploaded_file is not None:
        st.write("文件上传成功！")
        # 这里可以添加处理文件的代码

elif menu_option == "可视化":
    st.subheader("可视化")
    st.write("这里可以展示数据可视化。")

    # 示例图表
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt

    chart_data = pd.DataFrame(
        np.random.randn(20, 3),
        columns=['A', 'B', 'C']
    )

    st.line_chart(chart_data)

elif menu_option == "设置":
    st.subheader("设置")
    st.write("这里可以配置应用设置。")

    # 示例设置选项
    dark_mode = st.checkbox("启用暗黑模式")
    if dark_mode:
        st.write("暗黑模式已启用")
    else:
        st.write("亮色模式已启用")

# 页脚
st.divider()
st.caption("© 2025 OceanWing Web 应用")