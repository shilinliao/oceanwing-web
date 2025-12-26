"""
最简单的 Streamlit 应用
保证能部署成功
"""

import streamlit as st
from datetime import datetime

st.set_page_config(
    page_title="OceanWing",
    page_icon="🌊",
    layout="wide"
)

st.title("🌊 OceanWing 数据平台")
st.write("应用部署成功！")

st.markdown("---")

# 基本信息
col1, col2 = st.columns(2)
with col1:
    st.info(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col2:
    st.success(f"Streamlit 版本: {st.__version__}")

# 测试功能
st.markdown("## 🎯 功能测试")

# 文件上传测试
uploaded_file = st.file_uploader("测试文件上传", type=['txt', 'csv'])
if uploaded_file is not None:
    st.success(f"文件上传成功: {uploaded_file.name}")
    st.info(f"文件大小: {uploaded_file.size} 字节")

# 数据展示
st.markdown("## 📊 示例数据")
import pandas as pd
data = {
    'ID': [1, 2, 3, 4, 5],
    'Name': ['产品A', '产品B', '产品C', '产品D', '产品E'],
    'Price': [100, 200, 150, 300, 250]
}
df = pd.DataFrame(data)
st.dataframe(df, use_container_width=True)

# 图表测试
st.markdown("## 📈 示例图表")
st.bar_chart(df.set_index('Name')['Price'])

# 交互测试
st.markdown("## 🎮 交互测试")
if st.button("点击测试"):
    st.balloons()
    st.success("按钮点击成功！")

slider_value = st.slider("选择一个数值", 0, 100, 50)
st.write(f"选择的数值: {slider_value}")

# 页脚
st.markdown("---")
st.caption("© 2024 OceanWing | 部署成功")