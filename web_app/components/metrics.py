"""自定义Streamlit组件"""
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def create_metric_card(title, value, delta=None, delta_color="normal"):
    """创建指标卡片"""
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.metric(
            label=title,
            value=value,
            delta=delta,
            delta_color=delta_color
        )


def create_progress_chart(labels, values, title="进度图表"):
    """创建进度图表"""
    fig = go.Figure(go.Bar(
        x=values,
        y=labels,
        orientation='h',
        marker=dict(
            color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4'],
            line=dict(color='#000000', width=1)
        )
    ))

    fig.update_layout(
        title=title,
        xaxis_title="进度 (%)",
        yaxis_title="表名",
        showlegend=False,
        height=300
    )

    return fig


def create_performance_gauge(value, title="性能指标"):
    """创建性能仪表盘"""
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title},
        gauge={
            'axis': {'range': [None, 2000]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 500], 'color': 'lightgray'},
                {'range': [500, 1000], 'color': 'gray'}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 1500
            }
        }
    ))

    fig.update_layout(height=200)
    return fig