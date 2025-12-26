# 在 TaskProcessor 类中添加以下方法
def _get_time_field(self, target_table: str) -> str:
    """根据目标表获取时间字段"""
    time_fields = {
        "ods_campain": "Time",
        "ods_campaign_dsp": "TimeColumn",
        "ods_aws_asin_philips": "Time", 
        "ods_query": "Time"
    }
    return time_fields.get(target_table, "Time")

def _get_filter_condition(self, target_table: str) -> str:
    """根据目标表获取过滤条件"""
    filter_conditions = {
        "ods_campain": " AND `Profile Name` LIKE 'Philips%'",
        "ods_campaign_dsp": "",  # DSP表可能不需要Philips过滤
        "ods_aws_asin_philips": " AND `Profile_Name` LIKE 'Philips%'",
        "ods_query": " AND `Profile Name` LIKE 'Philips%'"
    }
    return filter_conditions.get(target_table, "")

def _build_select_sql(self, task: MigrationTask) -> str:
    """构建SELECT查询SQL（使用配置）"""
    select_fields = ", ".join([f"`{col.get_name()}`" for col in task.columns])
    select_sql = f"SELECT {select_fields} FROM {task.source_table}"

    # 添加时间过滤条件
    time_field = self._get_time_field(task.target_table)
    next_date_str = self._format_next_date(task.day)
    
    select_sql += f" WHERE `{time_field}` >= '{task.date_str}'"
    select_sql += f" AND `{time_field}` < '{next_date_str}'"

    # 添加其他过滤条件
    filter_condition = self._get_filter_condition(task.target_table)
    select_sql += filter_condition

    select_sql += f" ORDER BY {time_field}"
    return select_sql