"""配置验证脚本"""
from config.settings import Config


def validate_all_configurations():
    """验证所有配置"""
    print("开始验证配置...")

    # 验证基本配置
    if not Config.validate_config():
        print("❌ 基本配置验证失败")
        return False

    # 验证列映射完整性
    column_mapping = Config.get_table_columns_mapping()
    for target_table in Config.TARGET_TABLES:
        if target_table not in column_mapping:
            print(f"❌ 缺少表 {target_table} 的列映射配置")
            return False

        mapping = column_mapping[target_table]
        if len(mapping) == 0:
            print(f"❌ 表 {target_table} 的列映射为空")
            return False

        print(f"✅ 表 {target_table} 列映射配置正常 ({len(mapping)} 列)")

    # 验证迁移天数配置
    days_config = Config.get_table_migration_days()
    for target_table in Config.TARGET_TABLES:
        if target_table not in days_config:
            print(f"❌ 缺少表 {target_table} 的迁移天数配置")
            return False
        print(f"✅ 表 {target_table} 迁移天数: {days_config[target_table]} 天")

    print("🎉 所有配置验证通过！")
    return True


if __name__ == "__main__":
    validate_all_configurations()