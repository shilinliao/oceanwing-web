import psycopg2
from sqlalchemy import create_engine, text, MetaData, Table
from urllib.parse import quote_plus
import pandas as pd
import logging

logger = logging.getLogger(__name__)

POSTGRES_CONFIG = {
    'host': 'postgre.cluster-cavkqwqmyvuw.us-west-2.rds.amazonaws.com',  # 替换为实际的PostgreSQL主机
    'port': 5432,  # PostgreSQL默认端口
    'database': 'postgres',
    'user': 'owpostgre',
    'password': 'oceanwing-pg02',
    'client_encoding': 'utf8',
    'autocommit': False,
    'connect_timeout': 30
}

TABLES = {
    'ASIN_goal_philips': 'ods_asin_goal_philips',
    'ods_category': 'ods_category',
    'ods_asin_philips': 'ods_asin_philips',
    'SI_keyword_philips': 'ods_si_keyword_philips',
    'ods_goal_vcp': 'ods_goal_vcp',
    'ods_asin_sale_goal': 'ods_asin_sale_goal',
    'ods_date_event': 'ods_date_even',
    'ods_category_dsp': 'ods_category_dsp',
    'offline_deal_sku': 'offline_deal_sku',
    'offline_roas_subcategory': 'offline_roas_subcategory',
    'offline_target_daily': 'offline_target_daily',
}

def get_engine():
    """创建PostgreSQL数据库连接"""
    password_encoded = quote_plus(POSTGRES_CONFIG['password'])
    connection_string = f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{password_encoded}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
    return create_engine(connection_string)

def get_connection():
    """获取PostgreSQL数据库连接（原生psycopg2）"""
    return psycopg2.connect(
        host=POSTGRES_CONFIG['host'],
        port=POSTGRES_CONFIG['port'],
        database=POSTGRES_CONFIG['database'],
        user=POSTGRES_CONFIG['user'],
        password=POSTGRES_CONFIG['password'],
        client_encoding=POSTGRES_CONFIG.get('client_encoding', 'utf8')
    )

def init_pg_stat_activity_log_table():
    """初始化 pg_stat_activity_log 表"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS pg_stat_activity_log (
        id SERIAL PRIMARY KEY,
        pid INTEGER,
        usename VARCHAR(100),
        application_name VARCHAR(100),
        client_addr VARCHAR(50),
        client_port INTEGER,
        backend_start TIMESTAMP,
        query_start TIMESTAMP,
        state VARCHAR(20),
        query TEXT,
        wait_event_type VARCHAR(50),
        wait_event VARCHAR(100),
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(create_table_sql))
        logger.info("pg_stat_activity_log 表初始化成功")
    except Exception as e:
        logger.error(f"初始化 pg_stat_activity_log 表失败: {e}")
        raise

def get_pg_stat_activity():
    """获取当前 PostgreSQL 活动进程，排除当前查询自身"""
    # 获取当前连接的PID用于过滤
    current_pid = None
    try:
        conn = get_connection()
        current_pid = conn.get_backend_pid()  # 使用正确的API获取后端PID
        conn.close()
    except Exception as e:
        logger.warning(f"获取当前进程PID失败: {e}")

    query = text("""
        SELECT
            pid,
            COALESCE(usename, '') as usename,
            COALESCE(application_name, '') as application_name,
            COALESCE(client_addr::text, '') as client_addr,
            client_port,
            backend_start,
            query_start,
            COALESCE(state, '') as state,
            COALESCE(query, '') as query,
            COALESCE(wait_event_type, '') as wait_event_type,
            COALESCE(wait_event, '') as wait_event
        FROM pg_stat_activity
        WHERE state IS NOT NULL AND state != 'idle'
        ORDER BY query_start
    """)

    try:
        engine = get_engine()
        with engine.begin() as conn:
            df = pd.read_sql(query, conn)

        # 过滤掉当前查询自身的进程
        if current_pid is not None:
            df = df[df['pid'] != current_pid]

        logger.debug(f"获取到 {len(df)} 条活动进程")
        return df
    except Exception as e:
        logger.error(f"查询 pg_stat_activity 失败: {e}")
        return pd.DataFrame()

def insert_pg_stat_activity_log(df: pd.DataFrame):
    """将活动进程记录插入到日志表"""
    if df.empty:
        return 0

    # 确保列名一致
    df = df.copy()
    df.columns = df.columns.str.lower()

    # 填充 None 值为空字符串
    df = df.fillna('')

    # recorded_at 使用数据库默认值，不在代码中设置
    if 'recorded_at' in df.columns:
        df = df.drop(columns=['recorded_at'])

    try:
        engine = get_engine()
        with engine.begin() as conn:
            df.to_sql(
                'pg_stat_activity_log',
                conn,
                if_exists='append',
                index=False
            )
        logger.debug(f"已插入 {len(df)} 条记录到 pg_stat_activity_log")
        return len(df)
    except Exception as e:
        logger.error(f"插入 pg_stat_activity_log 失败: {e}")
        return 0

def get_table_columns( table_name, database):
    """获取数据库表的列名"""
    try:
        query = text(f"""SELECT column_name name
FROM information_schema.columns
WHERE table_name = '{table_name}'
ORDER BY ordinal_position """)
        with get_engine().begin() as conn:
            result = pd.read_sql(query, conn)
        return result['name'].tolist() if not result.empty else []
    except Exception as e:
        print(f'获取表结构失败: {str(e)}')
        raise e

def to_postgresql_data(table_name, upload_mode, df, batch_size=1000):
    """优化的分批插入版本 - PostgreSQL适配"""
    # try:
    #     to_mysql_data_safe(table_name, upload_mode, df)
    #     return True
    # except Exception as e:
    #     print(f"安全插入失败: {e}")

    engine = get_engine()
    table_name = TABLES[table_name]

    # 将列名转为小写
    df.columns = df.columns.str.lower()
    if 'ods_date_even' in table_name :
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'date_time'})
    # 处理替换模式 - PostgreSQL使用TRUNCATE或DELETE
    if upload_mode == 'replace':
        with engine.begin() as conn:
            try:
                # PostgreSQL的TRUNCATE语法
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                print(f"已清空表 {table_name}")
            except Exception as e:
                print(f"TRUNCATE失败，使用DELETE: {e}")
                conn.rollback()
                conn.execute(text(f"DELETE FROM {table_name}"))

    # 分批插入数据
    total_rows = len(df)
    inserted = 0

    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i:i + batch_size]

        try:
            # 每个批次使用独立的事务
            with engine.begin() as conn:
                # PostgreSQL不需要设置innodb_lock_wait_timeout
                # 可以设置语句超时（可选）
                conn.execute(text("SET statement_timeout = 300000"))  # 300秒

                batch_df.to_sql(
                    table_name,
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=5000
                )

            inserted += len(batch_df)
            print(f"已插入 {inserted}/{total_rows} 行")

        except Exception as e:
            print(f"插入第{i}-{min(i + batch_size, total_rows) - 1}行时失败: {e}")
            raise

    print(f"数据上传完成，共插入 {total_rows} 行")
    return True

def to_mysql_data_safe(table_name, upload_mode, df):
    """安全的批量插入 - PostgreSQL适配"""
    engine = get_engine()
    table_name = TABLES[table_name]

    with engine.connect() as conn:
        # PostgreSQL设置语句超时
        conn.execute(text("SET statement_timeout = 300000"))

        if upload_mode == 'replace':
            try:
                # PostgreSQL TRUNCATE不需要禁用外键检查
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                print(f"已清空表: {table_name}")
            except Exception as e:
                print(f"TRUNCATE失败，使用DELETE: {e}")
                conn.rollback()
                conn.execute(text(f"DELETE FROM {table_name}"))

        # 将列名转为小写
        df.columns = df.columns.str.lower()

        # 准备插入SQL - 使用PostgreSQL的占位符%s（与MySQL相同）
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # 分批插入
        batch_size = 100
        data = [tuple(x) for x in df.itertuples(index=False, name=None)]
        total_rows = len(data)

        for i in range(0, total_rows, batch_size):
            batch_data = data[i:i + batch_size]

            try:
                # 使用executemany批量插入
                with conn.connection.cursor() as cursor:
                    cursor.executemany(sql, batch_data)
                    conn.connection.commit()

                print(f"✅ 已插入 {min(i + batch_size, total_rows)}/{total_rows} 行")

            except Exception as e:
                print(f"❌❌ 批次插入失败: {e}")
                # 尝试单行插入
                for row_data in batch_data:
                    try:
                        with conn.connection.cursor() as cursor:
                            cursor.execute(sql, row_data)
                            conn.connection.commit()
                    except Exception as single_error:
                        print(f"单行插入失败: {single_error}")
                        continue

    print(f"🎉🎉 数据上传完成，共插入 {total_rows} 行")
    return True