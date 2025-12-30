"""
    Company:深圳遨腾云创科技有限公司
\n  Name：YYZ
\n  Title: 建表
\n  Description: 读取excel建表
\n  (1) 读取excel文件，将列名转拼音（数字开头加vc_，全小写）
\n  (2) 自动判断字段长度（>255用TEXT，否则VARCHAR(255)）
\n  (3) 添加id, create_time, update_time及自动更新触发器
\n  (4) 建表类型：
\n      普通表
\n      分区表: 分区表日分区，generate_partition_statements指定生成分区子表范围
"""
import pandas as pd
from pypinyin import lazy_pinyin
import re
import psycopg2
import sys
from datetime import datetime, timedelta
import os

class ExcelToPgSimple:
    """
    简易版Excel转PostgreSQL建表工具
    功能：
    1. 读取Excel，将列名转拼音（数字开头加vc_，全小写）
    2. 自动判断字段长度（>255用TEXT，否则VARCHAR(255)）
    3. 添加id, create_time, update_time及自动更新触发器
    4. 建表前询问用户是普通表还是分区表
    """

    def __init__(self, db_config):
        """
        Args:
            db_config (dict): 数据库连接配置
        """
        self.db_config = db_config

    def _get_db_connection(self):
        return psycopg2.connect(**self.db_config)

    def _process_column_name(self, original_name):
        """处理列名：中文转拼音，去特殊字符，转小写，数字开头处理"""
        # 转拼音
        if re.search(r'[\u4e00-\u9fa5]', str(original_name)):
            pinyin_list = lazy_pinyin(str(original_name))
            pinyin = '_'.join(pinyin_list)
        else:
            pinyin = str(original_name)
        
        # 清理特殊字符并转小写
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '', pinyin.lower())
        
        # 数字开头加vc_
        if clean_name and clean_name[0].isdigit():
            clean_name = f"vc_{clean_name}"
            
        return clean_name

    def create_table(self, excel_path, schema_name, table_name, table_comment=None):
        if not os.path.exists(excel_path):
            print(f"错误: 文件不存在 {excel_path}")
            return

        print(f"正在读取文件: {excel_path} ...")
        try:
            df = pd.read_excel(excel_path)
        except Exception as e:
            print(f"读取Excel失败: {e}")
            return

        # 1. 询问表类型
        print(f"\n准备创建表 {schema_name}.{table_name}")
        print("请选择建表类型:")
        print("1. 普通表")
        print("2. 分区表")
        
        while True:
            choice = input("请输入选项 (1/2): ").strip()
            if choice in ['1', '2']:
                break
            print("输入无效，请重新输入")

        partition_clause = ""
        is_partition = (choice == '2')
        
        if is_partition:
            print("\n请输入分区策略 (例如: PARTITION BY RANGE (create_time))")
            partition_clause = input("PARTITION BY clause: ").strip()
            if not partition_clause.upper().startswith("PARTITION BY"):
                partition_clause = f"PARTITION BY RANGE ({partition_clause})"
            
            print("\n注意: 分区表的主键必须包含分区键。")
            print("是否保留默认的主键(id)? (y/n)")
            keep_pk = input("输入 y 保留(可能报错)，输入 n 移除主键约束仅保留id列: ").strip().lower()
            if keep_pk != 'y':
                # 标记需要移除主键约束
                pass 
        
        # 再次确认
        print(f"\n即将创建表: {schema_name}.{table_name}")
        print(f"类型: {'分区表' if is_partition else '普通表'}")
        if is_partition:
            print(f"分区定义: {partition_clause}")
        
        confirm = input("确认开始建表? (y/n): ").strip().lower()
        if confirm != 'y':
            print("已取消操作")
            return

        # 2. 构建列定义
        columns_def = []
        comments_sql = []
        
        # 2.1 ID列
        if is_partition and choice == '2' and keep_pk != 'y':
             columns_def.append("    id SERIAL") # 移除 PRIMARY KEY
             comments_sql.append(f"COMMENT ON COLUMN {schema_name}.{table_name}.id IS '主键ID';")
        else:
             columns_def.append("    id SERIAL PRIMARY KEY")
             comments_sql.append(f"COMMENT ON COLUMN {schema_name}.{table_name}.id IS '主键ID';")

        # 2.2 Excel列
        print("正在分析列长度...")
        for col in df.columns:
            # 计算最大长度
            max_len = df[col].astype(str).apply(len).max() if not df[col].empty else 0
            
            # 处理列名
            clean_col_name = self._process_column_name(col)
            
            # 确定类型
            col_type = "TEXT" if max_len > 255 else "VARCHAR(255)"
            
            columns_def.append(f"    {clean_col_name} {col_type}")
            comments_sql.append(f"COMMENT ON COLUMN {schema_name}.{table_name}.{clean_col_name} IS '{col}';")
            
            print(f"  - {col} -> {clean_col_name} ({col_type})")

        # 2.3 时间列
        columns_def.append("    create_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP")
        comments_sql.append(f"COMMENT ON COLUMN {schema_name}.{table_name}.create_time IS '创建时间';")
        
        columns_def.append("    update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP")
        comments_sql.append(f"COMMENT ON COLUMN {schema_name}.{table_name}.update_time IS '更新时间';")

        # 3. 组装SQL
        full_table_name = f"{schema_name}.{table_name}"
        create_sql = f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n"
        create_sql += ",\n".join(columns_def)
        create_sql += "\n)"
        
        if is_partition:
            create_sql += f"\n{partition_clause}"
        
        create_sql += ";"

        # 表注释
        if table_comment:
            comments_sql.insert(0, f"COMMENT ON TABLE {full_table_name} IS '{table_comment}';")

        # 触发器 (分区表通常也支持行级触发器，但需注意)
        trigger_func_sql = f"""
CREATE OR REPLACE FUNCTION {schema_name}.update_{table_name}_update_time()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
        trigger_sql = f"""
CREATE OR REPLACE TRIGGER {table_name}_update_trigger
BEFORE UPDATE ON {full_table_name}
FOR EACH ROW
EXECUTE FUNCTION {schema_name}.update_{table_name}_update_time();
"""

        # 4. 执行
        try:
            with self._get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 建表
                    print("\n执行建表SQL...")
                    cursor.execute(create_sql)
                    
                    # 注释
                    print("添加注释...")
                    for c_sql in comments_sql:
                        cursor.execute(c_sql)
                    
                    # 触发器
                    # 注意：分区表上创建触发器可能需要Postgres 11+，且通常是行级触发器
                    print("创建触发器...")
                    cursor.execute(trigger_func_sql)
                    cursor.execute(trigger_sql)
                    
                    conn.commit()
            print(f"\n成功创建表 {full_table_name}！")
            
        except Exception as e:
            print(f"\n建表失败: {e}")

    def generate_partition_statements(self, table_name, partition_column, start_time=None, end_time=None):
        """
        生成分区表的创建语句
        
        Args:
            table_name (str): 表名
            partition_column (str): 分区字段名
            start_time (str, optional): 开始时间，格式：YYYY-MM-DD
            end_time (str, optional): 结束时间，格式：YYYY-MM-DD
            
        如果不提供开始和结束时间，默认生成未来一周的分区
        """
        # 确定时间范围
        if start_time is None and end_time is None:
            # 默认未来一周
            today = datetime.now().date()
            start_time = today
            end_time = today + timedelta(days=7)
        elif start_time is None:
            # 只有结束时间，开始时间 = 结束时间 - 7天
            end_time = datetime.strptime(end_time, "%Y-%m-%d").date()
            start_time = end_time - timedelta(days=7)
        elif end_time is None:
            # 只有开始时间，结束时间 = 开始时间 + 7天
            start_time = datetime.strptime(start_time, "%Y-%m-%d").date()
            end_time = start_time + timedelta(days=7)
        else:
            # 两者都有
            start_time = datetime.strptime(start_time, "%Y-%m-%d").date()
            end_time = datetime.strptime(end_time, "%Y-%m-%d").date()
        
        # 生成按日分区的语句
        current = start_time
        while current <= end_time:
            next_day = current + timedelta(days=1)
            partition_name = f"{table_name}_{current.strftime('%Y%m%d')}"
            
            # PostgreSQL 创建分区语句
            # 假设主表已经存在且是分区表
            sql = f"""
CREATE TABLE IF NOT EXISTS {partition_name}
PARTITION OF {table_name}
FOR VALUES FROM ('{current.strftime('%Y-%m-%d')}') TO ('{next_day.strftime('%Y-%m-%d')}');
"""
            print(sql)
            with self._get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 建表
                    print(f"\n执行建表分区表SQL: {partition_name}")
                    cursor.execute(sql)
                    
                    conn.commit()
            current = next_day


# 使用示例 (仅当直接运行此脚本时执行)
if __name__ == "__main__":
    # 示例配置，请根据实际情况修改
    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "auteng",
        "user": "***",
        "password": "***"
    }
    creator = ExcelToPgSimple(db_config)
    # # 示例：创建表（可选）
    # creator.create_table(
    # excel_path=r"D:\YYZ\临时数据\数仓建模数据\订单\总订单2025年11月1-10日\order_1320251227172053695_510470.xlsx", 
    # schema_name="ods", 
    # table_name="ods_sale_order",
    # table_comment="销售订单表"
    # )
    
    # # 测试生成分区语句
    # print("=== 测试生成分区语句 ===")
    creator.generate_partition_statements(
        table_name="ods.ods_sale_order",
        partition_column="create_time",
        start_time="2025-01-01",
        end_time="2025-12-31"
    )
    
    # print("\n=== 测试默认未来一周 ===")
    # creator.generate_partition_statements(
    #     table_name="ods.ods_sale_order",
    #     partition_column="create_time"
    # )
