"""
    Company:深圳遨腾云创科技有限公司
\n  Name：YYZ
\n  Title: 根据excel自动建表
\n  Description:
\n  (1)根据excel文件自动建表，支持自定义表名、表注释、模式名
\n  (2)支持压缩包内excel文件的自动解压导入
\n  (3) 支持映射关系管理，存储到dim_ods_table_column_map管理源数据表的列名映射关系.支持根据映射关系自动导入数据到目标表
\n  (4) ***支持针对店小秘导出列名发生变动管理，列名发生变动，停止更新数据，给出提示
\n  (5) create_time字段有新数据，直接导入；update_time字段有新数据，更新数据 ,可以根据数据的新建和修改时间修复数据，比如导入重复，根据创建时间剔除数据，重新导入
\n  (6) ***针对店小秘新增字段，照常导入，提示新增字段
"""
import numpy as np
import pandas as pd
from pypinyin import lazy_pinyin, pinyin
import re
import os
import glob
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
import zipfile
import rarfile
import py7zr


class ExcelPostgreSQLManager:
    """
    Excel与PostgreSQL数据同步管理类
    支持通过实例变量自定义表名、模式名
    主要功能：
    1. 按照指定文件建表
    2. 按照指定路径下包含关键词的excel文件导入数据
    3. 导入完数据后输出被修改的建表语句
    """

    def __init__(self, db_config, folder_path, table_name=None, table_comment=None, schema_name="public"):
        """
        初始化类

        Args:
            db_config (dict): PostgreSQL数据库连接配置，包含host, port, dbname, user, password等
            folder_path (str): Excel文件所在的文件夹路径
            table_name (str, optional): 自定义表名
            table_comment (str, optional): 自定义表注释
            schema_name (str, optional): 数据库模式名，默认为public
        """
        self.db_config = db_config
        self.folder_path = folder_path  # Excel文件夹路径
        self.table_name = table_name  # 自定义表名实例变量
        self.table_comment = table_comment  # 自定义表注释实例变量
        self.schema_name = schema_name  # 自定义模式名实例变量

        # 初始化建表语句类变量列表
        # 结构：[create_table_header, columns_def_list, create_table_footer]
        self.table_creation_sql = [None, [], None]
        # 存储列名映射关系（原始拼音列名 -> 处理后的列名）
        self.column_name_mapping = {}
        # 存储ODS表配置映射关系 {vc_comment: column_name}
        self.ods_table_column_map = {}
        # 加载配置表映射关系
        self._load_ods_table_column_map()

    def _get_ods_table_column_map_table_name(self):
        """获取配置表的全名"""
        return f"{self.schema_name}.dim_ods_table_column_map"

    def _load_ods_table_column_map(self):
        """加载配置表中的映射关系到内存"""
        if not self.table_name:
            return

        config_table = self._get_ods_table_column_map_table_name()
        
        # 检查配置表是否存在
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = '{self.schema_name}' 
                        AND table_name = 'dim_ods_table_column_map'
                    )
                """)
                if not cursor.fetchone()[0]:
                    # 如果配置表不存在，尝试创建
                    print(f"配置表 {config_table} 不存在，正在创建...")
                    cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {config_table} (
                            id SERIAL PRIMARY KEY,
                            table_name VARCHAR(255),
                            column_name VARCHAR(255),
                            vc_comment TEXT,
                            create_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        );
                        COMMENT ON TABLE {config_table} IS 'ODS表字段映射配置表';
                        CREATE INDEX IF NOT EXISTS idx_dim_ods_map_table_name ON {config_table}(table_name);
                    """)
                    conn.commit()
                    return

                # 读取映射关系
                cursor.execute(f"""
                    SELECT vc_comment, column_name 
                    FROM {config_table} 
                    WHERE table_name = %s
                """, (self.table_name,))
                rows = cursor.fetchall()
                self.ods_table_column_map = {row[0]: row[1] for row in rows}
                # print(f"已加载配置表映射关系: {len(self.ods_table_column_map)} 条")

    def _insert_ods_table_column_map_rows(self, rows):
        """插入行到配置表"""
        if not rows:
            return
            
        config_table = self._get_ods_table_column_map_table_name()
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, f"""
                    INSERT INTO {config_table} (table_name, column_name, vc_comment)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, rows)
                conn.commit()
                print(f"已更新配置表 {config_table}，新增 {len(rows)} 条映射")

    def refresh_ods_table_column_map_from_db_table(self):
        """
        从数据库目标表刷新映射关系到配置表
        逻辑：
        1. 获取目标表当前所有列（排除系统列）
        2. 获取列的注释（即Excel原始列名）
        3. 写入配置表
        """
        if not self.table_exists():
            return

        full_table_name = self._get_full_table_name()
        
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                # 获取列名和注释
                cursor.execute("""
                    SELECT
                        c.column_name,
                        pgd.description AS column_comment
                    FROM information_schema.columns c
                    LEFT JOIN pg_catalog.pg_namespace n
                        ON n.nspname = c.table_schema
                    LEFT JOIN pg_catalog.pg_class cls
                        ON cls.relname = c.table_name
                        AND cls.relnamespace = n.oid
                    LEFT JOIN pg_catalog.pg_description pgd
                        ON pgd.objoid = cls.oid
                        AND pgd.objsubid = c.ordinal_position
                    WHERE c.table_schema = %s
                        AND c.table_name = %s
                    ORDER BY c.ordinal_position
                """, (self.schema_name, self.table_name))
                columns_info = cursor.fetchall()

        # 过滤系统列
        system_columns = ['id', 'create_time', 'update_time']
        rows_to_insert = []
        new_map = {}

        for col_name, col_comment in columns_info:
            if col_name in system_columns:
                continue
            
            # 如果没有注释，暂时用列名代替（或者跳过，视业务需求而定，这里假设都有注释因为是我们建的表）
            vc_comment = col_comment if col_comment else col_name
            
            # 准备插入配置表的数据 (table_name, column_name, vc_comment)
            # 注意：vc_comment是用于匹配的key
            rows_to_insert.append((self.table_name, col_name, vc_comment))
            new_map[vc_comment] = col_name

        # 更新内存中的映射
        self.ods_table_column_map = new_map
        
        # 写入数据库配置表
        config_table = self._get_ods_table_column_map_table_name()
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM {config_table} WHERE table_name = %s", (self.table_name,))
                execute_values(cursor, f"""
                    INSERT INTO {config_table} (table_name, column_name, vc_comment)
                    VALUES %s
                """, rows_to_insert)
                conn.commit()
        
        print(f"已从表 {full_table_name} 刷新配置表映射关系")

    def _compare_excel_with_config_map(self, excel_file):
        """
        对比Excel文件列名与配置表映射关系
        Returns:
            status: "match" | "subset" | "mismatch"
            diff_details: dict
        """
        # 获取Excel的映射 {vc_comment: pinyin}
        # 注意：这里我们只需要vc_comment列表
        df = pd.read_excel(excel_file, nrows=0)
        excel_columns = set(df.columns.tolist())
        
        config_map = self.ods_table_column_map
        config_columns = set(config_map.keys())

        # 1. 配置表中的映射关系在Excel中都能找到 (config <= excel)
        missing_config_in_excel = config_columns - excel_columns
        
        # 2. Excel比配置表多出的映射关系
        extra_excel_in_config = excel_columns - config_columns

        if not missing_config_in_excel:
            if not extra_excel_in_config:
                return "match", {}, {}, {}, {} # 2.2.1 完全一致
            else:
                return "subset", {}, {}, extra_excel_in_config, {} # 2.2.2 Excel更多
        else:
            return "mismatch", {}, missing_config_in_excel, extra_excel_in_config, {} # 2.3 配置表有部分在Excel找不到

    def _get_full_table_name(self):
        """
        获取包含模式名的完整表名

        Returns:
            str: 完整表名（schema.table_name）
        """
        # 确保表名已设置
        if not self.table_name:
            raise ValueError("表名未设置，请在初始化类时提供table_name参数")
        return f"{self.schema_name}.{self.table_name}"

    @contextmanager
    def get_db_connection(self):
        """
        获取PostgreSQL数据库连接的上下文管理器

        Yields:
            connection: PostgreSQL数据库连接对象
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise Exception(f"数据库操作出错: {str(e)}")
        finally:
            if conn:
                conn.close()

    def get_column_pinyin_dict(self, excel_file):
        """
        读取Excel文件的列名，将中文列名翻译成拼音，英文列名保持不变

        Args:
            excel_file (str): Excel文件的绝对路径

        Returns:
            dict: {列名: 拼音}的字典
        """
        if not excel_file:
            raise ValueError("Excel文件未指定")

        try:
            # 读取Excel文件的第一行作为列名
            df = pd.read_excel(excel_file, nrows=0)
            columns = df.columns.tolist()

            column_pinyin_dict = {}

            for column in columns:
                # 判断列名是否包含中文
                if re.search(r'[\u4e00-\u9fa5]', str(column)):
                    # 将中文列名转换为拼音，使用空格分隔
                    pinyin_list = lazy_pinyin(str(column))
                    # 合并拼音为字符串，使用下划线连接
                    pinyin = '_'.join(pinyin_list)
                else:
                    # 英文列名保持不变
                    pinyin = str(column)

                column_pinyin_dict[column] = pinyin

            return column_pinyin_dict

        except FileNotFoundError:
            raise FileNotFoundError(f"Excel文件不存在: {excel_file}")
        except Exception as e:
            raise Exception(f"读取Excel文件时出错: {str(e)}")

    def generate_postgresql_create_table(self, excel_file, column_pinyin_dict=None):
        """
        根据列名拼音字典生成PostgreSQL建表语句

        Args:
            excel_file (str): 用于建表的Excel文件路径
            column_pinyin_dict (dict, optional): 列名拼音映射字典，格式为{列名: 拼音}
                                               若不提供则使用get_column_pinyin_dict方法生成

        Returns:
            str: PostgreSQL建表语句
        """
        try:
            if not excel_file:
                raise ValueError("建表文件未指定")

            # 如果没有提供列名拼音字典，则自动生成
            if column_pinyin_dict is None:
                column_pinyin_dict = self.get_column_pinyin_dict(excel_file)

            # 获取完整表名
            full_table_name = self._get_full_table_name()

            # 读取完整的Excel数据以计算每列的最大长度
            print(f"正在分析文件: {excel_file} 的数据长度...")
            df = pd.read_excel(excel_file)

            # 计算每列的最大长度
            column_max_length = {}
            for column in df.columns:
                # 转换为字符串类型并计算长度
                max_len = df[column].astype(str).apply(len).max()
                column_max_length[column] = max_len
                print(f"列 '{column}' 的最大长度: {max_len}")

            # 开始构建SQL语句
            create_table_header = f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n"

            # 存储列名映射以便后续生成注释
            column_name_map = {}

            # 为每个列生成定义
            columns_def_list = []

            # 1. 添加主键id字段
            columns_def_list.append("    id SERIAL PRIMARY KEY")
            column_name_map["id"] = "主键ID"

            # 重置列名映射关系
            self.column_name_mapping = {}

            # 2. 添加Excel表中的列
            for column_name, column_pinyin in column_pinyin_dict.items():
                # 列名转换为小写并处理特殊字符
                clean_column_name = re.sub(r'[^a-zA-Z0-9_]', '', column_pinyin.lower())

                # 检查列名是否以数字开头，如果是则添加vc前缀
                if clean_column_name and clean_column_name[0].isdigit():
                    clean_column_name = f"vc_{clean_column_name}"
                    print(f"列 '{column_name}' 的拼音列名以数字开头，已添加前缀变为: {clean_column_name}")

                # 保存原始拼音列名到处理后列名的映射关系
                self.column_name_mapping[column_pinyin.lower()] = clean_column_name

                # 根据列的最大长度决定数据类型
                max_len = column_max_length.get(column_name, 0)
                if max_len > 255:
                    data_type = "TEXT"
                    print(f"列 '{column_name}' 长度超过255，使用TEXT类型")
                else:
                    data_type = "VARCHAR(255)"

                # 添加列定义到列表
                columns_def_list.append(f"    {clean_column_name} {data_type}")
                # 保存列名映射
                column_name_map[clean_column_name] = column_name

            # 3. 添加创建时间字段
            columns_def_list.append("    create_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP")
            column_name_map["create_time"] = "创建时间"

            # 4. 添加更新时间字段
            columns_def_list.append("    update_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP")
            column_name_map["update_time"] = "更新时间"

            # 构建表注释和列注释
            comments_sql = ""
            if self.table_comment:
                comments_sql += f"COMMENT ON TABLE {full_table_name} IS '{self.table_comment}';\n"

            for clean_column_name, original_column_name in column_name_map.items():
                comments_sql += f"COMMENT ON COLUMN {full_table_name}.{clean_column_name} IS '{original_column_name}';\n"

            # 添加更新时间自动更新的触发器
            trigger_sql = f"\n-- 创建更新时间自动更新的触发器\n"
            trigger_sql += f"CREATE OR REPLACE FUNCTION {self.schema_name}.update_{self.table_name}_update_time()\n"
            trigger_sql += f"RETURNS TRIGGER AS $$\nBEGIN\n"
            trigger_sql += f"    NEW.update_time = CURRENT_TIMESTAMP;\n"
            trigger_sql += f"    RETURN NEW;\n"
            trigger_sql += f"END;\n$$ LANGUAGE plpgsql;\n\n"

            trigger_sql += f"CREATE OR REPLACE TRIGGER {self.table_name}_update_trigger\n"
            trigger_sql += f"BEFORE UPDATE ON {full_table_name}\n"
            trigger_sql += f"FOR EACH ROW\n"
            trigger_sql += f"EXECUTE FUNCTION update_{self.table_name}_update_time();\n"

            # 构建完整的表创建footer部分
            create_table_footer = "\n);\n\n" + comments_sql + trigger_sql

            # 将建表语句的三部分保存到类变量
            self.table_creation_sql = [create_table_header, columns_def_list, create_table_footer]

            # 生成完整的SQL语句用于返回
            full_sql = create_table_header + ",\n".join(columns_def_list) + create_table_footer
            return full_sql

        except Exception as e:
            raise Exception(f"生成建表语句时出错: {str(e)}")

    def create_table(self, excel_file, column_pinyin_dict=None):
        """
        创建PostgreSQL表

        Args:
            excel_file (str): 用于建表的Excel文件路径
            column_pinyin_dict (dict, optional): 列名拼音映射字典
                                               若不提供则使用get_column_pinyin_dict方法生成
        """
        sql = self.generate_postgresql_create_table(excel_file, column_pinyin_dict)

        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                conn.commit()
            print(f"表 {self._get_full_table_name()} 创建成功")

    def get_table_columns(self):
        """
        获取PostgreSQL表的列名

        Returns:
            list: 列名列表
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.schema_name}' 
                    AND table_name = '{self.table_name}' 
                    ORDER BY ordinal_position
                """)
                return [row[0] for row in cursor.fetchall()]

    def get_table_column_types(self):
        """
        获取PostgreSQL表的列名和类型

        Returns:
            dict: 列名到类型的映射字典
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = '{self.schema_name}' 
                    AND table_name = '{self.table_name}' 
                    ORDER BY ordinal_position
                """)
                return {row[0]: row[1] for row in cursor.fetchall()}

    def alter_column_to_text(self, column_name):
        """
        将指定列的数据类型修改为TEXT类型

        Args:
            column_name (str): 要修改的列名
        """
        try:
            full_table_name = self._get_full_table_name()
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 修改列类型为TEXT
                    alter_sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {column_name} TYPE TEXT;"
                    cursor.execute(alter_sql)
                    conn.commit()
                    print(f"成功将列 {column_name} 修改为TEXT类型")

                    # 更新类建表语句列表变量中的对应字段声明
                    if self.table_creation_sql[1]:  # 确保columns_def_list已初始化
                        for i, column_def in enumerate(self.table_creation_sql[1]):
                            if f"    {column_name} " in column_def:
                                # 替换为TEXT类型
                                self.table_creation_sql[1][i] = f"    {column_name} TEXT"
                                print(f"已更新建表语句中的列 {column_name} 为TEXT类型")
                                break

        except Exception as e:
            raise Exception(f"修改列类型时出错: {str(e)}")

    def table_exists(self):
        """
        检查表是否存在

        Returns:
            bool: 表是否存在
        """
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = '{self.schema_name}' 
                        AND table_name = '{self.table_name}'
                    )
                """)
                return cursor.fetchone()[0]

    def insert_data_from_file(self, excel_file, column_mapping=None, return_pending_manual_map=False):
        """
        从单个Excel文件导入数据到PostgreSQL表

        Args:
            excel_file (str): Excel文件的绝对路径
            column_mapping (dict, optional): 强制使用的列名映射字典 {Excel列名: 数据库字段名}
            return_pending_manual_map (bool, optional): 是否返回需要人工处理的新增映射关系
        """
        try:
            # 获取完整表名
            full_table_name = self._get_full_table_name()

            # 准备待人工处理的映射字典
            pending_manual_map = {}

            # 如果传入了强制映射，计算新增列
            if column_mapping and return_pending_manual_map:
                excel_full_map = self.get_column_pinyin_dict(excel_file)
                # 找出Excel中有但配置映射中没有的列
                extra_keys = set(excel_full_map.keys()) - set(column_mapping.keys())
                pending_manual_map = {k: excel_full_map[k] for k in extra_keys}

            # 如果没有提供映射，则默认从文件生成
            if column_mapping is None:
                # 获取列名拼音映射（使用建表时的文件，确保列名一致）
                column_pinyin_dict = self.get_column_pinyin_dict(excel_file)
                # 转换列名为数据库列名
                column_mapping = {}
                for original_name, pinyin in column_pinyin_dict.items():
                    # 先转换为小写并清理特殊字符
                    clean_pinyin = re.sub(r'[^a-zA-Z0-9_]', '', pinyin.lower())
                    # 检查是否在映射表中（处理以数字开头的列名）
                    db_column_name = self.column_name_mapping.get(clean_pinyin, clean_pinyin)
                    column_mapping[original_name] = db_column_name
            else:
                # 如果提供了映射，也需要生成column_pinyin_dict，以便后续错误处理时使用
                column_pinyin_dict = self.get_column_pinyin_dict(excel_file)

            # 读取数据
            print(f"读取文件数据: {excel_file}")
            df = pd.read_excel(excel_file)

            # 重命名DataFrame的列
            df_renamed = df.rename(columns=column_mapping)

            # 获取表的列名
            table_columns = self.get_table_columns()
            # print(f"DEBUG: 数据库实际列名: {table_columns}")

            # 排除系统字段（id, create_time, update_time）
            system_columns = ['id', 'create_time', 'update_time']
            table_columns = [col for col in table_columns if col not in system_columns]

            # 确保只插入表中存在的列
            columns_to_insert = [col for col in df_renamed.columns if col in table_columns]
            # print(f"DEBUG: 过滤后准备插入的列: {columns_to_insert}")

            # 二次检查：如果columns_to_insert里有任何列不在table_columns里（虽然理论上不可能），再次过滤
            columns_to_insert = [c for c in columns_to_insert if c in table_columns]

            if not columns_to_insert:
                print(f"警告: 没有有效列可插入到表 {full_table_name}")
                if return_pending_manual_map:
                    return pending_manual_map
                return

            df_filtered = df_renamed[columns_to_insert]

            # 更彻底地处理NaN值 - 替换所有类型的空值
            # 1. 首先替换所有NaN和NaT
            df_filtered = df_filtered.replace({pd.NA: None, pd.NaT: None, np.nan: None})

            # 2. 对于整列都是NaN的情况，将列数据类型转换为object，然后替换
            for col in df_filtered.columns:
                if df_filtered[col].dtype == 'float64' and df_filtered[col].isna().all():
                    df_filtered[col] = df_filtered[col].astype(object)
                    df_filtered[col] = df_filtered[col].fillna(None)

            # 3. 最后将DataFrame转换为列表，确保所有空值都是None
            data_list = df_filtered.values.tolist()
            # 遍历每一行，将NaN转换为None
            data_list = [[None if pd.isna(value) or value is np.nan else value for value in row] for row in data_list]

            # 生成插入SQL - 注意：execute_values只需要一个%s占位符
            columns_str = ', '.join(columns_to_insert)
            insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES %s"  # 只使用一个%s占位符

            # 执行批量插入
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, insert_sql, data_list)
                    conn.commit()
                    print(f"成功插入 {len(data_list)} 行数据到 {full_table_name} 表")

            if return_pending_manual_map:
                return pending_manual_map

        except Exception as e:
            # 处理数据错误，特别是长度限制错误
            error_msg = str(e)
            print(f"数据错误: {error_msg}")

            # 尝试识别是哪个列长度不够
            if "value too long for type character varying" in error_msg or "值太长了" in error_msg:
                # 获取表的列类型信息
                column_types = self.get_table_column_types()

                # 找出所有VARCHAR类型的列
                varchar_columns = [col for col, dtype in column_types.items() if dtype == 'character varying']

                if varchar_columns:
                    # 分析数据中实际超过长度限制的列
                    print(f"分析数据中实际超过长度限制的列...")

                    # 转换列名为数据库列名以便匹配
                    column_mapping = {}
                    for original_name, pinyin in column_pinyin_dict.items():
                        # 先转换为小写并清理特殊字符
                        clean_pinyin = re.sub(r'[^a-zA-Z0-9_]', '', pinyin.lower())
                        # 检查是否在映射表中（处理以数字开头的列名）
                        db_column_name = self.column_name_mapping.get(clean_pinyin, clean_pinyin)
                        column_mapping[original_name] = db_column_name

                    # 读取数据并分析每列的最大长度
                    df = pd.read_excel(excel_file)
                    df_renamed = df.rename(columns=column_mapping)

                    # 需要修改为TEXT的列
                    columns_to_alter = []

                    for col in varchar_columns:
                        if col in df_renamed.columns:
                            # 计算该列的最大长度
                            max_len = df_renamed[col].astype(str).apply(len).max()
                            print(f"列 '{col}' 的最大长度: {max_len}")

                            # 如果超过255，则需要修改
                            if max_len > 255:
                                columns_to_alter.append(col)

                    if columns_to_alter:
                        print(f"发现 {len(columns_to_alter)} 列实际超过长度限制: {columns_to_alter}")
                        print("正在将这些列转换为TEXT类型...")

                        # 只将实际超过长度限制的列转换为TEXT类型
                        for col in columns_to_alter:
                            self.alter_column_to_text(col)

                        # 转换完成后，重新尝试插入数据
                        print("重新尝试插入数据...")
                        self.insert_data_from_file(excel_file)
                    else:
                        # 没有找到超过长度限制的列，可能是其他问题
                        print("未发现超过长度限制的列，可能是其他原因导致的错误")
                        raise Exception(f"插入数据到PostgreSQL时出错: {str(e)}")
                else:
                    raise Exception(f"插入数据到PostgreSQL时出错: {str(e)}")
            else:
                raise Exception(f"插入数据到PostgreSQL时出错: {str(e)}")

    def import_data_from_folder(self, file_pattern, folder_path=None):
        """
        从指定路径下包含关键词的Excel文件导入数据到PostgreSQL表

        Args:
            file_pattern (str): 文件名模式，如"order"
            folder_path (str, optional): 文件夹路径，若不提供则使用初始化时的folder_path
        """
        # 确定要使用的文件夹路径
        path_to_use = folder_path if folder_path else self.folder_path
        if not path_to_use:
             raise ValueError("文件夹路径未指定")

        # 确保表名已设置
        if not self.table_name:
            raise ValueError("表名未设置，请在初始化类时提供table_name参数")

        try:
            # 遍历文件夹，找到所有Excel文件
            excel_files = glob.glob(os.path.join(path_to_use, "**", "*.xlsx"), recursive=True)

            # 过滤出文件名包含指定模式的文件
            matched_files = [f for f in excel_files if file_pattern.lower() in os.path.basename(f).lower()]

            print(f"找到 {len(matched_files)} 个包含 '{file_pattern}' 的Excel文件")
            if not matched_files:
                return

            # 情况 1: 目标表不存在
            if not self.table_exists():
                print(f"目标表 {self._get_full_table_name()} 不存在，执行初始化流程...")
                
                # 1.1 读取第一个Excel映射关系建表
                first_file = matched_files[0]
                self.create_table(excel_file=first_file)

                # 1.2 插入文件夹下所有Excel表的数据
                print("开始插入数据...")
                for file_path in matched_files:
                    # 对于初始化，我们可以直接插入，让insert_data_from_file自动推导映射
                    self.insert_data_from_file(file_path)

                # 1.3 输出建表语句
                print("\n" + "=" * 80)
                print("初始化完成，建表语句如下：")
                print("=" * 80)
                try:
                    print(self.get_current_table_creation_sql())
                except Exception:
                    print("（建表语句未缓存，可能是因为使用了已有表结构）")

                # 1.4 把ods模式表映射关系写进 ods模式的配置表
                self.refresh_ods_table_column_map_from_db_table()
                return

            # 情况 2: 目标表存在
            print(f"目标表 {self._get_full_table_name()} 已存在，执行增量流程...")
            
            # 先把ODS模式表映射关系刷新到配置表（确保配置表是最新的）
            self.refresh_ods_table_column_map_from_db_table()
            
            # 读取配置表的映射关系
            config_map = self.ods_table_column_map
            if not config_map:
                print("警告: 即使刷新了配置表，仍未获取到映射关系。")

            # 检查映射变动情况（以第一个文件为准，假设批次内文件结构一致）
            first_file = matched_files[0]
            status, _, missing_config_in_excel, extra_excel_in_config, _ = self._compare_excel_with_config_map(first_file)

            # 2.3 配置表的部分映射关系在excel中找不到
            if status == "mismatch":
                print("错误：配置表的部分映射关系在Excel中找不到（Scenario 2.3）")
                print(f"配置表在Excel中找不到的映射关系: {missing_config_in_excel}")
                print(f"Excel比配置表多出的映射关系: {extra_excel_in_config}")
                print("不执行插入操作。请人工检查文件或配置。")
                return

            # 2.1 & 2.2 配置表的映射关系在excel中都能找到（Excel可能更多）
            # 执行插入，使用配置表的映射关系（insert_data_from_file会自动过滤掉多余的列）
            print("校验通过，开始插入数据...")
            
            all_extra_mappings = {}
            for file_path in matched_files:
                print(f"\n处理文件: {file_path}")
                # 传入 config_map 作为 column_mapping，确保只识别已知的列
                # 同时请求返回 pending_manual_map 以捕获新增列
                pending = self.insert_data_from_file(
                    file_path, 
                    column_mapping=config_map, 
                    return_pending_manual_map=True
                )
                if pending:
                    all_extra_mappings.update(pending)

            # 2.2 输出多出来的映射关系
            if all_extra_mappings:
                print("\n" + "=" * 80)
                print("检测到Excel存在比配置表多出的映射关系（Scenario 2.2）")
                print("这些列的数据未插入数据库，且未写入配置表，请人工确认是否需要添加字段：")
                print(f"{all_extra_mappings}")
                print("=" * 80)
            else:
                print("\n配置表与Excel映射关系完全一致（Scenario 2.1），数据插入完成。")

        except Exception as e:
            raise Exception(f"处理Excel文件时出错: {str(e)}")

    def get_current_table_creation_sql(self):
        """
        获取当前的建表语句（包含所有修改）

        Returns:
            str: 完整的建表语句
        """
        if not all(self.table_creation_sql):
            raise ValueError("建表语句未生成，请先调用generate_postgresql_create_table方法")

        # 拼接三部分建表语句
        create_table_header, columns_def_list, create_table_footer = self.table_creation_sql
        full_sql = create_table_header + ",\n".join(columns_def_list) + create_table_footer
        return full_sql

    def extract_and_delete_archives(self, path=None):
        """
        解压指定路径下的所有压缩包并删除原文件

        支持的压缩格式: .zip, .rar, .7z

        参数:
            path: 包含压缩包的目录路径，若不提供则使用初始化时的folder_path
        """
        path_to_use = path if path else self.folder_path
        if not path_to_use:
             return

        # 获取指定路径下的所有文件
        files = os.listdir(path_to_use)

        for file in files:
            file_path = os.path.join(path_to_use, file)

            # 跳过目录
            if os.path.isdir(file_path):
                continue

            try:
                # 处理zip文件
                if file.lower().endswith('.zip'):
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        zip_ref.extractall(path_to_use)
                    os.remove(file_path)
                    print(f"已解压并删除: {file}")

                # 处理rar文件
                elif file.lower().endswith('.rar'):
                    with rarfile.RarFile(file_path, 'r') as rar_ref:
                        rar_ref.extractall(path_to_use)
                    os.remove(file_path)
                    print(f"已解压并删除: {file}")

                # 处理7z文件
                elif file.lower().endswith('.7z'):
                    with py7zr.SevenZipFile(file_path, 'r') as sevenz_ref:
                        sevenz_ref.extractall(path_to_use)
                    os.remove(file_path)
                    print(f"已解压并删除: {file}")

            except Exception as e:
                print(f"处理文件 {file} 时出错: {str(e)}")


# 示例用法
if __name__ == "__main__":
    # 1. 配置数据库连接参数
    db_config = {
        "host": "localhost",  # 数据库主机地址
        "port": 5432,  # 数据库端口
        "dbname": "auteng",  # 数据库名称
        "user": "***",  # 数据库用户名
        "password": "***"  # 数据库密码
    }
    
    folder_path = r"D:\YYZ\临时数据\数仓建模数据\订单\总订单2025年11月1-10日"  # 数据文件所在文件夹

    # 2. 创建ExcelPostgreSQLManager实例
    manager = ExcelPostgreSQLManager(
        db_config=db_config,
        folder_path=folder_path,
        table_name="ods_sale_order",  # 自定义表名
        table_comment="销售订单表",  # 自定义表注释
        schema_name="ods"
    )

    try:
        # 先解压并删除压缩包
        manager.extract_and_delete_archives()

        # 3. 从文件夹导入包含关键词的Excel文件（如果表不存在会自动创建）
        file_pattern = "order"  # 文件名关键词，如"order"

        print("\n开始导入数据...")
        manager.import_data_from_folder(file_pattern)

    except Exception as e:
        print(f"操作失败: {str(e)}")
