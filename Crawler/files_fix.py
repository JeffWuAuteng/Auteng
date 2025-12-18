# 一次性处理所有的数据并将结果保存到指定目录
import os
import pandas as pd
import datetime
import re
import logging
import shutil
import time
import openpyxl
import xlwings as xw
import csv
from pathlib import Path
import sys
from path_utils import PathManager, setup_path_logging, get_path_manager
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
def base_info(base_path, base_filename):
    info_path = Path(base_path)
    info_file = info_path / base_filename
    if not info_file.exists():
        raise FileNotFoundError(f"文件不存在: {info_file}")
    df = pd.read_excel(info_file, engine='openpyxl')
    ues_col = ['店铺账号', 'SBS账单编号', '订单编号', 'SKU ID', '长 CM', '宽 CM', '高 CM', '重量 G','操作费-平台账单/原币','应付操作费/原币']
    ues_col2 = ['店铺账号', 'SBS账单编号', '订单编号', 'SKU_ID', '长_CM', '宽_CM', '高_CM', '重量_G']
    df = df[ues_col]
    for col in df.columns:
        df.rename(columns={col: col.replace(' ', '_')}, inplace=True)
    df.dropna()
    df.drop_duplicates(subset=ues_col2, inplace=True)
    return df
def sum_data_info(info_path, sum_data_filename):
    info_path = Path(info_path)
    sum_data_file = info_path / sum_data_filename
    if not sum_data_file.exists():
        raise FileNotFoundError(f"文件不存在: {sum_data_file}")
    try:
        df = pd.read_excel(sum_data_file, sheet_name='Charging Report Summary', engine='openpyxl')
    except Exception as e:
        try:
            df = pd.read_excel(sum_data_file, sheet_name='Charging Report Summary', engine='xlrd')
        except Exception as e2:
            raise ValueError(f"无法读取Excel文件 {sum_data_file}，尝试的引擎都失败了。openpyxl错误: {e}, xlrd错误: {e2}")
    ues_col = ['来源文件', '收费报告 ID']
    df = df[ues_col]
    df.dropna(subset=['收费报告 ID'], inplace=True)
    df.drop_duplicates(subset=['收费报告 ID'], inplace=True)
    return df
def sjsm_info(info_path, info_filename):
    info_path = Path(info_path)
    info_file = info_path / info_filename
    if not info_file.exists():
        raise FileNotFoundError(f"文件不存在: {info_file}")
    df = pd.read_excel(info_file, sheet_name='数据说明',dtype=str)
    ues_col = ['店铺账号', '仓库名称', '站点', '外币币种','店铺ID']
    df = df[ues_col]
    df.dropna(subset=['店铺账号'], inplace=True)
    df.drop_duplicates(subset=['店铺账号'], inplace=True)
    return df
def calculate_handling_fee(country, length, width, height, weight_g, pcs):
    max_eg = max(length, width, height)
    h_val = weight_g
    i_val = pcs
    # 初始化结果变量
    result = 0
    # 新加坡(SG)的计算逻辑
    if country == "sg":
        if max_eg <= 20 and h_val <= 2000:
            result = 1 * i_val
        elif max_eg < 25 and h_val < 15500:
            result = 2 * i_val
        elif max_eg < 50 and h_val < 15500:
            result = 3 * i_val
        elif max_eg < 100 and h_val < 15500:
            result = 5 * i_val
        else:
            result = 10 * i_val
    # 马来西亚(MY)的计算逻辑
    elif country == "my":
        if max_eg <= 15 and h_val <= 50:
            if i_val <= 3:
                result = 1.2
            else:
                result = 1.2 + (i_val - 3) * 0.3
        elif max_eg <= 25 and h_val <= 1000:
            if i_val <= 3:
                result = 1.4
            else:
                result = 1.4 + (i_val - 3) * 0.3
        elif max_eg <= 50 and h_val <= 5000:
            if i_val <= 3:
                result = 1.7
            else:
                result = 1.7 + (i_val - 3) * 0.3
        elif max_eg <= 100 and h_val <= 10000:
            if i_val <= 3:
                result = 3.2
            else:
                result = 3.2 + (i_val - 3) * 1.7
        elif max_eg > 100 or h_val > 10000:
            if i_val <= 3:
                result = 6
            else:
                result = 6 + (i_val - 3) * 2.3
        else:
            result = 0
    # 菲律宾(PH)的计算逻辑
    elif country == "ph":
        if max_eg <= 15 and h_val <= 50:
            if i_val <= 3:
                result = 15
            else:
                result = 15 + (i_val - 3) * 4
        elif max_eg <= 25 and h_val <= 1000:
            if i_val <= 3:
                result = 17
            else:
                result = 17 + (i_val - 3) * 4
        elif max_eg <= 50 and h_val <= 5000:
            if i_val <= 3:
                result = 24
            else:
                result = 24 + (i_val - 3) * 4
        elif max_eg <= 100 and h_val <= 10000:
            if i_val <= 3:
                result = 80
            else:
                result = 80 + (i_val - 3) * 23
        elif max_eg > 100 or h_val > 10000:
            if i_val <= 3:
                result = 260
            else:
                result = 260 + (i_val - 3) * 39
        else:
            result = 0
    # 泰国/越南(TH/VN)的计算逻辑,南宁仓
    elif country in ["th", "vn"]:
        total_weight = h_val * i_val
        if total_weight <= 50:
            result = 0.49
        elif total_weight <= 250:
            result = 0.99
        elif total_weight <= 500:
            result = 1.49
        else:
            calc = 1.49 + ((total_weight - 500 + 99) // 100) * 0.59
            result = min(calc, 20)
    # 默认返回0
    else:
        result = 0
    return result
def reason_maker(sku_id, sku_num, len_max, actual_fee, handling_fee, country):
    reason = f"操作费计算错误(订单规格计算错误,实际SKU{sku_id},pcs是{sku_num}个,SKU规格最长边不超{len_max}cm,应收取操作费为{handling_fee}{country},实际收取操作费{actual_fee}{country}"
    return reason

def _derive_base_times_dir(output_dir: str) -> str:
    """根据输出目录向上回溯，推断形如YYYY.MM.DD的时间根目录。"""
    try:
        base = Path(output_dir)
        # 向上查找最多3级，定位日期目录
        for ancestor in [base, base.parent, base.parent.parent]:
            name = ancestor.name
            if re.match(r'^\d{4}\.\d{2}\.\d{2}$', name):
                return str(ancestor)
        # 回退：使用上一级目录
        return str(base.parent)
    except Exception:
        return output_dir

def _log_missing_sbs_bill(file_path: str, output_dir: str, use_info_df) -> str:
    """将缺失的SBS账单记录到CSV文件，并返回CSV路径。"""
    try:
        sbs_number = None
        if use_info_df is not None and hasattr(use_info_df, 'empty') and not use_info_df.empty:
            if 'SBS账单编号' in use_info_df.columns:
                sbs_number = str(use_info_df.iloc[0]['SBS账单编号']).strip()
        if not sbs_number:
            # 从文件名中尝试解析，如 _CRPH2508250654_ 这样的模式
            m = re.search(r'_(CR[A-Z]+\d+)_', os.path.basename(file_path))
            if m:
                sbs_number = m.group(1)

        base_times_dir = _derive_base_times_dir(output_dir)
        os.makedirs(base_times_dir, exist_ok=True)
        csv_path = os.path.join(base_times_dir, '缺失sbs账单.csv')

        need_header = not os.path.exists(csv_path)
        with open(csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if need_header:
                writer.writerow(['缺失时间', 'SBS账单编号', '文件名称', '文件路径'])
            writer.writerow([
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                sbs_number or '',
                os.path.basename(file_path),
                file_path
            ])
        logging.info(f"缺失账单记录已写入: {csv_path}")
        return csv_path
    except Exception as e:
        logging.error(f"记录缺失账单失败: {e}")
        return ''
def process_excel_file(file_path: str, filter_column_name: str, filter_values: list,use_info_df,  output_dir:  str, file_mode: str = "1") -> str:
    """
    处理Excel文件，筛选特定工作表中的行并着色，将结果保存到指定目录
    参数:
        file_path: 原始Excel文件路径
        filter_column_name: 筛选列名（例如: "订单编号"）
        filter_values: 筛选值列表
    返回:
        处理后的文件保存路径
    """
    # 检查文件是否存在
    if not os.path.exists(file_path):
        logging.error(f"文件不存在: {file_path}")
        # 记录缺失并跳过该文件
        csv_path = _log_missing_sbs_bill(file_path, output_dir, use_info_df)
        if csv_path:
            print(f"缺失账单文件，已记录到: {csv_path}")
        else:
            print(f"缺失账单文件，记录失败: {file_path}")
        return None

    # 确保输出目录存在
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"创建输出目录: {output_dir}")

    file_name = os.path.basename(file_path)
    output_file_name = file_name
    output_file_path = os.path.join(output_dir, output_file_name)

    # 检查文件是否已存在，根据全局模式处理
    if os.path.exists(output_file_path):
        if file_mode == "2":
            logging.info(f"补充模式：跳过已存在的文件 {output_file_path}")
            print(f"跳过处理，文件已存在: {output_file_path}")
            return output_file_path
        else:  # file_mode == "1" 或其他值默认为覆盖模式
            os.remove(output_file_path)
            logging.info(f"覆盖模式：已删除现有文件 {output_file_path}")

    # 复制原始文件到输出目录
    logging.info(f"复制文件 {file_path} 到 {output_file_path}")
    shutil.copy2(file_path, output_file_path)

    # 启动Excel应用程序
    app = xw.App(visible=False)
    try:
        logging.info(f"开始处理文件: {output_file_path}")
        # 打开工作簿
        wb = app.books.open(output_file_path)

        # 查找包含 "handling" 的工作表（大小写不敏感）
        handling_sheet = None
        # 修改后的代码片段
        handling_sheet = None
        start_row = 1  # 默认从第1行开始
        header_row = 1  # 默认表头在第1行

        # 优先查找包含完整"Weekly Handling Fee"的工作表
        for sheet in wb.sheets:
            if re.search(r'\bWeekly Handling Fee\b', sheet.name, re.IGNORECASE):
                handling_sheet = sheet
                logging.info(f"找到精确匹配工作表: {sheet.name}")
                start_row = 3  # 精确匹配时从第3行开始
                header_row = 3  # 表头在第3行
                break

        # 如果未找到精确匹配，查找包含"Handling"的工作表
        if not handling_sheet:
            for sheet in wb.sheets:
                if re.search(r'Handling', sheet.name, re.IGNORECASE):
                    handling_sheet = sheet
                    logging.info(f"找到模糊匹配工作表: {sheet.name}")
                    start_row = 1  # 模糊匹配默认从第1行开始
                    header_row = 1  # 表头在第1行
                    break

        # 如果仍未找到则抛出异常
        if not handling_sheet:
            raise ValueError("未找到包含 Handling 关键字的工作表")

        ws = handling_sheet

        # 获取数据范围（从第3行开始，跳过前两行）
        try:
            # 动态获取数据范围
            last_row = ws.range('A' + str(ws.cells.last_cell.row)).end('up').row
            logging.info(f"工作表最后一行: {last_row}")

            if last_row < start_row:
                logging.warning("工作表中没有足够的数据")
                return output_file_path

            # 动态获取表头范围
            last_column = ws.used_range.last_cell.column
            header_range = ws.range(f'A{header_row}:{chr(64 + last_column)}{header_row}')

            # 查找筛选列的索引
            filter_column_index = None
            for cell in header_range:
                if cell.value == filter_column_name:
                    filter_column_index = cell.column
                    logging.info(f"找到筛选列 '{filter_column_name}' 在第 {filter_column_index} 列")
                    break

            if filter_column_index is None:
                logging.error(f"未找到列名 '{filter_column_name}'")
                raise ValueError(f"未找到列名 '{filter_column_name}'")

            # 数据范围从第3行开始
            last_column_letter = chr(64 + last_column)
            data_range_start = f'A{start_row}'
            data_range = ws.range(f'{data_range_start}:{last_column_letter}{last_row}')
            logging.info(f"数据范围: {data_range_start}:{last_column_letter}{last_row}")

            # 创建自动筛选
            ws.api.AutoFilterMode = False  # 清除现有筛选
            logging.info("已清除现有筛选")

            # 尝试应用筛选
            filter_success = False
            try:
                logging.info("尝试方法1: 使用Range.AutoFilter应用筛选")
                ws.api.Range(f'{data_range_start}:{last_column_letter}{last_row}').AutoFilter(
                    Field=filter_column_index,
                    Criteria1=filter_values,
                    Operator=7  # xlFilterValues
                )
                filter_success = True
            except Exception as e:
                logging.warning(f"应用筛选方法1失败: {e}")
                # 尝试使用另一种方式应用筛选
                try:
                    logging.info("尝试方法2: 使用Worksheet.AutoFilter应用筛选")
                    ws.api.AutoFilter(Field=filter_column_index, Criteria1=filter_values, Operator=7)
                    filter_success = True
                except Exception as e:
                    logging.warning(f"应用筛选方法2也失败: {e}")

            # 获取筛选后的可见行并着色
            colored_rows = 0
            if filter_success:
                try:
                    # 尝试获取可见单元格
                    logging.info("尝试获取筛选后的可见单元格")
                    visible_cells = ws.range(
                        f'{data_range_start}:{last_column_letter}{last_row}').api.SpecialCells(12)
                    # 填充颜色（黄色）
                    for area in visible_cells.Areas:
                        area.Interior.Color = 65535  # RGB(255,255,0) 黄色
                        colored_rows += area.Rows.Count
                    logging.info(f"已成功着色 {colored_rows} 行数据")
                except Exception as e:
                    logging.warning(f"获取可见单元格失败: {e}")
                    logging.info("尝试替代方法1: 检查行是否隐藏")
                    try:
                        for row in range(start_row, last_row + 1):
                            if not ws.range(f'A{row}').api.EntireRow.Hidden:
                                ws.range(f'A{row}:{last_column_letter}{row}').color = (255, 255, 0)
                                colored_rows += 1
                        logging.info(f"替代方法1: 已成功着色 {colored_rows} 行数据")
                    except Exception as e:
                        logging.warning(f"替代方法1失败: {e}")
                        colored_rows = 0  # 重置计数器

            # 如果前面的方法都失败，直接查找匹配值
            if colored_rows == 0:
                logging.info("尝试替代方法2: 直接查找匹配值")
                for row in range(start_row, last_row + 1):
                    try:
                        cell_value = ws.range(f'{chr(64 + filter_column_index)}{row}').value
                        if cell_value in filter_values:
                            logging.info(f"找到匹配行: {row}, 值: {cell_value}")
                            ws.range(f'A{row}:{last_column_letter}{row}').color = (255, 255, 0)
                            colored_rows += 1
                    except Exception as e:
                        logging.warning(f"处理第 {row} 行时出错: {e}")
                logging.info(f"替代方法2: 已成功着色 {colored_rows} 行数据")
            # 开始计算操作费并添加申诉理由
            # 获取当前工作表的最后一列
            last_used_column = ws.used_range.last_cell.column
            last_column_letter = chr(64 + last_used_column)

            # 获取店铺账号信息，用于确定币种
            shop_info_df = sjsm_info(info_path, info_filename)

            # 遍历所有筛选出的行
            for row in range(start_row, last_row + 1):
                try:
                    # 检查是否为筛选后的可见行
                    cell_value = ws.range(f'{chr(64 + filter_column_index)}{row}').value
                    if cell_value in filter_values:
                        # 获取订单编号
                        order_id = cell_value

                        # 从base_df获取该订单的所有SKU信息
                        order_skus = use_info_df[use_info_df['订单编号'] == order_id]
                        # 获取当前行的SKU ID（如果存在）
                        current_sku_id = None
                        sku_columns = []  # 存储所有匹配的SKU列索引

                        # 第一阶段：收集所有匹配的SKU列
                        for col in range(1, last_used_column + 1):
                            header_value = ws.range(f'{chr(64 + col)}{header_row}').value
                            if header_value:
                                header_str = str(header_value).strip().upper()
                                if re.search(r'(?:^|_|\s)(?:P|V)?SKU[\s_-]?ID', header_str):
                                    sku_columns.append(col)
                                    logging.info(f"发现潜在SKU列[{header_value}]")
                        # 第二阶段：按顺序检查有效值
                        current_sku_ids = []
                        for col in sku_columns:
                            cell_value = ws.range(f'{chr(64 + col)}{row}').value
                            if pd.notnull(cell_value) and str(cell_value).strip() != '':
                                # 新增分割逻辑
                                raw_values = str(cell_value).split(',')
                                for raw_value in raw_values:
                                    cleaned_value = raw_value.strip()
                                    if cleaned_value:
                                        current_sku_ids.append(cleaned_value)
                                        logging.info(f"发现有效SKU值: {cleaned_value}")
                                if current_sku_ids:
                                    logging.info(f"使用非空SKU列[{ws.range(f'{chr(64 + col)}{header_row}').value}]")
                                    break
                        # 最终检查
                        if not current_sku_ids:
                            logging.error("所有匹配的SKU列均为空值，订单编号: %s", order_id)
                            continue

                        # 如果找到了SKU ID，则进一步筛选匹配的SKU
                        # print('filtered_skus',order_skus['SKU_ID'].head(3))
                        # has_valid_sku = False
                        # for current_sku_id in current_sku_ids:
                        #     filtered_skus = order_skus[order_skus['SKU_ID'] == current_sku_id]
                        #     if not filtered_skus.empty:
                        #         logging.info(f"通过订单编号和SKU ID联合匹配到SKU: {current_sku_id}")
                        #         order_skus = filtered_skus
                        #         has_valid_sku = True
                        #     else:
                        #         logging.warning(f"通过订单编号和SKU ID联合匹配不到SKU: {current_sku_id}")
                        #         continue
                        # if not has_valid_sku:
                        #     continue
                        # print('filtered_skus',order_skus['SKU_ID'])
                        # 获取当前行的店铺账号
                        shop_account = None
                        for col in range(1, last_used_column + 1):
                            header_value = ws.range(f'{chr(64 + col)}{header_row}').value
                            if header_value and '店铺' in str(header_value):
                                shop_account = ws.range(f'{chr(64 + col)}{row}').value
                                break
                        shop_id = None
                        for col in range(1, last_used_column + 1):
                            header_value = ws.range(f'{chr(64 + col)}{header_row}').value
                            if header_value and '店铺ID' in str(header_value):
                                shop_id = ws.range(f'{chr(64 + col)}{row}').value
                                break
                        # 获取币种信息
                        country_m = 'CNY'  # 默认币种
                        site = 'sg'  # 设置默认值
                        # if shop_account and shop_account in shop_info_df['店铺账号'].values:
                        #     shop_row = shop_info_df[shop_info_df['店铺账号'] == shop_account]
                        #     if not shop_row.empty:
                        #         # 处理站点信息
                        #         if '站点' in shop_row.columns and not shop_row['站点'].empty:
                        #             site_value = shop_row['站点'].values[0]
                        #             site = str(site_value).lower() if pd.notnull(site_value) else 'sg'
                        #         # 处理币种信息
                        #         if '外币币种' in shop_row.columns and not shop_row['外币币种'].empty:
                        #             currency_value = shop_row['外币币种'].values[0]
                        #             country_m = str(currency_value) if pd.notnull(currency_value) else 'CNY'
                        if shop_id and shop_id in shop_info_df['店铺ID'].values:
                            shop_row = shop_info_df[shop_info_df['店铺ID'] == shop_id]
                            if not shop_row.empty:
                                # 处理站点信息
                                if '站点' in shop_row.columns and not shop_row['站点'].empty:
                                    site_value = shop_row['站点'].values[0]
                                    site = str(site_value).lower() if pd.notnull(site_value) else'sg'
                                # 处理币种信息
                                if '外币币种' in shop_row.columns and not shop_row['外币币种'].empty:
                                    currency_value = shop_row['外币币种'].values[0]
                                    country_m = str(currency_value) if pd.notnull(currency_value) else 'CNY'
                        country = site  # 此时 site 必定已初始化
                        if site in ['th', 'vn']:
                            country_m = 'CNY'
                        # 获取当前行的pcs数量
                        pcs = 1  # 默认值
                        for col in range(1, last_used_column + 1):
                            header_value = ws.range(f'{chr(64 + col)}{header_row}').value
                            # print(header_value)
                            if header_value and (
                                    'Sold Qty' in str(header_value) or 'SKU销售出库数' in str(header_value)):
                                pcs_value = ws.range(f'{chr(64 + col)}{row}').value
                                pcs = int(pcs_value)
                                # print('pcsis', pcs)
                                break

                        # 为每个SKU添加申诉理由和信息
                        if order_skus.empty:
                            logging.warning(f"订单 {order_id} 没有找到匹配的SKU信息")
                            continue

                        for sku_index, current_sku_id in enumerate(current_sku_ids, 1):
                            # 从 order_skus 中筛选出该 SKU 的信息
                            filtered_sku = order_skus[order_skus['SKU_ID'] == current_sku_id]
                            if filtered_sku.empty:
                                logging.warning(f"未找到匹配的SKU信息: {current_sku_id}")
                                continue
                            # 获取该SKU的信息
                            sku_row = filtered_sku.iloc[0]  # 取第一条记录
                            sku_id = getattr(sku_row, 'SKU_ID')
                            length = getattr(sku_row, '长_CM')
                            width = getattr(sku_row, '宽_CM')
                            height = getattr(sku_row, '高_CM')
                            weight_g = getattr(sku_row, '重量_G')
                            weight_g = weight_g / 1000
                            # 计算最长边
                            len_max = max(length, width, height)
                            # 获取国家/站点信息
                            country = site if site else 'sg'  # 默认为sg
                            actual_fee = sku_row['操作费-平台账单/原币']
                            # 计算正确的操作费
                            # handling_fee = calculate_handling_fee(country, length, width, height, weight_g, pcs) # 25年3月前
                            handling_fee =sku_row['应付操作费/原币']
                            # 生成申诉理由
                            reason = reason_maker(sku_id, pcs, len_max, actual_fee, handling_fee, country_m)
                            # 计算该SKU应写入的列位置
                            next_col = last_used_column + 1 + (sku_index - 1) * 6
                            # 写入申诉理由和SKU信息
                            ws.cells(row, next_col).value = reason
                            ws.cells(row, next_col + 1).value = sku_id
                            ws.cells(row, next_col + 2).value = length
                            ws.cells(row, next_col + 3).value = width
                            ws.cells(row, next_col + 4).value = height
                            ws.cells(row, next_col + 5).value = weight_g

                            # 生成列标题
                            ws.cells(header_row, next_col).value = f'申诉理由{sku_index}'
                            ws.cells(header_row, next_col + 1).value = f'SKU ID'
                            ws.cells(header_row, next_col + 2).value = f'长 CM'
                            ws.cells(header_row, next_col + 3).value = f'宽 CM'
                            ws.cells(header_row, next_col + 4).value = f'高 CM'
                            ws.cells(header_row, next_col + 5).value = f'重量 KG'
                        reason_written = False
                        for col_offset in range(0, len(current_sku_ids) * 6, 6):
                            reason_col = last_used_column + 1 + col_offset
                            if ws.cells(row, reason_col).value:
                                reason_written = True
                                break
                        if not reason_written:
                            clear_range = f'A{row}:{last_column_letter}{row}'
                            try:
                                ws.range(clear_range).color = None  # 清除背景色
                                logging.info(f"已清除行 {row} 的背景色，因为未写入申诉理由")
                            except Exception as clear_error:
                                logging.warning(f"清除行 {row} 背景色时出错: {clear_error}")
                except Exception as e:
                    logging.warning(f"处理第 {row} 行时出错: {e}")
                    import traceback
                    logging.error(traceback.format_exc())  # 记录详细错误信息
                    continue

            # 保存工作簿
            wb.save()
            logging.info(f"处理完成，已保存到: {output_file_path}")
            logging.info(f"总共着色了 {colored_rows} 行数据")

        except Exception as e:
            logging.error(f"处理工作表时出错: {e}")
            import traceback
            logging.error(traceback.format_exc())

    except Exception as e:
        logging.error(f"处理过程中出错: {e}")
        # 打印详细的错误信息
        import traceback
        logging.error(traceback.format_exc())

    finally:
        # 关闭工作簿和Excel应用程序
        if 'wb' in locals():
            wb.close()
        app.quit()
        logging.info("已关闭Excel应用程序")

    return output_file_path
def default_dl_path(times):
    """获取默认下载路径，保持向后兼容"""
    path_manager = get_path_manager(times)
    return path_manager.base_dir

def get_target_path_for_sbs(times, excel_data, sbs_number):
    """
    根据SBS账单编号获取目标路径，支持新的文件夹结构
    
    Args:
        times: 申诉时间
        excel_data: Excel数据
        sbs_number: SBS账单编号
        
    Returns:
        目标路径
    """
    path_manager = get_path_manager(times)
    logger = logging.getLogger(__name__)
    
    # 尝试使用新的路径结构
    target_path = path_manager.find_path_by_sbs(sbs_number, excel_data)
    
    if target_path and os.path.exists(target_path):
        logger.info(f"使用现有路径: {target_path}")
        return target_path
    
    # 如果没有找到现有路径，尝试创建新的路径结构
    if excel_data is not None and '主运营' in excel_data.columns and 'SBS账单编号' in excel_data.columns:
        matching_rows = excel_data[excel_data['SBS账单编号'].astype(str).str.strip() == str(sbs_number).strip()]
        if not matching_rows.empty:
            operator = str(matching_rows.iloc[0]['主运营']).strip()
            if operator and operator != 'nan':
                try:
                    target_path = path_manager.create_directory_structure(operator, sbs_number)
                    logger.info(f"创建新路径结构: {target_path}")
                    return target_path
                except Exception as e:
                    logger.error(f"创建新路径结构失败: {e}")
    
    # 回退到旧的路径结构
    old_path = path_manager.get_old_sbs_path(sbs_number)
    os.makedirs(old_path, exist_ok=True)
    logger.info(f"使用旧路径结构: {old_path}")
    return old_path
def find_directories_without_keyword(path, keyword):
    # 检查路径是否存在
    if not os.path.exists(path):
        print(f"错误：路径 '{path}' 不存在")
        return {}
    base_path = os.path.abspath(path)
    result_list = []  # 存储结果的列表
    for root, dirs, files in os.walk(base_path):
        has_keyword_file = False
        for file in files:
            if keyword in file:
                has_keyword_file = True
                break
        if not has_keyword_file and files:  # 如果目录里有文件且都不包含关键字
            # 获取父目录名和当前目录名
            parent_dir = os.path.basename(os.path.dirname(root))
            current_dir = os.path.basename(root)
            # 将父目录名和当前目录名作为独立字段记录
            result_list.append({
                "父目录名": parent_dir,
                "当前目录名": current_dir
            })
    return result_list  # 返回结果列表
def save_missing_data_to_csv(results_background, results_length, output_path):
    """
    将结果保存到CSV文件。

    :param results_background: 包含“无后台截图”结果的字典
    :param results_length: 包含“无尺寸图”结果的字典
    :param output_path: 输出CSV文件的路径
    """
    # 合并所有可能的目录名称作为索引
    all_directories = set(
        (item["父目录名"], item["当前目录名"]) for item in results_background + results_length
    )


    # 构建DataFrame
    data = []
    for parent_dir, current_dir in sorted(all_directories):
        has_background = '是' if any(item["父目录名"] == parent_dir and item["当前目录名"] == current_dir for item in results_background) else ''
        has_length = '是' if any(item["父目录名"] == parent_dir and item["当前目录名"] == current_dir for item in results_length) else ''
        data.append({
            "父目录名": parent_dir,
            "当前目录名": current_dir,
            "无后台截图": has_background,
            "无尺寸图": has_length
        })

    df = pd.DataFrame(data)

    # 保存为CSV文件
    output_file = os.path.join(output_path, "资料缺失表.csv")
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"结果已保存至 {output_file}")
def main(times, o_data_path, info_path, info_filename, sum_data_filename, base_path, base_filename):
    # 全局文件处理模式选择
    print("\n请选择文件处理模式（此选择将应用于所有文件）：")
    file_mode = input("1. 覆盖模式（删除现有文件重新处理）\n2. 补充模式（跳过已存在的文件）\n请输入选择（1或2）: ").strip()
    if file_mode not in ["1", "2"]:
        print(f"无效选择 '{file_mode}'，默认使用覆盖模式")
        file_mode = "1"
    
    mode_desc = "覆盖模式" if file_mode == "1" else "补充模式"
    print(f"已选择: {mode_desc}\n")
    
    # 获取路径管理器和日志
    logger = logging.getLogger(__name__)
    path_manager = get_path_manager(times)
    
    base_df = base_info(base_path, base_filename)
    sbs_ids = base_df['SBS账单编号'].drop_duplicates().tolist()
    
    # 尝试读取申诉信息材料文件以获取主运营信息
    excel_data = None
    try:
        material_file = path_manager.get_material_file_path()
        if os.path.exists(material_file):
            excel_data = pd.read_excel(material_file)
            logger.info(f"成功读取申诉信息材料文件，共{len(excel_data)}行")
        else:
            logger.warning(f"申诉信息材料文件不存在: {material_file}")
    except Exception as e:
        logger.warning(f"读取申诉信息材料文件失败: {e}")
    
    # sum
    sum_df = sum_data_info(info_path, sum_data_filename)
    i = 1
    for id in sbs_ids:
        print(f"正在处理第{i}个账单: {id}")
        i += 1
        todo_file = sum_df["来源文件"][sum_df['收费报告 ID'] == id]
        if not todo_file.empty:
            print(f"正在处理文件:{todo_file}")
            file_path = os.path.join(o_data_path, todo_file.values[0])
            use_info_df = base_df[base_df['SBS账单编号'] == id].copy()
            
            # 使用新的路径结构
            output_dir = get_target_path_for_sbs(times, excel_data, id)
            logger.info(f"SBS账单编号 {id} 的输出目录: {output_dir}")
            
            os.makedirs(output_dir, exist_ok=True)
            filter_column_name = "订单编号"  # 筛选列名
            filter_values = use_info_df['订单编号'].drop_duplicates().tolist()  # 筛选值列表
            
            try:
                result = process_excel_file(file_path, filter_column_name, filter_values, use_info_df, output_dir, file_mode)
                if result:
                    file_name = os.path.basename(file_path)
                    output_file_path = os.path.join(output_dir, file_name)
                    print(f"处理完成，已保存到: {output_file_path}")
                    logger.info(f"申诉材料处理完成: {output_file_path}")
                else:
                    print(f"缺失账单文件，已记录，跳过: {file_path}")
                    logger.warning(f"缺失账单文件，跳过: {file_path}")
            except Exception as e:
                print(f"处理文件失败，跳过: {file_path} | 错误: {e}")
                logger.warning(f"处理文件失败（已跳过）{file_path}: {e}")
# 示例调用
if __name__ == "__main__":
    mod = input("请输入处理模式：1.编写申诉材料 2.补充申诉资料")
    times = input("请输入本次申诉信息材料上的时间，格式为YYYY.MM.DD：")
    
    if mod == "1":
        if times == "":
            times = datetime.datetime.now().strftime("%Y.%m.%d")
        
        # 设置路径日志
        setup_path_logging(times)
        logger = logging.getLogger(__name__)
        
        # 使用路径管理器
        path_manager = get_path_manager(times)
        
        logger.info(f"开始处理申诉材料生成，时间: {times}")
        
        o_data_path = r"\\Auteng\综合管理部\综合管理部_公共\12.数据收集\16.头程和海外仓费用账单\2.SBS费用报告shopee\SBS费用报告源数据-滚动更新"
        info_path = r"\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK"
        info_filename = "SBS-数据说明.xlsx"
        sum_data_filename = input("请输入本次费用账单汇总文件时间如(202503)：")
        # 如果格式不对，则使用前两个月的数据
        if not re.match(r'\d{6}', sum_data_filename):
            now = datetime.datetime.now()
            month = now.month
            year = now.year
            if month > 2:
                two_months_ago = now.replace(month=month - 2)
            else:
                two_months_ago = now.replace(year=year - 1, month=12 + month - 2)
            sum_data_filename = two_months_ago.strftime("%Y%m")
        sum_data_filename = f"SBS-费用账单汇总{sum_data_filename}.xlsx"
        print(f"正在处理{sum_data_filename}文件")
        
        # 使用路径管理器获取文件路径
        base_path = path_manager.MATERIAL_PATH
        base_filename = f"{times}申诉信息材料.xlsx"
        
        logger.info(f"申诉信息材料文件路径: {os.path.join(base_path, base_filename)}")
        logger.info(f"输出目录: {path_manager.base_dir}")
        
        main(times, o_data_path, info_path, info_filename, sum_data_filename, base_path, base_filename)
    elif mod == "2":
        if times == "":
            times = datetime.datetime.now().strftime("%Y.%m.%d")
        
        # 设置路径日志
        setup_path_logging(times)
        logger = logging.getLogger(__name__)
        
        # 使用路径管理器
        path_manager = get_path_manager(times)
        path = path_manager.base_dir
        
        logger.info(f"开始补充申诉资料，时间: {times}")
        logger.info(f"工作目录: {path}")
        
        # 尝试读取申诉信息材料文件以获取路径结构信息
        excel_data = None
        try:
            material_file = path_manager.get_material_file_path()
            if os.path.exists(material_file):
                excel_data = pd.read_excel(material_file)
                logger.info(f"成功读取申诉信息材料文件，共{len(excel_data)}行")
            else:
                logger.warning(f"申诉信息材料文件不存在: {material_file}")
        except Exception as e:
            logger.warning(f"读取申诉信息材料文件失败: {e}")
        
        # 获取两个关键字的结果
        background_results = find_directories_without_keyword(path, "后台")
        length_results = find_directories_without_keyword(path, "长")
        # 保存结果到CSV
        # 预先构建全局文件索引，避免重复遍历提升性能
        print("正在构建文件索引...")
        file_index = {}
        shared_root = path_manager.BASE_PATH
        for root, dirs, files in os.walk(shared_root):
            for file in files:
                # 使用文件名作为键，完整路径作为值
                if file not in file_index:  # 只记录第一个找到的文件
                    file_index[file] = os.path.join(root, file)
        print(f"文件索引构建完成，共索引 {len(file_index)} 个文件")
        # 处理后台截图缺失
        processed_background = []
        for item in background_results:
            parent_dir = item["父目录名"]
            current_dir = item["当前目录名"]
            file_name = f"{current_dir} 后台尺寸图.png"
            
            # 使用新的路径逻辑确定目标目录
            if excel_data is not None:
                target_base = get_target_path_for_sbs(times, excel_data, parent_dir)
                target_dir = os.path.join(target_base, current_dir)
            else:
                target_dir = os.path.join(path, parent_dir, current_dir)
            
            print(f"正在处理目录: {target_dir}")
            logger.info(f"处理后台截图: {parent_dir}/{current_dir} -> {target_dir}")
            # 使用索引快速查找
            if file_name in file_index:
                source_path = file_index[file_name]
                target_path = os.path.join(target_dir, file_name)
                # 避免自复制
                if os.path.abspath(source_path) != os.path.abspath(target_path):
                    try:
                        os.makedirs(target_dir, exist_ok=True)
                        shutil.copy(source_path, target_dir)
                        print(f"已复制文件 {file_name} 到 {target_dir}")
                        logger.info(f"成功复制: {source_path} -> {target_path}")
                        processed_background.append(item)
                    except Exception as e:
                        logging.error(f"复制文件失败 {file_name}: {e}")
                else:
                    logging.warning(f"跳过自复制文件: {file_name}")
                    processed_background.append(item)
            else:
                logging.warning(f"未找到文件: {file_name}")
        # 处理尺寸图缺失
        processed_length = []
        for item in length_results:
            parent_dir = item["父目录名"]
            current_dir = item["当前目录名"]
            
            # 使用新的路径逻辑确定目标目录
            if excel_data is not None:
                target_base = get_target_path_for_sbs(times, excel_data, parent_dir)
                target_dir = os.path.join(target_base, current_dir)
            else:
                target_dir = os.path.join(path, parent_dir, current_dir)
            
            print(f"正在处理目录: {target_dir}")
            logger.info(f"处理尺寸图: {parent_dir}/{current_dir} -> {target_dir}")
            item_processed = False
            # 批量处理所有尺寸图文件
            for suffix in [' 长.png', ' 宽.png', ' 高.png', ' 重量.png']:
                file_name = current_dir + suffix
                if file_name in file_index:
                    source_path = file_index[file_name]
                    target_path = os.path.join(target_dir, file_name)
                    if os.path.abspath(source_path) != os.path.abspath(target_path):
                        try:
                            os.makedirs(target_dir, exist_ok=True)
                            shutil.copy(source_path, target_dir)
                            print(f"已复制文件 {file_name} 到 {target_dir}")
                            logger.info(f"成功复制: {source_path} -> {target_path}")
                            item_processed = True
                        except Exception as e:
                            logging.error(f"复制文件失败 {file_name}: {e}")
                    else:
                        logging.warning(f"跳过自复制文件: {file_name}")
                        item_processed = True
                else:
                    logging.warning(f"未找到文件: {file_name}")
            # 标记该目录已处理（只要有任何一个文件被处理）
            if item_processed and item not in processed_length:
                processed_length.append(item)
        # 更新结果列表，移除已处理的项目
        background_results = [item for item in background_results if item not in processed_background]
        length_results = [item for item in length_results if item not in processed_length]
        print(f"处理完成：后台截图处理 {len(processed_background)} 项，尺寸图处理 {len(processed_length)} 项")
        print(f"剩余未处理：后台截图 {len(background_results)} 项，尺寸图 {len(length_results)} 项")
        
        logger.info(f"补充申诉资料处理完成")
        logger.info(f"详细日志保存在: {path_manager.get_log_file_path('files_fix')}")
        
        # 保存缺失数据
        save_missing_data_to_csv(background_results, length_results, path)
    else:
        print("请输入正确的模式")
