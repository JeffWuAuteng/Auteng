import os
import time
import re
import pandas as pd
import numpy as np
import openpyxl
import logging
import datetime
import shutil
from pathlib import Path
from collections import defaultdict
# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def shopee_cost():
    if True:
        savepath = r'\\Auteng\hot_data\中间结果'
        sale_data = pd.read_csv(savepath + "\日报数据\数据汇总\绩效核算基础数据全字段版.csv")
        # 统一解析时间列，避免字符串与Timestamp比较导致的报错
        if '下单时间' in sale_data.columns:
            sale_data['下单时间'] = pd.to_datetime(sale_data['下单时间'], errors='coerce', format='mixed')
        for col in ['发货时间', '付款时间', '退款时间', '回款时间', '平台刊登时间']:
            if col in sale_data.columns:
                sale_data[col] = pd.to_datetime(sale_data[col], errors='ignore', format='mixed')
        sale_data[pd.isna(sale_data['平台刊登时间'])][['产品ID', '店铺账号']].drop_duplicates()
        filepathres = r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\汇总结果\\"
        resfile = r"\\AUTENG\hot_data\数据处理\报表输出\订单核对\订单核算数据最新.csv"
        timetange = ["2025", ""]
        data_income_all_time = pd.to_datetime(
            time.ctime(os.path.getmtime(filepathres + timetange[0] + "各店铺拨款账单income.csv")))
        resfile_time = pd.to_datetime(time.ctime(os.path.getmtime(resfile)))
        if data_income_all_time > resfile_time:
            data_income_all = pd.read_csv(filepathres + timetange[0] + "各店铺拨款账单income.csv", encoding="utf-8-sig")
            data_adjust_all = pd.read_csv(filepathres + timetange[0] + "各店铺拨款账单adjust.csv", encoding="utf-8-sig")
            sale_data = sale_data.merge(data_income_all[["订单编号", "拨款金额", "店铺名称", "文件", "拨款批次", "拨款完成日期1","优惠码", '卖家优惠券金额']].drop_duplicates(
                subset=["订单编号", "拨款金额"]), left_on="订单号", right_on="订单编号", how="outer")
            data_adjust = data_adjust_all[data_adjust_all['调整维度'].str.contains("order")]
            data_adjust_table = pd.pivot_table(data_adjust, index='订单编号',
                                               values=['调整金额', '费用类型', '调整场景'], aggfunc="sum")
            sale_data = sale_data.merge(data_adjust_table, left_on="订单号", right_on="订单编号", how="outer")
            sale_data['店铺名称'] = np.where(pd.isna(sale_data['店铺名称_x']), sale_data['店铺名称_y'],sale_data['店铺名称_x'])
            sale_data['文件'] = np.where(pd.isna(sale_data['文件_x']), sale_data['文件_y'],sale_data['文件_x'])
            # 合并后补充解析拨款完成日期等时间列
            for col in ['拨款完成日期1']:
                if col in sale_data.columns:
                    sale_data[col] = pd.to_datetime(sale_data[col], errors='coerce', format='mixed')
            sale_data_backup = sale_data[
                ['商品SKU', 'SKU', '商品名称', '商品编码', '库存价格', '商品采购价', '称重重量', '产品数量', '订单号',
                 '下单时间', '发货时间', '付款时间', '退款时间', '退款金额', '产品售价', '订单状态', '店铺账号',
                 '币种缩写',
                 '平台渠道', '平台店铺ID', '收货人国家', '物流方式', '产品ID', 'ParentSKU', '预估利润', '销售利润率',
                 '订单金额', '买家支付运费', '平台刊登时间', '店铺', '汇率按店铺', '行销售额', '行销售额_售价',
                 '订单销售额', '行利润', '行退款金额', '行采购成本', '核算采购价格', '核算商品重量', '订单核算商品重量',
                 '组合订单序号',
                 '订单产品数', '成本缺失', '损失分类', '拒收损失', '仅退款费用', '行头程费用AC', '分类ID', '长度',
                 '宽度',
                 '高度', '体积', '回款金额RMB', '回款状态', '回款时间', '原始回款金额', '结算金额L', '调整金额RMB',
                 '订单标识', '订单分类', '海外仓渠道', "海外仓操作费分型", "海外仓操作费核对原币种",
                 "海外仓基础操作费原币种", "海外仓多件操作费原币种",
                 '退款理由', '修正利润', '调整金额RMB去重', "订单编号", "拨款金额", "店铺名称", "文件", "拨款批次",
                 "拨款完成日期1", "优惠码", '卖家优惠券金额', '调整金额', '费用类型', '调整场景']]
            # sale_data_backup[(sale_data_backup["下单时间"]<pd.to_datetime(datetime.date(2025,4,1)))&(sale_data_backup["下单时间"]>=pd.to_datetime(datetime.date(2025,1,1)))].to_csv(r"\\AUTENG\hot_data\数据处理\报表输出\订单核对\订单核算数据202501-03.csv",index=False)
            sale_data_backup[(sale_data_backup["下单时间"] >= pd.to_datetime(sale_data["下单时间"].max() - datetime.timedelta(120)))].to_csv(r"\\AUTENG\hot_data\数据处理\报表输出\订单核对\订单核算数据最新.csv", index=False)
            sale_data_backup[(sale_data_backup["下单时间"]<pd.to_datetime(datetime.date(2025,10,1)))&(sale_data_backup["下单时间"]>=pd.to_datetime(datetime.date(2025,6,1)))].to_csv(r"\\AUTENG\hot_data\数据处理\报表输出\订单核对\订单核算数据202507-09.csv",index=False)
            sale_data_backup[(sale_data_backup["下单时间"]<pd.to_datetime(datetime.date(2026,1,1)))&(sale_data_backup["下单时间"]>=pd.to_datetime(datetime.date(2025,10,1)))].to_csv(r"\\AUTENG\hot_data\数据处理\报表输出\订单核对\订单核算数据202510-12.csv",index=False)

    if True:
        sale_data[(sale_data["下单时间"] < pd.to_datetime(datetime.date(2025, 4, 1))) & (
                    sale_data["下单时间"] >= pd.to_datetime(datetime.date(2025, 1, 1)))].to_csv(savepath + "\日报数据\数据汇总\sale_data202401-03.csv", index=False)
        sale_data[(sale_data["下单时间"] < pd.to_datetime(datetime.date(2025, 7, 1))) & (
                    sale_data["下单时间"] >= pd.to_datetime(datetime.date(2025, 4, 1)))].to_csv(savepath + "\日报数据\数据汇总\sale_data202404-06.csv", index=False)
        sale_data[(sale_data["下单时间"] >= pd.to_datetime(datetime.date(2025, 7, 1)))].to_csv(savepath + "\日报数据\数据汇总\sale_data202407-最新.csv", index=False)

        # sale_data[(sale_data["下单时间"]<pd.to_datetime(datetime.date(2024,10,1)))&(sale_data["下单时间"]>=pd.to_datetime(datetime.date(2024,7,1)))].to_csv(savepath+"\日报数据\数据汇总\sale_data202407-09.csv",index=False)
        # sale_data[(sale_data["下单时间"]<pd.to_datetime(datetime.date(2025,1,1)))&(sale_data["下单时间"]>=pd.to_datetime(datetime.date(2024,10,1)))].to_csv(savepath+"\日报数据\数据汇总\sale_data202410-12.csv",index=False)
def sanitize_filename(name: str) -> str:
    # 替换 Windows 不允许的文件名字符
    return re.sub(r'[<>:"/\\|?*]', '_', name).strip()
def read_file_with_header_info(file_path):
    header_info = {}
    with open(file_path, 'r', encoding='utf-8') as file:
        for _ in range(6):
            line = file.readline().strip()
            parts = line.split(',', 1)  # 最多分割一次
            key = str(parts[0]).strip() if parts else ""
            # 处理值：有第二部分则取第二部分，否则置空
            value = str(parts[1]).strip() if len(parts) > 1 else ""
            header_info[key] = value
    df = pd.read_csv(file_path, header=6, encoding='utf-8')
    if '时间' in header_info and ' - ' in header_info['时间']:
        start_date, end_date = header_info['时间'].split(' - ', 1)
        header_info['期初'] = start_date.strip()
        header_info['期末'] = end_date.strip()
        del header_info['时间']  # 移除原始时间字段
    key_columns = ['商店名称', '商店ID', '期初', '期末']
    insert_data = {k: header_info[k] for k in key_columns if k in header_info}
    for col_name, col_value in reversed(list(insert_data.items())):
        df.insert(0, col_name, col_value)
    return header_info, df

def find_shop_file(dir_path):
    # 在指定目录下寻找相同店铺的文件
    # 按照-分割文件名，取最后一个字段作为店铺名称，先将文件类型分割出来再提取店铺名
    # 找到相同店铺的文件名后将文件路径保存到一个列表中输出
    shop_files = defaultdict(list)
    all_dates = set()
    missing_df = pd.DataFrame(columns=['商店名称', '期初', '期末'])
    missing_records = []

    # 新增：日志文件处理部分
    log_dir = r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\广告分析汇总数据'
    log_files = [f for f in os.listdir(log_dir) if f.startswith('站内广告下载日志') and f.endswith('.csv')]
    log_df_list = []

    for log_file in log_files:
        log_path = os.path.join(log_dir, log_file)
        try:
            # 读取日志文件并处理日期格式
            temp_df = pd.read_csv(log_path, encoding='utf-8-sig')
            # 统一日期格式为YYYY/M/D
            if '期初' in temp_df.columns:
                temp_df['期初'] = pd.to_datetime(temp_df['期初']).dt.strftime('%Y/%m/%d')
            if '期末' in temp_df.columns:
                temp_df['期末'] = pd.to_datetime(temp_df['期末']).dt.strftime('%Y/%m/%d')
            log_df_list.append(temp_df)
        except Exception as e:
            logging.error(f"读取日志文件 {log_file} 时出错: {str(e)}")

    # 合并所有日志文件
    if log_df_list:
        combined_log_df = pd.concat(log_df_list, ignore_index=True)
        # 去重处理
        combined_log_df = combined_log_df.drop_duplicates()
        logging.info(f"成功合并 {len(log_df_list)} 个日志文件，共 {len(combined_log_df)} 条记录")
    else:
        combined_log_df = pd.DataFrame(columns=['商店ID', '商店名称', '店铺主体', '期初', '期末'])
        logging.warning("未找到任何日志文件")

    # 创建用于标记已处理记录的集合
    processed_records = set()
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        if os.path.isfile(file_path):
            name_without_ext = os.path.splitext(filename)[0]
            parts = name_without_ext.split('-')
            if len(parts) > 3:  # 至少需要有前缀、数据类型、日期和店铺名
                date_match = re.search(r'(\d{4}_\d{2}_\d{2}-\d{4}_\d{2}_\d{2})', filename)
                if date_match:
                    date_range = date_match.group(1)
                    all_dates.add(date_range)
                    shop_name = parts[-1].strip()
                    shop_files[shop_name].append((file_path, date_range))

                    # 添加到已处理记录
                    start_date, end_date = date_range.split('-')
                    start_date_fmt = start_date.replace('_', '/')
                    end_date_fmt = end_date.replace('_', '/')
                    processed_records.add((shop_name, start_date_fmt, end_date_fmt))

    shop_file_counts = {shop: len(files) for shop, files in shop_files.items()}
    max_files = max(shop_file_counts.values()) if shop_file_counts else 0
    logging.warning(f"每个店铺应下载的文件数量: {max_files}")

    # 删除已处理的记录
    if not combined_log_df.empty:
        def is_processed(row):
            key = (
                row['商店名称'],
                row['期初'],
                row['期末']
            )
            return key in processed_records

        # 过滤掉已处理的记录
        updated_log_df = combined_log_df[~combined_log_df.apply(is_processed, axis=1)]

        # 保存更新后的日志文件
        updated_log_path = os.path.join(log_dir, '站内广告下载日志_更新.csv')
        updated_log_df.to_csv(updated_log_path, index=False, encoding='utf-8-sig')
        logging.info(f"已更新下载日志文件，删除了{len(combined_log_df) - len(updated_log_df)}条已处理记录")
    else:
        logging.warning("没有有效的日志记录可处理")

    # 原有缺失记录处理逻辑保持不变
    for shop_name, files in shop_files.items():
        if len(files) < max_files:
            logging.info(f"店铺 '{shop_name}' 的文件数量不足({len(files)}/{max_files})")
            shop_dates = set(date for _, date in files)
            missing_dates = all_dates - shop_dates
            if missing_dates:
                logging.info(f"店铺 '{shop_name}' 缺少的日期段: {', '.join(sorted(missing_dates))}")
                # 将店铺缺少的期初和期末添加到missing_df中
                for date in missing_dates:
                    start, end = date.split('-')
                    missing_records.append({'商店名称': shop_name,'期初': start,'期末': end})
    if missing_records:
        missing_df = pd.DataFrame(missing_records)

    return shop_files, missing_df

def get_dpqd_info(dpqd_info_file):
    df_dpqd_info = pd.read_excel(dpqd_info_file, sheet_name="店铺清单")
    df_dpqd_info['店铺ID'] = df_dpqd_info['店铺id'].astype(str)
    df_dpqd_info['平台店铺'] = df_dpqd_info['平台店铺账号'].astype(str)
    df_dpqd_info['店铺账号'] = df_dpqd_info['店铺账号'].fillna(df_dpqd_info['复制店铺账号']).astype(str)
    df_dpqd_info = df_dpqd_info[['店铺ID', '平台店铺', '站点', '店铺账号', '店铺']]
    unique_dp = df_dpqd_info[['店铺ID', '平台店铺']]
    unique_dp = unique_dp[unique_dp['店铺ID'].notna() & unique_dp['平台店铺'].notna()].drop_duplicates('店铺ID')
    unique_dp = unique_dp.drop_duplicates('店铺ID', keep='first')
    # 确保没有NaN值
    unique_dp = unique_dp.dropna()
    if not unique_dp.empty:
        dp_mapping = unique_dp.set_index('店铺ID')['平台店铺'].astype(str).str.strip()
    else:
        dp_mapping = pd.Series(dtype=str)
    
    unique_dpname = df_dpqd_info[['店铺ID', '平台店铺']]
    unique_dpname = unique_dpname[unique_dpname['平台店铺'].notna() & unique_dpname['店铺ID'].notna()].drop_duplicates('平台店铺')
    unique_dpname = unique_dpname.drop_duplicates('平台店铺', keep='first')
    # 确保没有NaN值
    unique_dpname = unique_dpname.dropna()
    if not unique_dpname.empty:
        dpname_mapping = unique_dpname.set_index('平台店铺')['店铺ID'].astype(str).str.strip()
    else:
        dpname_mapping = pd.Series(dtype=str)
    
    unique_dpzh = df_dpqd_info[['店铺ID', '店铺账号']]
    unique_dpzh = unique_dpzh[unique_dpzh['店铺账号'].notna() & unique_dpzh['店铺ID'].notna()].drop_duplicates('店铺账号')
    unique_dpzh = unique_dpzh.drop_duplicates('店铺账号', keep='first')
    # 确保没有NaN值
    unique_dpzh = unique_dpzh.dropna()
    if not unique_dpzh.empty:
        dpzhname_mapping = unique_dpzh.set_index('店铺账号')['店铺ID'].astype(str).str.strip()
    else:
        dpzhname_mapping = pd.Series(dtype=str)
    # 店组和店铺ID映射
    unique_dzname = df_dpqd_info[['店铺ID', '店铺']]
    unique_dzname = unique_dzname[unique_dzname['店铺'].notna() & (unique_dzname['店铺'] != '0') & unique_dzname['店铺ID'].notna()].drop_duplicates('店铺ID')
    unique_dzname = unique_dzname.drop_duplicates('店铺ID', keep='first')
    # 确保没有NaN值
    unique_dzname = unique_dzname.dropna()
    if not unique_dzname.empty:
        dzname_mapping = unique_dzname.set_index('店铺ID')['店铺'].astype(str).str.strip()
    else:
        dzname_mapping = pd.Series(dtype=str)
    # 站点和店铺ID映射
    unique_zd = df_dpqd_info[['站点', '店铺ID']]
    unique_zd = unique_zd[unique_zd['站点'].notna() & (unique_zd['站点'] != '0') & unique_zd['店铺ID'].notna()]
    unique_zd = unique_zd.drop_duplicates('店铺ID', keep='first')
    # 确保没有NaN值
    unique_zd = unique_zd.dropna()
    if not unique_zd.empty:
        zdname_mapping = unique_zd.set_index('店铺ID')['站点'].astype(str).str.strip()
    else:
        zdname_mapping = pd.Series(dtype=str)
    return dp_mapping, dpname_mapping, dpzhname_mapping, dzname_mapping,zdname_mapping
def merge_and_export_shop_files(dir_path):
    # 设置输出目录
    output_dir = r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\广告分析汇总数据'
    os.makedirs(output_dir, exist_ok=True)
    # 获取店铺文件分组
    shop_files, missing_df = find_shop_file(dir_path)
    # 处理每个店铺的文件
    for shop_name, file_list in shop_files.items():
        try:
            # 初始化店铺的DataFrame列表
            shop_dfs = []
            # 处理每个文件
            for file_path, _ in file_list:
                try:
                    # 读取文件并获取数据
                    header_info, df = read_file_with_header_info(file_path)
                    # 这里用正确的店铺名称 和 期初期末数据去重命名读取的文件
                    start_date = str(header_info.get('期初', '')).replace('/', '_').replace('-', '_')
                    end_date = str(header_info.get('期末', '')).replace('/', '_').replace('-', '_')
                    shop_real_name = str(header_info.get('商店ID', '')).strip()
                    # 执行重命名（同目录下），若规范文件已存在则从数据2开始递增编号规避重复
                    dir_of_file = os.path.dirname(file_path)
                    counter = 1
                    rename_needed = True
                    while True:
                        num_str = "" if counter == 1 else str(counter)
                        ad_file_name = f"Shopee广告-整体-数据{num_str}-{start_date}-{end_date}-{shop_real_name}"
                        ad_file_name = sanitize_filename(ad_file_name)
                        new_file_path = os.path.join(dir_of_file, f"{ad_file_name}.csv")
                        # 若目标名不存在，或当前文件已是该目标名，则跳出
                        if not os.path.exists(new_file_path):
                            break
                        if os.path.abspath(new_file_path) == os.path.abspath(file_path):
                            rename_needed = False
                            break
                        if counter == 1:
                            logging.info("目标文件已存在，开始按顺序增加编号规避重复")
                        counter += 1
                    try:
                        if rename_needed:
                            os.rename(file_path, new_file_path)
                            logging.info(f"已重命名: {file_path} -> {new_file_path}")
                            # 更新处理路径，后续日志/提示更准确
                            file_path = new_file_path
                        else:
                            logging.info(f"文件已是规范命名，无需重命名: {file_path}")
                    except Exception as rename_e:
                        logging.error(f"重命名失败: {file_path} -> {new_file_path}: {str(rename_e)}")

                    shop_dfs.append(df)
                except Exception as e:
                    logging.error(f"处理文件 {file_path} 时出错: {str(e)}")
                    continue
            # 合并所有DataFrame
            if shop_dfs:
                combined_df = pd.concat(shop_dfs, ignore_index=True)
                dpqd_info_file = r'\\Auteng\综合管理部\综合管理部_公共\数据分层处理\3数据处理dwd-库存周报\店铺清单最新.xlsx'
                dp_mapping, dpname_mapping, dpzhname_mapping, dzname_mapping, zdname_mapping = get_dpqd_info(dpqd_info_file)
                # 确保商店ID列不包含空值或NaN值
                combined_df['商店ID'] = combined_df['商店ID'].fillna('').astype(str)
                # 根据商店ID匹配出dzname和zdname
                combined_df['店组'] = combined_df['商店ID'].map(dzname_mapping).fillna('')
                combined_df['站点'] = combined_df['商店ID'].map(zdname_mapping).fillna('')
                # 记录未匹配到的商店ID
                unmapped_ids = combined_df[combined_df['店组'] == '']['商店ID'].unique()
                if len(unmapped_ids) > 0:
                    logging.warning(f"店铺 '{shop_name}' 中有{len(unmapped_ids)}个商店ID未能匹配到店组信息")
                unmapped_site_ids = combined_df[combined_df['站点'] == '']['商店ID'].unique()
                if len(unmapped_site_ids) > 0:
                    logging.warning(f"店铺 '{shop_name}' 中有{len(unmapped_site_ids)}个商店ID未能匹配到站点信息")
                # 输出合并后的DataFrame到CSV
                output_file = os.path.join(output_dir, f'广告分析-{shop_name}.csv')
                combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                logging.info(f"成功输出店铺 '{shop_name}' 的数据到 {output_file}")
            else:
                logging.warning(f"店铺 '{shop_name}' 没有有效的数据文件")
        except Exception as e:
            logging.error(f"处理店铺 '{shop_name}' 时出错: {str(e)}")
    # 输出缺失记录
    if not missing_df.empty:
        missing_df = missing_df.drop_duplicates()
        missing_file = os.path.join(output_dir, '缺失记录.csv')
        missing_df.to_csv(missing_file, index=False, encoding='utf-8-sig')
        logging.info(f"成功输出缺失记录到 {missing_file}")
    return len(shop_files)
def merge_ads_data(dir_path):
    # 查找所有以广告分析开头的csv文件
    files = [f for f in os.listdir(dir_path) if f.startswith('广告分析') and f.endswith('.csv')]
    all_dfs = []
    for file in files:
        file_path = os.path.join(dir_path, file)
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            all_dfs.append(df)
        except Exception as e:
            logging.error(f"读取文件 {file_path} 出错: {str(e)}")
    if not all_dfs:
        logging.warning("未找到任何广告分析相关的csv文件")
        return
    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df = merged_df.drop_duplicates()
    max_rows = 800000
    total_rows = len(merged_df)
    output_dir = dir_path
    if total_rows <= max_rows:
        output_file = os.path.join(output_dir, '站内广告汇总数据.csv')
        merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logging.info(f"已输出 {output_file}，共 {total_rows} 行")
    else:
        num_files = (total_rows // max_rows) + (1 if total_rows % max_rows else 0)
        for i in range(num_files):
            start = i * max_rows
            end = min((i + 1) * max_rows, total_rows)
            part_df = merged_df.iloc[start:end]
            output_file = os.path.join(output_dir, f'站内广告汇总数据_{i+1}.csv')
            part_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logging.info(f"已输出 {output_file}，行数: {end - start}")
    # 按店组列groupby输出
    category_map = {
        "家居": ["家居", "家装"],
        "汽配": ["汽配", "汽车配件"],
        "摩配": ["摩配", "摩托配件"],
        "大生活": ["大生活", "生活用品"]
    }
    target_paths = {
        "家居": r"\\AUTENG\share_files\运营部_家居\公共\产品开发\广告周报源数据",
        "汽配": r"\\AUTENG\share_files\运营部_汽配\公共\产品开发\广告周报源数据",
        "摩配": r"\\AUTENG\share_files\运营部_摩配\公共\产品开发\广告周报源数据",
        "大生活": r"\\AUTENG\share_files\运营部_大生活\公共\产品开发\广告周报源数据",
        "其他": r"\\AUTENG\share_files\运营部_公共\产品开发\广告周报源数据"
    }
    merged_df['category'] = '其他'
    for category, keywords in category_map.items():
        pattern = '|'.join(keywords)
        merged_df.loc[merged_df['店组'].str.contains(pattern, na=False, regex=True), 'category'] = category
    for category, group_df in merged_df.groupby('category'):
        group_df_to_save = group_df.drop(columns=['category'])
        output_path = os.path.join(target_paths[category], f'站内广告汇总数据-{category}.csv')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        group_df_to_save.to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f"已输出 {category} 类别的数据到 {output_path}")
def merge_ams_data(dir_path):
    files = [f for f in os.listdir(dir_path) if f.endswith('.csv')]
    files_path = [os.path.join(dir_path, f) for f in files]
    ams_all_df = pd.DataFrame()
    for file in files_path:
        ams_df = pd.read_csv(file, encoding='utf-8-sig')
        ams_all_df = pd.concat([ams_all_df, ams_df], ignore_index=True)
    return ams_all_df
def merge_ads_zhanwai_data(dir_path):
    pass
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    # 设置输入目录
    data_name = input("请输入要处理的数据名称：\n1.店铺广告数据 \n2.商业分析\n3.AMS数据汇总\n4.订单核算")
    if data_name == '1':
        input_directory = r'\\Auteng\综合管理部\自动化下载文件\站内广告'  # 请替换为实际的输入目录
        # 执行合并和导出操作
        shop_count = merge_and_export_shop_files(input_directory)
        merge_ads_data(r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\广告分析汇总数据')
        logging.info(f"共处理 {shop_count} 个店铺的数据")
    elif data_name == '2':
        input_directory = r'\\Auteng\综合管理部\自动化下载文件\商业分析'
        pass
    elif data_name == '3':
        # 更新最新or往期
        input_directory = r'\\Auteng\综合管理部\自动化下载文件\AMS营销费用'
        choice = input("请选择更新最新or往期：\n1.更新最新\n2.往期")
        if choice == '1':
            # 查找以AMS开头、账单结尾且包含6位连续数字的最大文件夹
            max_folder = None
            max_number = -1
            for folder_name in os.listdir(input_directory):
                folder_path = os.path.join(input_directory, folder_name)
                # 检查是否为文件夹且名称符合要求
                if os.path.isdir(folder_path) and folder_name.startswith('AMS') and '账单' in folder_name:
                    # 使用正则提取6位连续数字
                    match = re.search(r'\d{6}', folder_name)
                    if match:
                        number = int(match.group(0))
                        if number > max_number:
                            max_number = number
                            max_folder = folder_path
            if max_folder:
                input_directory = max_folder
                logging.info(f"选择最大账单文件夹: {max_folder}")
            else:
                logging.error("未找到符合要求的账单文件夹")
                number = input("请输入账单日期：\nyyyymm")
        else:
            number = input("请输入账单日期：\nyyyymm")
            input_directory = os.path.join(input_directory, f'AMS{number}账单')
        ams_all_df = merge_ams_data(input_directory)
        output_dir = r'\\Auteng\综合管理部\自动化下载文件\AMS营销费用'
        os.makedirs(output_dir, exist_ok=True)
        file_name = f'AMS{number}汇总数据.csv'
        ams_all_df.to_csv(os.path.join(output_dir, file_name), index=False, encoding='utf-8-sig')
    elif data_name == '4':
        print('pass')
        shopee_cost()
    elif data_name == '5':
        print('站外广告汇总')
        merge_ads_zhanwai_data(r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\站外广告分析汇总数据')
