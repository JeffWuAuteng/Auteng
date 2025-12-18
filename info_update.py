import pandas as pd
import os
import logging
import datetime
import re
from pathlib import Path
from path_utils import PathManager, setup_path_logging, validate_excel_structure
# 获取用户输入
month_between = input('请输入要处理的月份(202501-03,202504-06,202507-09):')
sum_data_filename = input("请输入本次费用账单汇总文件时间如(202503,202504,202506-25周)：")
times = input("请输入申诉时间（格式：YYYY.MM.DD）：")
if not times:
    times = datetime.datetime.now().strftime("%Y.%m.%d")

# 设置路径日志
setup_path_logging(times)
logger = logging.getLogger(__name__)

# 初始化路径管理器
path_manager = PathManager(times)

# 原有路径配置
info = r'\\Auteng\综合管理部\综合管理部_公共\数据分层处理\3数据处理dwd-库存周报\20.进出存台账-shopee海外仓-入库明细(已处理).xlsx'
to_do_info = r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\shopee申诉处理.xlsx'
base_info = f'\\\\AUTENG\\hot_data\\数据处理\\报表输出\\订单核对\\订单核算数据{month_between}.csv'
info_path = r"\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK"
quanqiu_info = r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\shopee全球产品'
dingdanhedui_info = r'\\AUTENG\hot_data\数据处理\报表输出\订单核对\订单核算数据最新.csv'
if True:
    # 获取所有订单对应的操作费,
    df_ddhd = pd.read_csv(dingdanhedui_info,encoding='utf-8-sig')
    df_ddhd = df_ddhd[(df_ddhd['订单分类']=='Shopee海外仓订单') & (df_ddhd['海外仓渠道']=='shopee官方仓')][['订单号','海外仓操作费核对原币种']].copy()
    df_ddhd.drop_duplicates().to_csv(r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\订单操作费核对.csv',encoding='utf-8-sig',index=False)
# 验证申诉信息材料文件
material_file = path_manager.get_material_file_path()
is_valid, error_msg = validate_excel_structure(material_file)
if not is_valid:
    logger.error(f"申诉信息材料文件验证失败: {error_msg}")
    print(f"错误: {error_msg}")
    input("按回车键退出...")
    exit(1)
logger.info(f"开始处理申诉信息，时间: {times}")
logger.info(f"申诉信息材料文件: {material_file}")
# 分割输入为列表
month_list = [m.strip() for m in month_between.split(',')]
sum_data_list = [s.strip() for s in sum_data_filename.split(',')]
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
info_path = Path(info_path)
sum_data_file = info_path / sum_data_filename
if not sum_data_file.exists():
    raise FileNotFoundError(f"文件不存在: {sum_data_file}")
# 如果没有to_do_info文件，则创建一个空的to_do_info文件
if not os.path.exists(to_do_info):
    to_do_info_df = pd.DataFrame(columns=['订单号'])
    to_do_info_df.to_excel(to_do_info, index=False)
    input("请输入订单号，按回车键结束输入")
to_do_info_df = pd.read_excel(to_do_info)
base_info_dfs = []
for month in month_list:
    base_info_path = base_info
    # base_info_path = f'\\\\auteng20\\5.共享文件\\报表输出\\订单核对\\订单核算数据{month}.csv'
    if not os.path.exists(base_info_path):
        print(f"文件不存在: {base_info_path}")
        continue
    df = pd.read_csv(base_info_path, dtype=str)
    base_info_dfs.append(df)
base_info_df = pd.concat(base_info_dfs, ignore_index=True)
base_info_df = base_info_df[['订单号', '产品ID', '商品编码']]
base_info_df['商品编码'] = (
    base_info_df['商品编码']
    .astype(str)
    .str.replace(r'\.0$', '', regex=True)
)
base_info_df = base_info_df[base_info_df['订单号'].isin(to_do_info_df['订单号'])]
print(f"正在处理{sum_data_filename}文件")
sum_data_dfs = []
for filename in sum_data_list:
    full_filename = f"SBS-费用账单汇总{filename}.xlsx"
    sum_data_file = info_path / full_filename
    if not sum_data_file.exists():
        print(f"文件不存在: {sum_data_file}")
        continue
    df = pd.read_excel(sum_data_file, sheet_name='Handling Fee', dtype=str)
    sum_data_dfs.append(df)
sum_data_df = pd.concat(sum_data_dfs, ignore_index=True)
sum_data_df = sum_data_df[['订单编号', 'SKU ID']]
sum_data_df = sum_data_df.groupby('订单编号')[['SKU ID']].last().reset_index()
to_do_df = pd.merge(base_info_df, sum_data_df, left_on='订单号', right_on='订单编号', how='left')
to_do_df.to_excel(os.path.join(info_path, 'shopee申诉处理.xlsx'), index=False)
quanqiu_info_df = pd.DataFrame()
global_dir = quanqiu_info
for file in os.listdir(global_dir):
    if file.endswith('.xlsx'):
        file_path = os.path.join(global_dir, file)  # 用临时变量存储文件路径
        quanqiu_sub_df = pd.read_excel(file_path, dtype=str)
        quanqiu_sub_df = quanqiu_sub_df[['全球产品ID', '产品id']]
        quanqiu_info_df = pd.concat([quanqiu_info_df, quanqiu_sub_df], ignore_index=True)
        print(f"已处理文件：{file}")
        # 删除处理过的文件
        os.remove(file_path)
    if file.endswith('.csv'):
        file_path = os.path.join(global_dir, file)  # 用临时变量存储文件路径
        quanqiu_sub_df = pd.read_csv(file_path, dtype=str,encoding='utf-8-sig')
        quanqiu_sub_df = quanqiu_sub_df[['全球产品ID', '产品id']]
        quanqiu_info_df = pd.concat([quanqiu_info_df, quanqiu_sub_df], ignore_index=True)
# 补充数据
all_quanqiu_info_df = pd.read_csv(r'\\AUTENG\hot_data\中间结果\日报数据\订单数据\产品数据汇总.csv', encoding='utf-8-sig')
all_quanqiu_info_df = all_quanqiu_info_df[['全球产品ID', '产品id']]
quanqiu_info_df = pd.concat([quanqiu_info_df,all_quanqiu_info_df]).drop_duplicates()
quanqiu_info_df.to_csv(os.path.join(global_dir, '全球产品ID与产品ID对应关系.csv'), index=False, encoding='utf-8-sig')
quanqiu_info_df = quanqiu_info_df.drop_duplicates(subset=['全球产品ID', '产品id'], keep='last')
to_do_list = pd.merge(to_do_df, quanqiu_info_df, left_on='产品ID', right_on='产品id', how='left')
info_df = pd.read_excel(info, dtype=str)
# to_do_list = pd.read_excel(to_do, sheet_name='6.17-2', dtype=str)
info_df = info_df[['商品编码','Shop SKU ID', 'Warehouse SKU ID']]
# 使用正确正则表达式删除括号及其内容
info_df['Shop SKU ID'] = info_df['Shop SKU ID'].str.replace(r'\([^)]*\)', '', regex=True)
# 对于to_do_list,如果SKU ID列出现,就代表需要按,分割成多行处理，有多少个SKU ID,就处理多少行,其他值都复制重复
to_do_list['SKU ID'] = to_do_list['SKU ID'].str.split(',')
to_do_list = to_do_list.explode('SKU ID').reset_index(drop=True)
# 用SKU ID 去 info_df 中匹配'Shop SKU ID'或'Warehouse SKU ID'，得到对应的商品编码返回到to_do_list中命名为商品编码(匹配)
shop_mapping = (
    info_df.drop_duplicates('Shop SKU ID', keep='last')
    .set_index('Shop SKU ID')['商品编码']
    .to_dict()
)
warehouse_mapping = (
    info_df.drop_duplicates('Warehouse SKU ID', keep='last')
    .set_index('Warehouse SKU ID')['商品编码']
    .to_dict()
)
to_do_list['商品编码(匹配)'] = (
    to_do_list['SKU ID'].map(shop_mapping)
    .fillna(to_do_list['SKU ID'].map(warehouse_mapping))
)
col = ['订单号', '全球产品ID', '产品ID', 'SKU ID', '商品编码(匹配)', '商品编码']
to_do_list = to_do_list[col]
to_do_list['商品编码(匹配)'] = to_do_list['商品编码(匹配)'].fillna(to_do_list['商品编码'])
output_file = r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\shopee申诉处理-匹配(ok).xlsx'
to_do_list.drop_duplicates().to_excel(output_file, index=False)

# 处理新的文件夹结构
logger.info("开始处理文件夹结构迁移...")
try:
    # 读取申诉信息材料文件以获取主运营和SBS账单编号信息
    material_df = pd.read_excel(material_file)
    
    # 检查是否需要迁移旧结构
    migration_report = path_manager.migrate_old_structure(material_df)
    
    # 记录迁移结果
    logger.info(f"迁移成功: {len(migration_report['success'])} 个文件夹")
    logger.info(f"迁移失败: {len(migration_report['failed'])} 个文件夹")
    logger.info(f"跳过迁移: {len(migration_report['skipped'])} 个文件夹")
    
    if migration_report['success']:
        print(f"成功迁移 {len(migration_report['success'])} 个文件夹到新结构")
        for success in migration_report['success']:
            logger.info(f"迁移成功: {success}")
    
    if migration_report['failed']:
        print(f"迁移失败 {len(migration_report['failed'])} 个文件夹")
        for failed in migration_report['failed']:
            logger.error(f"迁移失败: {failed}")
    
    # 为每个唯一的主运营和SBS账单编号组合创建目录结构
    if '主运营' in material_df.columns and 'SBS账单编号' in material_df.columns:
        unique_combinations = material_df[['主运营', 'SBS账单编号']].drop_duplicates()
        
        for _, row in unique_combinations.iterrows():
            operator = str(row['主运营']).strip()
            sbs_number = str(row['SBS账单编号']).strip()
            
            if pd.notna(operator) and pd.notna(sbs_number) and operator != 'nan' and sbs_number != 'nan':
                try:
                    target_path = path_manager.create_directory_structure(operator, sbs_number)
                    logger.info(f"确保目录结构存在: {target_path}")
                except Exception as e:
                    logger.error(f"创建目录结构失败 {operator}/{sbs_number}: {e}")
    
    print(f"文件夹结构处理完成，详细日志请查看: {path_manager.get_log_file_path('path_operations')}")
    
except Exception as e:
    logger.error(f"处理文件夹结构时发生错误: {e}")
    print(f"处理文件夹结构时发生错误: {e}")

logger.info("info_update.py 处理完成")