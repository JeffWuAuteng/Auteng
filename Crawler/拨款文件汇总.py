import zipfile
import pandas as pd
import os
import time
import datetime
import shutil
import re
def safe_read_csv(file_path):
    if not os.path.exists(file_path):
        print(f"文件不存在: {file_path}")
        return pd.DataFrame()
    else:
        encodings = ['utf-8', 'gbk', 'gb2312', 'latin1', 'iso-8859-1']
        for encoding in encodings:
            try:
                return pd.read_csv(file_path, encoding=encoding)
            except UnicodeDecodeError:
                continue
            except pd.errors.EmptyDataError:  # 新增空文件处理
                return pd.DataFrame()
        # 如果所有编码都失败，尝试二进制读取
        try:
            return pd.read_csv(file_path, encoding='utf-8', errors='replace')
        except pd.errors.EmptyDataError:  # 新增空文件处理
            return pd.DataFrame()
        except:
            return pd.DataFrame()
def get_income_xls():
    colname=pd.read_excel(r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\拨款文件字段统一.xlsx",sheet_name="income字段对应表-shopee")
    path =  r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\最新下载待处理"
    history_dir = r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\所有下载源文件-检测下载用"
    # path = r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\坏文件"
    # path_res= r"E:\我的电脑桌面\运营中心\销售管理\虾皮账单数据更新\按店铺原始账单汇总"
    # # done_data=pd.read_csv(r"E:\我的电脑桌面\运营中心\销售管理\虾皮账单数据更新\按店铺原始账单汇总\已处理文件" + ".csv")
    # # done_data.columns=["店铺账号","表格文件","sheet"]
    # # donelist=list(done_data["店铺账号"].drop_duplicates()[0:-1])
    zipfiles = os.listdir(path)
    data_income_all = pd.DataFrame()
    data_adjust_all = pd.DataFrame()
    data_service_all = pd.DataFrame()
    data_freereturn =pd.DataFrame()
    data_sumary_all  = pd.DataFrame()
    error_data = pd.DataFrame()
    count = 0
    donelist_now = list()
    # 记录汇总sheetname
    dontok_sheetname = {}
    mday="2025-1-1"
    buchong="ummary"
    timetange="2025"
    all_files = [f for f in zipfiles if (timetange in f) and (".xls" in f)]
    for zfile2 in all_files:  # 所有店铺-文件夹
        fileime = pd.to_datetime(time.ctime(os.path.getmtime(os.path.join(path, zfile2))))
        if (fileime > pd.to_datetime(mday)) and (zfile2 not in donelist_now):
            if 'trillion_auto..cl' in zfile2:
                zfile1 = 'trillion_auto..cl'
            else:
                if '-' not in zfile2:
                    zfile1 = '.'.join(zfile2.split(".")[1:3])
                else:
                    zfile1 = zfile2.split("-")[0]
            try:
                donelist_now.append(zfile2)
                sheet_names = list(pd.ExcelFile(os.path.join(path, zfile2)).sheet_names)
                dontok_sheetname[zfile2] = sheet_names
                for stn in sheet_names.copy():
                    if ("ncome" in stn):
                        #print("income", zfile1,zfile2)
                        data = pd.read_excel(os.path.join(path, zfile2), sheet_name=stn)
                        i = 0
                        while ("编号" not in str(data.iloc[i][0])) & ("編號" not in str(data.iloc[i][0]))& ("Sequence No." not in str(data.iloc[i][0])):
                            i = i + 1
                            start_rows = i + 1
                        data_income = pd.read_excel(os.path.join(path, zfile2), sheet_name=stn, skiprows=start_rows)

                        if "Voucher Code.1" in data_income.columns:
                            data_income.rename(columns={"Voucher Code.1": "优惠码"}, inplace=True)
                            data_income.rename(columns={"Voucher Code": "卖家优惠券金额"}, inplace=True)
                        for i in data_income.columns:
                            for c in range(colname.shape[1]):
                                if i in list(colname[~pd.isna(colname[colname.columns[c]])][colname.columns[c]]):
                                    data_income.rename(columns={i: colname.columns[c]}, inplace=True)
                                    break
                        data_income["文件"] = str(zfile2)+ "_" + str(stn)
                        data_income["店铺名称"] = str(zfile1)
                        data_income["拨款批次"] = datetime.datetime.strptime(re.findall(r'\d{8,}', zfile2)[0], '%Y%m%d').date()
                        if stn in dontok_sheetname[zfile2]:
                            dontok_sheetname[zfile2].remove(stn)
                        data_income_all = pd.concat([data_income_all, data_income]).drop_duplicates()  # 全部汇总

                        # error_data = pd.concat([error_data, pd.DataFrame([[str(zfile1), str(zfile2), stn]])])
                    # 处理adjust
                    elif ("djust" in stn):
                        #print("Adjust", zfile1,zfile2)
                        data = pd.read_excel(path + "\\" + zfile2, sheet_name=stn)
                        i = 0
                        while ("编号" not in str(data.iloc[i][0])) & ("編號" not in str(data.iloc[i][0]))& ("Sequence No." not in str(data.iloc[i][0])):
                            i = i + 1
                            start_rows = i + 1
                        data_adjust = pd.read_excel(path + "\\" + zfile2, sheet_name=stn, skiprows=start_rows)
                        data_adjust.columns = ['编号', '调整维度', '订单编号', '费用类型', '调整场景', '调整金额','备注']
                        data_adjust["文件"] = str(zfile2)+ "_" + str(stn)
                        data_adjust["店铺名称"] = str(zfile1)
                        data_adjust["拨款批次"] = datetime.datetime.strptime(re.findall(r'\d{8,}', zfile2)[0], '%Y%m%d').date()
                        if stn in dontok_sheetname[zfile2]:
                            dontok_sheetname[zfile2].remove(stn)
                        data_adjust_all = pd.concat([data_adjust_all, data_adjust]).drop_duplicates(subset=['编号','文件'],keep='last')  # 全部汇总
                        # error_data = pd.concat([error_data, pd.DataFrame([[str(zfile1), str(zfile2), stn]])])
                    #处理服务费
                    elif ("ervice" in stn):
                        #print("service", zfile1,zfile2)
                        data_service = pd.read_excel(path + "\\" + zfile2, sheet_name=stn,skiprows=1)
                        if data_service.shape[0]>=1:
                            for i in data_service.columns:
                                if ("訂單"  in i):
                                    data_service.rename(columns={i: "订单编号"},inplace=True)
                                if ("編號"  in i)&("訂單" not in i):
                                    data_service.rename(columns={i: "编号"},inplace=True)
                            data_service["文件"] = str(zfile2)+ "_" + str(stn)
                            data_service["店铺名称"] = str(zfile1)
                            data_service["拨款批次"] = datetime.datetime.strptime(re.findall(r'\d{8,}', zfile2)[0], '%Y%m%d').date()
                            if stn in dontok_sheetname[zfile2]:
                                dontok_sheetname[zfile2].remove(stn)
                            data_service_all = pd.concat([data_service_all, data_service]).drop_duplicates()  # 全部汇总
                        # else:
                        #     error_data = pd.concat([error_data, pd.DataFrame([[str(zfile1), str(zfile2),stn]])])
                    elif ("ummary" in stn):
                        #print("sumary", zfile1,zfile2)
                        # data = pd.read_excel(path + "\\" + zfile2, sheet_name=stn)
                        # i = 0
                        # while ("卖家帐号" not in str(data.iloc[i][0]))|(("" not in str(data.iloc[i][0]))) :
                        #     i = i + 1
                        #     start_rows = i + 1
                        #     #print(i, data.iloc[i][0])
                        data_sumary = pd.read_excel(path + "\\" + zfile2, sheet_name=stn,skiprows= start_rows)
                        if data_sumary.shape[0] > 1:
                            data_sumary.columns=['一级费用', '二级费用', '金额', '金额小结']
                            data_sumary["文件"] = str(zfile2)+ "_" + str(stn)
                            data_sumary["店铺名称"] = str(zfile1)
                            data_sumary["拨款批次"] =datetime.datetime.strptime(re.findall(r'\d{8,}', zfile2)[0], '%Y%m%d').date()
                            if stn in dontok_sheetname[zfile2]:
                                dontok_sheetname[zfile2].remove(stn)
                            data_sumary_all = pd.concat([data_sumary_all, data_sumary]).drop_duplicates()  # 全部汇总
                        # else:
                        #     error_data = pd.concat([error_data, pd.DataFrame([[str(zfile1), str(zfile2),stn]])])
                    else:
                        error_data = pd.concat([error_data, pd.DataFrame([[str(zfile1), str(zfile2), str(stn)+"未匹配"]])])
                    print("汇总操作ok", str(zfile1) + "_" + str(zfile2))
            except:
                print("文件错误", str(zfile1) + "_" + str(zfile2))
                error_data = pd.concat([error_data, pd.DataFrame([[str(zfile1), str(zfile2), "错误未汇总"]])])
                shutil.copy(path + "\\" + zfile2, r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\输出坏文件\\")
        else:
            error_data = pd.concat([error_data, pd.DataFrame([[str(zfile1), str(zfile2),"跳过"]])])
            print(zfile2, "pass")
        if zfile2 in dontok_sheetname and not dontok_sheetname[zfile2]:
            del dontok_sheetname[zfile2]
            print(f"文件{zfile2}的所有目标sheet已处理，移除文件记录")
    export_data = []
    for filename, sheet_list in dontok_sheetname.items():
        for sheetname in sheet_list:
            export_data.append({
                "文件名": filename,
                "工作簿名": sheetname
            })

    # 转换为DataFrame并导出
    if export_data:  # 若有未处理的sheet
        df_unprocessed = pd.DataFrame(export_data)
        df_unprocessed.to_csv(r'\\Auteng\hot_data\数据处理\12.数据收集\10.拨款账单shopee\汇总结果备份\未汇总工作簿.csv',encoding='utf-8-sig')
    #处理新顺序
    newcol=[]
    for cn in colname.columns:
        if cn in data_income_all.columns:
            newcol.append(cn)
    for o in data_income_all.columns:
        if o not in newcol:
            newcol.append(cn)
    #data_income_all=data_income_all[["文件","店铺名称","拨款批次"]+newcol]

    # error_data.to_csv(r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\输出坏文件 - 副本"+timetange+str(datetime.datetime.today().date())+".csv",index=False)
    # error_data.to_csv(r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\输出坏文件"+timetange+str(datetime.datetime.today().date())+".csv",index=False)

    error_data.to_csv(r"\\AUTENG\hot_data\运营中心\销售管理\虾皮账单数据更新\按店铺原始账单汇总\已处理文件101补充"+timetange+str(datetime.datetime.today().date())+".csv",index=False)
    error_data.to_csv(r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\汇总结果\\坏文件清单"+timetange+str(datetime.datetime.today().date())+".csv",index=False)
    if "拨款批次" in data_income_all.columns:
        data_income_all["拨款完成日期1"]=pd.to_datetime(data_income_all["拨款完成日期"],format='mixed')
    data_sumary_all["拨款批次1"] = pd.to_datetime(data_sumary_all["拨款批次"],format='mixed')
    if "拨款批次" in data_service_all.columns:
        data_service_all["拨款批次1"] = pd.to_datetime(data_service_all["拨款批次"],format='mixed')
    if "拨款批次" in data_adjust_all.columns:
        data_adjust_all["拨款批次1"] = pd.to_datetime(data_adjust_all["拨款批次"],format='mixed')
    filepathres = r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\汇总结果备份\\"
    filepathres_show = r'\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\汇总结果\\'
    # 根据 拨款完成日期字段分组 设置每两个月放一个文件，把timetange改为年月份两个月就是如202501-02这样储存在历史里面避免文件过大
    # 根据拨款完成日期字段分组，设置每两个月放一个文件
    if "拨款批次" in data_income_all.columns:
        data_income_all["时间段"] = data_income_all["拨款完成日期1"].apply(
            lambda x: f"{x.year}{str((x.month-1)//2*2+1).zfill(2)}-{str((x.month-1)//2*2+2).zfill(2)}"
        )
    data_sumary_all["时间段"] = data_sumary_all["拨款批次1"].apply(
        lambda x: f"{x.year}{str((x.month-1)//2*2+1).zfill(2)}-{str((x.month-1)//2*2+2).zfill(2)}"
    )
    if "拨款批次" in data_service_all.columns:
        data_service_all["时间段"] = data_service_all["拨款批次1"].apply(
            lambda x: f"{x.year}{str((x.month-1)//2*2+1).zfill(2)}-{str((x.month-1)//2*2+2).zfill(2)}"
        )
    if "拨款批次" in data_adjust_all.columns:
        data_adjust_all["时间段"] = data_adjust_all["拨款批次1"].apply(
            lambda x: f"{x.year}{str((x.month-1)//2*2+1).zfill(2)}-{str((x.month-1)//2*2+2).zfill(2)}"
        )
    # 处理income表
    if "拨款批次" in data_income_all.columns:
        for period, group in data_income_all.groupby("时间段"):
            file_path = filepathres + f"{period}_各店铺拨款账单income.csv"
            # 如果文件存在则读取并追加
            if os.path.exists(file_path):
                existing_data = safe_read_csv(file_path)
                combined = pd.concat([existing_data, group]).drop_duplicates(subset=['订单编号'],keep='last')
            else:
                combined = group.drop_duplicates(subset=['订单编号'],keep='last')
            combined.to_csv(file_path, index=False)
            if os.path.exists(file_path):
                shutil.copy(file_path, filepathres_show)
    # 处理sumary表
    for period, group in data_sumary_all.groupby("时间段"):
        file_path = filepathres + f"{period}_各店铺拨款账单sumarry.csv"
        if os.path.exists(file_path):
            existing_data = safe_read_csv(file_path)
            combined = pd.concat([existing_data, group]).drop_duplicates()
        else:
            combined = group.drop_duplicates()
        combined.to_csv(file_path, index=False)
    # 处理service表
    if "时间段" in data_service_all.columns:
        for period, group in data_service_all.groupby("时间段"):
            file_path = filepathres + f"{period}_各店铺拨款账单service.csv"
            if os.path.exists(file_path):
                existing_data = safe_read_csv(file_path)
                combined = pd.concat([existing_data, group]).drop_duplicates(subset=['订单编号','文件'],keep='last')
            else:
                combined = group.drop_duplicates(subset=['订单编号','文件'],keep='last')
            combined.to_csv(file_path, index=False)
    # 处理adjust表
    if "时间段" in data_adjust_all.columns:
        for period, group in data_adjust_all.groupby("时间段"):
            file_path = filepathres + f"{period}_各店铺拨款账单adjust.csv"
            if os.path.exists(file_path):
                existing_data = safe_read_csv(file_path)
                combined = pd.concat([existing_data, group]).drop_duplicates(subset=['编号','文件'],keep='last')
            else:
                combined = group.drop_duplicates(subset=['编号','文件'],keep='last')
            combined.to_csv(file_path, index=False)
    if "拨款完成日期" in data_freereturn.columns:
        data_freereturn["拨款完成日期1"] = pd.to_datetime(data_freereturn["拨款完成日期"])
        data_freereturn["时间段"] = data_freereturn["拨款完成日期1"].apply(
            lambda x: f"{x.year}{str((x.month-1)//2*2+1).zfill(2)}-{str((x.month-1)//2*2+2).zfill(2)}"
        )
        for period, group in data_freereturn.groupby("时间段"):
            file_path = filepathres + f"{period}_各店铺拨款账单return.csv"
            if os.path.exists(file_path):
                existing_data = safe_read_csv(file_path)
                combined = pd.concat([existing_data, group]).drop_duplicates()
            else:
                combined = group.drop_duplicates()
            combined.to_csv(file_path, index=False)
    for data in [data_income_all, data_sumary_all, data_service_all, data_adjust_all, data_freereturn]:
        if '拨款完成日期1' in data.columns:
            try:
                data['拨款完成日期1'] = data['拨款完成日期1'].dt.strftime('%Y/%m/%d')
            except:
                print('拨款完成日期1转换错误')
        if '拨款批次1' in data.columns:
            try:
                data['拨款批次1'] = data['拨款批次1'].dt.strftime('%Y/%m/%d')
            except:
                print('拨款批次1转换错误')
    data_income_all.drop_duplicates(subset=['订单编号'],keep='last').to_csv(filepathres + timetange + "当次各店铺拨款账单income.csv", index=False)
    data_sumary_all.drop_duplicates().to_csv(filepathres + timetange+ "当次各店铺拨款账单sumarry.csv", index=False)
    data_service_all.drop_duplicates(subset=['订单编号','文件'],keep='last').to_csv(filepathres +timetange + "当次各店铺拨款账单service.csv", index=False)
    # 判断文件是否重复
    data_adjust_all['文件名称'] = data_adjust_all['文件'].str.split('.xlsx').str[0]
    data_adjust_all.drop_duplicates(subset=['编号','文件名称'],keep='last').to_csv(filepathres + timetange+ "当次各店铺拨款账单adjust.csv", index=False)
    data_freereturn.drop_duplicates().to_csv(filepathres + timetange + "当次各店铺拨款账单return.csv", index=False)
    if True:
        data_income_all0=safe_read_csv(filepathres + timetange + "各店铺拨款账单income.csv")
        data_sumary_all0=safe_read_csv(filepathres + timetange + "各店铺拨款账单sumarry.csv")
        data_service_all0=safe_read_csv(filepathres + timetange + "各店铺拨款账单service.csv")
        data_adjust_all0=safe_read_csv(filepathres + timetange + "各店铺拨款账单adjust.csv")
        data_freereturn0=safe_read_csv(filepathres + timetange + "各店铺拨款账单return.csv")
        data_income_all1 = pd.concat([data_income_all0, data_income_all]).drop_duplicates(subset=['订单编号'],keep='last')
        data_sumary_all1 = pd.concat([data_sumary_all, data_sumary_all0]).drop_duplicates()
        data_service_all1 = pd.concat([data_service_all0, data_service_all]).drop_duplicates(subset=['订单编号','文件'],keep='last')
        data_adjust_all1 = pd.concat([data_adjust_all0, data_adjust_all]).drop_duplicates(subset=['编号','文件'],keep='last')
        data_adjust_all1['文件名称'] = data_adjust_all1['文件'].str.split('.xlsx').str[0]
        data_adjust_all1 = data_adjust_all1.drop_duplicates(subset=['编号','文件名称'],keep='last')
        data_freereturn1 = pd.concat([data_freereturn, data_freereturn0]).drop_duplicates()
        # 获取汇总的所有 店铺名称 拨款批次1,拨款完成日期1
        dp_date_info_all = pd.DataFrame()
        for data in [data_income_all1, data_sumary_all1, data_service_all1, data_adjust_all1, data_freereturn1]:
            if '拨款完成日期1' in data.columns:
                try:
                    data['拨款完成日期1'] = pd.to_datetime(
                        data['拨款完成日期1'].astype(str).str.split(' ').str[0],
                        errors='coerce'  # 无法解析的日期会被设为 NaT
                    )
                    data['拨款完成日期1'] = data['拨款完成日期1'].dt.strftime('%Y/%m/%d')
                    dp_date_info = data[['店铺名称','拨款完成日期1']].rename(columns={'拨款完成日期1': '已汇总日期'})
                    dp_date_info_all = pd.concat([dp_date_info_all, dp_date_info])
                except:
                    print('拨款完成日期1转换错误')
            if '拨款批次1' in data.columns:
                try:
                    data['拨款批次1'] = pd.to_datetime(
                        data['拨款批次1'].astype(str).str.split(' ').str[0],
                        errors='coerce'  # 无法解析的日期会被设为 NaT
                    )
                    data['拨款批次1'] = data['拨款批次1'].dt.strftime('%Y/%m/%d')
                    dp_date_info = data[['店铺名称','拨款批次1']].rename(columns={'拨款批次1': '已汇总日期'})
                    dp_date_info_all = pd.concat([dp_date_info_all, dp_date_info])
                except:
                    print('拨款批次1转换错误')
        dp_date_info_should_all = pd.DataFrame()
        for filename in os.listdir(filepathres):
            if filename.endswith('_各店铺应下载拨款日期汇总.csv'):
                dp_date_info_should = pd.read_csv(os.path.join(filepathres, filename))
                dp_date_info_should_all = pd.concat([dp_date_info_should_all, dp_date_info_should])
        dp_date_info_all = dp_date_info_all.drop_duplicates()
        dp_date_info_all = dp_date_info_all[dp_date_info_all['已汇总日期'].notnull()]
        dp_date_info_all['已汇总'] = '已汇总'
        dp_date_info_should_all = dp_date_info_should_all.drop_duplicates()
        
        # 检查dp_date_info_should_all是否包含必要的列
        if not dp_date_info_should_all.empty and '店铺名称' in dp_date_info_should_all.columns and '应下载日期' in dp_date_info_should_all.columns:
            dp_date_info_should_all = dp_date_info_should_all.merge(dp_date_info_all, left_on=['店铺名称', '应下载日期'],right_on=['店铺名称', '已汇总日期'], how='left')
        else:
            print(f"警告：dp_date_info_should_all为空或缺少必要列。当前列：{list(dp_date_info_should_all.columns) if not dp_date_info_should_all.empty else '空DataFrame'}")
            # 如果没有数据或缺少列，创建一个空的DataFrame以避免错误
            if dp_date_info_should_all.empty:
                dp_date_info_should_all = pd.DataFrame(columns=['店铺名称', '应下载日期', '已汇总日期', '已汇总'])
        try:
            dp_date_info_should_all.to_csv(os.path.join(filepathres_show, '各店铺应下载拨款日期汇总.csv'), index=False,encoding='utf-8-sig')
        except:
            print('未更新\\\\AUTENG\\hot_data\\数据处理\\12.数据收集\\10.拨款账单shopee\\汇总结果\\\\各店铺应下载拨款日期汇总.csv')
        data_income_all1 = data_income_all1.drop_duplicates(subset=['订单编号'], keep='last')
        data_income_all1.to_csv(filepathres_show + timetange + "各店铺拨款账单income.csv", index=False,encoding='utf-8-sig')
        data_income_all1.to_csv(filepathres + timetange + "各店铺拨款账单income.csv", index=False,encoding='utf-8-sig')
        data_sumary_all1 = data_sumary_all1.drop_duplicates(subset=[col for col in data_sumary_all.columns if col not in ['拨款批次1', '拨款完成日期1','时间段']], keep='last')
        data_sumary_all1.to_csv(filepathres + timetange + "各店铺拨款账单sumarry.csv", index=False,encoding='utf-8-sig')
        data_service_all1 = data_service_all1.drop_duplicates(subset=['订单编号','文件'], keep='last')
        data_service_all1.to_csv(filepathres + timetange + "各店铺拨款账单service.csv", index=False,encoding='utf-8-sig')
        data_adjust_all1 = data_adjust_all1.drop_duplicates(subset=['编号','文件名称'], keep='last')
        data_adjust_all1.to_csv(filepathres + timetange + "各店铺拨款账单adjust.csv", index=False,encoding='utf-8-sig')
        data_freereturn1 = data_freereturn1.drop_duplicates(subset=[col for col in data_freereturn.columns if col not in ['拨款批次1', '拨款完成日期1','时间段']], keep='last')
        data_freereturn1.to_csv(filepathres + timetange + "各店铺拨款账单return.csv", index=False,encoding='utf-8-sig')
    if not os.path.exists(history_dir):
        os.makedirs(history_dir)
        print(f"创建历史目录: {history_dir}")
    for zfile2 in all_files:
        src_path = os.path.join(path, zfile2)
        dest_path = os.path.join(history_dir, zfile2)
        try:
            # 如果目标文件已存在则覆盖
            if os.path.exists(dest_path):
                os.remove(dest_path)
            shutil.move(src_path, dest_path)
            print(f"文件已移动: {zfile2} -> {history_dir}")
        except Exception as e:
            print(f"移动文件失败: {zfile2}, 错误: {str(e)}")
    # ==== 文件移动结束 ====
    print("所有文件处理完成并已移动到历史目录")
def get_shangpinsku():
    print("汇总商品SKU")
    seller = pd.read_excel(r"\\AUTENG\hot_data\数据处理\日报源数据\账号分配表.xlsx", sheet_name="销售代码")
    fpath=r"\\AUTENG\hot_data\数据处理\5.2商品管理\商品管理源数据\\"
    zipfiles = os.listdir( fpath)
    product_all = pd.DataFrame()
    count = 0
    for zfile in zipfiles:
        count = count + 1
        print("\n进度", count, "/", len(zipfiles), "\n解压：", zfile)
        data_zfile = pd.DataFrame()
        if ".zip" in zfile:
            z = zipfile.ZipFile( fpath + zfile, "r")
            for filename in z.namelist():
                if ".xls" in filename:
                    print('文件:', filename)
                    content = z.open(filename)
                    data = pd.read_excel(content)
                    # data["店铺账号"] =  zfile[zfile.find("-")+1:zfile.find(".zip")]
                    data_zfile = pd.concat([data_zfile, data])

        else:
            data = pd.read_excel( fpath + zfile)
            print('文件:', zfile)
            # data["店铺账号"] = zfile[zfile.find("-")+1:zfile.find(".xls")]
            data_zfile = pd.concat([data_zfile, data])
            data_zfile = data_zfile.drop_duplicates()

        product_all = pd.concat([product_all, data_zfile])
    product_all["产品运营"] =   product_all['商品SKU'].apply(lambda x: get_seller(x, seller, 0))
    product_all["产品开发"] = product_all['商品SKU'].apply(lambda x: get_seller(x, seller, 1))
    product_all['图片URL']=""
    product_all['来源URL'] = ""
    product_all=product_all.drop_duplicates()
    product_all.to_excel(r"\\AUTENG\hot_data\数据处理\5.2商品管理\商品管理"+str(datetime.date.today())+".xlsx", index=False)
    print("汇总商品结束")
    #
    # product_all0 = pd.read_excel(r"\\AUTENG\hot_data\数据处理\报表输出\商品管理.xlsx")
    # product_all0['商品SKU']=product_all0['商品SKU'].astype(str)
    # product_all0["产品运营"] = product_all0['商品SKU'].apply(lambda x: get_seller(x, seller, 0))
    # product_all0["产品开发"] = product_all0['商品SKU'].apply(lambda x: get_seller(x, seller, 1))
    #product_all0.to_excel(r"\\AUTENG\hot_data\数据处理\报表输出\商品SKU汇总最新.xlsx", index=False)
def get_currency(x,currency,mode):
    if mode=="c":
        for i in range(len(currency)):
            if currency["币种缩写"].iloc[i] in x :
                y=currency["反向汇率"].iloc[i]
                break
            else:
                y = 0
    else:
        for i in range(len(currency)):
            if currency["站点"].iloc[i] in x :
                y=currency["反向汇率"].iloc[i]
                break
            else:
                y = 0

    return y
def get_seller(x,seller,mode):
    for i in range(len(seller)):
        if mode==0:
            if seller["销售代码"].iloc[i] in x.split("-")[0].split("_")[0]+"-":
                y=seller["人员"].iloc[i]
                break
            else:
                y = "未匹配"
        elif mode==1:
            if x.startswith(seller["开发代码"].iloc[i]) :
                y=seller["开发人员"].iloc[i]
                break
            else:
                y = "未匹配"
    return y
if __name__ == '__main__':
    mod = input("请输入更新内容：\n1.拨款文件汇总\n2.商品管理计算")
    if mod == "1":
        # 先合并应下载拨款批次
        get_income_xls()
    elif mod == "2":
        get_shangpinsku()