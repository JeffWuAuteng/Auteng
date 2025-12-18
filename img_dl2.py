#coding=utf-8
"""
Shopee图片下载和商品信息修改脚本
已升级支持持久化会话功能：
- 使用保存的登录信息，避免重复登录
- 支持多账号独立会话管理
- 自动检测登录状态，提高使用效率
"""
from selenium import webdriver
import time
import random
import winsound
import os
import shutil
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys  # 添加此行
from datetime import datetime
from PIL import Image
from selenium.webdriver.chrome.options import Options
import logging
# 导入shopee_dl中的持久化会话功能
from shopee_dl import init_driver, cleanup_chrome_locks, kill_conflicting_chrome_processes
# 导入路径处理工具
from path_utils import  setup_path_logging, get_path_manager
# 导入配置模块
try:
    from config import Config
except ImportError:
    print("警告：未找到config模块，将使用默认配置")
    class Config:
        @staticmethod
        def is_persistent_session_enabled():
            return True
        BROWSER_CONFIG = {"detach": True, "headless": False}
def log_modify_product(log_entries, global_id, original_values, new_values):
    """记录商品修改日志"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_row = [
        current_time,
        global_id,
        original_values.get("weight", ""),
        original_values.get("width", ""),
        original_values.get("height", ""),
        original_values.get("length", ""),
        new_values.get("weight", ""),
        new_values.get("width", ""),
        new_values.get("height", ""),
        new_values.get("length", "")
    ]
    log_entries.append(new_row)
def copy_file(sourceDir, targetDir):
    """递归复制文件"""
    for f in os.listdir(sourceDir):
        sourceF = os.path.join(sourceDir,f)
        targetF = os.path.join(targetDir,f)
        if os.path.isfile(sourceF):
            shutil.copy(sourceF,targetF)
        if os.path.isdir(sourceF):
            copy_file(sourceF,targetDir)
def read_excel():
    times = input("请输入本次申诉信息材料上的时间，格式为YYYY.MM.DD：")
    if not times:
        times = datetime.now().strftime("%Y.%m.%d")
    
    # 设置路径日志
    setup_path_logging(times)
    logger = logging.getLogger(__name__)
    
    # 使用路径管理器获取文件路径
    path_manager = get_path_manager(times)
    info = path_manager.get_material_file_path()
    
    logger.info(f"读取申诉信息材料文件: {info}")
    
    try:
        df_info = pd.read_excel(info)
        df_info.dropna(subset=["店铺账号", "SKU ID", "SBS账单编号", "全球产品ID", "主体店铺名称", "长 CM", "宽 CM", "高 CM", "重量 G"])
        
        # 验证必要的列是否存在
        required_columns = ['主运营', 'SBS账单编号']
        missing_columns = [col for col in required_columns if col not in df_info.columns]
        if missing_columns:
            logger.warning(f"Excel文件缺少列: {missing_columns}，将使用旧的路径结构")
        
        logger.info(f"成功读取 {len(df_info)} 条记录")
        return df_info, times
        
    except Exception as e:
        logger.error(f"读取Excel文件失败: {e}")
        raise
def get_global_info(df_info, global_dp_name):
    """处理全球商品信息"""
    global_info = df_info.groupby(["主体店铺名称","全球产品ID"])[["长 CM","宽 CM","高 CM","重量 G"]].last().reset_index()
    # 全球产品ID不是数值
    global_info['全球产品ID'] = (
        global_info['全球产品ID']
        .astype(str)
        .str.replace(r'\.0$', '', regex=True)  # 去除末尾的.0
        .str.lstrip('0')
    )
    global_info = global_info[global_info["主体店铺名称"] == global_dp_name]
    global_info[["长 CM", "宽 CM", "高 CM", "重量 G"]] = global_info[["长 CM", "宽 CM", "高 CM", "重量 G"]].astype(int)
    return global_info.astype(str)
def get_dp_info(df_info,global_dp_name):
    """处理店铺信息"""
    dp_info = df_info.groupby(["产品ID"])[["店铺账号","主体店铺名称","SKU ID","SBS账单编号"]].first().reset_index()
    dp_info = dp_info[dp_info["主体店铺名称"] == global_dp_name]
    return dp_info.astype(str)
def default_dl_path(times):
    """获取默认下载路径，保持向后兼容"""
    path_manager = get_path_manager(times)
    return path_manager.base_dir

def get_screenshot_save_path(times, df_info, sbs_number):
    """
    获取截图保存路径，支持新的文件夹结构
    
    Args:
        times: 申诉时间
        df_info: Excel数据
        sbs_number: SBS账单编号
        
    Returns:
        截图保存路径
    """
    path_manager = get_path_manager(times)
    logger = logging.getLogger(__name__)
    
    # 尝试使用新的路径结构
    target_path = path_manager.find_path_by_sbs(sbs_number, df_info)
    
    if target_path and os.path.exists(target_path):
        logger.info(f"使用现有路径: {target_path}")
        return target_path
    
    # 如果没有找到现有路径，尝试创建新的路径结构
    if '主运营' in df_info.columns and 'SBS账单编号' in df_info.columns:
        matching_rows = df_info[df_info['SBS账单编号'].astype(str).str.strip() == str(sbs_number).strip()]
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
def get_global_dp_name():
    """获取全球店铺名称"""
    valid_options = ["趣利恩", "海湃", "遨腾"]
    while True:
        global_dp_name = input("请输入主体店铺名称（趣利恩/海湃/遨腾）：")
        if global_dp_name in valid_options:
            break
    return global_dp_name
def settingChrome(times, account_name=None):
    """Chrome浏览器设置 - 使用持久化会话"""
    download_dir = default_dl_path(times)
    # 使用shopee_dl中的init_driver函数，启用持久化会话
    driver = init_driver(download_dir, use_persistent_session=True, account_name=account_name)
    return driver
def login_CNSC(driver):
    """登录Shopee卖家中心 - 支持持久化会话"""
    driver.get("https://seller.shopee.cn/")
    time.sleep(5)
    
    # 检查是否已经登录（通过检查是否存在账号名称元素）
    try:
        store_list_o = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "account-name")))
        print("检测到已登录状态：", store_list_o[0].get_attribute("innerText"))
        print("使用保存的登录信息，跳过登录步骤")
        return
    except:
        print("未检测到登录状态，需要重新登录")
    
    # 如果未登录，则进行登录流程
    ac = input("请输入账号：")
    pw = input("请输入密码：")
    try:
        element = WebDriverWait(driver, 60, 0.5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-input__input")))
        element[0].send_keys(ac)
        element[1].send_keys(pw)
        time.sleep(random.randint(1, 3) / 10)
        driver.find_element(by=By.CLASS_NAME, value=
        "eds-button.eds-button--primary.eds-button--large.eds-button--block.submit-btn").click()  # 登录
        time.sleep(random.randint(1, 3) / 10)
        time.sleep(10)
        input("请手动处理验证码后按回车继续...")
    except Exception as e:
        try:
            print("刷新登录")
            driver.get("https://seller.shopee.cn/")
            driver.refresh()
            time.sleep(10)
            element = WebDriverWait(driver, 120, 0.5).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-input__input")))
            element[0].send_keys(ac)
            element[1].send_keys(pw)
            time.sleep(random.randint(1, 3) / 10)
            driver.find_element(by=By.CLASS_NAME, value=
            "eds-button.eds-button--primary.eds-button--large.eds-button--block.submit-btn").click()  # 登录
            time.sleep(random.randint(1, 3) / 10)
            time.sleep(3)
            input("请手动处理验证码后按回车继续...")
        except Exception as e:
            pass
    
    # 验证登录状态
    for tt in range(5):
        try:
            store_list_o = WebDriverWait(driver, 30, 0.5).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "account-name")))
            print("已登录：", store_list_o[0].get_attribute("innerText"))
            break
        except:
            print("等待加载登录", tt)
            time.sleep(1)
def into_global_page(driver):
    """进入全球商品页面"""
    try:
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-popover__ref"))
        )
        for elem in elements:
            if "全球商品" in elem.get_attribute("innerText"):
                elem.click()
                break  # 找到后立即终止循环
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("进入全球商品页面失败，手动在浏览器页面点击全球商品页面，并按回车")
        time.sleep(1)
def modify_product(driver, global_info, log_entries):
    """修改商品信息"""
    try:
        elements_select = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-option"))
        )
        for elem in elements_select:
            if "商品ID" in elem.get_attribute("innerText"):
                driver.execute_script("arguments[0].click();", elem)
                break
        time.sleep(1)
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("选择搜索商品ID失败，手动在浏览器页面点击商品ID搜索按钮，不要输入商品ID，并按回车")
    """找到修改商品并进入商品修改页面"""
    for i in range(len(global_info)): # 循环遍历global_info中的每一行
        s = global_info.iloc[i]["全球产品ID"]
        print("正在修改的全球产品ID：", s)
        w = global_info.iloc[i]["重量 G"]
        # 转换单位
        w = float(w) / 1000
        if w < 0.01:
            w = 0.01
        w = round(w, 2)
        w = str(w)
        wi = global_info.iloc[i]["宽 CM"]
        li = global_info.iloc[i]["长 CM"]
        hi = global_info.iloc[i]["高 CM"]
        if wi == '0':
            wi = '1'
        if li == '0':
            li = '1'
        if hi == '0':
            hi = '1'
        print(f"原计划修改数值：重量-{w}-kg, 宽-{wi}-cm, 长-{li}-cm, 高-{hi}-cm")
        try:
            window_handles = driver.window_handles
            driver.switch_to.window(window_handles[-1])
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-input__input"))
            )
            element[0].clear()
            element[0].send_keys(s)
            time.sleep(random.randint(1, 3) / 10)
            element[0].send_keys(Keys.RETURN)
            time.sleep(random.randint(1, 3))
        except Exception as e:
            print(s)
            input("搜索全球产品ID失败，请手动输入ID...不要点编辑，并按回车继续")
        time.sleep(2)
        try:
            window_handles = driver.window_handles
            driver.switch_to.window(window_handles[-1])
            time.sleep(random.randint(1, 3) / 10)
            element_edit = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".eds-button.eds-button--link.eds-button--normal"))
            )
            for elem in element_edit:
                if "编辑" in elem.get_attribute("innerText"):
                    driver.execute_script("arguments[0].click();", elem)
                    break
        except Exception as e:
            print(s)
            input("进入商品修改页面失败，请手动进入商品修改页面，并按回车继续")
        time.sleep(3)
        """修改商品信息"""
        try:
            window_handles = driver.window_handles
            driver.switch_to.window(window_handles[-1])
            # 检测这个显示，再检测这个消失，如果消失，再开始填写
            try:
                target_classname = 'loading-container'
                loading = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located(
                        (By.CLASS_NAME, target_classname))
                )
                if loading:
                    WebDriverWait(driver, 60).until(
                        EC.invisibility_of_element_located((By.CLASS_NAME, target_classname))
                    )
            except:
                target_classname = 'loading-container'
                WebDriverWait(driver, 10).until(
                    EC.invisibility_of_element_located((By.CLASS_NAME, target_classname))
                )
            try:
                link = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.LINK_TEXT, "运费"))
                )
            except:
                link = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.LINK_TEXT, "重量与尺寸"))
                )
            # 点击链接
            # 如果页面上有填写的位置，就执行，否则跳过
            driver.execute_script("arguments[0].click();", link)
            time.sleep(3)
            element_wight = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".edit-input-small"))
            )[0].find_elements(by=By.CSS_SELECTOR, value=".eds-input__input")
            if len(element_wight) > 0:
                original_weight = element_wight[0].get_attribute("modelvalue")
                time.sleep(0.1)
                element_wight[0].send_keys("1")
                element_wight[0].clear()
                element_wight[0].send_keys(w)
                try:
                    time.sleep(0.5)
                    element_Width = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".product-edit-form-item.product-dimension-edit-input"))
                    )[0].find_element(by=By.CSS_SELECTOR, value=".eds-input__input")
                    original_width = element_Width.get_attribute("modelvalue")
                    element_Width.clear()
                    element_Width.send_keys(wi)
                    time.sleep(0.5)
                    element_Length = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".product-edit-form-item.product-dimension-edit-input"))
                    )[1].find_element(by=By.CSS_SELECTOR, value=".eds-input__input")
                    original_length = element_Length.get_attribute("modelvalue")
                    element_Length.clear()
                    element_Length.send_keys(li)
                    time.sleep(0.5)
                    element_Height = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".product-edit-form-item.product-dimension-edit-input"))
                    )[2].find_element(by=By.CSS_SELECTOR, value=".eds-input__input")
                    original_height = element_Height.get_attribute("modelvalue")
                    element_Height.clear()
                    element_Height.send_keys(hi)
                    time.sleep(0.5)
                    element_save = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".eds-button.eds-button--primary.eds-button--normal.eds-button--xl-large"))
                    )[0]
                    driver.execute_script("arguments[0].click();", element_save)
                    time.sleep(3)
                    original = {
                        "weight": original_weight,
                        "width": original_width,
                        "length": original_length,
                        "height": original_height
                    }
                    new = {
                        "weight": w,
                        "width": wi,
                        "length": li,
                        "height": hi
                    }
                    log_modify_product(log_entries, s, original, new)
                except Exception as e:
                    input("修改重量尺寸失败，请手动修改，并按回车继续")
            else:
                print(f"修改尺寸未成功（ID:{s}）")
            if len(window_handles) > 2:
                driver.close()
                driver.switch_to.window(window_handles[-2])
                time.sleep(1)
            print(f"进度：{i+1}/{len(global_info)}")
        except Exception as e: # 这里有bug，明明修改好了，还是报错
            print(f"修改全球产品ID未成功（ID:{s}）")
            winsound.Beep(500, 500)
            print(f"原计划修改数值：重量-{w}-kg, 宽-{wi}-cm, 长-{li}-cm, 高-{hi}-cm")
            window_handles = driver.window_handles
            if len(window_handles) > 2:
                driver.close()
                driver.switch_to.window(window_handles[-2])
                time.sleep(1)
            print(f"进度：{i+1}/{len(global_info)}")
            time.sleep(3)
            elements_select = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-option"))
            )
            for elem in elements_select:
                if "商品ID" in elem.get_attribute("innerText"):
                    driver.execute_script("arguments[0].click();", elem)
                    break
            continue
def popup_close(driver):
    """关闭弹窗"""
    driver.implicitly_wait(2)
    try:
        time.sleep(1)
        pop_ups = driver.find_elements(by=By.CSS_SELECTOR, value=".eds-modal__content.eds-modal__content--medium")
        if len(pop_ups) > 0:
            try:
                target_class_name = "eds-icon eds-modal__close"
                target_class_name = target_class_name.replace(" ", ".")
                elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name))
                )[-1]
                driver.execute_script("arguments[0].click();", elements)
                print("弹窗已关闭")
            except Exception as e:
                input("请手动处理弹窗后按回车继续...")
                winsound.Beep(500, 500)
                winsound.Beep(500, 500)
        else:
            pass
    except:
        input("请手动处理弹窗后按回车继续...")
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
def into_dp_page(driver):
    """进入店铺页面"""
    try:
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-popover__ref"))
        )
        for elem in elements:
            if "店铺商品" in elem.get_attribute("innerText"):
                elem.click()
                popup_close(driver)
                break
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("进入店铺商品页面失败，手动在浏览器页面点击店铺商品页面，并按回车")
        popup_close(driver)
def exchange_store(s, driver):
    try:
        element_curent_shop = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "shop-switcher")))[
            0].find_elements(by=By.CLASS_NAME, value="shop-select")[0].find_elements(by=By.CLASS_NAME,
                                                                                     value="content")
        acount_exchange = 0
        while s not in element_curent_shop[0].get_attribute("innerText"):
            acount_exchange = acount_exchange + 1
            if acount_exchange < 6:
                time.sleep(2 * acount_exchange)
                element_search = WebDriverWait(driver, 10, 0.5).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "shop-switcher")))[0].find_elements(
                    by=By.CLASS_NAME, value="shop-search")[0].find_elements(by=By.CLASS_NAME,
                                                                            value="eds-input.search-input")
                element_search[0].find_elements(by=By.CLASS_NAME, value="eds-input__input")[0].clear()
                element_search[0].find_elements(by=By.CLASS_NAME, value="eds-input__input")[0].send_keys(s)
                element_shop = WebDriverWait(driver, 10, 0.5).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "shop-switcher")))[
                    0].find_elements(by=By.CLASS_NAME, value="shop-search")[0].find_elements(by=By.CLASS_NAME,
                                                                                             value="shop")
                for es in element_shop:
                    if s in es.get_attribute("innerText"):
                        driver.execute_script("arguments[0].click();", es) # 点击
                        break
                time.sleep(3)
                element_curent_shop = WebDriverWait(driver, 10, 0.5).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "shop-switcher")))[
                    0].find_elements(by=By.CLASS_NAME, value="shop-select")[0].find_elements(by=By.CLASS_NAME,
                                                                                             value="content")
                popup_close(driver)
            else:
                print("无法登录")
                break
        print("进入：", s, element_curent_shop[0].get_attribute("innerText"))
    except Exception as e:
        print(e)
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("店铺切换失败，手动切换后回车")
def product_screenshot(driver, window_handles,sku_id,save_path):
    driver.switch_to.window(window_handles[-1])
    time.sleep(5)
    element_freight = WebDriverWait(driver, 10, 0.5).until(
        EC.presence_of_all_elements_located(
            (By.CLASS_NAME, "eds-tabs__nav-tab")))
    for ef in element_freight:
        if "运费" in ef.get_attribute("innerText"):
            driver.execute_script("arguments[0].click();", ef)
            break
    time.sleep(5)
    element_freight = WebDriverWait(driver, 10, 0.5).until(
        EC.presence_of_all_elements_located(
            (By.CLASS_NAME, "eds-tabs__nav-tab")))
    for ef in element_freight:
        if "运费" in ef.get_attribute("innerText"):
            driver.execute_script("arguments[0].click();", ef)
            break
    time.sleep(2)
    element_freight = WebDriverWait(driver, 10, 0.5).until(
        EC.presence_of_all_elements_located(
            (By.CLASS_NAME, "eds-tabs__nav-tab")))
    for ef in element_freight:
        if "运费" in ef.get_attribute("innerText"):
            driver.execute_script("arguments[0].click();", ef)
            break
    time.sleep(1)
    # 检测等待页面加载完成，检测到 量这个字出现在屏幕中
    target_text = "重量"
    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.XPATH, f"//*[contains(text(), '{target_text}')]"))
        )
    except Exception as e:
        print(f"没有尺寸：{str(e)}")
        if len(window_handles) > 2:
            driver.close()
            driver.switch_to.window(window_handles[-2])
        return
    print(f"成功进入产品修改页面，开始截图：{sku_id} 后台尺寸图.png")
    # target_element = WebDriverWait(driver, 10).until(
    #     EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-v-501612ae][data-v-610fc010]"))
    # )[0]
    """
    方法2
    """
    #         target_text = "运费"
    #         location = WebDriverWait(driver, 10).until(
    #     EC.presence_of_all_elements_located((By.XPATH, f"//*[contains(text(), '{target_text}')]")
    # ))
    #         locations = []
    #         for loc in location:
    #             if loc.get_attribute("innerText") == target_text:
    #                 locations.append(loc.location)
    #                 if len(locations) >= 2:
    #                     break
    #
    #         if len(locations) >= 2:
    #             location1 = locations[0]
    #             location2 = locations[1]
    #         else:
    #             raise Exception("未找到足够的运费元素")
    full_page_screenshot_path = 'screenshot.png'
    driver.save_screenshot(full_page_screenshot_path)
    im = Image.open(full_page_screenshot_path)
    # 获取当前窗口的坐标系
    original_size = driver.get_window_size()
    # 每个账号可能不一样，所以这里需要根据实际情况进行修改
    left = original_size['width'] / 5
    top = original_size['height'] / 20
    right = left + original_size['width'] / 2
    bottom = top + original_size['height'] / 1.5
    im = im.crop((left, top, right, bottom))
    im.save(save_path)
    # 删除整个页面的截图
    if os.path.exists(full_page_screenshot_path):
        os.remove(full_page_screenshot_path)
    time.sleep(2)
    element_freight = WebDriverWait(driver, 10, 0.5).until(
        EC.presence_of_all_elements_located(
            (By.CLASS_NAME, "eds-tabs__nav-tab")))
    for ef in element_freight:
        if "运费" in ef.get_attribute("innerText"):
            driver.execute_script("arguments[0].click();", ef)
            break
    if len(window_handles) > 2:
        driver.close()
        driver.switch_to.window(window_handles[-2])  # 切换回主窗口
def capture_screenshot(s,driver, dp_info,times,global_info,num):
    """运费页面截图"""
    logger = logging.getLogger(__name__)
    
    try:
        print("开始进入产品修改页面截图")
        logger.info(f"开始为店铺 {s} 截图")
        
        dp_info = dp_info.sort_values(by="店铺账号", ascending=True)
        dp_info_img = dp_info[dp_info["店铺账号"] == s].copy()
        main_window = driver.current_window_handle
        
        # 获取资料缺失表路径
        path_manager = get_path_manager(times)
        to_do_path = os.path.join(path_manager.base_dir, '资料缺失表.csv')
        
        if os.path.exists(to_do_path):
            to_do = pd.read_csv(to_do_path, encoding='utf-8-sig')
        else:
            to_do = pd.DataFrame(columns=['当前目录名'])
            
        for j in range(len(dp_info_img)):  #
            id = dp_info_img.iloc[j]["产品ID"]
            order_id = dp_info_img.iloc[j]["SBS账单编号"]
            sku_id = dp_info_img.iloc[j]["SKU ID"]
            
            if sku_id not in to_do['当前目录名'].values:
                continue
            try:
                # 使用新的路径获取逻辑
                sbs_base_path = get_screenshot_save_path(times, dp_info, order_id)
                dir_path = os.path.join(sbs_base_path, f"{sku_id}")
                os.makedirs(dir_path, exist_ok=True)
                save_path = os.path.join(dir_path, f"{sku_id} 后台尺寸图.png")
                
                logger.info(f"截图保存路径: {save_path}")
                print(f"初始化地址成功：\n产品ID:{id}\nSBS账单编号:{order_id}\nSKU ID:{sku_id}\n保存路径:{dir_path}\n进度：{num}/{len(global_info)}")
                element_search = WebDriverWait(driver, 10, 0.5).until(
                    EC.presence_of_all_elements_located((
                        By.CSS_SELECTOR, ".eds-input__inner.eds-input__inner--normal")))[1].find_elements(by=By.CLASS_NAME, value="eds-input__input")[0]
                element_search.click()
                element_search.clear()
                element_search.send_keys(id)
                time.sleep(random.uniform(0.2, 0.5))
                element_search.send_keys(Keys.RETURN)
                time.sleep(3)
                # 进入产品修改页面
                element_edit = WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, ".eds-button.eds-button--link.eds-button--normal"))
                )
                for elem in element_edit:
                    if "修改" in elem.get_attribute("innerText"):
                        driver.execute_script("arguments[0].click();", elem) # 这个点击，有些点不进去，直接使用action点击
                        break
                time.sleep(3)
                WebDriverWait(driver, 30).until(lambda d: len(d.window_handles) > 1)
                window_handles = driver.window_handles
                if len(window_handles) > 2:
                    product_screenshot(driver, window_handles, sku_id,save_path)
                    num+=1
                    print(f"进度：{num}/{len(global_info)}")
                    driver.switch_to.window(main_window)
                else:
                    print(f"未在店铺\n{s}\n找到产品ID为\n{id}\n的页面")
                    num+=1
            except Exception as e:
                print(f"截图失败1：{str(e)}")
    except Exception as e:
        print(f"截图失败2：{str(e)}")
def attach_to_running_browser(port=9222):
    # 创建Chrome选项
    chrome_options = Options()
    # 添加调试端口以连接到现有浏览器
    chrome_options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
    # 如果你需要设置下载路径等偏好，使用以下方式
    # prefs = {"download.default_directory": "C:\\Downloads"}
    # chrome_options.add_experimental_option("prefs", prefs)
    # 创建WebDriver实例
    driver = webdriver.Chrome(options=chrome_options)
    return driver
def main():
    # 启动程序
    print("程序启动...")
    # 初始化日志
    log_entries = []
    # 读取excel任务文件
    df_info, times = read_excel()
    # 生成默认下载路径
    default_dl_path(times)
    # 获取主体店铺名称
    global_dp_name = get_global_dp_name()
    # 读取需要修改的全球产品信息
    global_info = get_global_info(df_info, global_dp_name)
    if len(global_info) == 0:
        print("没有需要修改的全球产品信息")
        # return
    else:
        print("需要修改的全球产品有：", len(global_info))
        # 读取需要截图的店铺产品信息
        dp_info = get_dp_info(df_info, global_dp_name)
        # 获取账号名称用于持久化会话
        account_name = input("请输入账号名称（用于区分不同账号的会话，可留空使用默认）：").strip()
        if not account_name:
            account_name = None
        
        # 启动浏览器（使用持久化会话）
        driver = settingChrome(times, account_name)
        # 登录账号（会自动检查是否已登录）
        login_CNSC(driver)
        # 进入全球产品修改页
        goon = 1
        while goon != 0:
            mode = input('改尺寸:1')
            if mode == '1':
                into_global_page(driver)
                # 修改全球产品信息
                modify_product(driver, global_info, log_entries)
            # 弹窗问题处理
            popup_close(driver)
            # 进入店铺产品修改页
            into_dp_page(driver)
            # 切换店铺
            dp_info_i = dp_info.sort_values(by="店铺账号", ascending=True).drop_duplicates(subset=["店铺账号"],keep='first')
            num = 0
            for i in range(len(dp_info_i)):  #
                s = dp_info_i.iloc[i]["店铺账号"]
                exchange_store(s, driver)
                # 截图运费信息
                time.sleep(2)
                popup_close(driver)
                capture_screenshot(s,driver, dp_info, times,global_info,num)
            log_columns = [
                "时间", "全球产品ID",
                "原始重量(g)", "原始宽度(cm)", "原始高度(cm)", "原始长度(cm)",
                "修改后重量(kg)", "修改后宽度(cm)", "修改后高度(cm)", "修改后长度(cm)"
            ]
            base_dir = default_dl_path(times)
            os.makedirs(base_dir, exist_ok=True)
            LOG_FILE = os.path.join(base_dir, "商品重量尺寸修改日志_log.csv")
            df_log = pd.DataFrame(log_entries, columns=log_columns)
            if os.path.exists(LOG_FILE):
                # 追加模式：不写列名，保留原有数据
                df_log.to_csv(LOG_FILE, mode='a', header=False, index=False, encoding='utf-8')
            else:
                # 新文件：写入列名
                df_log.to_csv(LOG_FILE, index=False, encoding='utf-8')
            goon = int(input("0结束，其他数字继续"))
        # 结束程序
    print("程序结束")

if __name__ == "__main__":
    # test_num = input("是否是测试？，0是，其他任意键否...")
    test_num = '1'
    if test_num == "0":
        # 使用示例
        try:
            input("请手动启动Chrome并使用--remote-debugging-port=9222参数启动，并点击回车继续...")
            # 确保Chrome已使用--remote-debugging-port=9222参数启动
            driver = attach_to_running_browser()
            input("test")
            # 执行一些操作
            print(driver.title)
            # 启动程序
            print("程序启动...")
            input("请手动登录shopee后台，按回车继续...")
            # 初始化日志
            log_entries = []
            # 读取excel任务文件
            df_info, times = read_excel()
            # 生成默认下载路径
            default_dl_path(times)
            # 获取主体店铺名称
            global_dp_name = get_global_dp_name()
            # 读取需要修改的全球产品信息
            global_info = get_global_info(df_info, global_dp_name)
            if len(global_info) == 0:
                print("没有需要修改的全球产品信息")
                # return
            else:
                print("需要修改的全球产品有：", len(global_info))
                # 读取需要截图的店铺产品信息
                dp_info = get_dp_info(df_info, global_dp_name)
                input("进入shopee后台，按回车继续...")
                input("确认进入shopee后台，按回车继续...")
                print(driver.title)
                print(driver.current_url)
                goon = 1
                while goon != 0:
                    mode = input("是否需要修改全球产品信息？，0取消修改，其他任意键继续...")
                    if mode != "0":
                        into_global_page(driver)
                        # 修改全球产品信息
                        modify_product(driver, global_info, log_entries)
                    # 弹窗问题处理
                    mode = input("是否需要截图？，0取消截图，其他任意键继续...")
                    if mode != "0":
                        popup_close(driver)
                        # 进入店铺产品修改页
                        into_dp_page(driver)
                        # 切换店铺
                        dp_info_i = dp_info.sort_values(by="店铺账号", ascending=True).drop_duplicates(subset=["店铺账号"],
                                                                                                       keep='first')
                        num = 0
                        for i in range(len(dp_info_i)):  #
                            s = dp_info_i.iloc[i]["店铺账号"]
                            exchange_store(s, driver)
                            # 截图运费信息
                            time.sleep(2)
                            popup_close(driver)
                            capture_screenshot(s, driver, dp_info, times, global_info, num)
                        log_columns = [
                            "时间", "全球产品ID",
                            "原始重量(g)", "原始宽度(cm)", "原始高度(cm)", "原始长度(cm)",
                            "修改后重量(kg)", "修改后宽度(cm)", "修改后高度(cm)", "修改后长度(cm)"
                        ]
                        base_dir = default_dl_path(times)
                        os.makedirs(base_dir, exist_ok=True)
                        LOG_FILE = os.path.join(base_dir, "商品重量尺寸修改日志_log.csv")
                        df_log = pd.DataFrame(log_entries, columns=log_columns)
                        if os.path.exists(LOG_FILE):
                            # 追加模式：不写列名，保留原有数据
                            df_log.to_csv(LOG_FILE, mode='a', header=False, index=False, encoding='utf-8')
                        else:
                            # 新文件：写入列名
                            df_log.to_csv(LOG_FILE, index=False, encoding='utf-8')
                    goon = int(input("0结束，其他数字继续"))
                # 结束程序
            print("程序结束")
        except Exception as e:
            print(f"连接浏览器时出错: {e}")
            # 打印详细的错误堆栈
            import traceback

            print(traceback.format_exc())
        finally:
            # 不关闭浏览器，保持现有会话
            pass
    else:
        main()