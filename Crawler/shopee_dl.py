#coding=utf-8
from selenium import webdriver
import time
import random
import winsound
import os
import shutil
import pandas as pd
import logging
import math
from pathlib import Path
import numpy as np
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from datetime import datetime, timedelta
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException,ElementClickInterceptedException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import traceback
import re
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("shopee_download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
def cleanup_chrome_locks(user_data_dir):
    """清理Chrome用户数据目录中的锁定文件"""
    try:
        import psutil
        import glob
        
        # 清理可能的锁定文件
        lock_files = [
            os.path.join(user_data_dir, "SingletonLock"),
            os.path.join(user_data_dir, "SingletonSocket"),
            os.path.join(user_data_dir, "SingletonCookie")
        ]
        
        for lock_file in lock_files:
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                    logger.info(f"已清理锁定文件: {lock_file}")
                except Exception as e:
                    logger.warning(f"无法清理锁定文件 {lock_file}: {e}")
        
        # 清理临时文件
        temp_patterns = [
            os.path.join(user_data_dir, "**/Temp/*"),
            os.path.join(user_data_dir, "**/Cache/*"),
        ]
        
        for pattern in temp_patterns:
            for temp_file in glob.glob(pattern, recursive=True):
                try:
                    if os.path.isfile(temp_file):
                        os.remove(temp_file)
                except Exception:
                    pass  # 忽略临时文件清理错误
                    
    except ImportError:
        logger.warning("psutil未安装，跳过进程检查")
    except Exception as e:
        logger.warning(f"清理Chrome锁定文件时出错: {e}")
def kill_conflicting_chrome_processes(user_data_dir):
    """终止可能冲突的Chrome进程"""
    try:
        import psutil
        
        killed_processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                    cmdline = proc.info['cmdline']
                    if cmdline and any(user_data_dir in arg for arg in cmdline):
                        logger.info(f"终止冲突的Chrome进程: PID {proc.info['pid']}")
                        proc.terminate()
                        killed_processes.append(proc.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        
        # 等待进程终止
        if killed_processes:
            time.sleep(2)
            logger.info(f"已终止 {len(killed_processes)} 个冲突的Chrome进程")
            
    except ImportError:
        logger.warning("psutil未安装，跳过进程终止")
    except Exception as e:
        logger.warning(f"终止Chrome进程时出错: {e}")
def smart_move(source, destination, overwrite=False):
    """智能移动单个文件，处理同名冲突
    
    Args:
        source: 源文件路径
        destination: 目标文件路径
        overwrite: 是否覆盖同名文件，默认为False
    """
    if not os.path.exists(destination):
        shutil.move(source, destination)
        return destination

    # 如果设置了覆盖模式，直接覆盖
    if overwrite:
        shutil.move(source, destination)
        return destination

    dirname = os.path.dirname(destination)
    base_name, ext = os.path.splitext(os.path.basename(destination))

    match = re.search(r' \((\d+)\)$', base_name)
    if match:
        existing_num = int(match.group(1))
        base_name = base_name[:match.start()]
        new_num = existing_num + 1
    else:
        new_num = 1

    while True:
        new_name = f"{base_name} ({new_num}){ext}"
        new_destination = os.path.join(dirname, new_name)
        if not os.path.exists(new_destination):
            shutil.move(source, new_destination)
            return new_destination
        new_num += 1
def batch_smart_move_recursive(source_dir, target_dir, overwrite=False):
    """递归批量移动文件夹中的所有文件到目标文件夹
    
    Args:
        source_dir: 源文件夹路径
        target_dir: 目标文件夹路径
        overwrite: 是否覆盖同名文件，默认为False
    """
    os.makedirs(target_dir, exist_ok=True)

    for root, _, files in os.walk(source_dir):
        # 计算相对路径，保持目录结构
        relative_path = os.path.relpath(root, source_dir)
        target_subdir = os.path.join(target_dir, relative_path)
        os.makedirs(target_subdir, exist_ok=True)

        for filename in files:
            source_file = os.path.join(root, filename)
            target_file = os.path.join(target_subdir, filename)
            smart_move(source_file, target_file, overwrite)
            print(f"已移动: {os.path.relpath(source_file, source_dir)} → {os.path.relpath(target_file, target_dir)}")
# 重试装饰器
def retry_on_failure(func):
    def wrapper(*args, **kwargs):
        max_retries = 1
        retries = 0
        while retries <= max_retries:
            try:
                return func(*args, **kwargs)
            except (TimeoutException, NoSuchElementException, StaleElementReferenceException, ElementClickInterceptedException) as e:
                if retries < max_retries:
                    logger.warning(f"操作 {func.__name__} 失败，正在重试第 {retries + 1} 次... 错误: {e}")
                    retries += 1
                    time.sleep(2)  # 添加延迟等待页面稳定
                else:
                    logger.error(f"操作 {func.__name__} 重试后仍然失败，请手动处理。错误信息: {e}")
                    winsound.Beep(500, 500)
                    input("出现异常,按 Enter 键继续...")
                    return None
            except Exception as e:
                logger.error(f"操作 {func.__name__} 出现未预期的错误: {e}")
                winsound.Beep(500, 500)
                input(f"出现未预期的错误: {e}, 按 Enter 键继续...")
                return None
    return wrapper
@retry_on_failure
# 读取店铺 ID 和名称表格
def read_shop_table(info_path):
    try:
        df = pd.read_excel(info_path, sheet_name='数据说明')
        df = df[['公司主体', '平台店铺2', '店铺ID']]
        # 修改列名为实际需要的字段
        df = df.groupby('平台店铺2').first().reset_index()
        company_accounts = df['公司主体'].tolist()  # 用于确定登录账号
        platform_shops = df['平台店铺2'].tolist()   # 用于店铺切换和文件命名
        shop_ids = df['店铺ID'].tolist()  # 用于确认是否为跨店履约
        # 特殊店铺处理标记（这里先记录状态，后续流程处理）
        for i in range(len(platform_shops)):
            if platform_shops[i] in ['shopee南宁仓家居', 'shopee南宁仓汽配']:
                # 设置标记，后续流程处理切换仓库操作
                df.at[i, 'need_region_switch'] = True

        return company_accounts, platform_shops, shop_ids, df  # 返回新增的完整数据用于后续处理
    except FileNotFoundError:
        print("错误: 店铺表格文件未找到!")
        return [], [], [], None
    except KeyError as e:
        print(f"CSV 文件缺少必要列: {e}")
        return [], [], [], None
@retry_on_failure
# 初始化浏览器驱动
def init_driver(download_dir, use_persistent_session=True, account_name=None):
    from config import Config
    import getpass
    
    options = webdriver.ChromeOptions()
    prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': download_dir,
             "profile.default_content_setting_values.automatic_downloads": 1}
    options.add_experimental_option('prefs', prefs)
    
    # 添加Chrome稳定性参数
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-web-security')
    options.add_argument('--allow-running-insecure-content')
    options.add_argument('--disable-extensions')
    
    # 配置持久化会话
    if use_persistent_session and Config.is_persistent_session_enabled():
        # 为每个账号创建独立的用户数据目录
        username = getpass.getuser()
        if account_name:
            # 每个账号使用完全独立的用户数据目录
            user_data_dir = f"C:\\Users\\{username}\\AppData\\Local\\Google\\Chrome\\User Data\\AutomationProfile_{account_name}"
            profile_dir = "Default"  # 在独立目录下使用默认配置文件
        else:
            user_data_dir = f"C:\\Users\\{username}\\AppData\\Local\\Google\\Chrome\\User Data\\AutomationProfile"
            profile_dir = "Default"
        
        # 确保用户数据目录存在
        os.makedirs(user_data_dir, exist_ok=True)
        
        # 终止可能冲突的Chrome进程
        kill_conflicting_chrome_processes(user_data_dir)
        
        # 清理可能的Chrome锁定文件
        cleanup_chrome_locks(user_data_dir)
        
        options.add_argument(f"--user-data-dir={user_data_dir}")
        options.add_argument(f"--profile-directory={profile_dir}")
        
        # 添加端口隔离，避免多实例冲突
        if account_name:
            # 为每个账号分配不同的调试端口
            account_hash = hash(account_name) % 1000
            debug_port = 9222 + account_hash
            options.add_argument(f"--remote-debugging-port={debug_port}")
            logger.info(f"使用账号 '{account_name}' 的持久化会话: {user_data_dir}, 调试端口: {debug_port}")
        else:
            options.add_argument("--remote-debugging-port=9222")
            logger.info(f"使用默认持久化会话: {user_data_dir}")
    
    # 浏览器进程独立运行
    if Config.BROWSER_CONFIG.get("detach", True):
        options.add_experimental_option('detach', True)
    
    # 无头模式配置
    if Config.BROWSER_CONFIG.get("headless", False):
        options.add_argument('--headless')
    
    try:
        service = Service('./chromedriver.exe')
        driver = webdriver.Chrome(service=service, options=options)
        driver.implicitly_wait(5)  # 增加隐式等待时间
        return driver
    except Exception as e:
        logger.error(f"初始化浏览器失败: {e}")
        # 如果使用持久化会话失败，尝试不使用持久化会话
        if use_persistent_session:
            logger.info("尝试不使用持久化会话重新初始化浏览器...")
            return init_driver(download_dir, False, account_name)
        else:
            raise e
@retry_on_failure
# 登录 Shopee 账号
def login(driver, account):
    driver.get("https://seller.shopee.cn/")
    time.sleep(5)
    logger.info(f"登录账号: {account}")
    
    # 获取账号对应的密码，用于后续可能的安全验证
    if account in ['趣利恩', '海湃', '遨腾','五店']:
        if account == '趣利恩':
            ac = 'trillion:main'
            pw = 'atAOTENG2021'
        elif account == '海湃':
            ac = 'Trillion2020:wuwang'
            pw = 'qwer1121.'
        elif account == '遨腾':
            ac = 'yishida:wuwang'
            pw = 'wuwang2020'
        elif account == '五店':
            ac = 'Motor_Plus:main'
            pw = 'Jj352520564'
    else:
        ac = input(f"请输入your_account账号:")
        pw = input("请输入your_account密码:")
    
    # 检查是否已经登录（持久化会话可能已经保持登录状态）
    try:
        # 尝试查找已登录状态的元素
        store_name = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "account-name")))
        logger.info(f"已通过持久化会话登录：{store_name.get_attribute('innerText')}")
        # 返回密码，用于后续可能的安全验证
        return pw
    except (TimeoutException, NoSuchElementException):
        # 如果未找到登录状态元素，则需要手动登录
        logger.info("需要手动登录")
        
    try:
        # 等待输入框出现
        username_input, password_input = WebDriverWait(driver, 60, 0.5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-input__input")))
        
        # 清空输入框并输入账号密码
        username_input.clear()
        username_input.send_keys(ac)
        password_input.clear()
        password_input.send_keys(pw)
        
        # 点击登录按钮
        login_button = driver.find_element(by=By.CLASS_NAME, value=
        "eds-button.eds-button--primary.eds-button--large.eds-button--block.submit-btn")
        driver.execute_script("arguments[0].click();", login_button)
        
        time.sleep(10)
        input("请手动处理验证码后按回车继续...")
        
        # 验证登录成功
        for tt in range(5):
            try:
                store_name = WebDriverWait(driver, 30, 0.5).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "account-name")))
                logger.info(f"已登录：{store_name.get_attribute('innerText')}")
                return pw
            except (TimeoutException, NoSuchElementException):
                logger.warning(f"等待加载登录 {tt+1}/5")
                time.sleep(2)
                
        # 如果循环结束仍未登录成功
        logger.warning("登录状态验证失败，尝试继续操作")
        return pw  # 即使验证失败也返回密码，以便后续可能的安全验证
        
    except TimeoutException:
        # 尝试刷新页面重新登录
        logger.warning("登录页面加载超时，尝试刷新")
        driver.get("https://seller.shopee.cn/")
        driver.refresh()
        time.sleep(10)
        
        try:
            username_input, password_input = WebDriverWait(driver, 120, 0.5).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-input__input")))
            
            username_input.clear()
            username_input.send_keys(ac)
            password_input.clear()
            password_input.send_keys(pw)
            
            login_button = driver.find_element(by=By.CLASS_NAME, value=
            "eds-button.eds-button--primary.eds-button--large.eds-button--block.submit-btn")
            driver.execute_script("arguments[0].click();", login_button)
            
            time.sleep(3)
            input("请手动处理验证码后按回车继续...")
            
            # 验证登录成功
            for tt in range(5):
                try:
                    store_name = WebDriverWait(driver, 30, 0.5).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "account-name")))
                    logger.info(f"已登录：{store_name.get_attribute('innerText')}")
                    return pw
                except (TimeoutException, NoSuchElementException):
                    logger.warning(f"等待加载登录 {tt+1}/5")
                    time.sleep(2)
            
            # 即使验证失败也返回密码
            logger.warning("登录状态验证失败，尝试继续操作")
            return pw
        except Exception as e:
            logger.error(f"刷新后登录失败: {e}")
            # 如果已经获取了密码，即使登录失败也返回密码，以便后续可能的手动处理
            if 'pw' in locals():
                return pw
            else:
                # 如果没有获取到密码，提示用户手动输入
                pw = input("登录失败，请手动输入密码用于后续验证:")
                return pw
@retry_on_failure
# 处理弹窗
def handle_popup(driver):
    print("处理弹窗")
    """关闭弹窗"""
    driver.implicitly_wait(2)
    try:
        time.sleep(1)
        pop_ups = driver.find_elements(by=By.CSS_SELECTOR, value=".eds-modal__content.eds-modal__content--medium")
        if len(pop_ups) > 0:
            input("请手动处理弹窗后按回车继续...")
            winsound.Beep(500, 500)
            winsound.Beep(500, 500)
        else:
            pass
    except:
        pass
# 处理浏览器通知弹窗，一律点关闭
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

def main_popup_close(driver):
    """关闭所有可见的弹窗"""
    driver.implicitly_wait(2)
    max_attempts = 5  # 最大尝试次数防止无限循环
    attempts = 0
    closed_count = 0
    while attempts < max_attempts:
        try:
            # 定位所有可见的弹窗元素
            pop_ups = driver.find_elements(
                By.XPATH,
                "//*[contains(@class, 'eds-modal__box') and not(contains(@style, 'display: none;'))]"
            )
            if not pop_ups:
                if closed_count > 0:
                    logging.info(f"已关闭 {closed_count} 个弹窗")
                return  # 没有弹窗时退出
            for pop_up in pop_ups:
                try:
                    # 在弹窗内部查找关闭按钮
                    close_buttons = pop_up.find_elements(
                        By.CLASS_NAME, "eds-icon.eds-modal__close"
                    )
                    if close_buttons:
                        # 点击第一个可见的关闭按钮
                        close_button = next(
                            (btn for btn in close_buttons if btn.is_displayed()),
                            None
                        )
                        if close_button:
                            driver.execute_script("arguments[0].click();", close_button)
                            time.sleep(0.5)  # 等待弹窗关闭
                            closed_count += 1
                            print(f"关闭弹窗: {pop_up.text[:30]}...")
                            break  # 关闭一个后重新检查DOM
                except StaleElementReferenceException:
                    continue  # 元素已失效，继续下一个
            time.sleep(1)
            attempts += 1
        except Exception as e:
            print(f"弹窗处理异常: {e}")
            break
    if closed_count > 0:
        print(f"共关闭 {closed_count} 个弹窗")
    else:
        print("未检测到可关闭的弹窗")
def handle_alert(driver):
    """处理浏览器通知弹窗"""
    try:
        alert = WebDriverWait(driver, 10).until(
            EC.alert_is_present()
        )
        alert.dismiss()
        print("已处理浏览器通知弹窗")
    except TimeoutException:
        pass
# 进入 SBS 服务
def into_sidebar_page(driver,sidebar_name):
    """进入相关—侧边栏服务"""
    try:
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "sidebar-menu"))
        )[0].find_elements(by=By.XPATH, value=f".//*[contains(text(), '{sidebar_name}')]") # .find_elements(by=By.LINK_TEXT, value="Shopee服务")
        # 滑到元素位置
        # 如果有两个一样的，选择第二个元素
        if len(elements) > 1:
            element = elements[1]
        else:
            element = elements[0]
        ActionChains(driver).move_to_element(element).perform()
        element.click()
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("进入Shopee服务页面失败，手动在浏览器页面点击Shopee服务页面，并按回车")
        time.sleep(1)
@retry_on_failure
# 切换店铺
def switch_shop(driver, shop_name):
    print(f"切换到店铺 : {shop_name}")
    try:
        element_search = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-input__input")))[
            0]
        element_menu_shop = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-search__menu")))[
            0]
        acount_exchange = 0
        while shop_name in element_menu_shop.get_attribute("innerText"):
            acount_exchange = acount_exchange + 1
            if acount_exchange < 6:
                time.sleep(2 * acount_exchange)
                element_search.clear()
                element_search.send_keys(shop_name)
                element_shop = WebDriverWait(driver, 10, 0.5).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-option")))[
                    -1].find_elements(by=By.CLASS_NAME, value="shop-name-item-shop")[-1]
                if shop_name in element_shop.get_attribute("innerText"):
                    driver.execute_script("arguments[0].click();", element_shop) # 点击
                    break
                time.sleep(1)
                element_curent_shop = WebDriverWait(driver, 10, 0.5).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-selector__inner.line-clamp--1")))
                for elem in element_curent_shop:
                    if shop_name in elem.get_attribute("innerText"):
                        print("进入：", shop_name)
                        break

            else:
                print("无法切换，正在重试")
                break
    except TimeoutException as e:
        winsound.Beep(500, 500)
        input("switch_shop-TimeoutException店铺切换失败，手动切换后回车")
    except Exception as e:
        print(f"切换店铺失败: {str(e)}")
        winsound.Beep(500, 500)
        input("switch_shop-店铺切换失败，手动切换后回车")
# 切换所有店铺
@retry_on_failure
# 切换店铺
def switch_all_shop(driver):
    # 这里需要根据实际情况实现切换店铺逻辑
    print("切换到所有店铺一起下载")
    try:
        element_search = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "trigger-icon.eds-icon")))
        element_click = element_search[0]
        element_click.click()
        target_text = "所有店铺"
        element_select = driver.find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
        for elem in element_select:
            if target_text in elem.get_attribute("innerText"):
                driver.execute_script("arguments[0].click();", elem)
                break
        print('已切换到所有店铺')
    except:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("切换为所有店铺失败，请手动切换后回车")
@retry_on_failure
# 切换仓库
def switch_warehouse(driver, warehouse_name):
    print(f"切换仓库 : {warehouse_name}")
    try:
        select_box = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-select__options"))
        )[-1]

        # 查找所有选项
        options = select_box.find_elements(By.CLASS_NAME, "eds-option")

        found = False
        for option in options:
            if warehouse_name in option.get_attribute("innerText"):
                driver.execute_script("arguments[0].scrollIntoView();", option)
                driver.execute_script("arguments[0].click();", option)
                print("已切换到仓库")
                found = True
                break

        if not found:
            raise NoSuchElementException(f"未找到仓库选项: {warehouse_name}")

    except Exception as e:
        print(f"切换仓库失败: {str(e)}")
        input(f"切换仓库失败，请手动切换<{warehouse_name}>后回车")
@retry_on_failure
# 进入shopee服务库存页面
def switch_type_page(driver,target_text):
    """进入SBS库存"""
    try:
        # target_text = "Shopee服务库存"
        # target_text = "Shopee服务入库"
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-tabs__nav-tabs"))
        )[0].find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
        for elem in elements:
            if target_text in elem.get_attribute("innerText"):
                elem.click()
                break
            time.sleep(1)
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input(f"进入{target_text}失败，手动在浏览器页面点击库存页面，并按回车")
        time.sleep(1)
# 点击下载库存动销数据
def click_dl_kc_button(driver):
    """点击下载按钮"""
    # 点击导出
    target_text = "导出"
    elements = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-button.eds-button--large"))
    )[0].find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
    for elem in elements:
        if target_text == elem.get_attribute("innerText"):
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", elem)
            break
    time.sleep(random.randint(1, 3))
    target_output = "全部"
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-dropdown-menu"))
    )[-1].find_elements(By.CLASS_NAME, "eds-dropdown-item")[0].find_elements(By.XPATH,
                                                                             f"//*[contains(text(), '{target_output}')]")[
        -1]
    if target_output == element.get_attribute("innerText"):
        # 点击元素
        # element.click()
        driver.execute_script("arguments[0].click();", element)
    else:
        print("无法导出")
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("无法导出，请手动导出，并按回车")
        time.sleep(1)
@retry_on_failure
def download_kcdx_data(driver,start_time,final_time):
    """下载库存动销数据"""
    #进入库存动销
    try:
        time.sleep(1)
        target_text = "库存动销"
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-tabs__nav-tabs"))
        )[1].find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
        for elem in elements:
            if target_text in elem.get_attribute("innerText"):
                elem.click()
                break
            time.sleep(1)
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("进入库存动销页面失败，手动在浏览器页面点击库存页面，并按回车")
        time.sleep(1)
    # 选择下载时间
    try:
        """废案，直接用JavaScript修改编辑功能不够稳定"""
        # target_text = " – "
        # elements = WebDriverWait(driver, 10).until(
        #     EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-selector__inner.line-clamp--1"))
        # )[-1].find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
        # for elem in elements:
        #     time.sleep(0.3)
        #     if "/" in elem.get_attribute("innerText"):
        #         print(elem.get_attribute("innerText"))
        #         # print(elem.tag_name)
        #         if isinstance(start_time, datetime):  # 正确判断datetime对象
        #             start_time = start_time.strftime("%d/%m/%Y")
        #         if isinstance(final_time, datetime):  # 正确判断datetime对象
        #             final_time = final_time.strftime("%d/%m/%Y")
        #         # start_time = start_time.strftime("%d/%m/%Y")
        #         # final_time = final_time.strftime("%d/%m/%Y")
        #         new_text = start_time + target_text + final_time
        #         driver.execute_script("arguments[0].contentEditable = true;", elem)
        #         driver.execute_script("arguments[0].textContent = '';", elem)
        #         # driver.execute_script(f"arguments[0].textContent = {new_text};", elem)
        #         time.sleep(0.3)
        #         elem.send_keys(new_text)
        #         print(f"已将时间段修改为: {new_text}")
        target_text = " – "
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-selector__inner.line-clamp--1"))
        )[-1].find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
        for elem in elements:
            time.sleep(0.3)
            if "/" in elem.get_attribute("innerText"):
                driver.execute_script("arguments[0].click();", elem)
        input_date(driver, start_time, final_time)
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("选择下载输入时间错误，请手动调整")
        # 点击搜索
    try:
        target_text = "搜索"
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "search-btn.eds-button.eds-button--primary.eds-button--normal.eds-button--outline"))
        )[0].find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
        for elem in elements:
            if target_text == elem.get_attribute("innerText"):
                time.sleep(0.3)
                driver.execute_script("arguments[0].click();", elem)
                break
        time.sleep(1)
        click_dl_kc_button(driver)
        time.sleep(1)
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("点击下载按钮失败，请手动点击下载按钮，并按回车")
# 点击下载库龄数据
@retry_on_failure
def download_kl_data(driver):
    """下载库龄数据"""
    #进入库龄
    try:
        target_text = "库龄"
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-tabs__nav-tabs"))
        )[1].find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
        for elem in elements:
            if target_text in elem.get_attribute("innerText"):
                elem.click()
                break
            time.sleep(1)
        click_dl_kc_button(driver)
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("进入库龄页面失败，手动在浏览器页面点击库存页面，并按回车")
        time.sleep(1)
# 点击下载入库数据
@retry_on_failure
def download_rk_data(driver,final_time,chrome_dl_path):
    try:
        switch_all_shop(driver)
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("切换所有店铺失败，请手动切换后回车")
    try:
        traget_class_name = 'eds-pagination-sizes__content'
        traget_class_name = traget_class_name.replace(" ", ".")
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, traget_class_name))
        )
        driver.execute_script("arguments[0].click();", elements[0])
        traget_class_name = 'eds-dropdown-item'
        traget_class_name = traget_class_name.replace(" ", ".")
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, traget_class_name))
        )
        driver.execute_script("arguments[0].click();", elements[-1])
        time.sleep(1)
    except Exception as e:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("切换40个每页，请手动切换后回车")
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "fbs-ir-list"))
        )[0].find_element(By.XPATH, "//tbody[@data-v-06e73db8]")
        text = element.get_attribute("innerText")
        
        if not text:
            winsound.Beep(500, 500)
            input("入库页面为空，请检查，并按回车")
            
        first_line = text.split('\n')[1]
        digits = ''.join(filter(str.isdigit, first_line))
        first_eight_digits = digits[:8]
        
        try:
            date = datetime.strptime(first_eight_digits, "%Y%m%d")
        except ValueError:
            logger.error(f"无法解析日期: {first_eight_digits}")
            input("日期格式错误，请手动处理后按回车继续...")
            
        # 下载近90天的数据，但限制最多处理10页

        date += timedelta(days=90)
        page_count = 0
        max_pages = 10  # 设置最大处理页数
        if type(final_time) == 'str':
            final_time = datetime.strptime(final_time, "%Y%m%d")
        while final_time < date and page_count < max_pages:
            logger.info(f"处理入库数据页面 {page_count+1}/{max_pages}")
            page_count += 1
            dl_rk_sbs_all_data(driver)
            # wait_download(driver)
            download_with_retry(driver, chrome_dl_path)
            # 点击下一页
            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "eds-button.eds-button--small.eds-button--frameless.eds-button--block.eds-pager__button-next"))
                )
                driver.execute_script("arguments[0].click();", next_button)
                logger.info("已点击下一页按钮")
                time.sleep(5)
                # 每一页都判断一遍是否需要继续
                element = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "fbs-ir-list"))
                )[0].find_element(By.XPATH, "//tbody[@data-v-06e73db8]")
                text = element.get_attribute("innerText")
                if not text:
                    winsound.Beep(500, 500)
                    input("入库页面为空，请检查，并按回车")

                first_line = text.split('\n')[1]
                digits = ''.join(filter(str.isdigit, first_line))
                first_eight_digits = digits[:8]
                try:
                    date = datetime.strptime(first_eight_digits, "%Y%m%d")
                    date += timedelta(days=90)
                except ValueError:
                    logger.error(f"无法解析日期: {first_eight_digits}")
                    input("日期格式错误，请手动处理后按回车继续...")
            except Exception as e:
                logger.warning(f"点击下一页失败，可能已到最后一页: {e}")
                break
                
        if page_count >= max_pages:
            logger.warning(f"已达到最大页数限制({max_pages}页)，停止处理更多入库数据")
            
    except Exception as e:
        logger.error(f"下载入库数据失败: {e}")
        winsound.Beep(500, 500)
        input("下载入库数据失败，请手动下载，并按回车")
# 等待下载完成
@retry_on_failure
def wait_download(driver):
    start_dl_time = time.time()
    try:
        # 点击下载，并等待下载完成
        target_text = "任务中心"
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "task-center-text"))
        )[0].find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
        for elem in elements:
            if target_text in elem.get_attribute("innerText"):
                driver.execute_script("arguments[0].click();", elem)
                break
            time.sleep(1)
        # 下载最新的文件
        target_text = "处理中"
        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element_located((By.XPATH, f"//*[contains(text(), '{target_text}')]")
        ))
        target_text = "新的"
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, f"//*[contains(text(), '{target_text}')]")
        ))
        target_class_name = 'download eds-button eds-button--link eds-button--normal'
        target_class_name = target_class_name.replace(" ", ".")
        dl_new_data = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name)
                                                ))[0]
        driver.execute_script("arguments[0].click();", dl_new_data)
        print("等待下载完成...")
        time.sleep(5)
        # 关闭
        elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-icon.eds-modal__close"))
        )[-1]
        driver.execute_script("arguments[0].click();", elements)
        return start_dl_time
    except Exception as e:
        logger.error(f"等待下载失败: {e}")
        return start_dl_time
# 获取最新下载文件
@retry_on_failure
def get_latest_files(chrome_dl_path,start_dl_time): # 获取最新下载的文件
    list_of_files = [x for x in os.listdir(chrome_dl_path)
                     if os.path.isfile(os.path.join(chrome_dl_path, x))
                     and os.path.getctime(os.path.join(chrome_dl_path, x)) > start_dl_time]
    if not list_of_files:
        logger.warning(f"\n警告: 在 {chrome_dl_path} 目录中未找到 {datetime.fromtimestamp(start_dl_time)} 之后创建的excel文件")
        return None  # 返回空值让调用方跳过后续处理
    full_path = [os.path.join(chrome_dl_path, x) for x in list_of_files]
    latest_file = max(full_path, key=os.path.getctime)
    return latest_file
# 在文件末尾添加店铺名称
@retry_on_failure
def add_shop_name(file_path, shop_name):
    # 分离文件名和扩展名
    dir_path, filename = os.path.split(file_path)
    base_name, ext = os.path.splitext(filename)
    # 构造新文件名（原文件名 店铺名称.扩展名）
    new_filename = f"{base_name}{shop_name}{ext}"
    new_path = os.path.join(dir_path, new_filename)
    if os.path.exists(new_path):
        os.remove(new_path)
    # 重命名文件
    os.rename(file_path, new_path)
    return new_path
# 备份文件到指定目录
@retry_on_failure
def backup_file(chrome_dl_path, backup_dir):
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    for file in os.listdir(chrome_dl_path):
        file_path = os.path.join(chrome_dl_path, file)
        if os.path.isfile(file_path):
            shutil.copy2(file_path, backup_dir)
def default_dl_path(final_time):
    final_time = datetime.strftime(final_time, "%Y.%m.%d")
    info_path = r'\\Auteng\综合管理部\综合管理部_公共\数据分层处理\3数据处理dwd-库存周报\库存分摊数据说明.xlsx'
    download_dir = os.path.join(r'\\Auteng\综合管理部\综合管理部_公共\数据分层处理\1源数据下载\脚本下载数据',final_time)
    os.makedirs(download_dir, exist_ok=True)
    backup_dir = os.path.join(r'\\AUTENG\data_center\4.1进销存台账源数据周度',final_time)
    os.makedirs(backup_dir, exist_ok=True)
    return download_dir, backup_dir, info_path
def start_final_time():
    final_time = input("请输入本期期末时间（格式为yyyy.mm.dd）默认为下载当天的前一天的最后一刻：")
    frequency = input("请输入更新频率(周度or月度or天数)默认为周度更新：")
    if not final_time and not frequency:
        # 获取当前日期并减1天
        yesterday = datetime.now() - timedelta(days=1)
        final_time = yesterday
        frequency = 6
        start_time = final_time - timedelta(days=frequency)
    else:
        # 将输入字符串转为日期对象（便于后续可能需要的计算）
        final_time = datetime.strptime(final_time, "%Y.%m.%d")
        if not frequency:
            frequency = 6
        elif frequency == '月度':
            frequency = 30
        elif frequency == '周度':
            frequency = 6
        else:
            frequency = int(frequency)
        start_time = final_time - timedelta(days=frequency)
    print(f"期初时间：{start_time},期末时间：{final_time+timedelta(days=1)-timedelta(seconds=1)},周期：{frequency+1}天")
    return final_time, start_time
def download_and_process_data(driver, shop_name, start_time, final_time, chrome_dl_path):
    try:
        download_kcdx_data(driver, start_time, final_time)
        time.sleep(2)
        start_dl_time1 = wait_download(driver)
        time.sleep(2)
        latest_file1 = get_latest_files(chrome_dl_path, start_dl_time1)
        if latest_file1:
            add_shop_name(latest_file1, shop_name)
        else:
            input("库存动销未下载，请确认下载是否成功后按回车")
        download_kl_data(driver)
        time.sleep(2)
        start_dl_time2 = wait_download(driver)
        time.sleep(2)
        latest_file2 = get_latest_files(chrome_dl_path, start_dl_time2)
        if latest_file2:
            add_shop_name(latest_file2, shop_name)
        else:
            input("库龄数据未下载，请确认下载是否成功后按回车")
    except Exception as e:
        print(f"处理店铺 {shop_name} 时出现错误: {e}")
def bill_pw_verification(driver,account,pw):
    security_title = driver.find_elements(by=By.CSS_SELECTOR, value=".shopee-security-session__title")
    if len(security_title) == 0:
        security_title2 = driver.find_elements(by=By.LINK_TEXT,  value="点击重试")
        if len(security_title2) > 0:
            driver.execute_script("arguments[0].click();", security_title2[0])
            time.sleep(1)
        driver.implicitly_wait(2)
    security_title = driver.find_elements(by=By.CSS_SELECTOR, value=".shopee-security-session__title")
    if len(security_title) > 0:
    # 验证密码
        try:
            logger.info("检测到安全验证页面")
            traget_class_name = 'eds-input__input'
            traget_class_name = traget_class_name.replace(" ", ".")
            password_input = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, traget_class_name))
            )[-1]
            password_input.clear()
            password_input.send_keys(pw)
            time.sleep(0.3)
            WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., '进行验证')]"))
            ).click()
            logger.info("密码验证成功")
            time.sleep(1)
        except Exception as e:
            logger.error(f"{account} 密码验证失败")
            input(f"{account} 密码验证失败，请手动验证后按回车继续...")
    else:
        security_title = driver.find_elements(by=By.CSS_SELECTOR, value=".modal-verify-password__title")
        if len(security_title) > 0:
            try:
                logger.info("检测到安全验证页面")
                traget_class_name = 'eds-input__input'
                traget_class_name = traget_class_name.replace(" ", ".")
                password_input = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, traget_class_name))
                )[-2]
                password_input.clear()
                password_input.send_keys(pw)
                time.sleep(0.3)
                WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(., '进行验证')]"))
                ).click()
                logger.info("密码验证成功")
                time.sleep(1)
            except Exception as e:
                logger.error(f"{account} 密码验证失败")
                input(f"{account} 密码验证失败，请手动验证后按回车继续...")

def onlyswitch(driver, s):
    print("切换")
    try:
        element_curent_shop = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "shop-switcher")))[
            0].find_elements(by=By.CLASS_NAME, value="shop-select")[0].find_elements(by=By.CLASS_NAME,
                                                                                     value="content")
        acount_exchange = 0
        current_info = ""
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
                        driver.execute_script("arguments[0].click();", es)
                        break
                time.sleep(3)
                try:
                    element_curent_shop = WebDriverWait(driver, 10, 0.5).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, "shop-switcher")))[
                        0].find_elements(by=By.CLASS_NAME, value="shop-select")[0].find_elements(by=By.CLASS_NAME,
                                                                                                 value="content")
                    current_info = element_curent_shop[0].get_attribute("innerText")
                except:
                    traget_text = "You have not been active on our seller platform for more than 90 days. Click the reactive button to start selling again."
                    element_curent_shop = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.XPATH, f"//*[contains(text(), '{traget_text}')]"))
                    )
                    current_info = "90天没有活动，跳过该店铺"
                    traget_back = "返回"
                    if len(element_curent_shop) > 0:
                        # 刷新一下页面再退出
                        driver.refresh()
                        time.sleep(5)
                        try:
                            element_back = WebDriverWait(driver, 10).until(
                                EC.presence_of_all_elements_located((By.XPATH, f"//*[contains(text(), '{traget_back}')]"))
                            )
                        except:
                            element_back = WebDriverWait(driver, 10).until(
                                EC.presence_of_all_elements_located(
                                    (By.CLASS_NAME, "eds-button.eds-button--normal.back-btn"))
                            )
                        driver.execute_script("arguments[0].click();", element_back[0])
                        return 0
                    else:
                        # 刷新一下页面再退出
                        driver.refresh()
                        time.sleep(5)
                        return 0
            else:
                print("无法登录")
                break
        print("进入：", s)
        return s
        print("当前店铺信息：", current_info)
    except:
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        return 0
        print("onlyswitch-店铺切换失败，手动切换后回车")
def get_bill_shop_list(driver):
    # 点击下拉窗口
    # 先等待店铺选择框加载
    driver.implicitly_wait(2)
    target_class_name = 'shop-select'
    target_class_name = target_class_name.replace(" ", ".")
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name))
    )[0]
    driver.execute_script("arguments[0].click();", element)
    # 遍历所有国家
    time.sleep(10)
    country_elements = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, 'region-item'))
    )
    shop_list = []
    for country in country_elements:
        if country.is_displayed() and country.get_attribute("innerText") != "所有店铺":
            try:
                # 点击国家
                driver.execute_script("arguments[0].click();", country)
                country_name = country.get_attribute("innerText")
                # 遍历所有店铺，获取国家和对应的所有店铺名称
                shop_elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'shop-item-wrapper'))
                )
                for shop in shop_elements:
                    # 这个效率有点低，后续可以优化
                    # 无权限使用这个
                    # todo_shop = shop.find_elements(by=By.CLASS_NAME, value="eds-popper.eds-popover__popper.eds-popover__popper--light.with-arrow")
                    # if len(todo_shop) > 0:
                    #     break
                    shop_name = shop.get_attribute("innerText")
                    full_shop_name = shop_name
                    shop_name = shop_name.replace("SIP附属店铺","").strip()
                    # 跳过以 .xx 结尾的店铺名
                    if shop_name.endswith(".xx"):
                        continue
                    shop_list.append({
                        "shop_name": shop_name,
                        "country": country_name,
                        "full_shop_name": full_shop_name
                    })
            except Exception as e:
                logger.error(f"点击国家失败: {e}")
                input("请手动点击国家后按回车继续...")
    return shop_list
def dl_bill(driver, account,pw,date, max_wait=30): # 验证日期
    records = []
    file_name = ''
    dl_times = 0
    try:
        print('开始导出')
        # 解析本轮日期，支持多种格式
        original_date = date
        if isinstance(date, datetime):
            date_dt = date
            date = date.strftime("%Y%m%d")
        else:
            # 支持多种日期字符串格式
            date_formats = [
                "%Y%m%d",           # 20250930
                "%Y-%m-%d",         # 2025-09-30
                "%Y/%m/%d",         # 2025/09/30
                "%Y-%m-%d %H:%M:%S" # 2025-09-30 00:00:00
            ]
            date_dt = None
            for fmt in date_formats:
                try:
                    date_dt = datetime.strptime(str(date), fmt)
                    break
                except ValueError:
                    continue
            if date_dt is None:
                logger.error(f"无法解析日期格式: {date}")
                date_dt = datetime.now()
            date = date_dt.strftime("%Y%m%d")
        
        # 判断是否需要下载：今天距离本轮日期超过5天
        today = datetime.now()
        days_diff = (today - date_dt).days
        should_download = days_diff > 5
        
        logger.info(f"本轮日期: {date_dt.strftime('%Y-%m-%d')}, 今天: {today.strftime('%Y-%m-%d')}, 相差天数: {days_diff}, 是否需要下载: {should_download}")
        
        if not should_download:
            logger.info(f"距离本轮日期不足5天，跳过下载")
            return records
        success = False
        time.sleep(2)
        for attempt in range(2):
            try:
                # 使用 CSS 选择器更可靠地定位“导出”按钮
                elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".eds-button.report-exporter-button"))
                )
                clicked = False
                for elem in elements:
                    if "导出" == elem.get_attribute("innerText"):
                        driver.execute_script("arguments[0].click();", elem)
                        clicked = True
                        break
                if not clicked:
                    raise Exception("未找到导出按钮")
                # 尝试密码验证（有些页面会弹验证框）
                try:
                    bill_pw_verification(driver, account, pw)
                except Exception:
                    pass
                # 等待导出成功弹窗出现：remote-component-report-export-history
                try:
                    WebDriverWait(driver, 20).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, ".eds-popper.eds-popover__popper.eds-popover__popper--light.with-arrow.report-exporter-popover"))
                    )
                    logger.info("导出历史弹窗已出现，确认导出点击成功")
                    success = True
                    break
                except TimeoutException:
                    # 如果 20 秒内没有出现弹窗，则认为点击未成功，进入重试
                    raise Exception("导出历史弹窗未出现")
            except Exception as e:
                logger.warning(f"导出点击失败，尝试{attempt+1}/2: {e}")
                if attempt == 0:
                    driver.refresh()
                    wait_for_page_ready(driver)
                    try:
                        bill_pw_verification(driver, account, pw)
                    except Exception:
                        pass
                    try:
                        bill_date_selector = ".dropdown-item.eds-dropdown-item"
                        bill_date_dls = WebDriverWait(driver, 15).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, bill_date_selector))
                        )
                        matched = False
                        for bill_date in reversed(list(bill_date_dls)):
                            date_text = bill_date.get_attribute('innerText')
                            if contains_target_dates(date_text, date_dt, date_dt):
                                driver.execute_script("arguments[0].click();", bill_date)
                                matched = True
                                logger.info(f"刷新后已重新选中日期：{date_text}")
                                break
                        if not matched:
                            logger.warning(f"刷新后未找到匹配日期项：{date}")
                    except Exception as e2:
                        logger.error(f"刷新后重新选择日期失败：{e2}")
                else:
                    pass
        if not success:
            print('点击导出失败')
        else:
            bill_pw_verification(driver, account, pw)
            driver.refresh()
            bill_pw_verification(driver, account, pw)
    # 判断日期
    except Exception as e:
        print('点击下载失败')
    try:
        print('开始下载文件')
        # 在导出历史弹窗内等待列表项加载
        try:
            # 先确保弹窗可见，避免列表尚未挂载导致空或元素失效
            WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, "//*[contains(@class, 'eds-modal__box') and not(contains(@style, 'display: none'))]"))
            )
        except TimeoutException:
            # 若未检测到弹窗，继续尝试列表等待（某些页面无明显弹窗容器）
            pass
        try:
            output = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "remote-component-report-export-history .list-item"))
            )
        except TimeoutException:
            logger.warning("导出历史列表未加载，尝试使用全局列表项定位并延长等待")
            output = WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "list-item"))
            )
        # 使用之前已解析的本轮日期进行文件匹配
        logger.info(f"使用本轮日期 {date_dt.strftime('%Y-%m-%d')} 匹配文件项")
        # 额外输出列表项数量，方便诊断
        try:
            logger.info(f"导出历史列表项数量: {len(output)}")
        except Exception:
            pass
        found_match = False
        # 使用索引遍历，并在出现 StaleElementReference 时重新获取列表项
        i = 0
        while i < len(output):
            idx = len(output) - 1 - i  # 倒序
            retry = 0
            elem = None
            file_name = ''
            while retry < 2:
                try:
                    # 尝试重新获取列表项，避免引用失效
                    current_list = driver.find_elements(By.CSS_SELECTOR, "remote-component-report-export-history .list-item")
                    if not current_list:
                        current_list = driver.find_elements(By.CLASS_NAME, "list-item")
                    if not current_list:
                        break
                    if idx >= len(current_list):
                        idx = len(current_list) - 1
                        if idx < 0:
                            break
                    elem = current_list[idx]
                    file_name = elem.get_attribute('innerText')
                    break
                except StaleElementReferenceException:
                    retry += 1
                    time.sleep(0.5)
                    continue
            # 如果还是没拿到有效元素，跳到下一个索引
            if elem is None or not file_name:
                i += 1
                continue
            # 使用 contains_target_dates 兼容 YYYY/MM/DD、英文月份、中文日期等格式
            if contains_target_dates(file_name, date_dt, date_dt):
                found_match = True
                start_time = time.time()
                while time.time() - start_time < max_wait and dl_times < 2:
                    try:
                        download_btn = elem.find_element(By.CSS_SELECTOR, ".eds-button.eds-button--primary.eds-button--normal")
                    except (NoSuchElementException, StaleElementReferenceException):
                        download_btn = None
                    if download_btn is None:
                        try:
                            if elem.find_elements(By.XPATH,  "//*[contains(text(), '已下载')]"):
                                logger.info(f"{file_name} 文件已存在")
                                records.append([account, file_name, '已下载'])
                                dl_times += 1
                                break
                            elif elem.find_elements(By.XPATH,  "//*[contains(text(), '进行中')]"):
                                logger.info(f"{file_name} 文件生成中，等待10秒...")
                                time.sleep(10)
                            else:
                                logger.info(f"{file_name} 文件生成出错，手动下载...")
                                time.sleep(10)
                                records.append([account, file_name, '未下载'])
                        except StaleElementReferenceException:
                            # 元素失效则跳出内层等待，继续下一项
                            break
                    else:
                        try:
                            ok_dl = download_btn.get_attribute("innerText")
                        except StaleElementReferenceException:
                            ok_dl = None
                        if ok_dl == "下载":
                            try:
                                driver.execute_script("arguments[0].click();", download_btn)
                            except StaleElementReferenceException:
                                # 重新定位按钮尝试一次
                                try:
                                    download_btn = elem.find_element(By.CSS_SELECTOR, ".eds-button.eds-button--primary.eds-button--normal")
                                    driver.execute_script("arguments[0].click();", download_btn)
                                except Exception:
                                    logger.warning("下载按钮再次定位失败，跳过该项")
                                    break
                            logger.info(f"{file_name} 文件开始下载")
                            dl_times += 1
                            time.sleep(5)
                            # 成功触发下载也记录，避免返回空记录列表
                            records.append([account, file_name, '已触发下载'])
                        else:
                            logger.error(f"未知状态: {file_name}（按钮文本：{ok_dl}）")
                            time.sleep(5)
                            records.append([account, file_name, '下载出错'])
            # 继续下一个倒序元素
            i += 1
        if not found_match:
            # 同时输出两种日期格式，便于人工对照
            logger.info(f"未找到包含目标日期的文件项，目标日期：{date_dt.strftime('%Y/%m/%d')}（原始：{date}）")
        time.sleep(5)
        return records
    except Exception as e:
        logger.error(f"下载 {file_name} 失败: {str(e)}", exc_info=True)
        winsound.Beep(1000, 1000)  # 高频提示音
        dl_times += 1
        return records
def get_shop_name_and_date(file_path):
    # 从文件名中提取店铺名称和日期
    file_name = os.path.basename(file_path)
    county,sep, shop_name = file_name.split('.income.已拨款.')[0].partition(".")
    date = file_name.split('.income.已拨款.')[1].split(' ')[0]
    if "_" in date:
        date = date.split("_")[0]
    date = datetime.strptime(date, "%Y%m%d")
    return shop_name, date
def sum_data_info(info_path, sum_data_filename):
    info_path = Path(info_path)
    sum_data_file = info_path / sum_data_filename
    if not sum_data_file.exists():
        raise FileNotFoundError(f"文件不存在: {sum_data_file}")
    df = pd.read_excel(sum_data_file, sheet_name='Charging Report Summary')
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
def switch_site(driver, site,format_is):
    sites = []
    try:
        site_element = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-selector__inner.line-clamp--1"))
        )[0]
        driver.execute_script("arguments[0].click();", site_element)
        time.sleep(0.3)
        panel = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-select__options"))
        )
        def site_name(driver,num,max_retries=3, base_wait=3):
            try:
                panel_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-select__options"))
                )[num].find_elements(By.CLASS_NAME, "eds-option")
                for panel in panel_element:
                    site = panel.text
                    if site != '':
                        return panel_element
            except:
                pass
        for num in range(len(panel)):
            panel_element = site_name(driver,num)
            if panel_element is not None:
                break
        if format_is == 'switch':
            for panel in panel_element:
                if panel.text == site:
                    driver.execute_script("arguments[0].click();", panel)
                    logging.info(f"已切换到站点：{site}")
                    break
        else:
            for panel in panel_element:
                site = panel.text
                if site == '':
                    logging.error(f"站点为空：{panel.text}")
                sites.append(site)
            # 删除站点为空的站点
            sites = [site for site in sites if site != '']
            logging.info(f"共有这些站点：{sites}")
            site_element = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-selector__inner.line-clamp--1"))
            )[0]
            driver.execute_script("arguments[0].click();", site_element)
            time.sleep(1)
            return sites
    except  Exception as e:
        print(f"切换站点失败：{e}")
def hanzi_to_int(hanzi):
    """将汉字的月份转换为数字,或者英文"""
    month_map = {
        '一月': 1, '二月': 2, '三月': 3, '四月': 4, '五月': 5, '六月': 6,
        '七月': 7, '八月': 8, '九月': 9, '十月': 10, '十一月': 11, '十二月': 12
        , 'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
        'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
    }
    return month_map[hanzi]
def select_date(driver,s_or_e, date):
    """辅助函数用于精确选取给定年月日"""
    # 传入date，将其转换为数值的年，月，日
    year =  date.year
    month = date.month
    day = date.day
    # 限制操作时长，防止死循环
    time_out = 5
    start_time = time.time()
    # 是开始时间还是结束时间
    if s_or_e == 0:
        target_year = 1
        left_year = 0
        right_year = 1
        target_month = 0
        left_month = 1
        right_month = 0
    else:
        target_year = -1
        left_year = -2
        right_year = -1
        target_month = -2
        left_month = -1
        right_month = -2
    while True and time.time() - start_time < time_out:
        elements = driver.find_elements(By.CLASS_NAME, 'eds-picker-header__label.clickable')
        non_empty_elements = [elem for elem in elements if elem.text.strip() != '']
        current_year = non_empty_elements[target_year].text
        # current_year = driver.find_elements(By.CLASS_NAME, 'eds-picker-header__label.clickable')[target_year].text
        if f"{year}" in current_year:  # 判断当前视图是否为目标时间范围
            break
        elif year < int(current_year):
            elements = driver.find_elements(By.CLASS_NAME, "eds-icon.eds-picker-header__icon.eds-picker-header__prev")
            is_displayed_elements = [elem for elem in elements if elem.is_displayed()]
            previous_button = is_displayed_elements[left_year]
            # previous_button = driver.find_elements(By.CLASS_NAME, "eds-icon.eds-picker-header__icon.eds-picker-header__prev")[left_year]
            previous_button.click()
        else:
            elements = driver.find_elements(By.CLASS_NAME, "eds-icon.eds-picker-header__icon.eds-picker-header__next")
            is_displayed_elements = [elem for elem in elements if elem.is_displayed()]
            previous_button = is_displayed_elements[right_year]
            # previous_button = driver.find_elements(By.CLASS_NAME, "eds-icon.eds-picker-header__icon.eds-picker-header__next")[right_year]
            previous_button.click()
    while True and time.time() - start_time < time_out:
        elements = driver.find_elements(By.CLASS_NAME, 'eds-picker-header__label.clickable')
        non_empty_elements = [elem for elem in elements if elem.text.strip() != '']
        current_month = non_empty_elements[target_month].text
        # current_month = driver.find_elements(By.CLASS_NAME, 'eds-picker-header__label.clickable')[target_month].text
        current_month = hanzi_to_int(current_month)
        if month == current_month:
            break
        elif month < current_month:
            elements = driver.find_elements(By.CLASS_NAME, "eds-icon.eds-picker-header__icon.eds-picker-header__prev")
            is_displayed_elements = [elem for elem in elements if elem.is_displayed()]
            previous_button = is_displayed_elements[left_month]
            # previous_button = driver.find_elements(By.CLASS_NAME, "eds-icon.eds-picker-header__icon.eds-picker-header__prev")[left_month]
            previous_button.click()
        else:
            elements = driver.find_elements(By.CLASS_NAME, "eds-icon.eds-picker-header__icon.eds-picker-header__next")
            is_displayed_elements = [elem for elem in elements if elem.is_displayed()]
            previous_button = is_displayed_elements[right_month]
            # previous_button = driver.find_elements(By.CLASS_NAME, "eds-icon.eds-picker-header__icon.eds-picker-header__next")[right_month]
            previous_button.click()
    # elements = driver.find_elements(By.CLASS_NAME, "eds-date-table__cell-inner.normal")
    # target_day_cell = [elem for elem in elements if elem.is_displayed()]
    target_day_cell = driver.find_elements(By.CLASS_NAME, "eds-date-table__cell-inner.normal")
    if time.time() - start_time > time_out:
        return False
    if s_or_e == 0:
        for cell in target_day_cell:
            if cell.text == str(day):
                cell.click()
                time.sleep(0.5)
                return True
    else:
        for cell in reversed(list(target_day_cell)):
            if cell.text == str(day):
                cell.click()
                time.sleep(0.5)
                return True
def input_date(driver, start_time, final_time):
    s_or_e = 0
    is_ok_start = select_date(driver, s_or_e, start_time)
    if not is_ok_start:
        s_or_e = 1
        select_date(driver, s_or_e, start_time)
    time.sleep(0.3)
    s_or_e = 1
    is_ok_end = select_date(driver, s_or_e, final_time)
    if not is_ok_end:
        s_or_e = 0
        select_date(driver, s_or_e, final_time)
    try:
        WebDriverWait(driver, 5).until(
            EC.invisibility_of_element_located((By.CLASS_NAME, "eds-popper.eds-date-picker__picker"))
        )
        logger.info(f"选择日期成功")
    except TimeoutException:
        logger.error("选择日期失败")
    except NoSuchElementException:
        logger.info("日期选择器元素不存在")
    except Exception as e:
        logger.error(f"选择日期时出现错误: {e}")
def dl_rk_sbs_all_data(driver):
    try:
        checkbox = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "eds-checkbox__indicator"))
        )
        driver.execute_script("arguments[0].click();", checkbox)
        time.sleep(0.5)
    except (TimeoutException, NoSuchElementException) as e:
        logger.error(f"无法选择复选框: {e}")
        input("无法选择所有项目，请手动选择，并按回车")
    try:
        export_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, "//*[contains(text(), '导出(.xls)')]"))
        )
        for elem in export_elements:
            if "导出(.xls)" in elem.get_attribute("innerText"):
                driver.execute_script("arguments[0].click();", elem)
                logger.info("已点击导出按钮")
                time.sleep(2)
                break
    except Exception as e:
        logger.error(f"选择导出类型失败: {e}")
        input("选择导出类型失败，请手动选择，并按回车")
    try:
        checkbox = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "eds-checkbox__indicator"))
        )
        driver.execute_script("arguments[0].click();", checkbox)
        time.sleep(0.5)
    except Exception as e:
        logger.error(f"取消选择失败: {e}")
def download_with_retry(driver, chrome_dl_path, max_retries=3, base_wait=3):
    """带指数退避和双阶段重试的下载函数"""
    start_dl_time = None  # 先在外层声明变量
    def attempt_download(attempt_num):
        nonlocal start_dl_time
        logger.info(f"开始第 {attempt_num + 1} 次下载尝试")
        # 第一阶段：触发下载
        start_dl_time = wait_download(driver)
        # 第二阶段：等待文件出现（带指数退避）
        max_wait_times = 3
        wait_time = base_wait
        for wait_attempt in range(max_wait_times):
            dl_file_path = get_latest_files(chrome_dl_path, start_dl_time)
            if dl_file_path:
                logger.info(f"文件验证成功: {dl_file_path}")
                return dl_file_path
            # 指数退避等待
            sleep_time = wait_time * (2 ** wait_attempt)
            logger.warning(f"文件未找到，等待 {sleep_time}s 后重试...")
            time.sleep(sleep_time)
        logger.error("文件检测超时")
        return None
    # 主重试循环
    for retry in range(max_retries):
        file_path = attempt_download(retry)
        if file_path:
            return file_path
        logger.warning(f"第 {retry + 1}/{max_retries} 次整体重试")
        time.sleep(base_wait * (2 ** retry))  # 指数退避
    logger.warning("下载失败,已跳过该文件下载")
def save_info(driver):
    try:
        header_element = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-table__header-container"))
        )[0].text
        text_element = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-table__body-container"))
        )[0].text
        # 将header_element作为表头，text_element作为数据，保存到df_temp_data中，后面按照表头追加到df_sum_data，如果表头不存在，则创建
        header_list = header_element.split("\n")
        text_list = text_element.split("\n")
        # 将70个元素的列表重组为10x7的二维数组
        data = [text_list[i:i + len(header_list)]
                for i in range(0, len(text_list), len(header_list))]
        df_temp_data = pd.DataFrame(data, columns=header_list)
        time.sleep(2)
        return df_temp_data
    except Exception as e:
        logger.error(f"保存信息失败: {e}")
        input("保存信息失败，请手动保存，并按回车")
        return None
def wait_for_page_ready(driver):
        """等待页面加载完成"""
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(1)
        except Exception:
            pass
def get_info_data_billl(driver, shop_list, account, pw, onlyswitch, bill_pw_verification):
    # 导入必要的库
    import pandas as pd
    import time
    from datetime import datetime
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By

    # 初始化变量
    shop_data = []
    # 仅测试指定店铺
    # shop_list = [shop for shop in shop_list if shop.get('shop_name') == 'motor_genuine.ph']
    # print(f'仅测试店铺 motor_genuine.ph，过滤后店铺数量：{len(shop_list)}')

    def safe_password_verification(max_retries=3):
        """安全的密码验证，确保验证完成后继续操作"""
        for attempt in range(max_retries):
            try:
                bill_pw_verification(driver, account, pw)
                # 验证完成后等待页面稳定
                time.sleep(2)
                return True
            except Exception as e:
                print(f"密码验证第{attempt + 1}次尝试失败: {e}")
                if attempt < max_retries - 1:
                    time.sleep(3)
                else:
                    print("密码验证最终失败，跳过当前操作")
                    return False
        return False

    for shop_info in shop_list:
        print(f"切换店铺：{shop_info['shop_name']}")
        # 初始化当前循环的变量
        shop_id = None
        skip_shop = onlyswitch(driver, shop_info['shop_name'])

        # 安全的密码验证
        if not safe_password_verification():
            print(f"店铺 {shop_info['shop_name']} 密码验证失败，跳过")
            continue

        wait_for_page_ready(driver)

        if skip_shop == 0:
            print('跳过该店铺')
        else:
            current_url = driver.current_url
            print(current_url)
            if 'inactive-seller' in current_url:
                print('该店铺已停用')
                continue
            if 'cnsc_shop_id' in current_url:
                shop_id = current_url.split('cnsc_shop_id=')[1].split('&')[0]
                print(shop_id)
                print(skip_shop)
            else:
                continue
            if 'payoutDate' in current_url or 'cnsc_shop_id' in current_url:
                # 在关键操作前再次确保密码验证
                bill_pw_verification(driver, account, pw)
                if not safe_password_verification():
                    print(f"店铺 {shop_info['shop_name']} 账单页面密码验证失败，跳过")
                    continue
                final_time_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(., '最近拨款日期：')]"))
                )
                text = final_time_element.get_attribute('innerText')
                # 提取“最近拨款日期：”后面的内容，去掉括号内说明，并规范为YYYY/MM/DD
                match = []
                match = re.search(r'最近拨款日期：\s*(.+)', text)
                if match:
                    ft_raw = match.group(1).strip()
                    ft_raw = re.split(r'[（(]', ft_raw)[0].strip()
                    # 将中文日期转换为YYYY/MM/DD，不保留末尾的分隔符
                    ft_raw = ft_raw.replace('年','/').replace('月','/').replace('日','').strip('/')
                    final_time = ft_raw
                    # 判断日期字符串是否包含年份信息，若不包含则跳过
                    if '20' not in final_time:
                        continue
                    # 读取本轮的日期（final_time）并加入采集结果
                    try:
                        current_date = datetime.strptime(final_time, "%Y/%m/%d")
                        if shop_id and skip_shop and account and current_date:
                            shop_data.append({
                                "shop_name": skip_shop,
                                "shop_id": shop_id,
                                "bill_date": current_date,
                                "account": account,
                                "round_flag": "本轮"
                            })
                            print(f"添加数据: 店铺={skip_shop}, ID={shop_id}, 账号={account}, 日期={current_date}, 本轮标记=本轮")
                        else:
                            print(f"数据不完整，跳过本轮日期: 店铺={skip_shop}, ID={shop_id}, 账号={account}, 日期={final_time}")
                    except Exception as e:
                        print(f"解析本轮日期失败: {e}")
                else:
                    raise ValueError('未能提取最近拨款日期')
                wait_for_page_ready(driver)
                time.sleep(1)
                try:
                    bill_date_selector = ".dropdown-item.eds-dropdown-item"
                    bill_date_dls = WebDriverWait(driver, 15).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, bill_date_selector))
                    )

                    for bill_date in reversed(list(bill_date_dls)):  # 倒序下载
                        try:
                            date_text = bill_date.get_attribute('innerText')
                            if '无拨款周期' in date_text:
                                break
                            elif "/" in date_text:
                                if "(" in date_text:
                                    date_text = date_text.split("(")[0].replace(' ', '')
                                    current_date = datetime.strptime(date_text, "%m/%d/%Y")
                                else:
                                    date_text = date_text.replace(' ', '')
                                    current_date = datetime.strptime(date_text, "%Y/%m/%d")
                                # 验证数据完整性后再添加
                                if shop_id and skip_shop and account and current_date:
                                    shop_data.append({
                                        "shop_name": skip_shop,
                                        "shop_id": shop_id,
                                        "bill_date": current_date,
                                        "account": account,
                                        "round_flag": ""
                                    })
                                    print(
                                        f"添加数据: 店铺={skip_shop}, ID={shop_id}, 账号={account}, 日期={current_date}")
                                else:
                                    print(
                                        f"数据不完整，跳过: 店铺={skip_shop}, ID={shop_id}, 账号={account}, 日期={current_date}")
                        except Exception as e:
                            print(f"处理日期数据时出错: {e}")
                            continue

                except Exception as e:
                    print(f"获取账单日期列表失败: {e}")
                    # 可能需要重新验证密码
                    safe_password_verification()
                    continue
    # 创建DataFrame并进行数据清理
    shop_data_df = pd.DataFrame(shop_data)
    print(f"原始数据行数: {len(shop_data_df)}")

    if not shop_data_df.empty:
        # 显示空值统计
        print("空值统计:")
        print(shop_data_df.isnull().sum())

        # 删除任何字段为空的行
        shop_data_df = shop_data_df.dropna()
        print(f"清理后数据行数: {len(shop_data_df)}")
    else:
        print("没有收集到任何数据")
    # 重命名
    shop_data_df = shop_data_df.rename(columns={
        'shop_name': '店铺名称',
        'shop_id': '店铺ID',
        'account': '账号',
        'bill_date': '应下载日期',
        'round_flag': '本轮标记'
    })
    return shop_data_df

# 下载-拨款账单
def main_dl_bill_data(driver,shop_list):
    num = 0
    dl_records_list = []
    print('开始遍历店铺')
    # 仅测试指定店铺
    # shop_list = [shop for shop in shop_list if shop.get('shop_name') == 'motor_genuine.ph']
    # print(f'仅测试店铺 motor_genuine.ph，过滤后店铺数量：{len(shop_list)}')
    for shop_info in shop_list:
        if num == -1:  # 不需要跳过第一个店铺的切换
            num += 1
            continue
        else:
            print(f"切换店铺：{shop_info['shop_name']}")
            skip_shop = onlyswitch(driver, shop_info['shop_name'])
            if skip_shop == 0:
                print('跳过该店铺')
                num += 1
            else:
                current_url = driver.current_url
                print(current_url)
                if 'inactive-seller' in current_url:
                    print('该店铺已停用')
                    continue
                if 'cnsc_shop_id' in current_url:
                    shop_id = current_url.split('cnsc_shop_id=')[1].split('&')[0]
                    print(shop_id)
                    print(shop_info['shop_name'])
                    shop_data.append({
                        "shop_id": shop_id,
                        "shop_name": skip_shop,
                        "account": account
                    })
                    existing_dates = []
                    if shop_info['shop_name'] in df_ex_dl['店铺名称'].values:
                        # 得到该店已下载的日期数据
                        ex_dl_dates = df_ex_dl[df_ex_dl['店铺名称'] == shop_info['shop_name']]['下载日期']
                        existing_dates = [pd.to_datetime(d) for dl_list in ex_dl_dates for d in dl_list]

                else:
                    print('未找到cnsc_shop_id')
                    continue
                if 'payoutDate' in current_url:
                    bill_pw_verification(driver, account, pw)
                    final_time = current_url.split('payoutDate=')[1].split('&')[0]
                    final_time = final_time.replace('-', '/').strip('/')
                    # 读取下载日期
                    bill_date_selector = ".dropdown-item.eds-dropdown-item"
                    # 使用索引+重取方式，避免元素过期
                    try:
                        initial_list = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, bill_date_selector))
                        )
                    except TimeoutException:
                        initial_list = []
                    idx = len(initial_list) - 1
                    while idx >= 0:
                        # 每次循环都重取一次列表，避免 Stale 引用
                        try:
                            current_list = driver.find_elements(By.CSS_SELECTOR, bill_date_selector)
                        except Exception:
                            current_list = []
                        if not current_list:
                            break
                        if idx >= len(current_list):
                            idx = len(current_list) - 1
                            if idx < 0:
                                break
                        bill_date = current_list[idx]
                        # 读取文本，失败则重试一次
                        read_retry = 0
                        date_text = ''
                        while read_retry < 2:
                            try:
                                date_text = bill_date.get_attribute('innerText')
                                break
                            except StaleElementReferenceException:
                                read_retry += 1
                                time.sleep(0.3)
                                try:
                                    current_list = driver.find_elements(By.CSS_SELECTOR, bill_date_selector)
                                    if idx < len(current_list):
                                        bill_date = current_list[idx]
                                except Exception:
                                    pass
                        if not date_text:
                            idx -= 1
                            continue
                        # 判断下载日期是否在已下载的日期列表中
                        try:
                            if "/" in date_text:
                                if "(" in date_text:
                                    # 将下载日期转换为datetime对象
                                    raw_text = date_text.split("(")[0].replace(' ', '')
                                    current_date = datetime.strptime(raw_text, "%m/%d/%Y")
                                else:
                                    raw_text = date_text.replace(' ', '')
                                    current_date = datetime.strptime(raw_text, "%Y/%m/%d")
                                shop_data.append({
                                    "shop_name": shop_info['shop_name'],
                                    "bill_date": current_date
                                })
                                # 小于2025年1月1日的也不用下载
                                if current_date in existing_dates or current_date < datetime(2025, 1, 1):
                                    idx -= 1
                                    continue
                        except Exception:
                            # 日期解析失败，跳过该项
                            idx -= 1
                            continue
                        # 本轮逻辑需要读取第二项，重取列表再读，避免过期
                        if '本轮' in date_text:
                            try:
                                second_list = driver.find_elements(By.CSS_SELECTOR, bill_date_selector)
                                if len(second_list) > 1:
                                    last_bill_text = second_list[1].get_attribute('innerText')
                                    last_bill_text = last_bill_text.replace(' ', '')
                                    if "(" in last_bill_text:
                                        last_bill_text = last_bill_text.split("(")[0]
                                        last_bill_date = datetime.strptime(last_bill_text, "%m/%d/%Y")
                                    else:
                                        last_bill_date = datetime.strptime(last_bill_text, "%Y/%m/%d")
                                    ft_dt = datetime.strptime(final_time, "%Y/%m/%d")
                                    if last_bill_date + timedelta(days=7) >= ft_dt:
                                        idx -= 1
                                        continue
                                    else:
                                        driver.execute_script("arguments[0].click();", bill_date)
                                        final_time_str = ft_dt.strftime("%Y%m%d")
                                        print('开始下载该日期', final_time_str)
                                        records = dl_bill(driver, account, pw, final_time_str, max_wait=30)
                                        dl_records_list.extend(records)
                                else:
                                    ft_dt = datetime.strptime(final_time, "%Y/%m/%d")
                                    driver.execute_script("arguments[0].click();", bill_date)
                                    final_time_str = ft_dt.strftime("%Y%m%d")
                                    print('开始下载该日期', final_time_str)
                                    records = dl_bill(driver, account, pw, final_time_str, max_wait=30)
                                    dl_records_list.extend(records)
                            except StaleElementReferenceException:
                                # 元素过期，跳过本项
                                idx -= 1
                                continue
                        else:
                            try:
                                driver.execute_script("arguments[0].click();", bill_date)
                                current_date_str = current_date.strftime("%Y%m%d")
                                print('开始下载该日期', current_date_str)
                                records = dl_bill(driver, account, pw, current_date_str, max_wait=30)
                                dl_records_list.extend(records)
                                print("当前 records 内容：", records[:2])  # 打印前两个元素
                            except StaleElementReferenceException:
                                # 点击目标过期，跳过该项
                                pass
                        idx -= 1
                else:
                    bill_pw_verification(driver, account, pw)
                    final_time_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//*[contains(., '最近拨款日期：')]"))
                    )
                    text = final_time_element.get_attribute('innerText')
                    # 提取“最近拨款日期：”后面的内容，去掉括号内说明，并规范为YYYY/MM/DD
                    match = []
                    match = re.search(r'最近拨款日期：\s*(.+)', text)
                    if match:
                        ft_raw = match.group(1).strip()
                        ft_raw = re.split(r'[（(]', ft_raw)[0].strip()
                        # 将中文日期转换为YYYY/MM/DD，不保留末尾的分隔符
                        ft_raw = ft_raw.replace('年','/').replace('月','/').replace('日','').strip('/')
                        final_time = ft_raw
                        # 判断日期字符串是否包含年份信息，若不包含则跳过
                        if '20' not in final_time:
                            continue
                    else:
                        raise ValueError('未能提取最近拨款日期')
                    # 读取下载日期
                    bill_date_selector = ".dropdown-item.eds-dropdown-item"
                    # 使用索引+重取方式，避免元素过期
                    try:
                        initial_list = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, bill_date_selector))
                        )
                    except TimeoutException:
                        initial_list = []
                    idx = len(initial_list) - 1
                    while idx >= 0:
                        try:
                            current_list = driver.find_elements(By.CSS_SELECTOR, bill_date_selector)
                        except Exception:
                            current_list = []
                        if not current_list:
                            break
                        if idx >= len(current_list):
                            idx = len(current_list) - 1
                            if idx < 0:
                                break
                        bill_date = current_list[idx]
                        read_retry = 0
                        date_text = ''
                        while read_retry < 2:
                            try:
                                date_text = bill_date.get_attribute('innerText')
                                break
                            except StaleElementReferenceException:
                                read_retry += 1
                                time.sleep(0.3)
                                try:
                                    current_list = driver.find_elements(By.CSS_SELECTOR, bill_date_selector)
                                    if idx < len(current_list):
                                        bill_date = current_list[idx]
                                except Exception:
                                    pass
                        if not date_text:
                            idx -= 1
                            continue
                        try:
                            if "/" in date_text:
                                if "(" in date_text:
                                    raw_text = date_text.split("(")[0].replace(' ', '')
                                    current_date = datetime.strptime(raw_text, "%m/%d/%Y")
                                else:
                                    raw_text = date_text.replace(' ', '')
                                    current_date = datetime.strptime(raw_text, "%Y/%m/%d")
                                shop_data.append({
                                    "shop_name": shop_info['shop_name'],
                                    "bill_date": current_date
                                })
                                if current_date in existing_dates or current_date < datetime(2025, 1, 1):
                                    idx -= 1
                                    continue
                        except Exception:
                            idx -= 1
                            continue
                        if '本轮' in date_text:
                            try:
                                second_list = driver.find_elements(By.CSS_SELECTOR, bill_date_selector)
                                if len(second_list) > 1:
                                    last_bill_text = second_list[1].get_attribute('innerText')
                                    last_bill_text = last_bill_text.replace(' ', '')
                                    if "(" in last_bill_text:
                                        last_bill_text = last_bill_text.split("(")[0]
                                        last_bill_date = datetime.strptime(last_bill_text, "%m/%d/%Y")
                                    else:
                                        last_bill_date = datetime.strptime(last_bill_text, "%Y/%m/%d")
                                    ft_dt = datetime.strptime(final_time, "%Y/%m/%d")
                                    if last_bill_date + timedelta(days=7) >= ft_dt:
                                        idx -= 1
                                        continue
                                    else:
                                        driver.execute_script("arguments[0].click();", bill_date)
                                        final_time_str = ft_dt.strftime("%Y%m%d")
                                        print('开始下载该日期', final_time_str)
                                        records = dl_bill(driver, account, pw, final_time_str, max_wait=30)
                                        dl_records_list.extend(records)
                                else:
                                    ft_dt = datetime.strptime(final_time, "%Y/%m/%d")
                                    driver.execute_script("arguments[0].click();", bill_date)
                                    final_time_str = ft_dt.strftime("%Y%m%d")
                                    print('开始下载该日期', final_time_str)
                                    records = dl_bill(driver, account, pw, final_time_str, max_wait=30)
                                    dl_records_list.extend(records)
                            except StaleElementReferenceException:
                                idx -= 1
                                continue
                        else:
                            try:
                                driver.execute_script("arguments[0].click();", bill_date)
                                current_date_str = current_date.strftime("%Y%m%d")
                                print('开始下载该日期', current_date_str)
                                records = dl_bill(driver, account, pw, current_date_str, max_wait=30)
                                dl_records_list.extend(records)
                                print("当前 records 内容：", records[:2])
                            except StaleElementReferenceException:
                                pass
                        idx -= 1
                num += 1
    shop_data_df = pd.DataFrame(shop_data)
    # 对于除了bill_date为空值的，其他用上面的不为空的值填充
    shop_data_df = shop_data_df.dropna(subset=['bill_date'])
    shop_data_df = shop_data_df.ffill()
    # 重命名
    shop_data_df = shop_data_df.rename(columns={
        'shop_name': '店铺名称',
        'shop_id': '店铺ID',
        'account': '账号',
        'bill_date': '应下载日期'
    })
    shop_data_df.to_excel(f'\\\\Auteng\\综合管理部\\自动化下载文件\\{account}-店铺信息.xlsx', index=False)
    # 判断目录是否都有这些文件
    df_dl_bills = pd.DataFrame(dl_records_list, columns=['店铺账号', '文件名称', '状态'])
    for file_name in df_dl_bills['文件名称']:
        dl_file_name = file_name.split(' ')[0]# 下载下来文件的:转换为下划线了
        dl_file_name = f"{dl_file_name} 00_00.xlsx"
        file_path = os.path.join(chrome_dl_path, dl_file_name)
        if not os.path.exists(file_path):
            print(f'文件{dl_file_name}不存在')
            # 没找到就将df_dl_bills中该文件的状态改为未下载
            df_dl_bills.loc[df_dl_bills['文件名称'] == file_name, '状态'] = '未下载'
        else:
            df_dl_bills.loc[df_dl_bills['文件名称'] == file_name, '状态'] = '已下载'
    df_dl_bills.to_excel(f'\\\\Auteng\\综合管理部\\自动化下载文件\\{account}-下载状态.xlsx', index=False)
# 下载库存数据
def main_dl_kc_data():
    #  进入SBS页面
    sidebar_name = '官方仓服务'
    into_sidebar_page(driver, sidebar_name)
    # 获取该账号对应的所有店铺索引
    account_indices = [i for i, acc in enumerate(company_accounts) if acc == account]
    target_text = "Shopee服务库存"
    switch_type_page(driver, target_text)
    time.sleep(2)
    for i in account_indices:
        shop_name = platform_shops[i]
        shop_id = shop_ids[i]
        # 南宁仓家居和南宁仓汽配特殊处理
        if shop_name in ['shopee南宁仓家居', 'shopee南宁仓汽配']:
            switch_all_shop(driver)
            time.sleep(2)
            download_and_process_data(driver, shop_name, start_time, final_time, chrome_dl_path)
        # 常规店铺处理
        else:
            switch_shop(driver, shop_name)
            time.sleep(2)
            # 识别跨站履约
            current_url = driver.current_url
            if str(shop_id) in current_url:
                download_and_process_data(driver, shop_name, start_time, final_time, chrome_dl_path)
    # 下载入库数据
    time.sleep(2)
    target_text = "Shopee服务入库"
    switch_type_page(driver, target_text)
    time.sleep(2)
    download_rk_data(driver, final_time,chrome_dl_path)
    backup_file(chrome_dl_path, backup_dir)
    quit_ = input('是否要退出浏览器？按1退出，其他键继续：')
    if quit_ == '1':
        driver.quit()
    else:
        pass

# 下载SBS数据
@retry_on_failure
def main_dl_sbs_data():
    df_sum_data = pd.DataFrame()
    sidebar_name = '官方仓服务'
    into_sidebar_page(driver, sidebar_name)
    main_popup_close(driver)
    # 获取要下载的站点和日期信息
    target_text = "费用报告"
    switch_type_page(driver, target_text)
    switch_all_shop(driver)
    time.sleep(5)# 等待加载完成
    # 切换站点，循环下载
    sites = switch_site(driver,"",format_is='get_sites')
    if account == "遨腾" :
        sites = [site for site in sites if site not in ['中国大陆']]
    sites = [site for site in sites if site not in ['印度尼西亚','越南']]
    if "菲律宾" not in sites: # 判断是不是正确读取了站点信息
        sites = df_last_info[(df_last_info['主体名称'] == account)]['站点'].unique().tolist()
    else:
        pass
    for site in sites:
        # 通过account和site来获取df_last_info中的收费时段期末，如果找不到就设置为今年的一月一日
        start_time = df_last_info[(df_last_info['主体名称'] == account) & (df_last_info['站点'] == site)]['收费时段期末'].min()
        if pd.isnull(start_time):
            start_time = datetime(datetime.now().year, 1, 1)
        else:
            start_time = start_time.to_pydatetime() + timedelta(days=1)  # 将 Pandas Timestamp 转为 datetime 对象
        print(f"开始下载{account}的{site}站点，从{start_time}开始")
        final_time = datetime.now()
        print(f"结束下载{account}的{site}站点，到{final_time}结束")
        switch_site(driver, site,format_is='switch')
        time.sleep(15)# 等待加载完成
        main_popup_close( driver)
        # 直到出现费用报告页面
        target_text = "费用报告"
        switch_type_page(driver, target_text)
        try:
            time.sleep(2)
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, "//*[contains(text(), '费用报告')]"))
            )
            logger.info(f"选择费用报告成功")
        except TimeoutException:
            logger.error("选择费用报告失败")
            break
        # 选择日期
        main_popup_close(driver)
        date_element = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, "//*[contains(text(), '日期')]"))
        )
        for elem in date_element:
            if elem.text == '日期':
                elem.click()
                time.sleep(0.3)
        input_date(driver, start_time, final_time)
        try:
            time.sleep(8) # 等待加载成功
            total_dl = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "list-title"))
            )[0].text.split(" ")[0]
            total_dl = int(total_dl)
        except Exception as e:
            logger.error(f"获取总条数失败: {e}")
            total_dl = 0
        if total_dl > 0 and total_dl <= 10:
            df_temp_data = save_info(driver)
            df_sum_data = pd.concat([df_sum_data, df_temp_data], ignore_index=True)
            # 点击全选，直接下载
            dl_rk_sbs_all_data(driver)
            try:
                download_with_retry(driver, chrome_dl_path)
            except Exception as e:
                logger.error(str(e))
        elif total_dl > 10:
            times = math.ceil(total_dl / 10)
            if times > 10: # 10次下载
                times = 10
            print(f"共有{total_dl}条数据，需要分{times}次下载")
            for i in range(times):
                logger.info(f"第{i + 1}次下载")
                df_temp_data = save_info(driver)
                df_sum_data = pd.concat([df_sum_data, df_temp_data], ignore_index=True)
                dl_rk_sbs_all_data(driver)
                try:
                    download_with_retry(driver, chrome_dl_path)
                except Exception as e:
                    logger.error(str(e))
                if i+1 < times:
                    try:
                        next_button = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.CLASS_NAME,
                                                        "eds-button.eds-button--small.eds-button--frameless.eds-button--block.eds-pager__button-next"))
                        )
                        driver.execute_script("arguments[0].click();", next_button)
                        logger.info("已点击下一页按钮")
                        time.sleep(5)
                    except Exception as e:
                        logger.warning(f"点击下一页失败，可能已到最后一页: {e}")
        else:
            logger.warning(site,"没有数据")
    output_dir = r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\SBS费用报告处理源数据'
    os.makedirs(output_dir, exist_ok=True)
    df_sum_data.to_excel(os.path.join(output_dir, f"费用报告生成日期源文件-{account}.xlsx"), index=False)

def ams_switch_sidebar(driver, level1_name,level2_name):
    try:
        target_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'sub-menu-level2'))
        )
        for elem in target_elements:
            if elem.text == f'{level2_name}':
                driver.execute_script("arguments[0].click();", elem)
                return True
        # 没成功就应该是一级没打开
        target_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'sub-menu-level1'))
        )
        for elem in target_elements:
            if elem.text == f'{level1_name}':
                driver.execute_script("arguments[0].click();", elem)
                target_elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'sub-menu-level2'))
                )
                for elem in target_elements:
                    if elem.text == f'{level2_name}':
                        driver.execute_script("arguments[0].click();", elem)
                        return True
    except Exception as e:
        logger.error(f"切换侧边栏失败: {e}")
        input("请手动切换后按回车继续...")
        return True
def ams_output(driver):
    try:
        dl_time = datetime.now()
        target_elements = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'eds-react-table-row.eds-react-table-row-level-0'))
        )
        for elem in target_elements:
            if  "-" in elem.text or "/" in elem.text:
                data_time = elem.text.split(" ")[:2]
                for item  in data_time:
                    if ":" in item:
                        time_str = item
                    else:
                        date_str = item
                        break
                if date_str:
                    if date_str in {dl_time.strftime(fmt) for fmt in ["%d-%m-%Y", "%m/%d/%Y", "%m-%d-%Y"]}:
                        dl_button = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located(
                                (By.CLASS_NAME, 'eds-react-button.eds-react-button--link.eds-react-button--normal'))
                        )
                        dl_button[0].click()
                        return dl_time
    except Exception as e:
        logger.error(f"获取数据失败: {e}")
        input("请手动下载后按回车继续...")
        return dl_time

def ams_rename_file(chrome_dl_path, dl_time,shop):
    # 检测文件夹内文件的创建时间
    for file in os.listdir(chrome_dl_path):
        try:
            file_path = os.path.join(chrome_dl_path, file)
            file_time = datetime.fromtimestamp(os.path.getctime(file_path))

            # 在比较之前，先检查 dl_time 是否有效
            if dl_time is not None and (file_time + timedelta(seconds=14) >= dl_time):
                try:
                    os.rename(file_path, os.path.join(chrome_dl_path, f"{shop}.csv"))
                except FileExistsError:
                    logger.error(f"文件已存在: {os.path.join(chrome_dl_path, f'{shop}.csv')}")
                    os.remove(file_path)
                    # pass 在这里不是必须的，因为 except 块执行完后会继续

            # 如果 dl_time 是 None，可以选择记录日志或跳过
            elif dl_time is None:
                logger.warning(f"dl_time 为 None，无法比较文件 '{file}' 的时间，已跳过。")

        except Exception as e:
            logger.error(f"处理文件 '{file}' 时发生错误: {e}")

def ams_save_info():
    try:
        header_element = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-react-table-thead"))
        )[0].text
        text_elemens = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-react-table-row.eds-react-table-row-level-0"))
        )
        header_list = header_element.replace("\n", " ").split(" ")
        data_list = []
        # 遍历每一行数据
        for row in text_elemens:
            row_text = row.get_attribute("innerText")  # 获取每行文本并分割成列表
            row = row_text.replace("\n", " ").replace("\t", " ").split(" ")
            # 去掉为空的 数据
            row = [item for item in row if item != ""]  # 先统一去除空值
            # 判断第一列是否为长度为15的全数字
            if len(row) > 0 and (len(row[0]) != 15 or not row[0].isdigit()):
                # 如果不是15位数字，尝试用上一行的数据补充
                if data_list:  # 确保有上一行数据
                    prev_row = data_list[-1]
                    # 计算需要补充的列数
                    missing_cols = len(header_list) - len(row)
                    if missing_cols > 0:
                        # 从上一行取前面部分补充到当前行
                        prefix = prev_row[:missing_cols]
                        row = prefix + row

            # 如果数据的长度大于header_list的长度，则截断
            if len(row) > len(header_list):
                row = row[:len(header_list)]
            # print(row)
            # print(len(row))
            data_list.append(row)  # 将当前行的数据添加到列表中
        df = pd.DataFrame(data_list, columns=header_list)
        time.sleep(2)
        return df
    except Exception as e:
        logger.error(f"保存信息失败: {e}")
        input("保存信息失败，请手动保存，并按回车")
        return None
# 下载AMS数据
def main_dl_ams_data():
    sidebar_name = '联盟营销'
    into_sidebar_page(driver, sidebar_name)
    shop_list_path = os.path.join(default_dl_ams_path, "AMS下载店铺名称.xlsx")
    shop_list = pd.read_excel(shop_list_path, dtype=str)
    shop_list = shop_list[shop_list['主体名称'] == account]
    df_sum_data = pd.DataFrame()

    def get_ams_target_files(root_path):
        """
        遍历指定根路径，获取所有符合条件的文件：
        1. 子文件夹以 "AMS" 开头（不区分大小写）
        2. 文件后缀为 .csv, .xlsx, .xls（不区分大小写）

        参数:
            root_path: 根目录路径（支持网络路径如 \\Auteng\...）

        返回:
            list: 符合条件的文件名称列表（去掉路径、去掉后缀）
        """
        # 定义需要匹配的文件后缀（转为小写，方便不区分大小写判断）
        target_extensions = {'.csv', '.xlsx', '.xls'}
        # 存储结果的列表（仅存无后缀的文件名）
        matched_file_names = []

        # 遍历根路径下的所有文件和文件夹（包括子目录）
        for root, dirs, files in os.walk(root_path):
            # 获取当前目录的文件夹名称（仅取最后一级目录名）
            current_dir = os.path.basename(root)

            # 过滤条件1：当前文件夹以 "AMS" 开头（不区分大小写）
            if current_dir.lower().startswith('ams'):
                # 遍历当前文件夹下的所有文件
                for file in files:
                    # 拆分文件名和后缀（转为小写）
                    file_name, file_ext = os.path.splitext(file)
                    file_ext = file_ext.lower()
                    # 过滤条件2：文件后缀符合目标格式
                    if file_ext in target_extensions:
                        # 仅添加「去掉后缀的纯文件名」到结果列表
                        matched_file_names.append(file_name)

        return matched_file_names
    last_shop_list =get_ams_target_files(r'\\Auteng\综合管理部\自动化下载文件\AMS营销费用')
    new_file_name = [col for col in shop_list if col not in last_shop_list]
    shop_todo_list = [col for col in shop_list if col in last_shop_list]
    max_len = max(len(new_file_name), len(shop_todo_list))
    new_file_name_padded = pad_list_to_length(new_file_name, max_len)
    shop_todo_list_padded = pad_list_to_length(shop_todo_list, max_len)
    # 5. 先构建完整字典（核心：列名与值一一对应）
    df_dict = {
        "新店铺": new_file_name_padded,
        "下载店铺": shop_todo_list_padded,
        "主体名称": [account] * max_len  # 显式生成等长列表，更规范
    }
    # 6. 统一创建DataFrame（一次初始化，避免多次赋值的索引问题）
    df_log = pd.DataFrame(df_dict)
    df_log.to_csv(fr'\\Auteng\综合管理部\自动化下载文件\AMS营销费用\下载日志{account}-{datetime.now().strftime("%Y%m%d")}.csv')
    for shop in shop_todo_list['店铺名称']:
        onlyswitch(driver, shop)
        time.sleep(5)
        ams_switch_sidebar(driver, '报告', '验证账单')
        time.sleep(2)
        df_temp_data = ams_save_info()
        df_sum_data = pd.concat([df_sum_data, df_temp_data], ignore_index=True)
        try :
            out_put_element = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-react-button.eds-react-button--normal"))
            )
            for elem in out_put_element:
                if elem.text == "导出":
                    driver.execute_script("arguments[0].click();", elem)
            ams_switch_sidebar(driver, '操作记录', '导出')
            time.sleep(2)
            dl_time = ams_output(driver)
            # 等待下载完成
            time.sleep(5)
            ams_rename_file(chrome_dl_path, dl_time,shop)
        except Exception as e:
            logger.error(f"导出数据失败: {e}")
        # 删掉新窗口
        window_handles = driver.window_handles
        if len(window_handles) > 1:
            # 切换到最后一个窗口并关闭
            driver.switch_to.window(window_handles[-1])
            driver.close()
            # 确保回到主窗口
            driver.switch_to.window(window_handles[-2])
    # 清洗要保存的数据
    file_path = os.path.join(default_dl_ams_path, f"{account}验证日期.xlsx")
    cleaned_df =  (df_sum_data)
    if os.path.exists(file_path):
        # 如果文件存在，读取现有数据
        existing_df = pd.read_excel(file_path)
        # 将现有数据和新数据拼接
        combined_df = pd.concat([existing_df, cleaned_df], ignore_index=True)
        # 写回文件
        combined_df.to_excel(file_path, index=False)
    else:
        # 如果文件不存在，直接写入新数据
        df_sum_data.to_excel(file_path, index=False)
def switch_url(driver):
    current_url_0 = driver.current_url
    window_handles = driver.window_handles
    if len(window_handles) > 1:
        # 切换到最后一个窗口并关闭
        driver.switch_to.window(window_handles[-1])
        current_url_1 = driver.current_url
        if current_url_0 == current_url_1:
            logger.warning(f"切换窗口失败")
            print(f"当前窗口：{current_url_1}")
        else:
            logger.info(f"切换窗口成功")
def dl_zanwai_ads_data(driver,dl_time):
    try:
        logger.info(f"开始下载站外广告数据")
        if dl_time == "2":
            dl_time = "本月"
        elif dl_time == "3":
            dl_time = "过去30天"
        elif dl_time == "4":
            dl_time = "过去14天"
        elif dl_time == "5":
            dl_time = "过去7天"
        elif dl_time == "6":
            dl_time = "昨天"
        else:
            dl_time = "上个月"
        # 选择下载周期
        select_dl_time_element = WebDriverWait(driver, 10, 0.5).until(EC.presence_of_all_elements_located(
            (By.XPATH, f"//*[contains(text(), '数据周期')]")))
        driver.execute_script("arguments[0].click();", select_dl_time_element[0])
        time.sleep(1)
        select_dl_time_element = WebDriverWait(driver, 10, 0.5).until(EC.presence_of_all_elements_located(
            (By.XPATH, f"//*[contains(text(), '{dl_time}')]")))
        driver.execute_script("arguments[0].click();", select_dl_time_element[0])
        logger.info(f"选择下载周期成功")
        if select_dl_time_element:
            dl_button = WebDriverWait(driver, 10, 0.5).until(
                EC.presence_of_all_elements_located((By.XPATH, f"//*[contains(text(), '导出数据')]"))
            )
            driver.execute_script("arguments[0].click();", dl_button[0])
            time.sleep(1)
            dl_button = WebDriverWait(driver, 10, 0.5).until(
                EC.presence_of_all_elements_located((By.XPATH, f"//*[contains(text(), '活动级别')]"))
            )
            driver.execute_script("arguments[0].click();", dl_button[0])
        else:
            logger.error(f"选择下载周期失败")
    except Exception as e:
        logger.error(f"下载失败: {e}")
def main_dl_zanwai_ads_data(driver, shop_list,dl_time):
    num = 0
    shop_data = []  # 收集店铺信息
    for shop_info in shop_list:
        print(f"切换店铺：{shop_info['shop_name']}")
        skip_shop = onlyswitch(driver, shop_info['shop_name'])
        if skip_shop == 0:
            print('跳过该店铺')
            num += 1
            continue
        else:
            time.sleep(2)
            current_url = driver.current_url
            if "no_permission" in current_url:
                print(shop_info['shop_name'], "无权限")
                num += 1
                continue
            elif 'cnsc_shop_id' in current_url:
                shop_id = current_url.split('cnsc_shop_id=')[1].split('&')[0]
                shop_data.append({
                    "shop_id": shop_id,
                    "shop_name": shop_info['shop_name'],
                    "account": account
                })
                try:
                # 查找是否有 导出数据的按钮
                    no_data = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.XPATH,"//*[contains(text(), '开始使用')]"))
                    )
                    if no_data:
                        print("没有数据")
                        num += 1
                        continue
                except TimeoutException:
                    try:
                        # 查找是否有 导出数据的按钮
                        out_put = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.XPATH, "//*[contains(text(), '导出数据')]"))
                        )
                        if out_put:
                            logger.info(f"有导出数据的按钮-{shop_info['shop_name']}")
                    except:
                        logger.info(f"没有导出数据的按钮-{shop_info['shop_name']}")
        logger.info("开始下载数据，现在时间是：" + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        dl_zanwai_ads_data(driver, dl_time)
def contains_target_dates(date_str, start_time, final_time):
    """检查字符串是否包含开始/结束日期的年月日数字（支持多格式）"""
    # 提取日期部分（忽略时间）
    start_date = start_time.date()
    final_date = final_time.date()

    # 目标数字集合（去前导零）
    target_numbers = {
        str(start_date.year),
        str(start_date.month).lstrip('0'),
        str(start_date.day).lstrip('0'),
        str(final_date.year),
        str(final_date.month).lstrip('0'),
        str(final_date.day).lstrip('0')
    }

    # 预处理：将英文月份转换为数字（不区分大小写）
    month_map = {
        'jan': '1', 'feb': '2', 'mar': '3', 'apr': '4', 'may': '5', 'jun': '6',
        'jul': '7', 'aug': '8', 'sep': '9', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    normalized_str = date_str.lower()
    for eng_month, num in month_map.items():
        normalized_str = re.sub(rf'\b{eng_month}\w*\b', num, normalized_str)

    # 提取所有数字（包括长数字串的分割）
    found_numbers = set()
    for num_str in re.findall(r'\d+', normalized_str):
        # 处理长数字串 (YYYYMMDD 格式)
        if len(num_str) == 8 and num_str.isdigit():
            year = num_str[0:4]
            month = str(int(num_str[4:6]))  # 去除前导零
            day = str(int(num_str[6:8]))
            found_numbers.update([year, month, day])
        else:
            # 常规数字处理
            normalized_num = num_str.lstrip('0') or '0'  # 保留'0'
            found_numbers.add(normalized_num)

    return target_numbers.issubset(found_numbers)
def generate_weekly_periods(choice):
    """
    生成时间段（周一开始到周日结束）
    :param choice: 用户选择 (1-5)
    :return: 时间段列表 [{'start_time': datetime, 'end_time': datetime}]
    """
    today = datetime.now()
    time_periods = []

    # 1. 上周（周一至周日）
    if choice == '1':
        # 计算本周一
        this_monday = today - timedelta(days=today.weekday())
        # 上周日 = 本周一 - 1天
        last_sunday = this_monday - timedelta(days=1)
        # 上周一 = 上周日 - 6天
        last_monday = last_sunday - timedelta(days=6)
        time_periods.append({
            'start_time': last_monday,
            'end_time': last_sunday
        })
    # 上两周（连续两周）
    elif choice == '2':
        # 计算本周一
        this_monday = today - timedelta(days=today.weekday())
        # 上周日 = 本周一 - 1天
        end_date = this_monday - timedelta(days=1)

        for i in range(2):
            end_week = end_date - timedelta(weeks=i)
            start_week = end_week - timedelta(days=6)
            time_periods.append({
                'start_time': start_week,
                'end_time': end_week
            })
        time_periods.reverse()  # 按时间顺序排序
    # 2. 上四周（连续四周）
    elif choice == '3':
        # 计算本周一
        this_monday = today - timedelta(days=today.weekday())
        # 上周日 = 本周一 - 1天
        end_date = this_monday - timedelta(days=1)

        for i in range(4):
            end_week = end_date - timedelta(weeks=i)
            start_week = end_week - timedelta(days=6)
            time_periods.append({
                'start_time': start_week,
                'end_time': end_week
            })
        time_periods.reverse()  # 按时间顺序排序

    # 3. 上八周（连续八周）
    elif choice == '4':
        # 计算本周一
        this_monday = today - timedelta(days=today.weekday())
        # 上周日 = 本周一 - 1天
        end_date = this_monday - timedelta(days=1)

        for i in range(8):
            end_week = end_date - timedelta(weeks=i)
            start_week = end_week - timedelta(days=6)
            time_periods.append({
                'start_time': start_week,
                'end_time': end_week
            })
        time_periods.reverse()
    # 4. 上十二周（连续十二周）
    elif choice == '5':
        # 计算本周一
        this_monday = today - timedelta(days=today.weekday())
        # 上周日 = 本周一 - 1天
        end_date = this_monday - timedelta(days=1)

        for i in range(12):
            end_week = end_date - timedelta(weeks=i)
            start_week = end_week - timedelta(days=6)
            time_periods.append({
                'start_time': start_week,
                'end_time': end_week
            })
        time_periods.reverse()
    # 5. 上个月（整个自然月）
    elif choice == '6':
        # 获取上个月第一天和最后一天
        first_day_of_prev_month = today.replace(day=1) - timedelta(days=1)
        last_day_of_prev_month = first_day_of_prev_month.replace(day=1)

        time_periods.append({
            'start_time': last_day_of_prev_month,
            'end_time': first_day_of_prev_month
        })
    # 6. 上两个月（整个自然月）
    elif choice == '7':
        # 从当前月份开始迭代
        current_month = today.month
        current_year = today.year
        for i in range(2):
            # 计算当前迭代的月份（上个月、上上个月）
            target_month = current_month - (i + 1)
            target_year = current_year
            # 处理跨年情况（如当前为1月，上个月是去年12月）
            while target_month <= 0:
                target_month += 12
                target_year -= 1
            # 获取目标月份的最后一天（利用下个月的第一天减1天）
            next_month = target_month + 1
            next_year = target_year
            if next_month > 12:
                next_month = 1
                next_year += 1
            last_day = (datetime(next_year, next_month, 1) - timedelta(days=1)).day
            # 构建时间范围（第一天和最后一天）
            start_time = datetime(target_year, target_month, 1)
            end_time = datetime(target_year, target_month, last_day)
            time_periods.append({
                'start_time': end_time,
                'end_time': start_time
            })
    elif choice == '8':
        # 从当前月份开始迭代
        current_month = today.month
        current_year = today.year
        for i in range(3):
            # 计算当前迭代的月份（上个月、上上个月）
            target_month = current_month - (i + 1)
            target_year = current_year
            # 处理跨年情况（如当前为1月，上个月是去年12月）
            while target_month <= 0:
                target_month += 12
                target_year -= 1
            # 获取目标月份的最后一天（利用下个月的第一天减1天）
            next_month = target_month + 1
            next_year = target_year
            if next_month > 12:
                next_month = 1
                next_year += 1
            last_day = (datetime(next_year, next_month, 1) - timedelta(days=1)).day
            # 构建时间范围（第一天和最后一天）
            start_time = datetime(target_year, target_month, 1)
            end_time = datetime(target_year, target_month, last_day)
            time_periods.append({
                'start_time': end_time,
                'end_time': start_time
            })
    # 8. 自定义
    elif choice == '9':
        start_str = input("请输入期初时间(格式: yyyy.mm.dd，多个日期用分号隔开): ")
        end_str = input("请输入期末时间(格式: yyyy.mm.dd，多个日期用分号隔开): ")
        # 处理多个时间段输入
        start_dates = [datetime.strptime(s.strip(), "%Y.%m.%d") for s in start_str.split(';')]
        end_dates = [datetime.strptime(e.strip(), "%Y.%m.%d") for e in end_str.split(';')]
        # 确保日期数量匹配
        if len(start_dates) != len(end_dates):
            print("错误：期初和期末日期数量不匹配")
            return time_periods
        # 处理每个时间段
        for start_date, end_date in zip(start_dates, end_dates):
            # 不强制调整到周一，保留原始日期
            current = start_date
            while current <= end_date:
                start_week = current
                end_week = min(current + timedelta(days=6), end_date)
                time_periods.append({
                    'start_time': start_week,
                    'end_time': end_week
                })
                current = end_week + timedelta(days=1)  # 从下一日开始
    # 11. Excel文件模式 - 返回空列表，时间数据将由excel_dl_zannei_ads_data函数提供
    elif choice == '11':
        return []
    return time_periods
def shopee_zhanting_ads(driver):
    to_do_path = r'C:\Users\lifet\Downloads\补充.xlsx'
    to_do_df = pd.read_excel(to_do_path)# ,skiprows=7
    to_do_list = to_do_df[to_do_df['措施'] == '暂停']['商品编号'].drop_duplicates().astype(str).str.strip().tolist()
    to_do_list = [x for x in to_do_list if x != '']
    last_try = []
    input('进入暂停站内广告页面后按回车继续')
    for to_do in to_do_list:
        try:
            input_selector = (By.CSS_SELECTOR, 'input.eds-input__input[placeholder="搜索"]')
            input_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(input_selector)
            )
            # 确保元素可见
            driver.execute_script("arguments[0].style.visibility='visible';", input_element)
            driver.execute_script("arguments[0].style.display='block';", input_element)
            # 强制清除值
            driver.execute_script("arguments[0].value = '';", input_element)
            # 触发Vue.js的数据更新
            driver.execute_script("""
                const event = new Event('input', { bubbles: true });
                arguments[0].dispatchEvent(event);
            """, input_element)
            time.sleep(5)
            input_element.send_keys(to_do)
            time.sleep(5)
            # 按回车键
            input_element.send_keys(Keys.ENTER)
            time.sleep(5)
        except Exception as e:
            print(f"输入商品编号{to_do}失败：{e}")
            break
        try:
            try:
                select_ok = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'eds-checkbox__indicator'))
                )[-2]
                driver.execute_script("arguments[0].click();", select_ok)
            except Exception as e:
                print(f"选择商品{to_do}失败：{e}")
                break
            change_button = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME,'eds-button.eds-button--normal'))
            )
            button_clicked = False
            for elem in change_button:
                if "更改状态" in elem.get_attribute("innerText"):
                    driver.execute_script("arguments[0].click();", elem)
                    button_clicked = True
                    break
            if not button_clicked:
                print(f"商品{to_do}未找到更改状态按钮")
                # 因为两条，所以重试就行
                last_try = last_try.append(to_do)
                continue
            time.sleep(10)
            zanting_button = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME,'eds-dropdown-item'))
            )
            for elem in zanting_button:
                if "暂停" in elem.get_attribute("innerText"):
                    driver.execute_script("arguments[0].click();", elem)
                    break
            time.sleep(1)
            confirm_button = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME,'eds-modal__footer-buttons'))
            )[-1].find_element(By.CLASS_NAME,'eds-button.eds-button--primary.eds-button--normal')
            driver.execute_script("arguments[0].click();", confirm_button)
            time.sleep(10)
            try:
                select_ok = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'eds-checkbox__indicator'))
                )[-2]
                driver.execute_script("arguments[0].click();", select_ok)
            except Exception as e:
                print(f"取消择商品{to_do}失败：{e}")
                break
        except:
            print(f"暂停商品{to_do}失败")
            break
    if not last_try:
        for to_do in last_try:
            try:
                input_selector = (By.CSS_SELECTOR, 'input.eds-input__input[placeholder="搜索"]')
                input_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(input_selector)
                )
                # 确保元素可见
                driver.execute_script("arguments[0].style.visibility='visible';", input_element)
                driver.execute_script("arguments[0].style.display='block';", input_element)
                # 强制清除值
                driver.execute_script("arguments[0].value = '';", input_element)
                # 触发Vue.js的数据更新
                driver.execute_script("""
                    const event = new Event('input', { bubbles: true });
                    arguments[0].dispatchEvent(event);
                """, input_element)
                time.sleep(5)
                input_element.send_keys(to_do)
                time.sleep(5)
                # 按回车键
                input_element.send_keys(Keys.ENTER)
                time.sleep(5)
            except Exception as e:
                print(f"输入商品编号{to_do}失败：{e}")
                break
            try:
                try:
                    select_ok = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, 'eds-checkbox__indicator'))
                    )[-2]
                    driver.execute_script("arguments[0].click();", select_ok)
                except Exception as e:
                    print(f"选择商品{to_do}失败：{e}")
                    break
                change_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'eds-button.eds-button--normal'))
                )
                button_clicked = False
                for elem in change_button:
                    if "更改状态" in elem.get_attribute("innerText"):
                        driver.execute_script("arguments[0].click();", elem)
                        button_clicked = True
                        break
                if not button_clicked:
                    print(f"商品{to_do}未找到更改状态按钮")
                    # 因为两条，所以重试就行
                    last_try = last_try.append(to_do)
                    continue
                time.sleep(10)
                zanting_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'eds-dropdown-item'))
                )
                for elem in zanting_button:
                    if "暂停" in elem.get_attribute("innerText"):
                        driver.execute_script("arguments[0].click();", elem)
                        break
                time.sleep(1)
                confirm_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'eds-modal__footer-buttons'))
                )[-1].find_element(By.CLASS_NAME, 'eds-button.eds-button--primary.eds-button--normal')
                driver.execute_script("arguments[0].click();", confirm_button)
                time.sleep(10)
                try:
                    select_ok = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, 'eds-checkbox__indicator'))
                    )[-2]
                    driver.execute_script("arguments[0].click();", select_ok)
                except Exception as e:
                    print(f"取消择商品{to_do}失败：{e}")
                    break
            except:
                print(f"暂停商品{to_do}失败")
                break
def dl_zannei_ads_data(start_time, final_time,shop_name):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 日期选择
            def find_and_click_date_element(driver):
                """查找并点击包含日期关键词的元素"""
                date_elements = WebDriverWait(driver, 10, 0.5).until(
                    EC.presence_of_all_elements_located(
                        (By.CLASS_NAME, "eds-selector__inner.line-clamp--1")
                    )
                )
                keywords = ["天", "周", "月", "-"]
                for elem in date_elements:
                    if any(kw in elem.text for kw in keywords):
                        driver.execute_script("arguments[0].click();", elem)
                        return True
                return False
            # 尝试三次查找并点击日期元素
            max_attempts = 3
            for attempt in range(max_attempts):
                if find_and_click_date_element(driver):
                    try:
                        # 等待下拉框出现
                        panel_element = WebDriverWait(driver, 10, 0.5).until(
                            EC.visibility_of_element_located((By.CLASS_NAME, "eds-date-picker__picker"))
                        )
                        if panel_element:
                            break  # 成功找到下拉框，退出循环
                    except Exception as e:
                        print(f"等待下拉框出现失败 (尝试 {attempt + 1}/{max_attempts}): {e}")
            else:
                print("达到最大尝试次数，仍未找到下拉框")
            input_date(driver, start_time, final_time) # list index out of range
            # 下载操作
            # dl_manu = WebDriverWait(driver, 10, 0.5).until(
            #     EC.presence_of_all_elements_located(
            #         (By.XPATH, "//*[contains(text(), '导出数据')]")
            #     )
            # )
            dl_manu = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@data-testid='export-data-dropdown-trigger']"))
            )
            driver.execute_script("arguments[0].click();", dl_manu)
            time.sleep(2)
            # dl_button = WebDriverWait(driver, 10, 0.5).until(
            #     EC.presence_of_all_elements_located(
            #         (By.XPATH, "//*[contains(text(), '综合广告数据')]")
            #     )
            # )
            dl_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//li[@data-testid='export-data-dropdown-item' and contains(normalize-space(.), '综合广告数据')]"
                                            ))
            )
            driver.execute_script("arguments[0].click();", dl_button)
            time.sleep(10)
            # 等待最新报告出现
            try:
                latest_report = WebDriverWait(driver, 10, 0.5).until(
                    EC.visibility_of_all_elements_located(
                        (By.XPATH, "//*[contains(text(), '最新报告')]")
                    )
                )
            except TimeoutException:
                latest_report = WebDriverWait(driver, 10, 0.5).until(
                    EC.invisibility_of_element_located(
                        (By.XPATH, "//*[contains(text(), '最新报告')]")
                    )
                )
                click_element = WebDriverWait(driver, 10, 0.5).until(
                    EC.presence_of_all_elements_located(
                        (By.CLASS_NAME, "btn.eds-button.eds-button--normal")
                    )
                )
                driver.execute_script("arguments[0].click();", click_element[0])
                print("点击最新报告")
                time.sleep(0.3)
            # 文件选择
            select_file_dl = WebDriverWait(driver, 10, 0.5).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "list-item"))
            )
            logger.info("开始下载文件")
            for elem in reversed(select_file_dl):
                if contains_target_dates(elem.text, start_time, final_time):
                    logger.info(f"开始下载文件-{elem.text}")
                    download_btn = elem.find_element(
                        By.CLASS_NAME, "eds-button.eds-button--primary.eds-button--normal"
                    )
                    download_btn_text = download_btn.text
                    if "下载" in download_btn_text:
                        driver.execute_script("arguments[0].click();", download_btn)
                        logger.info("下载完成")
                        time.sleep(50)
                        return True  # 成功下载，退出函数
                    elif " " in download_btn_text:
                        logger.info("文件处理中，等待下载")
                        time.sleep(10)
                        download_btn = elem.find_element(
                            By.CLASS_NAME, "eds-button.eds-button--primary.eds-button--normal"
                        )
                        if "下载" in download_btn_text:
                            driver.execute_script("arguments[0].click();", download_btn)
                    elif "失败" in download_btn_text:
                        logger.warning("文件下载失败，重试")
                        continue
            # 如果没有找到可下载的文件
            logger.warning("未找到符合条件的下载文件")
        except Exception as e:
            logger.error(f"尝试 {attempt + 1}/{max_retries} 失败: {traceback.format_exc()}")
        # 重试前刷新页面
        driver.refresh()
        time.sleep(20)
    logger.error(f"超过最大重试次数，下载失败：{shop_name}-{start_time}-{final_time}")
    return False
def main_dl_zannei_ads_data(driver, shop_list,time_list):
    num = 0
    shop_data = []  # 收集店铺信息
    # 跳过full_shop_name中含有SIP的字典
    shop_list = [shop for shop in shop_list if "SIP" not in shop["full_shop_name"]]
    # 这些店铺不需要下载，如果下载会卡住
    # dont_dl_shop = ['lifetool_07.th','lifetool_08.th','lifetool_09.th','lifetool_10.th','tryfordream_02.th','']
    # shop_list = [shop for shop in shop_list if shop["shop_name"] not in dont_dl_shop]
    for shop_info in shop_list:
        print(f"切换店铺：{shop_info['shop_name']}")
        skip_shop = onlyswitch(driver, shop_info['shop_name'])
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        main_popup_close(driver)
        if skip_shop == 0:
            print('跳过该店铺')
            num += 1
            continue
        else:
            time.sleep(2)
            current_url = driver.current_url
            if "no_permission" in current_url:
                print(shop_info['shop_name'], "无权限")
                num += 1
                continue
            elif 'cnsc_shop_id' in current_url:
                shop_id = current_url.split('cnsc_shop_id=')[1].split('&')[0]
                should_skip = False
                skip_reason = ""
                try:
                    target_class = "metric-item"
                    no_data = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, target_class))
                    )
                    # 检查所有metric-item元素
                    # 使用显式等待确保元素稳定
                    spend_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, "//div[contains(@class, 'metric-item') and contains(., '花费')]")
                        )
                    )
                    # 直接获取文本避免元素过期
                    spend_text = spend_element.text
                    num_str = ''.join(filter(str.isdigit, spend_text)) or '0'
                    spend_val = float(num_str) if num_str else 0.0

                    if spend_val <= 0:
                        # logger.info(f"{shop_info['shop_name']} 花费为0，跳过")
                        # num += 1
                        # continue
                        pass # 历史有数据，但是花费为0，不跳过
                except TimeoutException:
                    print(f"{shop_info['shop_name']} 未找到metric-item元素")
                    try:
                        # 检查导出按钮作为备用方案
                        out_put = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.XPATH, "//*[contains(text(), '导出数据')]"))
                        )
                        if out_put:
                            print(f"有导出按钮-{shop_info['shop_name']}")
                        else:
                            should_skip = True
                    except:
                        skip_reason = (f"无导出按钮-{shop_info['shop_name']}")
                        should_skip = True
                # 关键修改：根据标志判断是否跳过整个店铺
                if should_skip:
                    logger.info(skip_reason)
                    num += 1
                    continue  # 此处跳过整个店铺循环
        shop_name = shop_info['shop_name']
        logger.info(f"开始下载店铺-{shop_name}-数据，现在时间是：" + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if any('shop_name' in d for d in time_list):
            to_do_time_list = [d for d in time_list if d['shop_name'] == shop_name]
        else:
            to_do_time_list = time_list
        for time_info in to_do_time_list:
            start_time = time_info['start_time']
            final_time = time_info['end_time']
            shop_data.append({
                "商店ID": shop_id,
                "商店名称": shop_name,
                "店铺主体": account,
                "期初": start_time,  # 期初时间
                "期末": final_time      # 期末时间
            })
            dl_ok = dl_zannei_ads_data(start_time, final_time,shop_name)
            if dl_ok:
                logger.info(f"{shop_name}-{start_time}-{final_time} 下载成功")
                # 修改文件名，读取chrome_dl_path最新的文件，在文件后添加-shop_name
                files = [f for f in os.listdir(chrome_dl_path) if os.path.isfile(os.path.join(chrome_dl_path, f))]
                if files:
                    file_paths = [os.path.join(chrome_dl_path, f) for f in files]
                    latest_file = max(file_paths, key=os.path.getctime)
                    shop_name_ = f"-{shop_name}"
                    add_shop_name(latest_file, shop_name_)
    return shop_data
def log_dl_zannei_ads_data():
    # 读取日志文件
    log_file_path = r'\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK\广告分析汇总数据\站内广告下载日志_更新.csv'
    # 确保文件存在
    if not os.path.exists(log_file_path):
        logger.error(f"日志文件不存在: {log_file_path}")
        input("按回车键继续...")
        return
    try:
        # 读取CSV文件
        log_df = pd.read_csv(log_file_path, encoding='utf-8-sig')
        # 检查必要的列是否存在
        required_columns = ['商店ID', '商店名称', '店铺主体', '期初', '期末']
        if not all(col in log_df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in log_df.columns]
            logger.error(f"日志文件缺少必要列: {', '.join(missing_cols)}")
            input("按回车键继续...")
            return
        # 过滤当前account的记录
        account_log_df = log_df[log_df['店铺主体'] == account]
        # 初始化shop_list和time_list
        shop_list = []
        time_list = []
        # 处理日期格式并分组
        for _, row in account_log_df.iterrows():
            try:
                # 转换日期格式
                start_date = datetime.strptime(row['期初'], '%Y/%m/%d')
                end_date = datetime.strptime(row['期末'], '%Y/%m/%d')
                # 添加到time_list
                time_list.append({
                    'start_time': start_date,
                    'end_time': end_date,
                    'shop_name': row['商店名称']
                })
                # 添加到shop_list
                shop_list.append({
                    'shop_id': row['商店ID'],
                    'shop_name': row['商店名称'],
                    'account': row['店铺主体'],
                    'country': '',  # 需要从其他来源获取
                    'full_shop_name': row['商店名称']  # 简化处理
                })
            except ValueError as e:
                logger.error(f"日期格式转换错误: {row['期初']} 或 {row['期末']} - {str(e)}")
                continue
        logger.info(f"成功读取日志文件，共 {len(shop_list)} 个店铺记录")
        # 调用下载函数
        return shop_list, time_list
    except Exception as e:
        logger.error(f"处理日志文件失败: {str(e)}", exc_info=True)
        input("按回车键继续...")

def excel_dl_zannei_ads_data(current_account=None):
    """
    从Excel文件读取店铺和日期数据
    Excel文件格式：第一列为shop，第二列为sunday_date，第三列为account
    
    Args:
        current_account (str): 当前登录的账号，只返回该账号对应的店铺数据
    """
    excel_file_path = r"C:\Users\lifet\Downloads\工作簿1.xlsx"
    
    # 确保文件存在
    if not os.path.exists(excel_file_path):
        logger.error(f"Excel文件不存在: {excel_file_path}")
        input("按回车键继续...")
        return [], []
    
    try:
        # 读取Excel文件
        df = pd.read_excel(excel_file_path, engine='openpyxl')
        
        # 检查必要的列是否存在
        required_columns = ['shop', 'sunday_date', 'account']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Excel文件缺少必要列: {missing_columns}")
            logger.info(f"当前列: {list(df.columns)}")
            input("按回车键继续...")
            return [], []
        
        # 初始化shop_list和time_list
        shop_list = []
        time_list = []
        
        # 处理数据
        for _, row in df.iterrows():
            try:
                shop_name = row['shop']
                sunday_date_str = row['sunday_date']
                account_name = row['account']
                
                # 如果指定了当前账号，只处理匹配的店铺
                if current_account and account_name != current_account:
                    continue
                
                # 处理日期格式 - 从Sunday日期计算周一到周日的范围
                if isinstance(sunday_date_str, str):
                    # 处理字符串格式的日期
                    if '-' in sunday_date_str:
                        # MM-DD-YY 格式，如 "06-29-25"
                        sunday_date = datetime.strptime(sunday_date_str, '%m-%d-%y')
                    elif '/' in sunday_date_str:
                        # YYYY/M/D 格式，如 "2025/6/29"
                        sunday_date = datetime.strptime(sunday_date_str, '%Y/%m/%d')
                    else:
                        # 其他格式尝试
                        sunday_date = pd.to_datetime(sunday_date_str)
                else:
                    # 处理pandas datetime格式或其他数值格式
                    sunday_date = pd.to_datetime(sunday_date_str)
                
                # 计算该周的周一（Sunday - 6天）
                monday_date = sunday_date - timedelta(days=6)
                
                # 添加到time_list
                time_list.append({
                    'start_time': monday_date,
                    'end_time': sunday_date,
                    'shop_name': shop_name,
                    'account': account_name
                })
                
                # 添加到shop_list（去重）
                shop_exists = any(shop['shop_name'] == shop_name for shop in shop_list)
                if not shop_exists:
                    shop_list.append({
                        'shop_id': '',  # Excel中没有shop_id，留空
                        'shop_name': shop_name,
                        'account': account_name,  # 使用Excel中的账号
                        'country': '',  # 需要从其他来源获取
                        'full_shop_name': shop_name
                    })
                    
            except Exception as e:
                logger.error(f"处理Excel行数据错误: {row.to_dict()} - {str(e)}")
                continue
        
        logger.info(f"成功读取Excel文件，共 {len(shop_list)} 个店铺，{len(time_list)} 个时间段")
        return shop_list, time_list
        
    except Exception as e:
        logger.error(f"处理Excel文件失败: {str(e)}", exc_info=True)
        input("按回车键继续...")
        return [], []

def select_ba_dl_time(driver,dl_fomat,date):
    try:
        target_text = '统计时间'
        select_dl_time_element = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.XPATH, f"//*[contains(text(), '{target_text}')]"))
        )
        driver.execute_script("arguments[0].click();", select_dl_time_element[0])
        time.sleep(0.3)
        fomat_element = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.XPATH, f"//*[contains(text(), '{dl_fomat}')]"))
        )
        ActionChains(driver).move_to_element(fomat_element[0]).perform()
        if date is not None:
            s_or_e = 0
            select_ok = select_date(driver, s_or_e, date)
            if select_ok is False:
                s_or_e = 1
                select_ok = select_date(driver, s_or_e, date)
                if select_ok is False:
                    print("时间选择失败")
                    return False
        else:
            ActionChains(driver).move_to_element(fomat_element[0]).click().perform()
            select_ok = True
        if select_ok is True:
            time.sleep(3)
            target_class_name = "eds-button eds-button--normal track-click-normal-export"
            target_class_name = target_class_name.replace(" ", ".")
            elements = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME,target_class_name))
            )
            driver.execute_script("arguments[0].click();", elements[0])
            time.sleep(60)
            return True
    except Exception as e:
        print(e)
def main_dl_ba_data(driver, shop_list, dl_fomat, date):
    num = 0
    shop_data = []  # 收集店铺信息
    # 跳过full_shop_name中含有SIP的字典
    shop_list = [shop for shop in shop_list if "SIP" not in shop["full_shop_name"]]
    for shop_info in shop_list:
        print(f"切换店铺：{shop_info['shop_name']}")
        skip_shop = onlyswitch(driver, shop_info['shop_name'])
        main_popup_close(driver)
        if skip_shop == 0:
            print('跳过该店铺')
            num += 1
            continue
        else:
            time.sleep(2)
            current_url = driver.current_url
            if "no_permission" in current_url:
                print(shop_info['shop_name'], "无权限")
                num += 1
                continue
            elif 'cnsc_shop_id' in current_url:
                shop_id = current_url.split('cnsc_shop_id=')[1].split('&')[0]
                shop_data.append({
                    "shop_id": shop_id,
                    "shop_name": shop_info['shop_name'],
                    "account": account
                })
                # 所有店铺都下载数据
                shop_name = shop_info['shop_name']
                logger.info(f"开始下载店铺-{shop_name}-数据，现在时间是：" + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                for d in date:
                    select_ba_dl_time(driver, dl_fomat, d)
def get_info_inb():
    element = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CLASS_NAME, "title")))[0]
    num_todo = int(re.findall(r'\d+', element.text)[0])
    if num_todo < 1 :
        return pd.DataFrame()
    else:
        df_result = pd.DataFrame()
        for num in range((num_todo + 9) // 10):
            print(f"下载第{num + 1}页")
            # 等待数据加载成功
            # 确认机制：等待加载遮罩消失，并且确认表格行数据已更新
            try:
                 # 1. 等待加载遮罩（如果存在）消失
                WebDriverWait(driver, 10).until(
                    EC.invisibility_of_element_located((By.CLASS_NAME, "eds-spin"))
                )
            except TimeoutException:
                 pass # 忽略超时，可能没有遮罩
            df = get_index()
            target_class_name = 'eds-button eds-button--small eds-button--frameless eds-button--block eds-pager__button-next'
            target_class_name = target_class_name.replace(" ", ".")
            next_button = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name))
            )
            driver.execute_script("arguments[0].click();", next_button[0])
            df_result = pd.concat([df_result,df])
        return df_result
def get_index():
    df_result = pd.DataFrame()
    # 优化代码：按照 get_child_nodes_data 思路，结构化提取子节点数据
    target_class = 'eds-table eds-table-scrollX-left eds-table-scrollY eds-table--with-append'
    # CLASS_NAME 不支持复合类名，需转换为 CSS Selector
    css_selector = "." + target_class.replace(" ", ".")
    try:
        # 定位父节点（表格容器）
        # 策略优化：优先定位表头，通过表头找到父容器（表格整体），比复杂的 CSS Selector 更稳定
        try:
            header_element = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'eds-table__header-container'))
            )
            table_container = header_element.find_element(By.XPATH, "./..")
        except TimeoutException:
            # 备选方案：尝试使用最基础的 eds-table 类
            logger.warning("未找到表头，尝试使用 .eds-table 定位")
            table_container = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".eds-table"))
            )

        # 获取整个表格容器的HTML（只进行一次Selenium I/O）
        table_html = table_container.get_attribute('outerHTML')

        # 使用BeautifulSoup解析
        soup = BeautifulSoup(table_html, 'lxml')

        # 尝试从HTML中提取表头
        header_container = soup.find(class_='eds-table__header-container')
        if header_container:
            # 获取表头文本并分割
            header_text = header_container.get_text('\n', strip=True)
            columns = [col.strip() for col in header_text.split('\n') if col.strip()]
        else:
            # 如果找不到表头，使用Selenium单独获取（作为备选）
            try:
                header_element = driver.find_element(By.CLASS_NAME, 'eds-table__header-container')
                columns = [col.strip() for col in header_element.text.split('\n') if col.strip()]
            except:
                # 兜底：如果没有表头，使用默认命名
                columns = []

        # 获取所有 tbody（分别对应：左侧固定列、中间滚动列、右侧操作列）
        tbodies = soup.find_all(class_="eds-table__body")

        # 获取每个 tbody 下的所有行 (tr)
        bodies_rows = [body.find_all("tr") for body in tbodies]

        child_nodes_data = []

        # 遍历行，zip 还原行对应关系
        for idx, rows in enumerate(zip(*bodies_rows)):
            # rows 包含该行在不同部分的 tr 元素 (Tag 对象)

            # 提取该行的所有单元格数据
            row_cells = []
            links = []

            # 按顺序遍历每个部分的 tr
            for tr in rows:
                # 提取 td 文本
                tds = tr.find_all('td')
                for td in tds:
                    row_cells.append(td.get_text(' ', strip=True))

                # 提取链接
                hrefs = [a['href'] for a in tr.find_all("a", href=True)]
                links.extend(hrefs)

            # 构建行数据字典
            row_data = {}

            # 自动修正列错位：如果数据列比表头多1列，且第一列为空（通常是复选框），则移除第一列数据
            if len(row_cells) == len(columns) + 1:
                # 简单的启发式判断：如果第一个是空字符串，或者看起来像复选框占位
                # 这里直接移除第一个，因为用户反馈整体向后移动了一列
                row_cells = row_cells[1:]

            # 如果列数匹配，则映射到列名
            if len(columns) == len(row_cells):
                row_data = dict(zip(columns, row_cells))
            else:
                # 列数仍然不匹配时，使用索引作为列名
                # 优先匹配后方（通常操作列在最后），前方多余的放入 Extra
                if len(row_cells) > len(columns):
                    # 数据比表头多，截取后半部分匹配
                    offset = len(row_cells) - len(columns)
                    row_data = dict(zip(columns, row_cells[offset:]))
                    # 记录多余的
                    for i in range(offset):
                        row_data[f"Extra_{i}"] = row_cells[i]
                else:
                    # 数据比表头少，按顺序匹配
                    for i, cell in enumerate(row_cells):
                        col_name = columns[i] if i < len(columns) else f"Column_{i + 1}"
                        row_data[col_name] = cell

            # 添加额外信息
            row_data["链接"] = links
            # row_data["行HTML"] = [str(r) for r in rows] # 如果不需要HTML可以注释掉以节省内存

            child_nodes_data.append(row_data)

        # 直接转换为 DataFrame
        df_result = pd.DataFrame(child_nodes_data)
        logger.info(f"成功提取 {len(df_result)} 行数据")
        # print(df_result.head()) # 调试用

    except Exception as e:
        logger.error(f"获取表格子节点数据失败: {str(e)}")
    return df_result
def main_dl_stock_eta(driver):
    sidebar_name = '官方仓服务'
    into_sidebar_page(driver, sidebar_name)
    sites = switch_site(driver,"",format_is='get_sites')
    df_result_inb = pd.DataFrame()
    df_result_done = pd.DataFrame()
    for site in sites:
        switch_site(driver, site,format_is='switch')
        main_popup_close( driver)
        # 直到出现费用报告页面
        target_text = "入库管理"
        switch_type_page(driver, target_text)
        try:
            time.sleep(2)
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, "//*[contains(text(), '入库管理')]"))
            )
            logger.info(f"选择入库管理成功")
        except TimeoutException:
            logger.error("选择入库管理失败")
            break
        main_popup_close(driver)
        target_text = "已批准"
        switch_type_page(driver, target_text)
        df_result_ = get_info_inb()
        df_result_inb = pd.concat([df_result_inb,df_result_])
        target_text = "已完成"
        switch_type_page(driver, target_text)
        df_result__ = get_info_inb()
        df_result_done = pd.concat([df_result_done,df_result__])
# debugger_chrome
def attach_to_running_browser(port=9222):
    # 创建Chrome选项
    chrome_options = Options()
    # 添加调试端口以连接到现有浏览器
    chrome_options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
    chrome_options.add_argument("--disable-features=TabSearch")
    # 如果你需要设置下载路径等偏好，使用以下方式 测试页面不可以指定下载路径
    # prefs = {"download.default_directory": chrome_dl_path}
    # chrome_options.add_experimental_option("prefs", prefs)
    # 创建WebDriver实例
    driver = webdriver.Chrome(options=chrome_options)
    return driver
def debugger_chrome():
    try:
        driver = attach_to_running_browser()
        # 在这里可以继续使用driver进行操作
        print(driver.title)
        print(driver.current_url)
        return driver
    except Exception as e:
        print(f"连接失败: {e}")
        return None
def clean_currency_columns(df):
    """
    清洗带括号的列名并将数据移动到统一列
    """
    # 创建基础列名到币种后缀的映射
    base_columns_map = {}

    # 第一步：识别所有带括号的列
    for col in df.columns:
        if '(' in col and ')' in col:
            # 提取基础列名和币种
            match = re.search(r'^(.*?)\((.*?)\)$', col)
            if match:
                base_name = match.group(1).strip()
                currency = match.group(2).strip()
                # 添加到映射
                if base_name not in base_columns_map:
                    base_columns_map[base_name] = []
                base_columns_map[base_name].append((col, currency))
    # 第二步：为每个基础列名创建新列（如果不存在）
    for base_name in base_columns_map:
        if base_name not in df.columns:
            df[base_name] = pd.NA
    # 第三步：移动数据
    for base_name, currency_cols in base_columns_map.items():
        for orig_col, currency in currency_cols:
            # 遍历每一行
            for idx in df.index:
                val = df.at[idx, orig_col]
                # 检查有效值（包括0）
                is_valid = (
                        not pd.isna(val) and
                        val != "" and
                        str(val).strip() not in ['-', '--', 'NaN', 'nan']
                )
                if is_valid:
                    # 移动数据到基础列
                    df.at[idx, base_name] = val
                    if '币种' not in df.columns:
                        df['币种'] = ''
                    # 更新币种列（如果存在）
                    df.at[idx, '币种'] = currency
    # 第四步：删除原始带括号列
    cols_to_drop = [col for cols in base_columns_map.values() for col, _ in cols]
    df = df.drop(columns=cols_to_drop)
    return df
def click_dl_selected_button(driver):
    """点击下载按钮"""
    # 点击导出
    target_text = "导出"
    elements = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-button.eds-button--large"))
    )[0].find_elements(By.XPATH, f"//*[contains(text(), '{target_text}')]")
    for elem in elements:
        if target_text == elem.get_attribute("innerText"):
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", elem)
            break
    time.sleep(random.randint(1, 3))
    target_output = "已选"
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, "eds-dropdown-menu"))
    )[-1].find_elements(By.CLASS_NAME, "eds-dropdown-item")[0].find_elements(By.XPATH,f"//*[contains(text(), '{target_output}')]")[-1]
    if target_output == element.get_attribute("innerText"):
        # 点击元素
        # element.click()
        driver.execute_script("arguments[0].click();", element)
    else:
        print("无法导出")
        winsound.Beep(500, 500)
        winsound.Beep(500, 500)
        input("无法导出，请手动导出，并按回车")
        time.sleep(1)
def dl_vsku(driver):
    # 先拿到页数
    target_class_name = 'eds-pager__page'
    target_class_name = target_class_name.replace(" ", ".")
    pages = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name))
    )
    print(pages[-1].text)
    # 转换为整数
    max_page = int(pages[-1].text.strip())
    for page in range(1, max_page + 1):
        selected_data_num = None
        while selected_data_num == None:
            # 点击全选
            target_class_name = 'eds-checkbox__indicator'
            target_class_name = target_class_name.replace(" ", ".")
            select_all_button = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name))
            )
            driver.execute_script("arguments[0].click();", select_all_button[0])
            # 确认已选择
            target_class_name = 'translate-var-text'
            target_class_name = target_class_name.replace(" ", ".")
            selected_data = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name))
            )
            selected_data_num = selected_data[0].text
            selected_data_num = int(selected_data_num) if selected_data_num.strip() else None
        click_dl_selected_button(driver)
        time.sleep(15)
        download_with_retry(driver, chrome_dl_path)
        print(f'已下载-{page}')
        while selected_data_num != None:
            target_class_name = 'eds-checkbox__indicator'
            target_class_name = target_class_name.replace(" ", ".")
            select_all_button = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name))
            )
            driver.execute_script("arguments[0].click();", select_all_button[0])
            # 确认已取消
            target_class_name = 'translate-var-text'
            target_class_name = target_class_name.replace(" ", ".")
            selected_data = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name))
            )
            selected_data_num = selected_data[0].text
            selected_data_num = int(selected_data_num) if selected_data_num.strip() else None
        if page == max_page:
            print('已下载完成')
        else:
            # 点击下一页
            target_class_name = 'eds-button eds-button--small eds-button--frameless eds-button--block eds-pager__button-next'
            target_class_name = target_class_name.replace(" ", ".")
            next_button = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, target_class_name))
            )
            driver.execute_script("arguments[0].click();", next_button[0])
if __name__ == "__main__":
    driver = None
    pw = None
    chrome_dl_path = r"\\Auteng\综合管理部\自动化下载文件\DefaultDownloadPath"
    dl_data_type = input('请选择下载数据类型(默认下载库存数据)：\n-1.登录shopee\n0.测试\n1.库存数据\n2.拨款账单\n3.sbs费用报告\n4.AMS费用报告\n5.站外广告数据\n6.站内广告数据\n7.商业分析\n8.VSKU\n')
    account = input('请输入账号：\n1.趣利恩\n2.海湃\n3.遨腾\n5.五店')
    if dl_data_type != '0':
        if account == '1':
            account = '趣利恩'
        elif account == '2':
            account = '海湃'
        elif account == '3':
            account = '遨腾'
        elif account == '5':
            account = '五店'
        else:
            account = '趣利恩'
            print('输入有误，默认下载趣利恩数据')
        chrome_dl_path = r"\\Auteng\综合管理部\自动化下载文件\DefaultDownloadPath"+"_"+account
    if dl_data_type == '0': # 测试
        driver = debugger_chrome()
        # 进入测试页面
        input("请手动进入测试页面后按回车继续...")
        window_handles = driver.window_handles
        if len(window_handles) > 1:
            driver.switch_to.window(window_handles[-1])
            print(driver.title)
            print(driver.current_url)
            print("已成功进入测试页面")
    elif dl_data_type == '-1':
        logging.info(f"处理账号：{account}") # 登录账号
        if driver == None:
            driver = init_driver(chrome_dl_path, account_name=account)
            pw = login(driver, account)
        handle_popup(driver)
        try:
            while True:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"当前时间: {current_time}")
                time.sleep(60) 
        except KeyboardInterrupt:
            print("\n程序被用户手动停止。")
    elif dl_data_type == '2': # 下载拨款账单
        default_dl_bill_path = r"\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\所有下载源文件-检测下载用"
        default_dl_bill_path_new = r'\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\最新下载待处理'
        # 读取目录里已经下载过的文件，识别店铺名称和日期
        shop_download_dates = {}
        for file_name in os.listdir(default_dl_bill_path):
            if file_name.endswith('.xlsx'):
                file_path = os.path.join(default_dl_bill_path, file_name)
                shop_name, date = get_shop_name_and_date(file_path)
                if shop_name not in shop_download_dates:
                    shop_download_dates[shop_name] = []
                shop_download_dates[shop_name].append(date)
        for file_name in os.listdir(default_dl_bill_path_new):
            if file_name.endswith('.xlsx'):
                file_path = os.path.join(default_dl_bill_path_new, file_name)
                shop_name, date = get_shop_name_and_date(file_path)
                if shop_name not in shop_download_dates:
                    shop_download_dates[shop_name] = []
                shop_download_dates[shop_name].append(date)
        df_ex_dl = pd.DataFrame(shop_download_dates.items(), columns=['店铺名称', '下载日期'])
        shop_data = []  # 收集店铺信息
        logging.info(f"处理账号：{account}") # 登录账号
        chrome_dl_path = r"\\Auteng\综合管理部\自动化下载文件\DefaultDownloadPath"+"_"+account
        if driver == None:
            driver = init_driver(chrome_dl_path, account_name=account)
            pw = login(driver, account)
        main_popup_close( driver)
        sidebar_name = '我的收入'
        into_sidebar_page(driver, sidebar_name)
        time.sleep(5)
        bill_pw_verification(driver, account, pw)
        driver.refresh()
        time.sleep(5)
        shop_list = get_bill_shop_list(driver)
        # 主下载流程
        main_dl_bill_data(driver,shop_list)
        batch_smart_move_recursive(chrome_dl_path, default_dl_bill_path_new,overwrite=True)
        shop_data_df = get_info_data_billl(driver, shop_list, account, pw, onlyswitch, bill_pw_verification)
        # 先读取源文件，与当前文件拼接，然后去重
        shop_data_df_result_path = r'\\AUTENG\hot_data\数据处理\12.数据收集\10.拨款账单shopee\汇总结果备份'
        shop_data_df_ex_path = os.path.join(shop_data_df_result_path, f'{account}_各店铺应下载拨款日期汇总.csv')
        if '应下载日期' in shop_data_df.columns:
            # 不指定 format，让 pandas 自动识别
            shop_data_df['应下载日期'] = pd.to_datetime(shop_data_df['应下载日期'],format='mixed')
            shop_data_df['应下载日期'] = shop_data_df['应下载日期'].dt.strftime('%Y/%m/%d')
            shop_data_df = shop_data_df[shop_data_df['应下载日期'].notnull()]
        shop_data_df.drop_duplicates().to_csv(shop_data_df_ex_path, index=False, encoding='utf-8-sig')
    elif dl_data_type == '3': # 下载SBS费用报告
        # 1，读取存放文件的路径
        info_path = r"\\Auteng\综合管理部\综合管理部_公共\报表输出-YSK"
        info_filename = "SBS费用报告生成日期_滚动更新.xlsx"
        dl_path = r"\\Auteng\综合管理部\综合管理部_公共\数据分层处理\1源数据下载"
        dl_dir_name = r"SBS费用报告"
        last_dl_date_info = os.path.join(info_path, info_filename)
        df_last_info = pd.read_excel(last_dl_date_info, sheet_name="已下载最新批次", dtype=str)
        df_last_info.dropna(subset=['主体名称', '收费时段期末'], inplace=True)
        df_last_info['海外仓'] = df_last_info['海外仓'].replace("0", 'shopee官方仓')
        # 如果海外仓列为shopee南宁仓，将对应的站点改为中国大陆
        df_last_info.loc[df_last_info['海外仓'] == 'shopee南宁仓', '站点'] = '中国大陆'
        df_last_info = df_last_info.groupby(['主体名称', '站点', '海外仓'], as_index=False)[['收费时段期末']].min()
        # 将收费时段期末转换为日期格式
        df_last_info['收费时段期末'] = pd.to_datetime(df_last_info['收费时段期末'], format='%Y-%m-%d %H:%M:%S')
        today = datetime.now().strftime("%Y.%m.%d")
        dl_dir_name = dl_dir_name + today
        dl_dir = os.path.join(dl_path, dl_dir_name)
        if not os.path.exists(dl_dir):
            os.makedirs(dl_dir)
            print(f"创建目录：{dl_dir}")
        logging.info(f"处理账号：{account}") # 登录账号
        if driver == None:
            driver = init_driver(chrome_dl_path, account_name=account)
            pw = login(driver, account)
        df_sum_data = pd.DataFrame()
        handle_popup(driver)
        main_popup_close( driver)
        main_dl_sbs_data()
        batch_smart_move_recursive(chrome_dl_path, dl_dir)
    elif dl_data_type == '4': # 下载AMS费用报告
        # 这个数据无法筛选下载，所以每次都需要下载全量数据
        default_dl_ams_path = r"\\Auteng\综合管理部\自动化下载文件\ams营销费用"
        # 获取当前时间的上一个月
        last_month = (datetime.now().replace(day=1) - timedelta(days=1)).strftime("%Y%m")
        dl_dir_name = f"AMS{last_month}账单"
        dl_dir_path = os.path.join(default_dl_ams_path, dl_dir_name)
        if not os.path.exists(dl_dir_path):
            os.makedirs(dl_dir_path)
            logging.info(f"创建目录：{dl_dir_path}")
        logging.info(f"处理账号：{account}") # 登录账号
        if driver == None:
            driver = init_driver(chrome_dl_path, account_name=account)
            pw = login(driver, account)
        if pw == None:
            pw = input('请输入密码：')
        handle_popup(driver)
        main_dl_ams_data()
        batch_smart_move_recursive(chrome_dl_path, dl_dir_path)
    elif dl_data_type == '5': # 下载站外广告数据
        default_dl_ads_path = r"\\Auteng\综合管理部\自动化下载文件\站外广告"
        # 处理前置数据
        dl_time = input('请输入下载时间(默认下载上个月):\n1.上个月\n2.本月\n3.过去30天\n4.过去14天\n5.过去7天\n6.昨天\n')
        logging.info(f"处理账号：{account}") # 登录账号
        if driver == None:
            driver = init_driver(chrome_dl_path, account_name=account)
            pw = login(driver, account)
        handle_popup(driver)
        sidebar_name = '营销中心'
        into_sidebar_page(driver, sidebar_name)
        # 点击站外广告会切换窗口
        # 这里有很多种类别，站外广告或meta广告，找到一个就行
        locators = [
            (By.XPATH, "//div[contains(@class, 'title') and contains(text(), 'Meta广告')]"),
            (By.XPATH, "//div[contains(@class, 'title') and contains(text(), '站外广告')]")
        ]
        # 尝试点击广告元素
        ad_clicked = False
        for locator in locators:
            try:
                elements = WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located(locator)
                )
                if elements:
                    for element in elements:
                        try:
                            driver.execute_script("arguments[0].click();", element)
                            ad_clicked = True
                            print(f"成功点击{locator[1]}对应的广告")
                            break
                        except ElementClickInterceptedException:
                            print("元素被遮挡，无法点击")
                            continue
                    if ad_clicked:
                        break
            except TimeoutException:
                print(f"在15秒内未找到{locator[1]}对应的广告")
            except Exception as e:
                print(f"发生未知异常: {str(e)}")
        if not ad_clicked:
            print('未找到站外广告或Meta广告')
        time.sleep(2)
        switch_url(driver)
        shop_list = get_bill_shop_list(driver)
        main_dl_zanwai_ads_data(driver, shop_list,dl_time)
        batch_smart_move_recursive(chrome_dl_path, default_dl_ads_path)
    elif dl_data_type == '6':
        default_dl_ads_path = r"\\Auteng\综合管理部\自动化下载文件\站内广告"
        # 处理前置数据
        choice = input("请输入要下载的时间段：\n1.上周\n2.上两周\n3.上四周\n4.上八周\n5.上十二周\n6.上个月\n7.上两个月\n8.上三个月\n9.自定义\n10.补充下载\n11.Excel文件模式\n")
        time_list = generate_weekly_periods(choice)
        logging.info(f"处理账号：{account}") # 登录账号
        if driver == None:
            driver = init_driver(chrome_dl_path, account_name=account)
            pw = login(driver, account)
        handle_popup(driver)
        sidebar_name = 'Shopee广告'
        into_sidebar_page(driver, sidebar_name)
        main_popup_close( driver)
        time.sleep(10)
        shop_list = get_bill_shop_list(driver)
        if account:
            shop_list_dl = [ "autoking.my","autoking.ph","autoking.th","autoking.vn","genuine_auto.my","genuine_auto.ph",
                             "genuine_auto.th","genuine_auto.vn","lovelive.ph","triillion_auto.my","triillion_auto.th",
                             "trillion_auto.ph","trillion_auto.vn","trillion_co_ltd.my","trillion_co_ltd.vn","trillion_fashion.br",
                             "trillion_co_ltd.sg","trillion_co_ltd.th","SparkLight_auto.ph","sensor_auto.ph","coilspark_auto.ph",
                             "powerspeed_auto.ph","lifetool_09.sg","lifetool_06.vn","lifetool.ph","lifetool.vn","lifetool.my",
                             "lifetool.sg","lovelivef4.sg","trillionfas.vn","trillion_co_ltd.ph","sensor_auto.my","funway.th",
                             "lifetool_home.my","tryfordream.my","tryfordream_01.my","tryfordream_02.my","luckyfunny.ph",
                             "tryfordream_01.ph","tryfordream_04.ph","tryfordream_02.vn","tryfordream_05.vn","tryfordream01q1.vn",
                             "tryfordream88.sg","tryfordream_04.sg","tryfordream_03.sg","tryfordream999.th","tryfordream06.vn",
                             "tryfordream06.my","tryfordream06.th","tryfordream06.ph",
                             "tryfordream06.sg",'eastar.ph','lifetool_06.ph','lifetool_09.ph','swits_motor.ph','eastar.th','eastar0.th',
                             'eastar881.th','eastar88.my','eastar880.my','lifetool_10.my','lifetool_07.vn','lifetool_06.br',
                             'motor_plus.my','motor_genuine.ph','fire_racing.vn','motorgenuinezz.th']
            shop_list = [shop for shop in shop_list if shop['shop_name'] in shop_list_dl]
        if choice == '10':
            shop_list, time_list = log_dl_zannei_ads_data()
            shop_list = pd.DataFrame(shop_list).drop_duplicates(keep='first').to_dict('records')
            shop_list = [shop for shop in shop_list if shop['shop_name'] in shop_list_dl]
        elif choice == '11':
            shop_list, time_list = excel_dl_zannei_ads_data(current_account=account)
            shop_list = pd.DataFrame(shop_list).drop_duplicates(keep='first').to_dict('records')
            shop_list = [shop for shop in shop_list if shop['shop_name'] in shop_list_dl]
        all_dl_data_info = main_dl_zannei_ads_data(driver, shop_list,time_list)
        all_dl_data_info_df = pd.DataFrame(all_dl_data_info)
        all_dl_data_info_df.to_csv(os.path.join(chrome_dl_path, f'站内广告下载日志_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'), index=False)
        batch_smart_move_recursive(chrome_dl_path, default_dl_ads_path)
    elif dl_data_type == '7':
        default_dl_ba_path = r"\\Auteng\综合管理部\自动化下载文件\商业分析"
        # 处理前置数据
        while True:
            dl_fomat_dict = {
                '1': '今日实时',
                '2': '昨天',
                '3': '过去7 天',
                '4': '过去30 天',
                '5': '按日',
                '6': '按周',
                '7': '按月',
                '8': '按年'
            }
            # 动态生成提示信息
            prompt = "请选择下载形式：\n" + "\n".join([f"{k}.{v}" for k, v in dl_fomat_dict.items()]) + "\n"
            # 获取用户输入
            print("7,8待开发")
            dl_fomat_num = input(prompt)
            dl_fomat = dl_fomat_dict.get(dl_fomat_num)
            if dl_fomat:
                break
            else:
                print("输入有误，请重新输入")
        date = None
        if dl_fomat_num not in ['1', '2', '3', '4']:
            if dl_fomat_num == '8':
                date = input("请输入需要下载的年份,如果有多个年份用逗号隔开(2024,2025)\n")
                date = [int(year) for year in date.split(',')]
            elif dl_fomat_num == '7':
                date = input("请输入需要下载的月份,如果有多个月份用逗号隔开(2024.01,2024.02)\n")
                date = [datetime.strptime(month, '%Y.%m') for month in date.split(',')]
            elif dl_fomat_num == '6':
                choice = input("请输入要下载的时间段：\n1.上周\n2.上四周\n3.上八周\n4.上十二周\n")
                if choice in ['1', '2', '3', '4']:
                    date = generate_weekly_periods(choice)
                    date = [item['start_time'] for item in date]
            elif dl_fomat_num == '5':
                date = input("请输入需要下载的日期,如果有多个日期用逗号隔开(2024.01.01,2024.01.02)\n")
                date = [datetime.strptime(day, '%Y.%m.%d') for day in date.split(',')]
        logging.info(f"处理账号：{account}") # 登录账号
        if driver == None:
            driver = init_driver(chrome_dl_path, account_name=account)
            pw = login(driver, account)
        main_popup_close( driver)
        sidebar_name = '商业分析'
        into_sidebar_page(driver, sidebar_name)
        bill_pw_verification(driver, account, pw)
        main_popup_close( driver)
        shop_list = get_bill_shop_list(driver)
        main_dl_ba_data(driver, shop_list, dl_fomat, date)
    elif dl_data_type == '8':
        default_dl_vsku_path = r"\\Auteng\综合管理部\自动化下载文件\VSKU"
        logging.info(f"处理账号：{account}") # 登录账号
        if driver == None:
            driver = init_driver(chrome_dl_path, account_name=account)
            pw = login(driver, account)
        main_popup_close( driver)
        sidebar_name = '官方仓服务'
        into_sidebar_page(driver, sidebar_name)
        bill_pw_verification(driver, account, pw)
        main_popup_close( driver)
        input('进入商品管理，选择VSKU，选择需要下载的站点，选择所有店铺')
        # 先手动进入
        dl_vsku(driver)
        batch_smart_move_recursive(chrome_dl_path, default_dl_vsku_path)
    else: # 默认下载库存数据
        final_time, start_time = start_final_time()
        download_dir, backup_dir, info_path = default_dl_path(final_time)
        # 获取修改后的数据(公司主体、平台店铺、完整数据)
        company_accounts, platform_shops, shop_ids, df = read_shop_table(info_path)
        logging.info(f"处理账号：{account}") # 登录账号
        if driver == None:
            driver = init_driver(chrome_dl_path, account_name=account)
            pw = login(driver, account)
        handle_popup(driver)
        main_dl_kc_data()
        batch_smart_move_recursive(chrome_dl_path, download_dir)
