# -*- coding: utf-8 -*-
import time
import random
import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

def dxm_caozuorizhi(driver):
    # 首页
    ac = "jeffff"
    pw = "Wjf4170564@"
    driver.get("https://www.dianxiaomi.com/index.htm")
    time.sleep(5)
    try:
        driver.find_elements(by=By.ID, value="exampleInputName")[0].send_keys(ac)
        driver.find_elements(by=By.ID, value="exampleInputPassword")[0].send_keys(pw)
        input("输入验证码再输入继续，并点击跳转到日志页面")
        time.sleep(random.randint(1, 3) / 10)
        driver.find_element(by=By.ID, value="loginBtn").click()  # 登录
    except:
        pass

    if True:
        element = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.ID, 'upPage')))
        text = element[0].get_attribute("innerText")
        page_all = int(text[text.rfind("/")+1:text.rfind("\n")])


        product_all=pd.DataFrame()
        for p in range(page_all):
            # 加载完成再往下
            try:
                elements_content =  driver.find_element(by=By.ID,value="goodsContent").find_elements(by=By.CLASS_NAME,value="content")
                for i in range(0,len(elements_content)):
                    listing = []
                    staff = elements_content[i-1].find_elements(by=By.CLASS_NAME,value="w100.minW100.maxW100.p-left10")[0].get_attribute("innerText")
                    packgetime = elements_content[i - 1].find_elements(by=By.CLASS_NAME,value="w150.minW150.maxW150")[0].get_attribute("innerText")
                    posision = elements_content[i - 1].find_elements(by=By.CLASS_NAME,value="w150.minW150.maxW150")[1].get_attribute("innerText")
                    task = elements_content[i - 1].find_elements(by=By.CLASS_NAME,value="w450.minW400.white-space")[0].get_attribute("innerText")
                    listing.append(staff)
                    listing.append(packgetime)
                    listing.append(posision)
                    listing.append(task)
                    product_all = pd.concat([product_all,pd.DataFrame([listing])])
                print("1")

                elementnext =  WebDriverWait(driver, 10, 0.5).until(EC.presence_of_all_elements_located((By.ID, 'downPage')))[-1].find_elements(by=By.TAG_NAME,value="a")
                for en in  elementnext :
                    if "下一页" in en.get_attribute("outerHTML"):
                        driver.execute_script("arguments[0].click();", en)
                        break
                # 处理下一页
                for tt in range(30):
                    elementloading = WebDriverWait(driver, 10, 0.5).until(EC.presence_of_all_elements_located((By.ID, 'loading')))[0].get_attribute("outerHTML")
                    if  "aria-hidden=\"true\""  in elementloading:
                        break
                    else:
                        print(tt)
                        time.sleep(1)

                # 滚动加载
                # print("2")
                # js = "var q=document.body.scrollHeight ;return(q)"
                # Text_height = driver.execute_script(js)
                # driver.execute_script('window.scrollBy(0,' + str(-Text_height + 1000) + ')')
                # for j in range(int(Text_height / 500.0)):
                #     time.sleep(random.randint(1, 2) / 10)
                #     driver.execute_script('window.scrollBy(0,500)')
                print(p,page_all)
            except:
                product_all0=product_all
                input("备份成功，刷新页面并继续")

        product_all.columns = ["人员", "操作时间","操作位置","操作内容"]
        product_all.drop_duplicates(inplace=True)
        product_all.to_excel(r"\\auteng20\5.共享文件\报表输出\发货操作日志"+datetime.datetime.today().strftime("%Y%m%d") + ".xlsx",index=False)  # +datetime.datetime.today().strftime("%Y%m%d")

    if True:
        input("处理订单日志，搜索:有货-月度了吗")
        input("处理订单日志，搜索有货了吗")
        element = WebDriverWait(driver, 10, 0.5).until(
            EC.presence_of_all_elements_located((By.ID, 'upPage')))
        text = element[0].get_attribute("innerText")
        page_all = int(text[text.rfind("/") + 1:text.rfind("\n")])

        product_all = pd.DataFrame()
        for p in range(page_all):
            # 加载完成再往下
            try:
                elements_content = driver.find_element(by=By.ID, value="goodsContent").find_elements(by=By.CLASS_NAME,
                                                                                                     value="content")
                for i in range(0, len(elements_content)):
                    listing = []
                    staff = elements_content[i - 1].find_elements(by=By.CLASS_NAME, value="w100.minW100.maxW100.p-left10")[
                        0].get_attribute("innerText")
                    packgetime = elements_content[i - 1].find_elements(by=By.CLASS_NAME, value="w150.minW150.maxW150")[
                        0].get_attribute("innerText")
                    posision = elements_content[i - 1].find_elements(by=By.CLASS_NAME, value="w150.minW150.maxW150")[
                        1].get_attribute("innerText")
                    task = elements_content[i - 1].find_elements(by=By.CLASS_NAME, value="w450.minW400.white-space")[
                        0].get_attribute("innerText")
                    listing.append(staff)
                    listing.append(packgetime)
                    listing.append(posision)
                    listing.append(task)
                    product_all = pd.concat([product_all, pd.DataFrame([listing])])

                # 处理下一页
                elementnext =  WebDriverWait(driver, 10, 0.5).until(EC.presence_of_all_elements_located((By.ID, 'downPage')))[-1].find_elements(by=By.TAG_NAME,value="a")
                for en in  elementnext :
                    if "下一页" in en.get_attribute("outerHTML"):
                        driver.execute_script("arguments[0].click();", en)
                        break
                # 处理下一页
                for tt in range(30):
                    elementloading = WebDriverWait(driver, 10, 0.5).until(EC.presence_of_all_elements_located((By.ID, 'loading')))[0].get_attribute("outerHTML")
                    if  "aria-hidden=\"true\""  in elementloading:
                        break
                    else:
                        print(tt)
                        time.sleep(1)
                print(p, page_all)
            except:
                product_all0 = product_all
                input("备份成功，刷新页面并继续")

        product_all.columns = ["人员", "操作时间", "操作位置", "操作内容"]
        product_all.drop_duplicates(inplace=True)
        product_all.to_excel(r"\\auteng20\5.共享文件\报表输出\订单操作日志" + datetime.datetime.today().strftime("%Y%m%d") + ".xlsx",index=False)  # +datetime.datetime.today().strftime("%Y%m%d")

    driver.quit()

if __name__ == '__main__':
    # 初始化Chrome浏览器
    chrome_options = Options()
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(executable_path='chromedriver.exe', options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    try:
        dxm_caozuorizhi(driver)
    except Exception as e:
        print(f"执行过程中出现错误: {e}")
        driver.quit()