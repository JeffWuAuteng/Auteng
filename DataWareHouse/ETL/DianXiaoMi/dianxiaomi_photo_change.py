"""
    Company:深圳遨腾云创科技有限公司
\n  Name：YYZ
\n  Title: 更换店小秘的图片
\n  Description: 更换店小秘SKU的图片，完成登录，单独/批量添加图片，设置主图的功能
"""
import logging
from datetime import datetime
from pathlib import Path
from playwright.sync_api import Page, sync_playwright
import time
import random
from random import uniform

class SKJLogger:
    def __init__(
        self,
        name: str = "自动化",
        log_dir: str = "./logs",
        level: int = logging.INFO
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # 创建日志目录
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 设置文件名格式
        log_file = self.log_dir / f"{name}-{datetime.now().strftime('%Y%m%d')}.log"

        # 通用格式
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )

        # 文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # 控制台处理器（可选）
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        # 返回类属性，该属性是logging类的实例对象
        return self.logger

 # 全局日志实例
logger = SKJLogger('店小秘更换图片', log_dir='D:\YYZ\logs').get_logger()

class DianXiaoMiPhotoManager:
    """
    店小秘图片管理器类
    用于自动化操作店小秘平台的SKU图片替换功能
    """

    def __init__(self, username="DataTEAM04", password="Yinyanzhi2020", headless=False):
        """
        初始化店小秘图片管理器

        Args:
            username: 登录用户名
            password: 登录密码
            headless: 是否无头模式
        """
        self.username = username
        self.password = password
        self.headless = headless
        self.browser = None
        self.context = None
        self.cookies = None
        self.playwright = None
        self.log_path =''

        # 浏览器启动参数
        self.browser_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security',
            '--disable-site-isolation-trials'
        ]

    def __enter__(self):
        """上下文管理器入口"""
        self.playwright = sync_playwright().start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()



    def start_browser(self):
        """启动浏览器"""
        if not self.playwright:
            self.playwright = sync_playwright().start()

        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=self.browser_args
        )
        return self.browser

    def stop_browser(self):
        """停止浏览器"""
        if self.browser:
            self.browser.close()
            self.browser = None
        if self.playwright:
            self.playwright.stop()
            self.playwright = None

    def login(self, url='https://www.dianxiaomi.com/'):
        """
        登录店小秘平台

        Args:
            url: 登录页面URL

        Returns:
            bool: 登录是否成功
        """
        try:
            if not self.browser:
                self.start_browser()

            # 创建上下文
            self.context = self.browser.new_context()
            page = self.context.new_page()

            # 导航到网页
            page.goto(
                url=url,
                timeout=60000,
                wait_until="domcontentloaded"
            )

            # 添加鼠标移动模拟
            page.mouse.move(
                x=uniform(100, 500),
                y=uniform(100, 500),
                steps=random.randint(5, 15)
            )
            time.sleep(uniform(1, 3))

            # 点击登录按钮
            page.click("#loginBtn", timeout=10000)
            logger.info("点击了登录按钮")



            # 等待登录弹窗出现
            page.wait_for_selector("input[name='account']", timeout=15000)
            logger.info("登录弹窗已出现")

            # 输入用户名和密码
            page.fill("input[name='account']", self.username)
            logger.info("输入了用户名")

            page.fill("input[name='password']", self.password)
            logger.info("输入了密码")

            # 输入验证码
            verify_code = input("请输入验证码:")
            page.fill("input[name='verifyCode']", verify_code)
            logger.info("输入了验证码")

            # 等待一下再点击登录按钮
            time.sleep(2)

            page.click("button:has-text('登录')", timeout=5000)
            logger.info("点击了登录按钮")

            # 等待登录完成
            page.wait_for_timeout(5000)

            # 保存cookies
            self.cookies = self.context.cookies()
            logger.info("保存登录状态")

            page.close()
            return True

        except Exception as e:
            logger.error("登录过程中出现错误: %s", e)
            if page:
                page.screenshot(path="login_error.png")
            return False

    def _close_popup(self, page):
        """
        关闭页面上的弹窗

        Args:
            page: 页面对象

        Returns:
            bool: 是否成功关闭弹窗
        """
        try:
            if page.is_visible("button:has-text('关闭')", timeout=3000):
                page.click("button:has-text('关闭')", timeout=5000)
                logger.info("成功关闭弹窗")
                return True
        except Exception as e:
            logger.error(f"关闭弹窗过程中出现错误: {e}")
            return False

    def search_sku(self, sku, url='https://www.dianxiaomi.com/warehouseProduct/index.htm'):
        """
        搜索SKU并获取商品详情页URL

        Args:
            sku: SKU编号
            url: 库存页面URL

        Returns:
            str: 商品详情页URL，失败返回None
        """
        try:
            if not self.context and self.cookies:
                # 创建新上下文并设置cookies
                self.context = self.browser.new_context()
                self.context.add_cookies(self.cookies)

            # 创建页面
            page = self.context.new_page()

            # 导航到库存页面
            page.goto(
                url=url,
                timeout=60000,
                wait_until="domcontentloaded"
            )

            logger.info(f"打开店铺库存页面: {url}")

            # 检查登录状态
            current_url = page.url
            if "login" in current_url or "signin" in current_url:
                logger.info("检测到被重定向到登录页面，登录状态可能已失效")
                page.screenshot(path="redirected_to_login.png")
                return None

            # 添加鼠标移动模拟
            page.mouse.move(
                x=uniform(100, 500),
                y=uniform(100, 500),
                steps=random.randint(5, 15)
            )
            time.sleep(uniform(1, 3))

            logger.info(f"页面标题: {page.title()}")

            # 等待页面加载完成
            time.sleep(5)

            # 尝试关闭弹窗
            popup_closed = self._close_popup(page)
            if popup_closed:
                logger.info("弹窗已成功关闭")

            # 输入SKU并搜索
            page.fill("input[class='form-control batchSearchType p-left30']", sku)
            logger.info(f"输入了SKU: {sku}")

            time.sleep(1)
            page.click("#btnSearch", timeout=10000)

            # 等待搜索结果加载
            time.sleep(5)

            # 获取商品链接
            try:
                href = page.get_attribute("a[class='inline-block limingcentUrlpic']", "href", timeout=10000)
                sku_id = href.split('=')[-1]
                sku_url = f"https://www.dianxiaomi.com/dxmCommodityProduct/openEditModal.htm?id={sku_id}&type=0&editOrCopy=0"
                logger.info(f"获取到的商品链接: {sku_url}")

                page.close()
                return sku_url if href else None

            except Exception as e:
                # print(f"获取商品链接失败: {e}")
                logger.error(f"获取商品链接失败: {e}")
                page.screenshot(path="link_debug.png")
                return None

        except Exception as e:
            logger.error("搜索SKU过程中出现错误: %s", e)
            return None

    def _set_main_image(self, page, photo_url):
        """
        设置图片为主图

        Args:
            page: 页面对象
            photo_url: 图片URL

        Returns:
            bool: 设置是否成功
        """
        logger.info("开始设置主图")
        time.sleep(5)

        # 方法1：定位最后一个图片标签并操作
        try:
            # 找到所有图片容器，选择最后一个
            img_containers_selector = "li.img-add.imgAdd[data-type='back']"
            page.wait_for_selector(img_containers_selector, timeout=10000)

            all_img_containers = page.query_selector_all(img_containers_selector)
            logger.info(f"找到 {len(all_img_containers)} 个图片容器")

            if len(all_img_containers) > 0:
                last_img_container = all_img_containers[-1]
                logger.info("定位到最后一个图片容器")

                # 悬停显示控制按钮
                last_img_container.hover()
                logger.info("鼠标悬停在图片容器上")
                time.sleep(2)

                # 找到并悬停设置主图按钮
                set_main_btn = last_img_container.query_selector("span.img-tool.tooltipHover.setMainPicBtn")
                if set_main_btn:
                    logger.info("找到设置主图按钮")
                    set_main_btn.hover()
                    logger.info("鼠标悬停在设置主图按钮上")
                    time.sleep(2)

                    # 检查工具提示
                    tooltip_selector = "div.tooltip.fade.bottom.in[role='tooltip']"
                    try:
                        page.wait_for_selector(tooltip_selector, timeout=3000)
                        logger.info("工具提示已出现")
                    except:
                        logger.info("工具提示未出现")

                    # 点击设置主图按钮
                    set_main_btn.click()
                    logger.info("成功点击了设置为主图按钮")
                    return True
                else:
                    logger.error("未找到设置主图按钮")
                    raise Exception("未找到设置主图按钮")
            else:
                logger.error("未找到任何图片容器")
                raise Exception("未找到任何图片容器")

        except Exception as e1:
            logger.error(f"方法1失败: {e1}")

            # 方法2：通过图片URL定位
            try:
                img_selector = f"img[src='{photo_url}']"
                page.wait_for_selector(img_selector, timeout=10000)

                all_matching_imgs = page.query_selector_all(img_selector)
                logger.info(f"找到 {len(all_matching_imgs)} 个匹配的图片")

                if len(all_matching_imgs) > 0:
                    last_matching_img = all_matching_imgs[-1]
                    logger.info("定位到最后一个匹配的图片")

                    img_container = last_matching_img.evaluate_handle("(img) => img.closest('li.img-add.imgAdd')")
                    img_container.hover()
                    logger.info("鼠标悬停在图片上")
                    time.sleep(2)

                    set_main_btn = img_container.query_selector("span.setMainPicBtn")
                    if set_main_btn:
                        set_main_btn.hover()
                        logger.info("鼠标悬停在设置主图按钮上")
                        time.sleep(2)
                        set_main_btn.click()
                        logger.info("方法2：成功点击了设置为主图按钮")
                        return True
                    else:
                        logger.error("未找到设置主图按钮")
                        raise Exception("未找到设置主图按钮")
                else:
                    logger.error("未找到匹配的图片")
                    raise Exception("未找到匹配的图片")

            except Exception as e2:
                logger.error(f"方法2失败: {e2}")
                return False

    def replace_photo(self, sku, photo_url):
        """
        替换SKU的图片

        Args:
            sku: SKU编号
            photo_url: 要替换的图片URL

        Returns:
            bool: 操作是否成功
        """
        try:
            # 搜索SKU获取详情页URL
            sku_url = self.search_sku(sku)
            if not sku_url:
                logger.error("获取SKU详情页URL失败")
                return False

            # 创建新页面
            page = self.context.new_page()

            # 导航到SKU详情页
            page.goto(
                url=sku_url,
                timeout=60000,
                wait_until="domcontentloaded"
            )

            logger.info(f"打开SKU详情页面: {sku_url}")
            logger.info(f"准备添加图片URL: {photo_url}")

            time.sleep(3)

            # 第一步：点击"选择图片"按钮
            logger.info("第一步：点击'选择图片'按钮")
            page.click("button:has-text('选择图片')", timeout=5000)
            time.sleep(2)

            # 第二步：点击"网络图片"选项
            logger.info("第二步：点击'网络图片'选项")
            page.evaluate("webUrlModal()")
            logger.info("成功调用webUrlModal()函数")
            time.sleep(3)

            # 第三步：填写图片URL
            logger.info("第三步：在URL输入框中填写图片URL")
            input_selector = "textarea.form-component.m-h200#webImgUrl"
            page.fill(input_selector, photo_url, timeout=5000)
            logger.info(f"成功填写URL: {photo_url}")
            time.sleep(1)

            # 第四步：点击"添加"按钮
            logger.info("第四步：点击'添加'按钮")
            page.evaluate("addWebUrl()")
            logger.info("JavaScript调用addWebUrl()成功")
            time.sleep(3)

            # 第五步：关闭弹窗
            logger.info("第五步：点击'关闭'按钮")
            page.keyboard.press("Escape")
            time.sleep(2)
            logger.info("URL添加操作完成")

            # 点击保存按钮
            page.click("button[class='button btn-orange m-left10']")
            logger.info("点击了保存按钮")

            # 第六步：设置为主图
            main_set_success = self._set_main_image(page, photo_url)
            if not main_set_success:
                logger.error("设置主图失败")

            # 等待操作完成
            time.sleep(3)

            # 确认主图设置成功
            try:
                main_img_indicator = page.query_selector("span.f-red:has-text('主图')")
                if main_img_indicator:
                    logger.info("主图设置成功！")
                else:
                    logger.warning("主图设置可能未成功，请手动确认")
            except Exception as e:
                logger.error(f"检查主图状态失败: {e}")

            # 等待用户确认
            # input("按回车键关闭页面...")
            page.close()
            return True

        except Exception as e:
            logger.error(f"替换图片过程中出现错误: {e}")
            return False

    def batch_replace_photos(self, sku_photo_dict):
        """
        批量替换多个SKU的图片

        Args:
            sku_photo_dict: {sku: photo_url} 字典

        Returns:
            dict: 每个SKU的操作结果
        """
        results = {}
        for sku, photo_url in sku_photo_dict.items():
            logger.info(f"\n开始处理SKU: {sku}")
            results[sku] = self.replace_photo(sku, photo_url)

        return results


def main():
    """
    单个和批量示范操作
    :return:
    """
    # 使用上下文管理器自动管理资源
    with DianXiaoMiPhotoManager() as manager:
        # 登录
        if manager.login():
            logger.info("登录成功！")

            # # 单个SKU操作示例
            # sku = "MLTYH-PH_QCYP-CD_50604_017"
            # photo_url = "https://cf.shopee.com.mx/file/sg-11134201-7rauu-mal5b8wrzi0ja0"
            #
            # success = manager.replace_photo(sku, photo_url)
            # if success:
            #     logger.info(f"SKU {sku} 图片替换成功！")
            # else:
            #     logger.error(f"SKU {sku} 图片替换失败！")

            # 批量操作示例
            sku_photos = {
                "MLTYH-PH_QCYP-CD_50604_017-1PC": "https://cf.shopee.com.mx/file/sg-11134201-7rauu-mal5b8wrzi0ja0",
                "MLTYH-TH_QCYP-HB_41209_08-silver": "https://cf.shopee.com.mx/file/sg-11134201-7rffb-m45yfk3ljjp011",
                "MLTYH-TH_QCYP-HB_41209_08-black": "https://cf.shopee.com.mx/file/sg-11134201-7rfh2-m45yfkwgburn8a"
            }
            results = manager.batch_replace_photos(sku_photos)
            logger.info(f"批量操作结果: {results}")
            print("批量操作结果:", results)
        else:
            print("登录失败！")


if __name__ == '__main__':
    main()