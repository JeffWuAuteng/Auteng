#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
shopee订单申诉脚本执行器
按顺序执行 info_update.py, img_dl2.py, img_save_excel.py, files_fix.py
支持单独执行模式和顺序执行模式
"""

import os
import sys
import subprocess
import time
from pathlib import Path

class ScriptRunner:
    def __init__(self):
        # 处理调试器环境中 __file__ 未定义的情况
        try:
            self.script_dir = Path(__file__).parent
        except NameError:
            # 在调试器中运行时，使用指定的项目目录
            self.script_dir = Path(r"E:\PythonProject2")
            print("⚠️  检测到调试器环境，使用指定的项目目录作为脚本目录")
            print(f"   脚本目录：{self.script_dir}")
        self.scripts = {
            '1': {
                'name': 'info_update.py',
                'description': '信息更新脚本 - 处理订单数据和产品信息匹配（支持新路径结构：主运营/SBS账单编号）',
                'next_step': '请确认生成的shopee申诉处理-匹配(ok).xlsx文件是否正确，然后准备申诉信息材料文件'
            },
            '2': {
                'name': 'img_dl2.py', 
                'description': '图片下载脚本 - 自动下载商品图片和修改商品信息（支持新路径结构：主运营/SBS账单编号）',
                'next_step': '请确认图片下载完成，商品信息修改正确，然后准备Excel文件中的图片提取'
            },
            '3': {
                'name': 'img_save_excel.py',
                'description': 'Excel图片提取脚本 - 从Excel文件中提取嵌入的图片（支持新路径结构：主运营/SBS账单编号）',
                'next_step': '请确认图片提取完成，然后准备进行文件修复和申诉材料生成'
            },
            '4': {
                'name': 'files_fix.py',
                'description': '文件修复脚本 - 处理申诉材料和费用计算（支持新路径结构：主运营/SBS账单编号）',
                'next_step': '脚本执行完成！请检查生成的申诉材料文件是否保存到正确的路径结构中'
            }
        }
    
    def display_menu(self):
        """显示主菜单"""
        print("\n" + "="*60)
        print("           shopee订单申诉脚本执行器")
        print("="*60)
        print("请选择执行模式：")
        print("0. 按顺序执行所有脚本")
        print("-" * 40)
        for key, script in self.scripts.items():
            print(f"{key}. 单独执行 {script['name']} - {script['description']}")
        print("-" * 40)
        print("q. 退出程序")
        print("="*60)
    
    def check_script_exists(self, script_name):
        """检查脚本文件是否存在"""
        script_path = self.script_dir / script_name
        if not script_path.exists():
            print(f"❌ 错误：脚本文件 {script_name} 不存在！")
            print(f"   请确保文件位于：{script_path}")
            return False
        return True
    
    def run_script(self, script_name):
        """执行单个脚本"""
        script_path = self.script_dir / script_name
        
        if not self.check_script_exists(script_name):
            return False
        
        print(f"\n🚀 开始执行脚本：{script_name}")
        print("-" * 50)
        
        try:
            # 使用当前Python解释器执行脚本
            result = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(self.script_dir),
                capture_output=False,  # 允许实时输出
                text=True
            )
            
            if result.returncode == 0:
                print(f"\n✅ 脚本 {script_name} 执行成功！")
                return True
            else:
                print(f"\n❌ 脚本 {script_name} 执行失败！退出代码：{result.returncode}")
                return False
                
        except KeyboardInterrupt:
            print(f"\n⚠️  用户中断了脚本 {script_name} 的执行")
            return False
        except Exception as e:
            print(f"\n❌ 执行脚本 {script_name} 时发生错误：{str(e)}")
            return False
    
    def confirm_and_next_step(self, script_key):
        """确认当前步骤并提示下一步"""
        script_info = self.scripts[script_key]
        print("\n" + "="*60)
        print(f"📋 {script_info['name']} 执行完成")
        print("="*60)
        print(f"📌 下一步准备工作：")
        print(f"   {script_info['next_step']}")
        print("="*60)
        while True:
            confirm = input("\n请确认当前步骤是否成功完成？(y/n/q): ").lower().strip()
            if confirm == 'y':
                print("✅ 确认成功，继续下一步...")
                return True
            elif confirm == 'n':
                print("❌ 当前步骤未成功，请检查并重新执行")
                return False
            elif confirm == 'q':
                print("🚪 用户选择退出")
                return None
            else:
                print("⚠️  请输入 y(是)、n(否) 或 q(退出)")
    
    def run_all_scripts(self):
        """按顺序执行所有脚本"""
        print("\n🎯 开始按顺序执行所有脚本...")
        print("="*60)
        
        for script_key in sorted(self.scripts.keys()):
            script_info = self.scripts[script_key]
            script_name = script_info['name']
            
            print(f"\n📍 当前步骤 {script_key}/4: {script_info['description']}")
            
            # 执行脚本
            success = self.run_script(script_name)
            
            if not success:
                print(f"\n❌ 脚本 {script_name} 执行失败，停止后续执行")
                break
            
            # 如果不是最后一个脚本，需要用户确认
            if script_key != '4':
                confirm_result = self.confirm_and_next_step(script_key)
                if confirm_result is None:  # 用户选择退出
                    break
                elif not confirm_result:  # 用户确认失败
                    retry = input("\n是否重新执行当前脚本？(y/n): ").lower().strip()
                    if retry == 'y':
                        # 重新执行当前脚本
                        success = self.run_script(script_name)
                        if success:
                            confirm_result = self.confirm_and_next_step(script_key)
                            if not confirm_result:
                                break
                        else:
                            break
                    else:
                        break
            else:
                # 最后一个脚本，显示完成信息
                self.confirm_and_next_step(script_key)
        
        print("\n🎉 所有脚本执行流程结束！")
    
    def run_single_script(self, script_key):
        """执行单个脚本"""
        if script_key not in self.scripts:
            print("❌ 无效的脚本选择！")
            return
        
        script_info = self.scripts[script_key]
        script_name = script_info['name']
        
        print(f"\n📍 准备执行：{script_info['description']}")
        
        success = self.run_script(script_name)
        
        if success:
            self.confirm_and_next_step(script_key)
        else:
            print(f"\n❌ 脚本 {script_name} 执行失败")
    
    def run(self):
        """主运行方法"""
        print("\n🎯 欢迎使用shopee订单申诉脚本执行器！")
        
        while True:
            self.display_menu()
            
            choice = input("\n请输入您的选择: ").strip()
            
            if choice == 'q':
                print("\n👋 感谢使用，再见！")
                break
            elif choice == '0':
                self.run_all_scripts()
            elif choice in self.scripts:
                self.run_single_script(choice)
            else:
                print("\n⚠️  无效选择，请重新输入！")
            
            # 询问是否继续
            if choice != 'q':
                continue_choice = input("\n是否继续使用？(y/n): ").lower().strip()
                if continue_choice != 'y':
                    print("\n👋 感谢使用，再见！")
                    break

def main():
    """主函数"""
    try:
        runner = ScriptRunner()
        runner.run()
    except KeyboardInterrupt:
        print("\n\n⚠️  程序被用户中断")
    except Exception as e:
        print(f"\n❌ 程序运行时发生错误：{str(e)}")
    finally:
        print("\n程序结束")

if __name__ == "__main__":
    main()