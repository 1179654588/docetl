import os
import subprocess
import sys

def main():
    # 设置环境变量
    env = os.environ.copy()
    env['ARK_API_KEY'] = 'afda58f0-0e4e-423d-904e-de22e0c6854b'
    env['OPENAI_API_BASE'] = 'https://ark.cn-beijing.volces.com/api/v3'
    env['OPENAI_API_KEY'] = 'afda58f0-0e4e-423d-904e-de22e0c6854b'
    env['MODEL_NAME'] = 'doubao-seed-1-8-251228'
    env['PYTHONUTF8'] = '1'
    
    # 构建 docetl 命令 - 直接调用可执行文件
    docetl_exe = r'd:\code\doc-etl\docetl\.venv\Scripts\docetl.exe'
    cmd = [docetl_exe, 'run', 'pdf_extract.yaml']
    
    print(f"执行命令: {' '.join(cmd)}")
    print("=" * 60)
    
    # 执行命令
    try:
        result = subprocess.run(
            cmd, 
            env=env, 
            check=True, 
            capture_output=False,  # 直接显示输出
            text=True
        )
        print("=" * 60)
        print("命令执行成功!")
        return result.returncode
    except subprocess.CalledProcessError as e:
        print("=" * 60)
        print(f"命令执行失败，退出码: {e.returncode}")
        return e.returncode
    except FileNotFoundError:
        print(f"错误: 找不到 docetl 可执行文件: {docetl_exe}")
        print("请检查虚拟环境路径是否正确")
        return 1

if __name__ == '__main__':
    sys.exit(main())
