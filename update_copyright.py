#!/usr/bin/env python3
"""
批量更新版权信息脚本
- 修改2025年10月后longzhenren提交的文件
- 删除Claude/GPT标志
- 添加longzhenren(amurzzb@gmail.com)版权信息
"""

import os
import re
import subprocess

# 获取2025年10月后longzhenren修改的文件
def get_modified_files():
    cmd = 'git log --since="2025-10-01" --author="longzhenren" --name-only --pretty=format: | sort -u'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd='/home/user/PegasusSimulator-5.1')
    files = [f.strip() for f in result.stdout.split('\n') if f.strip() and f.strip().endswith(('.py', '.sh', '.cpp', '.h', '.hpp', '.c', '.js', '.ts', '.java'))]
    return files

# 处理单个文件
def process_file(filepath):
    full_path = os.path.join('/home/user/PegasusSimulator-5.1', filepath)

    if not os.path.exists(full_path):
        print(f"跳过不存在的文件: {filepath}")
        return False

    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"读取文件失败 {filepath}: {e}")
        return False

    original_content = content
    modified = False

    # 删除包含Claude/GPT/Anthropic/OpenAI的版权/作者/生成信息
    patterns_to_remove = [
        r'#.*[Cc]opyright.*[Cc]laude.*\n?',
        r'#.*[Aa]uthor.*[Cc]laude.*\n?',
        r'#.*[Gg]enerated.*[Cc]laude.*\n?',
        r'#.*[Cc]opyright.*GPT.*\n?',
        r'#.*[Aa]uthor.*GPT.*\n?',
        r'#.*[Gg]enerated.*GPT.*\n?',
        r'#.*[Cc]opyright.*Anthropic.*\n?',
        r'#.*[Aa]uthor.*Anthropic.*\n?',
        r'#.*[Cc]opyright.*OpenAI.*\n?',
        r'#.*[Aa]uthor.*OpenAI.*\n?',
        r'//.*[Cc]opyright.*[Cc]laude.*\n?',
        r'//.*[Aa]uthor.*[Cc]laude.*\n?',
        r'//.*[Gg]enerated.*[Cc]laude.*\n?',
        r'//.*[Cc]opyright.*GPT.*\n?',
        r'//.*[Aa]uthor.*GPT.*\n?',
        r'//.*[Gg]enerated.*GPT.*\n?',
        r'/\*.*[Cc]laude.*\*/\n?',
        r'/\*.*GPT.*\*/\n?',
    ]

    for pattern in patterns_to_remove:
        new_content = re.sub(pattern, '', content, flags=re.MULTILINE | re.IGNORECASE)
        if new_content != content:
            content = new_content
            modified = True

    # 检查是否已有longzhenren的版权信息
    has_longzhenren_copyright = bool(re.search(r'[Cc]opyright.*longzhenren', content, re.IGNORECASE))

    # 如果没有longzhenren的版权信息，添加到文件开头
    if not has_longzhenren_copyright:
        # 确定注释符号
        if filepath.endswith('.py'):
            copyright_line = "# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)\n"
        elif filepath.endswith('.sh'):
            copyright_line = "# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)\n"
        elif filepath.endswith(('.cpp', '.h', '.hpp', '.c', '.js', '.ts', '.java')):
            copyright_line = "// Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)\n"
        else:
            copyright_line = "# Copyright (c) 2025-2026 longzhenren (amurzzb@gmail.com)\n"

        # 如果文件以shebang开始，在shebang后添加
        if content.startswith('#!'):
            lines = content.split('\n', 1)
            if len(lines) > 1:
                content = lines[0] + '\n' + copyright_line + lines[1]
            else:
                content = lines[0] + '\n' + copyright_line
        else:
            content = copyright_line + content

        modified = True

    # 如果有修改，写回文件
    if modified and content != original_content:
        try:
            with open(full_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✓ 已更新: {filepath}")
            return True
        except Exception as e:
            print(f"✗ 写入失败 {filepath}: {e}")
            return False
    else:
        print(f"- 无需修改: {filepath}")
        return False

# 主函数
def main():
    print("正在获取需要修改的文件列表...")
    files = get_modified_files()

    print(f"\n找到 {len(files)} 个文件需要处理\n")

    updated_count = 0
    for filepath in files:
        if process_file(filepath):
            updated_count += 1

    print(f"\n" + "="*60)
    print(f"处理完成！")
    print(f"总文件数: {len(files)}")
    print(f"已更新: {updated_count}")
    print(f"未修改: {len(files) - updated_count}")
    print("="*60)

if __name__ == '__main__':
    main()
