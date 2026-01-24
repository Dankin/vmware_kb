#!/usr/bin/env python3
"""
测试脚本：分析KB页面的附件下载逻辑
"""
import requests
from bs4 import BeautifulSoup
import re

def analyze_attachments(kb_number):
    url = f"https://knowledge.broadcom.com/external/article/{kb_number}"
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    print(f"正在访问: {url}")
    response = session.get(url, timeout=15)
    
    if response.status_code != 200:
        print(f"错误: HTTP {response.status_code}")
        return
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # 查找所有script标签
    scripts = soup.find_all('script')
    print(f"\n找到 {len(scripts)} 个script标签\n")
    
    # 查找附件相关的JavaScript代码
    print("=" * 80)
    print("查找附件相关的JavaScript代码:")
    print("=" * 80)
    
    for i, script in enumerate(scripts):
        if script.string:
            content = script.string
            # 查找包含 attachment 或 download 的代码
            if 'attachment' in content.lower() or 'download' in content.lower():
                print(f"\n--- Script {i+1} (包含attachment/download) ---")
                # 只显示相关部分
                lines = content.split('\n')
                relevant_lines = []
                for j, line in enumerate(lines):
                    if 'attachment' in line.lower() or 'download' in line.lower() or 'file' in line.lower():
                        # 显示前后各2行上下文
                        start = max(0, j-2)
                        end = min(len(lines), j+3)
                        if start not in [l[0] for l in relevant_lines]:
                            relevant_lines.append((start, end))
                
                # 去重并排序
                relevant_lines = sorted(set(relevant_lines))
                for start, end in relevant_lines[:5]:  # 最多显示5个片段
                    print(f"\n行 {start+1}-{end}:")
                    for k in range(start, end):
                        if k < len(lines):
                            print(f"  {k+1:4d}: {lines[k]}")
    
    # 查找API配置
    print("\n" + "=" * 80)
    print("查找API配置:")
    print("=" * 80)
    
    api_domain = None
    kb_download_domain = None
    
    for script in scripts:
        if script.string:
            if 'var apiDomain' in script.string:
                api_match = re.search(r"var apiDomain\s*=\s*['\"]([^'\"]+)['\"]", script.string)
                if api_match:
                    api_domain = api_match.group(1)
                    print(f"apiDomain: {api_domain}")
            
            if 'var kbDownloadDomain' in script.string:
                domain_match = re.search(r"var kbDownloadDomain\s*=\s*['\"]([^'\"]+)['\"]", script.string)
                if domain_match:
                    kb_download_domain = domain_match.group(1)
                    print(f"kbDownloadDomain: {kb_download_domain}")
    
    # 查找attachment-card
    print("\n" + "=" * 80)
    print("查找attachment-card:")
    print("=" * 80)
    
    attachment_cards = soup.find_all('div', class_=lambda x: x and 'attachment-card' in str(x).lower())
    print(f"找到 {len(attachment_cards)} 个attachment-card\n")
    
    for i, card in enumerate(attachment_cards):
        print(f"--- Card {i+1} ---")
        print(f"HTML: {card}")
        print(f"所有属性: {card.attrs}")
        
        # 查找文件名
        name_elem = card.find('span', class_=lambda x: x and 'attachment-name' in str(x).lower())
        if name_elem:
            filename = name_elem.get_text(strip=True)
            print(f"文件名: {filename}")
        
        # 查找下载链接
        download_link = card.find('a', class_=lambda x: x and 'download' in str(x).lower())
        if download_link:
            print(f"下载链接属性: {download_link.attrs}")
        
        print()

if __name__ == '__main__':
    analyze_attachments(385107)

