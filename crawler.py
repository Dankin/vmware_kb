"""
VMware KB多线程爬取脚本（优化版：带延迟和重试机制）
"""
import argparse
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from models import get_db_session, Article, Product, init_db
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from tqdm import tqdm
import sys
from collections import defaultdict
import queue
import time
import random
import re
import unicodedata
import os
import hashlib

# 线程安全的计数器
success_count = 0
fail_count = 0
skip_count = 0
lock = threading.Lock()

# KB基础URL
BASE_URL = "https://knowledge.broadcom.com/external/article/"

# 全局session池（每个线程一个）
session_pool = threading.local()

# 请求速率控制（每个线程的最后一个请求时间）
last_request_time = threading.local()
request_lock = threading.Lock()

# 全局请求延迟配置（秒）
REQUEST_DELAY_MIN = 0.1  # 最小延迟100ms
REQUEST_DELAY_MAX = 0.3  # 最大延迟300ms


def get_session():
    """获取线程本地的session"""
    if not hasattr(session_pool, 'session'):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        # 设置连接池大小和超时
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=0  # 手动控制重试
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session_pool.session = session
        # 初始化线程的最后一个请求时间
        last_request_time.last_time = 0
    return session_pool.session


def clean_html_content(html_content):
    """清理HTML内容中的特殊字符和不可见字符"""
    if not html_content:
        return ""
    
    # 移除BOM字符和零宽字符
    # 移除常见的不可见Unicode字符：零宽空格、零宽非断空格、零宽断空格、左到右标记、右到左标记等
    invisible_chars = [
        '\u200b',  # 零宽空格
        '\u200c',  # 零宽非断空格
        '\u200d',  # 零宽断空格
        '\ufeff',  # BOM字符
        '\u200e',  # 左到右标记
        '\u200f',  # 右到左标记
        '\u202a',  # 左到右嵌入
        '\u202b',  # 右到左嵌入
        '\u202c',  # 弹出方向格式
        '\u202d',  # 左到右覆盖
        '\u202e',  # 右到左覆盖
        '\u2060',  # 词连接符
        '\u2061',  # 函数应用
        '\u2062',  # 不可见乘号
        '\u2063',  # 不可见分隔符
        '\u2064',  # 不可见加号
        '\u00a0',  # 不间断空格（但保留，因为可能是HTML实体）
    ]
    
    # 移除不可见字符（但保留正常的空格和换行）
    for char in invisible_chars:
        html_content = html_content.replace(char, '')
    
    # 移除常见的编码错误字符（如Â，通常是UTF-8编码错误导致的）
    # Â 通常是 UTF-8 编码错误，可能是原本的不间断空格（\u00a0）被错误解析
    # 移除单独的Â字符（前后都是空格或标点的情况）
    html_content = re.sub(r'\s+Â\s+', ' ', html_content)  # 空格+Â+空格 -> 空格
    html_content = re.sub(r'Â\s+', ' ', html_content)  # Â+空格 -> 空格
    html_content = re.sub(r'\s+Â', ' ', html_content)  # 空格+Â -> 空格
    html_content = re.sub(r'^Â\s*', '', html_content, flags=re.MULTILINE)  # 行首的Â
    html_content = re.sub(r'\s*Â$', '', html_content, flags=re.MULTILINE)  # 行尾的Â
    
    # 移除其他常见的编码错误字符组合
    html_content = html_content.replace('Â', '')  # 直接移除所有Â字符
    
    # 规范化Unicode字符（将组合字符转换为预组合形式）
    # 但注意：这可能会改变某些特殊字符，所以谨慎使用
    # html_content = unicodedata.normalize('NFKC', html_content)
    
    # 清理多余的空白字符（但保留HTML结构）
    # 不要过度清理，因为HTML中可能有重要的空格
    
    return html_content


def download_and_localize_images(content, kb_number, session=None):
    """下载KB内容中的图片并本地化"""
    if not content:
        return content, {}
    
    # 提取图片URL
    img_pattern = r'<img[^>]+src=["\']([^"\']+)["\']'
    images = re.findall(img_pattern, content, re.IGNORECASE)
    
    # 过滤外部图片
    external_images = [img for img in images if img.startswith('http://') or img.startswith('https://')]
    if not external_images:
        return content, {}
    
    # 创建图片目录
    images_dir = 'static/images/kb'
    kb_image_dir = os.path.join(images_dir, str(kb_number))
    os.makedirs(kb_image_dir, exist_ok=True)
    
    image_mapping = {}
    
    for img_url in external_images:
        # 生成文件名
        url_hash = hashlib.md5(img_url.encode()).hexdigest()[:8]
        filename = f"{kb_number}_{url_hash}.jpg"
        local_path = f"/static/images/kb/{kb_number}/{filename}"
        filepath = os.path.join(kb_image_dir, filename)
        
        # 如果文件已存在，跳过下载
        if os.path.exists(filepath):
            image_mapping[img_url] = local_path
            continue
        
        # 下载图片
        try:
            if session is None:
                img_session = requests.Session()
                img_session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
            else:
                img_session = session
            
            response = img_session.get(img_url, timeout=5, stream=True)  # 减少超时时间，避免卡死
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    # 添加超时控制，避免长时间等待
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # 验证文件大小
                if os.path.getsize(filepath) > 100:  # 大于100字节才认为成功
                    image_mapping[img_url] = local_path
        except Exception:
            # 下载失败，保留原URL
            pass
    
    # 更新内容中的图片URL
    if image_mapping:
        updated_content = content
        for old_url, local_path in image_mapping.items():
            updated_content = updated_content.replace(old_url, local_path)
        return updated_content, image_mapping
    
    return content, {}


def download_and_localize_attachments(content, kb_number, session=None, soup=None):
    """
    下载KB内容中的附件并本地化
    
    Args:
        content: HTML内容
        kb_number: KB编号
        session: requests session对象
        soup: BeautifulSoup对象（用于解析附件链接）
    
    Returns:
        (updated_content, attachment_mapping): 更新后的内容和附件映射
    """
    if not content:
        return content, {}
    
    # 创建附件目录
    attachments_dir = 'static/attachments/kb'
    kb_attachment_dir = os.path.join(attachments_dir, str(kb_number))
    os.makedirs(kb_attachment_dir, exist_ok=True)
    
    attachment_mapping = {}
    attachment_links = []
    file_extensions = ['pdf', 'zip', 'doc', 'docx', 'xls', 'xlsx', 'txt', 'rar', '7z', 'tar', 'gz', 'exe', 'msi', 'tar.gz']
    
    # 只处理attachment区域内的附件（不处理内容中所有的文件链接）
    # 方法1: 如果提供了soup对象，查找附件区域
    if soup:
        # 先查找API配置（只需要查找一次）
        if soup is None:
            if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                print(f"[KB {kb_number}] 警告: soup对象为None，无法查找API配置")
            return content, {}
        
        scripts = soup.find_all('script')
        api_domain = None
        kb_download_domain = None
        
        debug_mode = os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true'
        
        if debug_mode:
            print(f"[KB {kb_number}] 查找API配置: 找到 {len(scripts)} 个script标签")
        
        for i, script in enumerate(scripts):
            script_content = script.string if script.string else ''
            
            if debug_mode and i < 10:  # 只显示前10个script的信息
                if len(script_content) > 0:
                    print(f"[KB {kb_number}] Script {i+1}: 长度={len(script_content)}, 包含apiDomain={'apiDomain' in script_content}, 包含kbDownloadDomain={'kbDownloadDomain' in script_content}")
            
            if 'apiDomain' in script_content or 'kbDownloadDomain' in script_content:
                if debug_mode:
                    print(f"[KB {kb_number}] Script {i+1} 包含API配置相关代码 (长度: {len(script_content)})")
                    # 显示包含 apiDomain 的行
                    if 'apiDomain' in script_content:
                        lines = script_content.split('\n')
                        for j, line in enumerate(lines):
                            if 'apiDomain' in line and '=' in line:
                                print(f"  行 {j+1}: {line.strip()[:150]}")
                                break
            
            # 查找 apiDomain（支持多种格式）
            if 'apiDomain' in script_content:
                # 尝试多种匹配模式
                patterns = [
                    r"var\s+apiDomain\s*=\s*['\"]([^'\"]+)['\"]",  # var apiDomain = "..."
                    r"apiDomain\s*[:=]\s*['\"]([^'\"]+)['\"]",  # apiDomain: "..." 或 apiDomain = "..."
                    r"['\"]apiDomain['\"]\s*:\s*['\"]([^'\"]+)['\"]",  # "apiDomain": "..."
                ]
                for pattern in patterns:
                    api_match = re.search(pattern, script_content, re.IGNORECASE)
                    if api_match:
                        api_domain = api_match.group(1)
                        if debug_mode:
                            print(f"[KB {kb_number}] 找到 apiDomain: {api_domain} (模式: {pattern})")
                        break
            
            # 查找 kbDownloadDomain（支持多种格式）
            if 'kbDownloadDomain' in script_content:
                patterns = [
                    r"var\s+kbDownloadDomain\s*=\s*['\"]([^'\"]+)['\"]",  # var kbDownloadDomain = "..."
                    r"kbDownloadDomain\s*[:=]\s*['\"]([^'\"]+)['\"]",  # kbDownloadDomain: "..." 或 kbDownloadDomain = "..."
                    r"['\"]kbDownloadDomain['\"]\s*:\s*['\"]([^'\"]+)['\"]",  # "kbDownloadDomain": "..."
                ]
                for pattern in patterns:
                    domain_match = re.search(pattern, script_content, re.IGNORECASE)
                    if domain_match:
                        kb_download_domain = domain_match.group(1)
                        if debug_mode:
                            print(f"[KB {kb_number}] 找到 kbDownloadDomain: {kb_download_domain} (模式: {pattern})")
                        break
            
            # 如果都找到了，可以提前退出
            if api_domain and kb_download_domain:
                break
        
        # 查找附件下载链接（通过data-uniquefileid属性）
        attachment_download_links = soup.find_all('a', attrs={'data-uniquefileid': True})
        for link_elem in attachment_download_links:
            file_id = link_elem.get('data-uniquefileid')
            if file_id:
                # 提取文件名（从同一个attachment-card内的附件名称元素）
                # 先找到attachment-card父元素
                card = link_elem.find_parent('div', class_=lambda x: x and 'attachment-card' in str(x).lower())
                if card:
                    # 在card内查找attachment-name
                    attachment_name_elem = card.find('span', class_=lambda x: x and 'attachment-name' in str(x).lower())
                    if attachment_name_elem:
                        filename = attachment_name_elem.get_text(strip=True)
                        
                        if api_domain and kb_download_domain:
                            # 构建完整的API URL
                            api_domain_clean = api_domain.rstrip('/')
                            api_url = f"{api_domain_clean}/es/attachments/download_attachment?domain={kb_download_domain}"
                            # 使用特殊格式标记这是API下载
                            attachment_links.append(f"API_DOWNLOAD:{api_url}:{file_id}:{filename}")
                        else:
                            # 如果没有API配置，尝试查找直接下载链接
                            href = link_elem.get('href', '')
                            if href and (href.startswith('http://') or href.startswith('https://')):
                                attachment_links.append(href)
                            elif not href:
                                # 如果没有href，尝试从onclick事件中提取URL
                                onclick = link_elem.get('onclick', '')
                                if onclick:
                                    # 尝试从onclick中提取URL
                                    url_match = re.search(r'["\'](https?://[^"\']+)["\']', onclick)
                                    if url_match:
                                        attachment_links.append(url_match.group(1))
        
        # 方法2: 深入分析JavaScript代码，查找下载函数和附件ID
        # 查找所有可能的下载函数定义
        download_functions = []
        attachment_data = {}  # 存储附件ID到文件名的映射
        
        for script in scripts:
            if script.string:
                script_content = script.string
                # 查找 downloadAttachment 函数
                if 'downloadAttachment' in script_content:
                    # 尝试提取函数定义和参数
                    func_match = re.search(r'function\s+downloadAttachment\s*\([^)]*\)\s*\{[^}]*\}', script_content, re.DOTALL)
                    if func_match:
                        download_functions.append(func_match.group(0))
                    
                    # 查找附件数据（可能在JavaScript对象或数组中）
                    # 尝试查找包含附件信息的对象
                    attachment_data_match = re.search(r'attachments?\s*[:=]\s*\[(.*?)\]', script_content, re.DOTALL | re.IGNORECASE)
                    if attachment_data_match:
                        # 尝试提取附件数据
                        pass
                
                # 查找可能的附件ID模式（UUID、数字ID等）
                # 查找 data-* 属性中的ID
                id_patterns = [
                    r'["\']([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})["\']',  # UUID
                    r'["\']([a-f0-9]{32})["\']',  # MD5 hash
                    r'["\'](\d{10,})["\']',  # 长数字ID
                ]
                for pattern in id_patterns:
                    ids = re.findall(pattern, script_content, re.IGNORECASE)
                    if ids:
                        # 尝试将这些ID与附件关联
                        pass
        
        # 方法3: 直接遍历所有attachment-card（即使没有data-uniquefileid）
        all_attachment_cards = soup.find_all('div', class_=lambda x: x and 'attachment-card' in str(x).lower())
        for card in all_attachment_cards:
            # 提取文件名
            name_elem = card.find('span', class_=lambda x: x and 'attachment-name' in str(x).lower())
            if not name_elem:
                # 尝试其他可能的选择器
                name_elem = card.find(['span', 'div', 'a'], class_=lambda x: x and ('name' in str(x).lower() or 'title' in str(x).lower()))
            if not name_elem:
                # 尝试从card的文本内容中提取文件名
                card_text = card.get_text(strip=True)
                # 查找可能的文件名（包含扩展名）
                filename_match = re.search(r'([\w\-_\.]+\.(?:' + '|'.join(file_extensions) + '))', card_text, re.IGNORECASE)
                if filename_match:
                    filename = filename_match.group(1)
                else:
                    continue
            else:
                filename = name_elem.get_text(strip=True)
            
            if not filename:
                continue
            
            # 查找下载链接或按钮
            file_id = None
            download_url = None
            
            # 方法3a: 查找所有data-*属性（可能包含文件ID）
            for attr_name, attr_value in card.attrs.items():
                if attr_name.startswith('data-') and attr_value:
                    # 检查是否是可能的文件ID
                    if isinstance(attr_value, str) and len(attr_value) > 8:
                        # 可能是文件ID
                        if 'file' in attr_name.lower() or 'id' in attr_name.lower():
                            file_id = attr_value
                            break
            
            # 方法3b: 查找data-uniquefileid（在子元素中）
            download_link = card.find('a', attrs={'data-uniquefileid': True})
            if download_link:
                file_id = download_link.get('data-uniquefileid')
            
            # 方法3c: 查找所有data-*属性在下载链接中
            if not file_id:
                download_links = card.find_all('a', class_=lambda x: x and 'download' in str(x).lower())
                for link in download_links:
                    for attr_name, attr_value in link.attrs.items():
                        if attr_name.startswith('data-') and attr_value:
                            if isinstance(attr_value, str) and len(attr_value) > 8:
                                if 'file' in attr_name.lower() or 'id' in attr_name.lower():
                                    file_id = attr_value
                                    break
                    if file_id:
                        break
            
            # 方法3d: 查找onclick事件中的file_id（在card或子元素中）
            if not file_id:
                onclick_elements = card.find_all(attrs={'onclick': True})
                if not onclick_elements:
                    # 也检查card本身
                    if card.get('onclick'):
                        onclick_elements = [card]
                
                for elem in onclick_elements:
                    onclick = elem.get('onclick', '')
                    # 尝试从onclick中提取file_id（可能是downloadAttachment(fileId)格式）
                    file_id_match = re.search(r'downloadAttachment\(["\']?([^"\']+)["\']?\)', onclick, re.IGNORECASE)
                    if file_id_match:
                        file_id = file_id_match.group(1)
                        break
                    # 尝试提取其他可能的ID（UUID、hash等）
                    id_patterns = [
                        r'["\']([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})["\']',  # UUID
                        r'["\']([a-f0-9]{32})["\']',  # MD5 hash
                        r'["\'](\d{10,})["\']',  # 长数字ID
                    ]
                    for pattern in id_patterns:
                        id_match = re.search(pattern, onclick, re.IGNORECASE)
                        if id_match:
                            file_id = id_match.group(1)
                            break
                    if file_id:
                        break
            
            # 方法3e: 从JavaScript代码中查找附件ID（通过文件名匹配）
            if not file_id and filename:
                # 在JavaScript代码中搜索包含此文件名的代码
                for script in scripts:
                    if script.string and filename in script.string:
                        # 尝试在文件名附近查找ID
                        # 查找类似 "filename": "xxx", "id": "yyy" 的模式
                        pattern = rf'["\']{re.escape(filename)}["\'][^}}]*["\']id["\']\s*:\s*["\']([^"\']+)["\']'
                        match = re.search(pattern, script.string, re.IGNORECASE)
                        if match:
                            file_id = match.group(1)
                            break
                        # 或者查找 "id": "xxx", "filename": "yyy" 的模式
                        pattern = rf'["\']id["\']\s*:\s*["\']([^"\']+)["\'][^}}]*["\']{re.escape(filename)}["\']'
                        match = re.search(pattern, script.string, re.IGNORECASE)
                        if match:
                            file_id = match.group(1)
                            break
            
            # 方法2c: 查找直接下载链接（href）
            if not download_url:
                href_links = card.find_all('a', href=True)
                for link in href_links:
                    href = link.get('href', '')
                    if href and (href.startswith('http://') or href.startswith('https://')):
                        # 检查是否是附件链接
                        link_lower = href.lower()
                        if any(link_lower.endswith(f'.{ext}') or f'.{ext}' in link_lower for ext in file_extensions):
                            download_url = href
                            break
            
            # 调试输出：显示找到的信息
            debug_mode = os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true'
            if debug_mode:
                print(f"[KB {kb_number}] 附件 '{filename}': file_id={file_id}, download_url={download_url}")
            
            # 如果找到了file_id和API配置，构建API下载URL
            if file_id and api_domain and kb_download_domain:
                api_domain_clean = api_domain.rstrip('/')
                api_url = f"{api_domain_clean}/es/attachments/download_attachment?domain={kb_download_domain}"
                attachment_links.append(f"API_DOWNLOAD:{api_url}:{file_id}:{filename}")
                if debug_mode:
                    print(f"[KB {kb_number}] 构建API下载URL: {api_url} (file_id: {file_id})")
            elif download_url:
                # 使用直接下载链接
                attachment_links.append(download_url)
                if debug_mode:
                    print(f"[KB {kb_number}] 使用直接下载链接: {download_url}")
            elif file_id:
                # 有file_id但没有API配置，尝试构建可能的下载URL
                # 这可能不工作，但值得尝试
                if api_domain:
                    api_domain_clean = api_domain.rstrip('/')
                    # 尝试常见的附件下载路径
                    possible_urls = [
                        f"{api_domain_clean}/es/attachments/download_attachment?fileId={file_id}",
                        f"{api_domain_clean}/attachments/download/{file_id}",
                        f"{api_domain_clean}/api/attachments/{file_id}",
                    ]
                    # 先尝试第一个，如果失败会在下载时处理
                    attachment_links.append(f"API_DOWNLOAD:{possible_urls[0]}:{file_id}:{filename}")
                    if debug_mode:
                        print(f"[KB {kb_number}] 尝试构建下载URL（无domain）: {possible_urls[0]}")
                else:
                    if debug_mode:
                        print(f"[KB {kb_number}] 警告: 找到file_id '{file_id}' 但没有API配置，无法构建下载URL")
            else:
                if debug_mode:
                    print(f"[KB {kb_number}] 警告: 附件 '{filename}' 未找到file_id或下载链接")
        
        # 方法3: 查找"Attachments"标题下的attachment-container或attachment-card
        attachment_sections = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], 
                                           string=lambda text: text and 'attachment' in text.lower())
        for section in attachment_sections:
            # 查找该section下的attachment-container
            parent = section.find_parent()
            if parent:
                # 查找attachment-container
                attachment_containers = parent.find_all('div', class_=lambda x: x and 'attachment-container' in str(x).lower())
                for container in attachment_containers:
                    # 查找container内的attachment-card（如果还没处理过）
                    container_cards = container.find_all('div', class_=lambda x: x and 'attachment-card' in str(x).lower())
                    for card in container_cards:
                        # 检查是否已经处理过（通过检查card是否在all_attachment_cards中）
                        if card not in all_attachment_cards:
                            # 使用与方法2相同的逻辑处理
                            name_elem = card.find('span', class_=lambda x: x and 'attachment-name' in str(x).lower())
                            if name_elem:
                                filename = name_elem.get_text(strip=True)
                                download_link = card.find('a', attrs={'data-uniquefileid': True})
                                if download_link:
                                    file_id = download_link.get('data-uniquefileid')
                                    if api_domain and kb_download_domain:
                                        api_domain_clean = api_domain.rstrip('/')
                                        api_url = f"{api_domain_clean}/es/attachments/download_attachment?domain={kb_download_domain}"
                                        attachment_links.append(f"API_DOWNLOAD:{api_url}:{file_id}:{filename}")
                
                # 如果没有找到attachment-container，查找直接链接（在Attachments标题下的）
                if not attachment_containers:
                    links_in_section = parent.find_all('a', href=True)
                    for link_elem in links_in_section:
                        href = link_elem.get('href', '')
                        if href and (href.startswith('http://') or href.startswith('https://')):
                            link_lower = href.lower()
                            if any(link_lower.endswith(f'.{ext}') or f'.{ext}' in link_lower for ext in file_extensions):
                                if href not in attachment_links:
                                    attachment_links.append(href)
    
    # 去重
    attachment_links = list(set(attachment_links))
    
    # 调试输出：显示找到的附件数量
    if attachment_links:
        print(f"[KB {kb_number}] 找到 {len(attachment_links)} 个附件链接")
    else:
        if soup:
            # 如果提供了soup但没有找到附件，输出调试信息
            # 检查是否有附件相关的元素
            attachment_cards = soup.find_all('div', class_=lambda x: x and 'attachment-card' in str(x).lower())
            data_uniquefileid_links = soup.find_all('a', attrs={'data-uniquefileid': True})
            attachment_sections = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], 
                                               string=lambda text: text and 'attachment' in text.lower())
            attachment_containers = soup.find_all('div', class_=lambda x: x and 'attachment-container' in str(x).lower())
            
            # 检查API配置
            scripts = soup.find_all('script')
            api_domain = None
            kb_download_domain = None
            for script in scripts:
                if script.string:
                    if 'var apiDomain' in script.string:
                        api_match = re.search(r"var apiDomain\s*=\s*['\"]([^'\"]+)['\"]", script.string)
                        if api_match:
                            api_domain = api_match.group(1)
                    if 'var kbDownloadDomain' in script.string:
                        domain_match = re.search(r"var kbDownloadDomain\s*=\s*['\"]([^'\"]+)['\"]", script.string)
                        if domain_match:
                            kb_download_domain = domain_match.group(1)
            
            debug_mode = os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true'
            if debug_mode:
                print(f"[KB {kb_number}] 调试信息:")
                print(f"  - attachment-card: {len(attachment_cards)} 个")
                print(f"  - data-uniquefileid链接: {len(data_uniquefileid_links)} 个")
                print(f"  - Attachments标题: {len(attachment_sections)} 个")
                print(f"  - attachment-container: {len(attachment_containers)} 个")
                print(f"  - apiDomain: {api_domain if api_domain else '未找到'}")
                print(f"  - kbDownloadDomain: {kb_download_domain if kb_download_domain else '未找到'}")
                
                # 输出前2个attachment-card的HTML结构（用于调试）
                if len(attachment_cards) > 0:
                    print(f"\n[KB {kb_number}] attachment-card HTML结构示例（前2个）:")
                    for i, card in enumerate(attachment_cards[:2]):
                        print(f"\n--- Card {i+1} ---")
                        # 只输出前500个字符，避免输出过长
                        card_html = str(card)[:500]
                        print(card_html)
                        if len(str(card)) > 500:
                            print("... (已截断)")
            else:
                # 即使不在调试模式，也显示关键信息
                if len(attachment_cards) > 0 or len(data_uniquefileid_links) > 0:
                    print(f"[KB {kb_number}] 发现 {len(attachment_cards)} 个attachment-card, {len(data_uniquefileid_links)} 个data-uniquefileid链接，但未找到有效的附件下载链接")
                    print(f"  提示: 设置 CRAWLER_DEBUG=true 查看详细调试信息")
        else:
            print(f"[KB {kb_number}] 警告: 未提供soup对象，无法解析附件")
    
    if not attachment_links:
        return content, {}
    
    # 下载附件
    for attach_url in attachment_links:
        try:
            # 检查是否是API下载格式
            if attach_url.startswith('API_DOWNLOAD:'):
                # 格式: API_DOWNLOAD:api_url:file_id:filename
                # 注意：api_url可能包含:，所以需要更智能的解析
                prefix_len = len('API_DOWNLOAD:')
                rest = attach_url[prefix_len:]
                # 从后往前分割，因为filename可能包含特殊字符
                # 先找到最后一个:，那是file_id和filename的分隔
                last_colon = rest.rfind(':')
                if last_colon > 0:
                    filename_part = rest[last_colon+1:]
                    # 再找倒数第二个:，那是api_url和file_id的分隔
                    second_last_colon = rest[:last_colon].rfind(':')
                    if second_last_colon > 0:
                        api_url = rest[:second_last_colon]
                        file_id = rest[second_last_colon+1:last_colon]
                        original_filename = filename_part
                    # 使用POST请求下载（带重试机制）
                    max_retries = 5
                    retry_delays = [2, 5, 10, 15, 20]  # 重试延迟（秒）
                    download_success = False
                    
                    safe_filename = re.sub(r'[^\w\-_\.]', '_', original_filename)
                    if not safe_filename:
                        safe_filename = f"{kb_number}_{hashlib.md5(file_id.encode()).hexdigest()[:8]}.bin"
                    
                    local_path = f"/static/attachments/kb/{kb_number}/{safe_filename}"
                    filepath = os.path.join(kb_attachment_dir, safe_filename)
                    
                    # 如果文件已存在，跳过下载
                    if os.path.exists(filepath):
                        attachment_mapping[attach_url] = local_path
                        continue
                    
                    for attempt in range(max_retries):
                        try:
                            if session is None:
                                attach_session = requests.Session()
                                attach_session.headers.update({
                                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                                    'Referer': f'https://knowledge.broadcom.com/external/article/{kb_number}',
                                    'Content-Type': 'application/x-www-form-urlencoded',
                                })
                            else:
                                attach_session = session
                            
                            # POST请求，data参数是JSON格式
                            import json
                            post_data = {'data': json.dumps({'uniqueFileId': file_id})}
                            
                            # 根据重试次数增加超时时间
                            timeout = 30 + (attempt * 10)  # 30秒, 40秒, 50秒
                            
                            if attempt > 0:
                                delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                                print(f"[KB {kb_number}] 重试下载附件 {safe_filename} (第 {attempt + 1}/{max_retries} 次，等待 {delay} 秒后重试)...")
                                time.sleep(delay)
                            
                            response = attach_session.post(api_url, data=post_data, timeout=timeout, stream=True, allow_redirects=True)
                            
                            if response.status_code == 200:
                                # 下载文件（使用stream模式，避免内存问题）
                                try:
                                    with open(filepath, 'wb') as f:
                                        downloaded_size = 0
                                        max_size = 100 * 1024 * 1024  # 限制最大100MB
                                        first_chunk = True
                                        
                                        for chunk in response.iter_content(chunk_size=8192):
                                            if chunk:
                                                # 检查第一个chunk是否是HTML错误页面
                                                if first_chunk:
                                                    first_chunk = False
                                                    chunk_preview = chunk[:100]
                                                    is_html_error = (b'<!DOCTYPE' in chunk_preview or 
                                                                   b'<html' in chunk_preview.lower() or
                                                                   (b'error' in chunk_preview.lower()[:50] and b'<' in chunk_preview))
                                                    if is_html_error:
                                                        # 是HTML错误页面，停止下载
                                                        f.close()
                                                        os.remove(filepath)
                                                        if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                                                            print(f"API返回HTML错误页面 (KB {kb_number}, File ID: {file_id})")
                                                        raise Exception("API返回HTML错误页面")
                                                
                                                f.write(chunk)
                                                downloaded_size += len(chunk)
                                                if downloaded_size > max_size:
                                                    f.close()
                                                    os.remove(filepath)
                                                    if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                                                        print(f"文件太大，停止下载 (KB {kb_number}, File ID: {file_id})")
                                                    raise Exception("文件太大")
                                        
                                    # 下载完成，验证文件大小
                                    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                                        attachment_mapping[attach_url] = local_path
                                        print(f"[KB {kb_number}] 成功下载附件: {safe_filename} ({os.path.getsize(filepath)} 字节)")
                                        download_success = True
                                        break  # 成功，退出重试循环
                                    else:
                                        # 文件大小为0，删除并重试
                                        if os.path.exists(filepath):
                                            os.remove(filepath)
                                        raise Exception("下载的文件大小为0")
                                        
                                except Exception as e:
                                    # 下载过程中出错
                                    if os.path.exists(filepath):
                                        os.remove(filepath)
                                    if attempt < max_retries - 1:
                                        if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                                            print(f"文件下载出错 (KB {kb_number}, File ID: {file_id}, 尝试 {attempt + 1}/{max_retries}): {e}")
                                        continue  # 继续重试
                                    else:
                                        # 最后一次尝试失败
                                        if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                                            print(f"文件下载出错 (KB {kb_number}, File ID: {file_id}): {e}")
                                        raise
                            else:
                                # HTTP状态码不是200
                                if attempt < max_retries - 1:
                                    print(f"[KB {kb_number}] API返回状态码 {response.status_code}，将重试...")
                                    continue
                                else:
                                    raise Exception(f"API返回状态码 {response.status_code}")
                                    
                        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                            # 超时或连接错误，可以重试
                            if attempt < max_retries - 1:
                                if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                                    print(f"API附件下载超时/连接错误 (KB {kb_number}, File ID: {file_id}, 尝试 {attempt + 1}/{max_retries}): {e}")
                                continue  # 继续重试
                            else:
                                # 最后一次尝试失败
                                print(f"API附件下载失败 (KB {kb_number}, File ID: {file_id}): {e}")
                                break
                        except Exception as e:
                            # 其他错误
                            if attempt < max_retries - 1:
                                if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                                    print(f"API附件下载出错 (KB {kb_number}, File ID: {file_id}, 尝试 {attempt + 1}/{max_retries}): {e}")
                                continue  # 继续重试
                            else:
                                # 最后一次尝试失败
                                if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                                    print(f"API附件下载失败 (KB {kb_number}, File ID: {file_id}): {e}")
                                break
                    
                    if not download_success:
                        # 所有重试都失败
                        if os.path.exists(filepath):
                            os.remove(filepath)
                continue
            
            # 普通URL下载
            # 从URL中提取文件名
            url_path = attach_url.split('?')[0]  # 移除查询参数
            original_filename = os.path.basename(url_path)
            
            # 如果没有文件名或文件名不包含扩展名，尝试从Content-Disposition获取
            if not original_filename or '.' not in original_filename:
                # 先发送HEAD请求获取文件名
                try:
                    if session is None:
                        attach_session = requests.Session()
                        attach_session.headers.update({
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                        })
                    else:
                        attach_session = session
                    
                    head_response = attach_session.head(attach_url, timeout=5, allow_redirects=True)
                    content_disposition = head_response.headers.get('Content-Disposition', '')
                    if content_disposition:
                        # 从Content-Disposition中提取文件名
                        filename_match = re.search(r'filename[^;=\n]*=(([\'"]).*?\2|[^;\n]*)', content_disposition)
                        if filename_match:
                            original_filename = filename_match.group(1).strip('"\'')
                except:
                    pass
            
            # 如果还是没有文件名，使用URL的hash
            if not original_filename or '.' not in original_filename:
                url_hash = hashlib.md5(attach_url.encode()).hexdigest()[:8]
                # 尝试从URL中推断扩展名
                ext = None
                for file_ext in file_extensions:
                    if f'.{file_ext}' in attach_url.lower():
                        ext = file_ext
                        break
                if ext:
                    original_filename = f"{kb_number}_{url_hash}.{ext}"
                else:
                    original_filename = f"{kb_number}_{url_hash}.bin"
            
            # 清理文件名（移除特殊字符）
            safe_filename = re.sub(r'[^\w\-_\.]', '_', original_filename)
            if not safe_filename:
                safe_filename = f"{kb_number}_{hashlib.md5(attach_url.encode()).hexdigest()[:8]}.bin"
            
            local_path = f"/static/attachments/kb/{kb_number}/{safe_filename}"
            filepath = os.path.join(kb_attachment_dir, safe_filename)
            
            # 如果文件已存在，跳过下载
            if os.path.exists(filepath):
                attachment_mapping[attach_url] = local_path
                continue
            
            # 下载附件
            if session is None:
                attach_session = requests.Session()
                attach_session.headers.update({
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
            else:
                attach_session = session
            
            # 设置更大的超时时间（附件可能较大）
            response = attach_session.get(attach_url, timeout=30, stream=True, allow_redirects=True)
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    downloaded_size = 0
                    max_size = 100 * 1024 * 1024  # 限制最大100MB
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            if downloaded_size > max_size:
                                # 文件太大，停止下载
                                os.remove(filepath)
                                break
                    else:
                        # 下载完成，验证文件大小
                        if os.path.getsize(filepath) > 0:
                            attachment_mapping[attach_url] = local_path
                            print(f"[KB {kb_number}] 成功下载附件: {safe_filename} ({os.path.getsize(filepath)} 字节)")
        except Exception as e:
            # 下载失败，保留原URL
            if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                print(f"附件下载失败 (KB {kb_number}, URL: {attach_url[:50]}...): {e}")
            pass
    
    # 更新内容中的附件URL
    if attachment_mapping:
        updated_content = content
        for old_url, local_path in attachment_mapping.items():
            # 如果是API下载格式，需要替换附件下载链接的onclick事件
            if old_url.startswith('API_DOWNLOAD:'):
                # 提取file_id和filename
                prefix_len = len('API_DOWNLOAD:')
                rest = old_url[prefix_len:]
                last_colon = rest.rfind(':')
                if last_colon > 0:
                    second_last_colon = rest[:last_colon].rfind(':')
                    if second_last_colon > 0:
                        file_id = rest[second_last_colon+1:last_colon]
                        attachment_filename = rest[last_colon+1:]  # 获取完整的文件名
                        
                        # 方法1: 替换onclick事件为href
                        pattern1 = rf'<a([^>]*)\s+onclick\s*=\s*["\']downloadAttachment\(["\']?{re.escape(file_id)}["\']?\)["\']([^>]*)>'
                        def replace_onclick(match):
                            attrs_before = match.group(1)
                            attrs_after = match.group(2)
                            # 移除onclick，添加href
                            attrs_before = re.sub(r'\s+onclick\s*=\s*["\'][^"\']*["\']', '', attrs_before)
                            attrs_after = re.sub(r'\s+onclick\s*=\s*["\'][^"\']*["\']', '', attrs_after)
                            return f'<a{attrs_before} href="{local_path}" download="{attachment_filename}"{attrs_after}>'
                        updated_content = re.sub(pattern1, replace_onclick, updated_content, flags=re.IGNORECASE)
                        
                        # 方法2: 如果链接没有href，添加href属性（通过data-uniquefileid查找）
                        attachment_link_pattern = rf'<a([^>]*)\s+data-uniquefileid=["\']{re.escape(file_id)}["\']([^>]*)>'
                        def add_href_if_missing(match):
                            attrs_before = match.group(1)
                            attrs_after = match.group(2)
                            if 'href=' not in attrs_before and 'href=' not in attrs_after:
                                return f'<a{attrs_before} data-uniquefileid="{file_id}" href="{local_path}" download="{attachment_filename}"{attrs_after}>'
                            return match.group(0)
                        updated_content = re.sub(attachment_link_pattern, add_href_if_missing, updated_content, flags=re.IGNORECASE)
                        
                        # 方法2b: 查找attachment-card中包含该file_id的下载链接（即使没有data-uniquefileid）
                        # 先找到包含该file_id的attachment-card
                        attachment_card_pattern = rf'<div[^>]*attachment-card[^>]*>.*?{re.escape(attachment_filename)}.*?</div>'
                        def add_href_to_card_download_link(match):
                            card_html = match.group(0)
                            # 查找attachment-download链接
                            download_link_pattern = r'<a([^>]*class=["\'][^"\']*attachment-download[^"\']*["\'][^>]*)>'
                            def add_href_to_link(link_match):
                                link_attrs = link_match.group(1)
                                if 'href=' not in link_attrs:
                                    return f'<a{link_attrs} href="{local_path}" download="{attachment_filename}">'
                                return link_match.group(0)
                            card_html = re.sub(download_link_pattern, add_href_to_link, card_html, flags=re.IGNORECASE)
                            return card_html
                        updated_content = re.sub(attachment_card_pattern, add_href_to_card_download_link, updated_content, flags=re.IGNORECASE | re.DOTALL)
                        
                        # 方法3: 将整个attachment-card转换为可点击链接
                        filename_escaped = re.escape(attachment_filename)
                        # 查找包含该文件名的attachment-card，将整个card转换为链接
                        attachment_card_full_pattern = rf'<div([^>]*class=["\'][^"\']*attachment-card[^"\']*["\'][^>]*)>(.*?{filename_escaped}.*?)</div>'
                        def wrap_card_in_link(match):
                            card_attrs = match.group(1)
                            card_content = match.group(2)
                            # 检查card是否已经是链接
                            if 'href=' in card_attrs:
                                return match.group(0)
                            # 提取文件名文本（移除HTML标签）
                            filename_text_match = re.search(rf'({filename_escaped})', card_content)
                            filename_text = filename_text_match.group(1) if filename_text_match else attachment_filename
                            # 移除card内部的链接和图标
                            card_content_clean = re.sub(r'<a[^>]*>([^<]*)</a>', r'\1', card_content)
                            card_content_clean = re.sub(r'<span[^>]*material-icons[^>]*>.*?</span>', '', card_content_clean, flags=re.IGNORECASE | re.DOTALL)
                            card_content_clean = re.sub(r'<span[^>]*attachment-name[^>]*>([^<]*)</span>', r'\1', card_content_clean, flags=re.IGNORECASE)
                            card_content_clean = re.sub(r'<span[^>]*attachment-download[^>]*>.*?</span>', '', card_content_clean, flags=re.IGNORECASE | re.DOTALL)
                            card_content_clean = re.sub(r'<a[^>]*attachment-download[^>]*>.*?</a>', '', card_content_clean, flags=re.IGNORECASE | re.DOTALL)
                            # 清理空白
                            card_content_clean = re.sub(r'\s+', ' ', card_content_clean).strip()
                            if not card_content_clean:
                                card_content_clean = filename_text
                            return f'<a href="{local_path}" download="{attachment_filename}" class="attachment-card">{card_content_clean}</a>'
                        updated_content = re.sub(attachment_card_full_pattern, wrap_card_in_link, updated_content, flags=re.IGNORECASE | re.DOTALL)
            else:
                # 普通URL替换
                updated_content = re.sub(
                    re.escape(old_url),
                    local_path,
                    updated_content,
                    flags=re.IGNORECASE
                )
        return updated_content, attachment_mapping
    
    return content, {}


def rate_limit():
    """请求速率限制：确保每个请求之间有适当的延迟"""
    current_time = time.time()
    if hasattr(last_request_time, 'last_time'):
        elapsed = current_time - last_request_time.last_time
        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        if elapsed < delay:
            time.sleep(delay - elapsed)
    last_request_time.last_time = time.time()


def parse_kb_page(session, kb_number, max_retries=3):
    """解析单个KB页面（带重试机制）"""
    url = f"{BASE_URL}{kb_number}"
    
    # 重试机制：最多重试3次，使用指数退避
    for attempt in range(max_retries):
        try:
            # 速率限制
            rate_limit()
            
            # 请求延迟（指数退避）
            if attempt > 0:
                backoff_delay = (2 ** attempt) + random.uniform(0, 1)  # 2秒, 4秒, 8秒...
                time.sleep(backoff_delay)
            
            response = session.get(url, timeout=15, stream=False, allow_redirects=True)  # 增加超时时间到15秒
        
            # 快速检查404
            if response.status_code == 404:
                return None
            
            # 检查是否被重定向到错误页面
            if response.status_code != 200:
                # 如果是429（Too Many Requests）或503（Service Unavailable），重试
                if response.status_code in [429, 503] and attempt < max_retries - 1:
                    continue
                return None
            
            # 检查响应内容是否为空或太小（可能是错误页面）
            if len(response.content) < 1000:
                # 如果内容太小且不是最后一次尝试，重试
                if attempt < max_retries - 1:
                    continue
                return None
            # 使用html.parser替代lxml（更快）
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 快速提取标题（尝试多种方式）
            title_elem = None
            # 优先查找h3.wolken-h3（VMware KB的标准标题格式）
            title_elem = soup.find('h3', class_=lambda x: x and 'wolken-h3' in str(x).lower())
            if not title_elem:
                title_elem = soup.find('h1') or soup.find('h2') or soup.find('h3')
            if not title_elem:
                title_elem = soup.find('title')
            title = title_elem.get_text(strip=True) if title_elem else f"KB {kb_number}"
            
            # 快速检查是否是错误页面
            if '404' in title or 'not found' in title.lower():
                return None
            
            # 快速提取Article ID
            article_id = None
            for text in soup.stripped_strings:
                if 'Article ID:' in text or 'Article ID' in text:
                    parts = text.split('Article ID')
                    if len(parts) > 1:
                        article_id = parts[-1].replace(':', '').strip()
                        break
            
            # 提取更新时间（从JavaScript中提取）
            updated_date = None
            # 方法1: 从script标签中查找date_time的赋值
            scripts = soup.find_all('script')
            import re
            for script in scripts:
                if script.string and "getElementById('date_time')" in script.string:
                    # 查找 var d = '日期' 或类似的赋值语句
                    date_pattern = r"var\s+d\s*=\s*['\"]([^'\"]+)['\"]"
                    match = re.search(date_pattern, script.string)
                    if match:
                        updated_date = match.group(1).strip()
                        break
            
            # 方法2: 如果方法1没找到，尝试从JSON-LD中提取dateModified
            if not updated_date:
                for script in scripts:
                    if script.string and 'application/ld+json' in str(script.get('type', '')):
                        import json
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, dict) and 'dateModified' in data:
                            updated_date = data['dateModified']
                            break
                        elif isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and 'dateModified' in item:
                                    updated_date = item['dateModified']
                                    break
                            if updated_date:
                                break
                    except:
                        pass
        
            # 方法3: 如果还没找到，尝试从文本中提取（旧方法）
            if not updated_date:
                for text in soup.stripped_strings:
                    if 'Updated On:' in text:
                        parts = text.split('Updated On:')
                        if len(parts) > 1:
                            potential_date = parts[1].strip()
                            # 如果包含日期格式，提取它
                            date_match = re.search(r'\d{1,2}[-/]\d{1,2}[-/]\d{2,4}', potential_date)
                            if date_match:
                                updated_date = date_match.group(0)
                                break
        
            # 提取产品信息（从product-chip中提取）
            products = []
            # 方法1: 查找product-container中的product-chip
            product_containers = soup.find_all('div', class_=lambda x: x and 'product-container' in str(x).lower())
            for container in product_containers:
                chips = container.find_all('span', class_=lambda x: x and 'product-chip' in str(x).lower())
                for chip in chips:
                    product_name = chip.get_text(strip=True)
                    if product_name and product_name not in products and len(product_name) < 200:
                        products.append(product_name)
        
            # 方法2: 如果方法1没找到，直接查找所有product-chip
            if not products:
                all_chips = soup.find_all('span', class_=lambda x: x and 'product-chip' in str(x).lower())
                for chip in all_chips:
                    product_name = chip.get_text(strip=True)
                    if product_name and product_name not in products and len(product_name) < 200:
                        products.append(product_name)
        
            # 提取完整内容（保留所有部分：Products、Issue/Introduction、Environment、Resolution、Additional Information）
            # 保存原始的soup对象（包含script标签），用于附件下载
            original_soup = BeautifulSoup(response.content, 'html.parser')
            
            # 移除导航、搜索框、页眉页脚等无关元素
            for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
                tag.decompose()
        
            # 移除包含特定class的元素（导航、搜索等）
            # 注意：不要移除article-detail-card-header，那是内容的一部分
            nav_keywords = ['nav', 'menu', 'search', 'footer', 'sidebar', 'breadcrumb', 'feedback', 'subscribe']
            for tag in soup.find_all(class_=lambda x: x and any(keyword in str(x).lower() for keyword in nav_keywords)):
                tag.decompose()
        
            # 移除真正的header（但不是article-detail-card-header）
            for tag in soup.find_all(class_=lambda x: x and 'header' in str(x).lower() and 'article-detail-card-header' not in str(x).lower()):
                tag.decompose()
        
            # 移除包含特定文本的元素（反馈按钮等）
            for tag in soup.find_all(string=lambda text: text and any(keyword in text.lower() for keyword in ['search', 'cancel', 'subscribe', 'feedback', 'thumb_up', 'thumb_down'])):
                if tag.parent:
                    tag.parent.decompose()
        
            # 查找主要内容容器（包含所有article-detail-card的容器）
            main_container = (soup.find('div', class_=lambda x: x and 'wolken-content-container' in str(x).lower()) or
                         soup.find('div', class_=lambda x: x and 'article-container' in str(x).lower()) or
                         soup.find('article') or 
                         soup.find('main') or
                         soup.find('body'))
        
            if main_container:
                # 再次移除导航元素
                for tag in main_container(["script", "style", "nav", "header", "footer", "aside"]):
                    tag.decompose()
                
                # 移除"Show More"和"Show Less"按钮
                for tag in main_container.find_all(string=lambda text: text and ('show more' in text.lower() or 'show less' in text.lower())):
                    if tag.parent:
                        tag.parent.decompose()
            
            # 优先使用wolken-content-container（包含所有部分）
            content_html = None
            if main_container.get('class') and any('wolken-content-container' in str(c).lower() for c in main_container.get('class', [])):
                # 直接使用整个container，保留所有card和结构
                for tag in main_container.find_all(True):
                    attrs_to_keep = ['class', 'id', 'href', 'src', 'colspan', 'rowspan', 'target', 'rel', 'data-uniquefileid', 'onclick']
                    tag.attrs = {k: v for k, v in tag.attrs.items() if k in attrs_to_keep}
                content_html = str(main_container)
            
            # 如果没有wolken-content-container，查找所有article-detail-card
            article_cards = []
            if not content_html:
                article_cards = main_container.find_all('div', class_=lambda x: x and 'article-detail-card' in str(x).lower())
            
            if not content_html and article_cards:
                # 创建一个容器来包含所有card
                from bs4 import BeautifulSoup as BS
                content_soup = BS('<div class="article-content-wrapper"></div>', 'html.parser')
                wrapper = content_soup.find('div')
                
                # 简化逻辑：保留所有有意义的card，按标题去重（保留内容更多的）
                seen_titles = {}
                
                for card in article_cards:
                    card_text = card.get_text(strip=True)
                    if len(card_text) < 3:
                        continue
                    
                    # 查找标题
                    title_elem = card.find(['h2', 'h3', 'h4', 'h5'], class_=lambda x: x and 'wolken-h' in str(x).lower())
                    title_text = None
                    if title_elem:
                        title_text = title_elem.get_text(strip=True)
                    
                    # 清理card的属性
                    card_copy = BS(str(card), 'html.parser').find('div')
                    if not card_copy:
                        continue
                    
                    for tag in card_copy.find_all(True):
                        attrs_to_keep = ['class', 'id', 'href', 'src', 'colspan', 'rowspan', 'target', 'rel', 'data-uniquefileid', 'onclick']
                        tag.attrs = {k: v for k, v in tag.attrs.items() if k in attrs_to_keep}
                    
                    # 如果有标题，去重：保留内容更多的
                    if title_text:
                        if title_text in seen_titles:
                            existing_card, existing_len = seen_titles[title_text]
                            if len(card_text) > existing_len:
                                # 移除旧的，添加新的
                                if existing_card in wrapper.contents:
                                    existing_card.extract()
                                wrapper.append(card_copy)
                                seen_titles[title_text] = (card_copy, len(card_text))
                        else:
                            wrapper.append(card_copy)
                            seen_titles[title_text] = (card_copy, len(card_text))
                    else:
                        # 没有标题的card，如果内容足够长或包含特殊内容，也添加
                        has_special = (card.find('div', class_=lambda x: x and 'product-container' in str(x).lower()) or
                                     card.find('table') or
                                     len(card_text) > 30)
                        if has_special:
                            wrapper.append(card_copy)
                
                content_html = str(wrapper)
            
            # 如果还没有content_html，尝试其他方法
            if not content_html:
                # 尝试查找article-detail-card-content
                article_content_div = main_container.find('div', class_=lambda x: x and 'detail-card-content' in str(x).lower())
                
                if article_content_div:
                    # 清理属性
                    for tag in article_content_div.find_all(True):
                        attrs_to_keep = ['class', 'id', 'href', 'src', 'colspan', 'rowspan', 'target', 'rel', 'data-uniquefileid', 'onclick']
                        tag.attrs = {k: v for k, v in tag.attrs.items() if k in attrs_to_keep}
                    content_html = str(article_content_div)
                else:
                    # 使用整个容器
                    for tag in main_container.find_all(True):
                        attrs_to_keep = ['class', 'id', 'href', 'src', 'colspan', 'rowspan', 'target', 'rel', 'data-uniquefileid', 'onclick']
                        tag.attrs = {k: v for k, v in tag.attrs.items() if k in attrs_to_keep}
                    content_html = str(main_container)
            
            content = content_html if content_html else ""
            
            # 清理内容中的特殊字符和不可见字符
            if content:
                content = clean_html_content(content)
            
            # 下载并本地化图片（默认启用）
            if content:
                try:
                    content, image_mapping = download_and_localize_images(content, kb_number, session)
                    if image_mapping:
                        # 记录下载的图片数量（可选）
                        pass
                except Exception as e:
                    # 图片下载失败不影响主流程，只记录警告
                    print(f"图片下载警告 (KB {kb_number}): {e}")
                    pass
            
            # 下载并本地化附件（默认启用）
            # 使用原始的soup对象（包含script标签），因为附件下载需要script中的API配置
            if content:
                try:
                    content, attachment_mapping = download_and_localize_attachments(content, kb_number, session, original_soup)
                    if attachment_mapping:
                        # 记录下载的附件数量
                        print(f"[KB {kb_number}] 成功下载 {len(attachment_mapping)} 个附件")
                    else:
                        print(f"[KB {kb_number}] 未找到或未下载任何附件")
                except Exception as e:
                    # 附件下载失败不影响主流程，但输出警告
                    print(f"[KB {kb_number}] 附件下载异常: {e}")
                    if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                        import traceback
                        traceback.print_exc()
        
            return {
                'kb_number': kb_number,
                'title': title[:1000],  # 增加标题长度限制
                'content': content[:200000],  # 大幅增加内容长度限制（200KB）
                'article_id': article_id[:100] if article_id else None,
                'updated_date': updated_date[:100] if updated_date else None,
                'url': url,
                'products': products[:20]  # 增加产品数量限制
            }
            
        except requests.exceptions.Timeout:
            # 超时异常，如果是最后一次尝试，返回None；否则重试
            if attempt < max_retries - 1:
                continue
            return None
        except requests.exceptions.ConnectionError:
            # 连接错误，如果是最后一次尝试，返回None；否则重试
            if attempt < max_retries - 1:
                continue
            return None
        except requests.exceptions.RequestException as e:
            # 其他网络请求异常，如果是最后一次尝试，返回None；否则重试
            if attempt < max_retries - 1:
                continue
            return None
        except Exception as e:
            # 其他异常（解析错误等），不重试，直接返回None
            # 添加调试信息（仅在开发时启用）
            if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
                import traceback
                print(f"解析异常 (KB {kb_number}): {e}")
                traceback.print_exc()
            return None
    
    # 所有重试都失败
    return None


def crawl_single_kb(kb_number, existing_kb_set, product_cache, db_lock):
    """爬取单个KB文章（优化版：减少锁竞争）"""
    global success_count, fail_count, skip_count
    
    # 快速检查是否已存在（无锁）
    if kb_number in existing_kb_set:
        with lock:
            skip_count += 1
        return {'status': 'skipped', 'kb_number': kb_number}
    
    session = get_session()
    
    # 爬取和解析（无锁）
    try:
        data = parse_kb_page(session, kb_number)
    except Exception as e:
        # 添加异常捕获和调试信息
        if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
            import traceback
            print(f"parse_kb_page异常 (KB {kb_number}): {e}")
            traceback.print_exc()
        with lock:
            fail_count += 1
        return {'status': 'failed', 'kb_number': kb_number}
    
    if not data:
        if os.getenv('CRAWLER_DEBUG', 'false').lower() == 'true':
            print(f"parse_kb_page返回None (KB {kb_number})")
        with lock:
            fail_count += 1
        return {'status': 'failed', 'kb_number': kb_number}
    
    # 数据库操作（使用锁保护，避免并发冲突）
    db = None
    try:
        db = get_db_session()
        
        # 双重检查（可能在爬取过程中其他线程已插入）
        existing = db.query(Article).filter_by(kb_number=kb_number).first()
        if existing:
            existing_kb_set.add(kb_number)
            db.close()
            with lock:
                skip_count += 1
            return {'status': 'skipped', 'kb_number': kb_number}
        
        # 创建文章
        article = Article(
            kb_number=data['kb_number'],
            title=data['title'],
            content=data['content'],
            article_id=data['article_id'],
            updated_date=data['updated_date'],
            url=data['url']
        )
        db.add(article)
        db.flush()
        
        # 处理产品（使用缓存，减少数据库查询）
        for product_name in data['products']:
            if product_name:
                # 先查缓存
                if product_name in product_cache:
                    product = product_cache[product_name]
                else:
                    # 使用锁保护产品查询和创建
                    with db_lock:
                        product = db.query(Product).filter_by(name=product_name).first()
                        if not product:
                            product = Product(name=product_name)
                            db.add(product)
                            db.flush()
                        product_cache[product_name] = product
                article.products.append(product)
        
        # 提交事务（先提交，获取article.id）
        db.commit()
        
        existing_kb_set.add(kb_number)
        
        with lock:
            success_count += 1
        
        if db:
            db.close()
        return {'status': 'success', 'kb_number': kb_number, 'title': data['title']}
        
    except IntegrityError:
        if db:
            db.rollback()
            db.close()
        existing_kb_set.add(kb_number)
        with lock:
            skip_count += 1
        return {'status': 'skipped', 'kb_number': kb_number}
    except Exception as e:
        if db:
            db.rollback()
            db.close()
        with lock:
            fail_count += 1
        return {'status': 'failed', 'kb_number': kb_number}


def crawl_range(start, end, threads=50):
    """爬取指定范围的KB（优化版：带延迟和重试）"""
    global success_count, fail_count, skip_count
    
    # 重置计数器
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    kb_numbers = list(range(start, end + 1))
    total = len(kb_numbers)
    
    print(f"开始爬取KB {start} 到 {end}，共 {total} 篇文章，使用 {threads} 个线程（优化模式：带延迟和重试）...")
    print(f"请求延迟: {REQUEST_DELAY_MIN}-{REQUEST_DELAY_MAX}秒，最大重试次数: 3次")
    
    # 预先加载已存在的KB号（减少数据库查询）
    print("正在加载已存在的KB号...")
    db = get_db_session()
    existing_kb_set = set(db.query(Article.kb_number).all())
    existing_kb_set = {kb[0] for kb in existing_kb_set}
    db.close()
    print(f"已加载 {len(existing_kb_set)} 个已存在的KB号")
    
    # 产品缓存（线程安全）
    product_cache = {}
    product_cache_lock = threading.Lock()
    
    # 使用共享的existing_kb_set和product_cache
    def crawl_with_cache(kb_number):
        return crawl_single_kb(kb_number, existing_kb_set, product_cache, product_cache_lock)
    
    print(f"开始并发爬取...")
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=threads) as executor:
        # 提交所有任务
        futures = {executor.submit(crawl_with_cache, kb): kb for kb in kb_numbers}
        
        # 使用tqdm显示进度
        with tqdm(total=total, desc="爬取进度", unit="篇", ncols=100, mininterval=0.5) as pbar:
            completed = 0
            for future in as_completed(futures, timeout=None):
                try:
                    result = future.result(timeout=30)  # 每个任务最多等待30秒
                    pbar.update(1)
                    completed += 1
                    
                    # 计算速度
                    elapsed = time.time() - start_time
                    if elapsed > 0:
                        speed = completed / elapsed
                    else:
                        speed = 0
                    
                    # 更新进度条描述
                    pbar.set_postfix({
                        '成功': success_count,
                        '失败': fail_count,
                        '跳过': skip_count,
                        '速度': f"{speed:.1f}篇/秒"
                    })
                except Exception as e:
                    # 处理任务异常
                    pbar.update(1)
                    completed += 1
                    with lock:
                        fail_count += 1
    
    elapsed_time = time.time() - start_time
    print(f"\n爬取完成！")
    print(f"成功: {success_count}, 失败: {fail_count}, 跳过: {skip_count}")
    if success_count + fail_count + skip_count > 0:
        print(f"总耗时: {elapsed_time:.2f}秒")
        print(f"平均速度: {(success_count + fail_count + skip_count) / max(elapsed_time, 0.1):.2f}篇/秒")


def crawl_single(kb_number):
    """爬取单个KB"""
    global success_count, fail_count, skip_count
    
    success_count = 0
    fail_count = 0
    skip_count = 0
    
def crawl_single(kb_number, force_update=False):
    """爬取单个KB
    
    Args:
        kb_number: KB编号
        force_update: 如果为True，即使KB已存在也重新爬取
    """
    print(f"爬取KB {kb_number}...")
    
    # 如果强制更新，先删除已存在的KB
    if force_update:
        db = get_db_session()
        try:
            existing = db.query(Article).filter_by(kb_number=kb_number).first()
            if existing:
                db.delete(existing)
                db.commit()
                print(f"已删除KB {kb_number}，准备重新爬取...")
        finally:
            db.close()
    
    existing_kb_set = set()
    product_cache = {}
    result = crawl_single_kb(kb_number, existing_kb_set, product_cache, threading.Lock())
    
    if result['status'] == 'success':
        print(f"✓ 成功爬取KB {kb_number}: {result.get('title', '')}")
    elif result['status'] == 'skipped':
        print(f"- 跳过KB {kb_number}（已存在）")
        if not force_update:
            print("  提示: 使用 --force 参数可以强制重新爬取")
    else:
        print(f"✗ 失败KB {kb_number}")


def main():
    parser = argparse.ArgumentParser(description='VMware KB爬取工具（高性能版，无间隔，无重试）')
    parser.add_argument('--kb', type=int, help='爬取单个KB号')
    parser.add_argument('--start', type=int, help='起始KB号')
    parser.add_argument('--end', type=int, help='结束KB号')
    parser.add_argument('--threads', type=int, default=50, help='线程数（默认50，建议20-100）')
    parser.add_argument('--force', action='store_true', help='强制重新爬取（即使KB已存在）')
    
    args = parser.parse_args()
    
    # 初始化数据库
    init_db()
    
    if args.kb:
        # 爬取单个KB
        crawl_single(args.kb, force_update=args.force)
    elif args.start and args.end:
        # 爬取范围
        if args.start > args.end:
            print("错误: 起始KB号不能大于结束KB号")
            return
        crawl_range(args.start, args.end, args.threads)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
