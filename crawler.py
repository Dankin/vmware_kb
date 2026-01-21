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
                    attrs_to_keep = ['class', 'id', 'href', 'src', 'colspan', 'rowspan', 'target', 'rel']
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
                        attrs_to_keep = ['class', 'id', 'href', 'src', 'colspan', 'rowspan', 'target', 'rel']
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
                        attrs_to_keep = ['class', 'id', 'href', 'src', 'colspan', 'rowspan', 'target', 'rel']
                        tag.attrs = {k: v for k, v in tag.attrs.items() if k in attrs_to_keep}
                    content_html = str(article_content_div)
                else:
                    # 使用整个容器
                    for tag in main_container.find_all(True):
                        attrs_to_keep = ['class', 'id', 'href', 'src', 'colspan', 'rowspan', 'target', 'rel']
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
    
    print(f"爬取KB {kb_number}...")
    existing_kb_set = set()
    product_cache = {}
    result = crawl_single_kb(kb_number, existing_kb_set, product_cache, threading.Lock())
    
    if result['status'] == 'success':
        print(f"✓ 成功爬取KB {kb_number}: {result.get('title', '')}")
    elif result['status'] == 'skipped':
        print(f"- 跳过KB {kb_number}（已存在）")
    else:
        print(f"✗ 失败KB {kb_number}")


def main():
    parser = argparse.ArgumentParser(description='VMware KB爬取工具（高性能版，无间隔，无重试）')
    parser.add_argument('--kb', type=int, help='爬取单个KB号')
    parser.add_argument('--start', type=int, help='起始KB号')
    parser.add_argument('--end', type=int, help='结束KB号')
    parser.add_argument('--threads', type=int, default=50, help='线程数（默认50，建议20-100）')
    
    args = parser.parse_args()
    
    # 初始化数据库
    init_db()
    
    if args.kb:
        # 爬取单个KB
        crawl_single(args.kb)
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
