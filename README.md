# VMware Knowledge Base

一个功能完整的VMware Knowledge Base爬取和展示系统，支持在线爬取、全文搜索、产品分类和离线浏览。

## ✨ 功能特性

- 🚀 **高效爬取**：在线爬取，支持断点续传，自动跳过已存在的KB
- 🔍 **智能搜索**：支持按KB号、标题、内容全文搜索，搜索结果按更新时间排序
- 🏷️ **产品分类**：自动提取产品标签，支持产品筛选和统计
- 📊 **数据统计**：提供KB总数、产品数量、最新KB等统计信息
- 🎨 **美观界面**：参考官方VMware KB设计的现代化Web界面
- 💾 **离线支持**：完全离线运行，所有资源本地化（Bootstrap、图片、附件等）
- ⚡ **高性能**：SQLite数据库优化，支持大量数据快速查询
- 🖼️ **图片本地化**：自动下载并本地化KB内容中的图片
- 📎 **附件下载**：自动下载KB附件区域内的附件（ZIP、PDF、文档等），支持本地访问和下载

## 🛠️ 技术栈

- **后端框架**: FastAPI
- **数据库**: SQLite (SQLAlchemy ORM)
- **前端**: Bootstrap 5 + Jinja2 模板
- **爬虫**: requests + BeautifulSoup4
- **并发**: ThreadPoolExecutor

## 📋 系统要求

- Python 3.8+
- pip

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 爬取KB文章

**爬取单个KB：**
```bash
python crawler.py --kb {kb_number}
```

**强制重新爬取（即使KB已存在）：**
```bash
python crawler.py --kb {kb_number} --force
```

**爬取指定范围：**
```bash
python crawler.py --start 1 --end 1000
```

**爬取参数说明：**
- `--kb`: 爬取单个KB号
- `--start`: 起始KB号
- `--end`: 结束KB号
- `--force`: 强制重新爬取（即使KB已存在）

**注意：** 
- 爬虫默认会自动下载并本地化KB内容中的图片
- 爬虫会自动下载KB附件区域内的附件（ZIP、PDF、文档等），保存到 `static/attachments/kb/{kb_number}/` 目录

### 3. 启动Web服务

```bash
python main.py
```

访问 `http://localhost:21000` 查看KB列表

## 📁 项目结构

```
vmware_kb/
├── crawler.py              # 多线程爬取脚本
├── main.py                 # FastAPI Web应用主文件
├── models.py               # 数据库模型定义
├── start.sh                # 启动脚本
├── requirements.txt        # Python依赖包
├── README.md               # 项目说明文档
├── templates/              # HTML模板目录
│   ├── index.html         # 首页（KB列表）
│   ├── article.html       # KB详情页
│   └── stats.html         # 统计信息页
└── static/                 # 静态文件目录
    ├── css/
    │   └── style.css      # 自定义样式
    ├── images/
    │   └── kb/            # KB图片（自动下载）
    ├── attachments/
    │   └── kb/            # KB附件（自动下载）
    └── vendor/
        └── bootstrap/     # Bootstrap本地文件
```

## 🗄️ 数据库结构

- **articles**: 存储KB文章信息（标题、内容、更新时间等）
- **products**: 存储产品名称
- **article_products**: 文章和产品的多对多关系表

## 📖 使用说明

### 搜索功能

- 支持按KB号精确搜索
- 支持按标题和内容全文搜索
- 搜索结果按更新时间排序（最新的在前）

### 产品筛选

- 在首页可以按产品筛选KB文章
- 统计页面显示各产品的KB数量

### 分页浏览

- 支持首页、尾页、上一页、下一页
- 支持直接跳转到指定页码
- 每页显示20条记录

### 附件下载

- 爬虫会自动下载KB附件区域内的附件（ZIP、PDF、文档等）
- 附件保存到 `static/attachments/kb/{kb_number}/` 目录
- 在阅读界面，附件显示为可点击的蓝色标签样式（参考product-chip设计）
- 点击附件标签即可下载到本地
- 附件下载功能只处理"Attachments"标题下的附件区域，确保只下载真正的附件

## ⚠️ 注意事项

1. **爬取频率**：请控制爬取频率，避免对目标服务器造成压力。默认每个请求间隔0.1-0.3秒。

2. **断点续传**：已存在的KB号会自动跳过，支持中断后继续爬取。

3. **数据库锁定**：多线程爬取时，数据库操作已做线程安全处理。

4. **图片下载**：图片下载功能默认启用，会自动下载并本地化KB内容中的图片。

5. **附件下载**：
   - 附件下载功能默认启用，会自动下载KB附件区域内的附件
   - 附件文件保存在 `static/attachments/kb/{kb_number}/` 目录
   - 支持的附件类型：ZIP、PDF、DOC、DOCX、XLS、XLSX、TXT等
   - 附件大小限制：最大100MB
   - 如果附件下载失败，不会影响KB文章的爬取

6. **数据量**：建议定期备份 `kb.db` 数据库文件。

7. **强制更新**：如果KB已存在但需要重新爬取（例如更新附件），可以使用 `--force` 参数。

## 📄 许可证

MIT License

