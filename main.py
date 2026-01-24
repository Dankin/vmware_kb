"""
FastAPI Web应用主文件（多表结构版本）
"""
from fastapi import FastAPI, Request, Query, Depends, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_, and_, func, text, case, cast, String
from models import get_db, Article, Product, init_db, init_fts5, check_fts5_status
from typing import Optional, List
from urllib.parse import urlencode
import math
import base64

app = FastAPI(title="VMware KB展示系统", docs_url=None, redoc_url=None)

# 模板和静态文件
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# 初始化数据库
init_db()


def encode_cursor(kb_number: int) -> str:
    """编码游标（kb_number转base64）"""
    return base64.urlsafe_b64encode(str(kb_number).encode()).decode().rstrip('=')


def decode_cursor(cursor: str) -> Optional[int]:
    """解码游标（base64转kb_number）"""
    try:
        # 补齐padding
        padding = 4 - len(cursor) % 4
        cursor += '=' * padding
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except:
        return None


def _search_with_fts5(db: Session, search_term: str, base_query=None, limit=5000):
    """
    使用FTS5进行全文搜索
    
    Args:
        db: 数据库会话
        search_term: 搜索关键词
        base_query: 基础查询对象（用于产品筛选等）
        limit: 限制返回结果数量（优化性能，避免返回过多结果）
        
    Returns:
        匹配的Article ID列表，如果FTS5不可用则返回None
    """
    try:
        # 转义FTS5特殊字符
        # FTS5特殊字符: " ' * + - : AND OR NOT
        escaped_term = search_term.replace('"', '""').replace("'", "''")
        
        # 构建FTS5查询：搜索title和content字段
        # 使用OR连接，匹配title或content中包含关键词的记录
        fts_query = f'"{escaped_term}" OR title:"{escaped_term}" OR content:"{escaped_term}"'
        
        # 查询FTS5表获取匹配的rowid（对应articles表的id）
        # 限制返回数量，避免返回过多结果影响性能
        fts_result = db.execute(text(f"""
            SELECT rowid FROM articles_fts 
            WHERE articles_fts MATCH :query
            ORDER BY rank
            LIMIT :limit
        """), {"query": fts_query, "limit": limit})
        
        article_ids = [row[0] for row in fts_result]
        
        # 如果base_query有产品筛选，需要进一步过滤
        if base_query is not None:
            # 先获取产品筛选后的ID集合
            product_filtered_query = base_query.with_entities(Article.id)
            product_filtered_ids = set(row[0] for row in product_filtered_query.all())
            # 取交集
            article_ids = [aid for aid in article_ids if aid in product_filtered_ids]
        
        return article_ids
    except Exception as e:
        # FTS5不可用或查询失败，返回None，让调用者回退到LIKE搜索
        return None


def query_articles(
    db: Session,
    search: Optional[str] = None,
    product_ids: Optional[List[int]] = None,
    cursor: Optional[str] = None,
    page: Optional[int] = None,
    per_page: int = 20
):
    """
    查询文章（使用offset分页 + LIKE全文匹配）
    
    优化点：
    1. 使用offset分页，支持所有页码跳转
    2. 使用LIKE全文匹配进行内容搜索
    3. 确保使用索引进行排序（kb_number DESC）
    """
    
    # 构建基础查询
    base_query = db.query(Article)
    
    # 搜索条件 - 优先使用FTS5全文搜索，回退到LIKE搜索
    fts5_article_ids = None
    if search:
        search_clean = search.strip()
        if search_clean:
            # 优化：如果搜索词是纯数字，优先使用kb_number精确匹配（使用索引，最快）
            if search_clean.isdigit():
                # 数字搜索：优先精确匹配kb_number（使用唯一索引，O(1)查找）
                search_filter = Article.kb_number == int(search_clean)
                base_query = base_query.filter(search_filter)
            else:
                # 文本搜索：优先使用FTS5全文搜索
                # 先构建基础查询（包含产品筛选）
                temp_base_query = db.query(Article)
                if product_ids:
                    temp_base_query = temp_base_query.join(Article.products).filter(Product.id.in_(product_ids))
                
                # 尝试使用FTS5搜索
                fts5_article_ids = _search_with_fts5(db, search_clean, temp_base_query if product_ids else None)
                
                if fts5_article_ids is not None and len(fts5_article_ids) > 0:
                    # FTS5搜索成功，使用FTS5结果
                    # 同时也要搜索kb_number（FTS5中kb_number是UNINDEXED）
                    # 优化：合并查询，减少数据库往返
                    kb_number_ids = []
                    if search_clean.isdigit() or any(c.isdigit() for c in search_clean):
                        # 如果搜索词包含数字，也搜索kb_number
                        kb_number_matches = db.query(Article.id).filter(
                            Article.kb_number.like(f"%{search_clean}%")
                        ).all()
                        kb_number_ids = [row[0] for row in kb_number_matches]
                    
                    # 合并FTS5结果和kb_number匹配结果
                    all_matched_ids = list(set(fts5_article_ids + kb_number_ids))
                    
                    # 如果有产品筛选，在base_query中应用，而不是单独查询
                    if product_ids:
                        # 先过滤产品，再过滤ID
                        base_query = base_query.join(Article.products).filter(Product.id.in_(product_ids))
                        base_query = base_query.filter(Article.id.in_(all_matched_ids)).distinct()
                    else:
                        # 直接使用IN查询过滤
                        base_query = base_query.filter(Article.id.in_(all_matched_ids))
                else:
                    # FTS5不可用或没有结果，回退到LIKE搜索
                    search_filter = or_(
                        Article.kb_number.like(f"%{search_clean}%"),
                        Article.title.like(f"%{search_clean}%"),
                        Article.content.like(f"%{search_clean}%")
                    )
                    base_query = base_query.filter(search_filter)
    
    # 产品筛选
    if product_ids:
        base_query = base_query.join(Article.products).filter(Product.id.in_(product_ids))
        base_query = base_query.distinct()
    
    # Offset分页：支持所有页码跳转
    if page is None:
        page = 1
    
    offset = (page - 1) * per_page
    
    # 分页处理：使用offset分页
    # 如果有搜索条件，按时间排序（由新往旧）；否则按kb_number排序
    if search:
        # 搜索结果按时间排序（由新往旧）
        # 优化：如果FTS5返回的结果数量较少，直接排序；否则先限制数量再排序
        if fts5_article_ids is not None and len(fts5_article_ids) > 0:
            # FTS5搜索：先对结果进行排序，再分页
            # 使用updated_date排序（KB的更新时间），但优化为使用索引字段
            # 如果updated_date可用，优先使用；否则使用created_at
            order_by_expr = text("""
                CASE 
                    WHEN updated_date IS NOT NULL AND updated_date != 'N/A' AND updated_date != ''
                    THEN (
                        substr(updated_date, 7, 4) || '-' || 
                        substr(updated_date, 1, 2) || '-' || 
                        substr(updated_date, 4, 2) || ' ' || 
                        substr(updated_date, 12, 5)
                    )
                    ELSE '1970-01-01 00:00'
                END DESC
            """)
            # 限制排序的数据量（最多5000条），避免排序过多数据
            max_sort_count = min(len(fts5_article_ids), 5000)
            sorted_ids_query = db.query(Article.id).filter(
                Article.id.in_(fts5_article_ids[:max_sort_count])
            ).order_by(order_by_expr)
            sorted_ids = [row[0] for row in sorted_ids_query.all()]
            # 分页
            article_ids = sorted_ids[offset:offset + per_page]
            order_by_clause = order_by_expr
        else:
            # LIKE搜索：使用created_at排序（有索引，更快）
            article_ids_query = base_query.with_entities(Article.id).order_by(Article.created_at.desc())
            article_ids = [row[0] for row in article_ids_query.offset(offset).limit(per_page).all()]
            order_by_clause = Article.created_at.desc()
        
        # 根据ID加载完整数据（包括products）
        if article_ids:
            articles = db.query(Article).filter(Article.id.in_(article_ids)).options(joinedload(Article.products)).all()
            # 保持排序顺序（按article_ids的顺序）
            article_dict = {a.id: a for a in articles}
            articles = [article_dict[aid] for aid in article_ids if aid in article_dict]
        else:
            articles = []
    else:
        # 无搜索条件，按kb_number排序（保持原有逻辑）
        articles_query = base_query.options(joinedload(Article.products)).order_by(Article.kb_number.desc())
        articles = articles_query.offset(offset).limit(per_page).all()
        order_by_clause = Article.kb_number.desc()
    
    # 计算是否有下一页（查询per_page+1条判断）
    has_next_query = base_query.order_by(order_by_clause)
    has_next_check = has_next_query.offset(offset + per_page).limit(1).first()
    has_next = has_next_check is not None
    
    current_page = page
    
    # 计算总数（用于显示和分页）
    if not search and not product_ids:
        total = db.query(func.count(Article.id)).scalar()
    else:
        # 有筛选条件：优化count查询
        # 如果base_query已经包含了所有过滤条件，直接使用它（避免重复查询）
        if search and fts5_article_ids is not None and len(fts5_article_ids) > 0:
            # FTS5搜索已应用，base_query已经包含了所有过滤条件
            total = base_query.count()
        else:
            # 构建count查询
            count_query = db.query(Article)
            if search:
                search_clean = search.strip()
                if search_clean:
                    if search_clean.isdigit():
                        count_query = count_query.filter(Article.kb_number == int(search_clean))
                    else:
                        # 回退到LIKE搜索
                        search_filter = or_(
                            Article.kb_number.like(f"%{search_clean}%"),
                            Article.title.like(f"%{search_clean}%"),
                            Article.content.like(f"%{search_clean}%")
                        )
                        count_query = count_query.filter(search_filter)
            if product_ids:
                count_query = count_query.join(Article.products).filter(Product.id.in_(product_ids)).distinct()
            total = count_query.count()
    
    # 计算总页数（用于offset分页）
    total_pages = math.ceil(total / per_page) if total > 0 else 1
    
    return {
        'articles': articles,
        'total': total,
        'per_page': per_page,
        'has_next': has_next,
        'cursor': None,
        'next_cursor': None,
        'page': current_page,
        'total_pages': total_pages,
        'use_cursor': False
    }


def get_all_products(db: Session):
    """获取所有产品列表"""
    return db.query(Product).order_by(Product.name).all()


@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    search: Optional[str] = Query(None, description="搜索关键词"),
    product: Optional[str] = Query(None, description="产品ID（多个用逗号分隔）"),
    cursor: Optional[str] = Query(None, description="游标（已废弃，保留兼容性）"),
    page: Optional[int] = Query(None, ge=1, description="页码"),
    db: Session = Depends(get_db)
):
    """首页 - KB列表（offset分页）"""
    # 解析产品ID列表
    product_ids = None
    if product:
        try:
            product_ids = [int(p.strip()) for p in product.split(',') if p.strip()]
        except ValueError:
            product_ids = None
    
    # 查询文章（混合分页）
    result = query_articles(db, search=search, product_ids=product_ids, cursor=cursor, page=page)
    
    # 构建查询参数字符串（用于分页URL）
    def build_query_string(next_cursor=None, page_num=None):
        params = {}
        if search:
            params["search"] = search
        if next_cursor:
            params["cursor"] = next_cursor
        elif page_num:
            params["page"] = page_num
        return urlencode(params) if params else ""
    
    # 构建分页URL
    def build_page_url(page_num):
        return build_query_string(page_num=page_num)
    
    def build_next_url():
        current = result.get('page', 1)
        return build_query_string(page_num=current + 1)
    
    def build_prev_url():
        current = result.get('page', 1)
        if current > 1:
            return build_query_string(page_num=current - 1)
        return build_query_string()
    
    # 生成分页页码列表（仅用于offset分页）
    def get_pagination_pages(current_page, total_pages):
        """生成分页页码列表，每次只显示10页"""
        # 计算显示的页码范围
        max_pages = 10  # 最多显示10页
        
        if total_pages <= max_pages:
            # 如果总页数不超过10页，显示所有页
            return list(range(1, total_pages + 1))
        
        # 计算起始页和结束页
        # 尽量让当前页在中间
        half = max_pages // 2
        start_page = max(1, current_page - half)
        end_page = min(total_pages, start_page + max_pages - 1)
        
        # 如果结束页接近总页数，调整起始页
        if end_page - start_page < max_pages - 1:
            start_page = max(1, end_page - max_pages + 1)
        
        return list(range(start_page, end_page + 1))
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "articles": result['articles'],
        "total": result['total'],
        "per_page": result['per_page'],
        "has_next": result['has_next'],
        "cursor": result['cursor'],
        "next_cursor": result['next_cursor'],
        "page": result.get('page'),
        "total_pages": result.get('total_pages', 1),
        "use_cursor": result.get('use_cursor', False),
        "search": search or "",
        "build_page_url": build_page_url,
        "build_next_url": build_next_url,
        "build_prev_url": build_prev_url,
        "pagination_pages": get_pagination_pages(result.get('page', 1), result.get('total_pages', 1)) if result.get('page') else []
    })


@app.get("/article/{kb_number}", response_class=HTMLResponse)
async def article_detail(
    request: Request,
    kb_number: int,
    db: Session = Depends(get_db)
):
    """文章详情页"""
    article = db.query(Article).options(joinedload(Article.products)).filter_by(kb_number=kb_number).first()
    
    if not article:
        raise HTTPException(status_code=404, detail="文章不存在")
    
    return templates.TemplateResponse("article.html", {
        "request": request,
        "article": article
    })


@app.get("/stats", response_class=HTMLResponse)
async def stats(request: Request, db: Session = Depends(get_db)):
    """统计页面 - 显示产品分类统计"""
    # 获取总KB数
    total_articles = db.query(func.count(Article.id)).scalar()
    
    # 获取产品数
    total_products = db.query(func.count(Product.id)).scalar()
    
    # 获取最新KB编号
    latest_kb = db.query(func.max(Article.kb_number)).scalar()
    latest_kb_number = latest_kb if latest_kb else 0
    
    # 获取产品分类统计
    product_stats = db.query(
        Product.id,
        Product.name,
        func.count(Article.id).label('article_count')
    ).join(
        Product.articles
    ).group_by(
        Product.id,
        Product.name
    ).order_by(
        func.count(Article.id).desc()
    ).all()
    
    return templates.TemplateResponse("stats.html", {
        "request": request,
        "total_articles": total_articles,
        "total_products": total_products,
        "latest_kb_number": latest_kb_number,
        "product_stats": product_stats
    })


@app.get("/api/search")
async def api_search(
    search: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    cursor: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """API搜索接口（返回JSON，游标分页）"""
    product_ids = None
    if product:
        try:
            product_ids = [int(p.strip()) for p in product.split(',') if p.strip()]
        except ValueError:
            product_ids = None
    
    result = query_articles(db, search=search, product_ids=product_ids, cursor=cursor)
    
    return {
        "articles": [
            {
                "kb_number": a.kb_number,
                "title": a.title,
                "article_id": a.article_id,
                "updated_date": a.updated_date,
                "url": a.url,
                "products": [p.name for p in a.products]
            }
            for a in result['articles']
        ],
        "total": result['total'],
        "per_page": result['per_page'],
        "has_next": result['has_next'],
        "cursor": result['cursor'],
        "next_cursor": result['next_cursor']
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=21000, reload=True)
