"""
数据库模型定义（多表结构，禁用WAL模式）
"""
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey, Table, text, Index, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

Base = declarative_base()

# 多对多关系表
article_products = Table(
    'article_products',
    Base.metadata,
    Column('article_id', Integer, ForeignKey('articles.id'), primary_key=True),
    Column('product_id', Integer, ForeignKey('products.id'), primary_key=True)
)


class Article(Base):
    """KB文章模型"""
    __tablename__ = 'articles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    kb_number = Column(Integer, unique=True, nullable=False, index=True)
    title = Column(Text, nullable=False)
    content = Column(Text)
    article_id = Column(String(50))  # 官方Article ID
    updated_date = Column(String(50))  # 更新时间字符串
    created_at = Column(DateTime, default=datetime.now, index=True)
    url = Column(Text)

    # 多对多关系
    products = relationship('Product', secondary=article_products, back_populates='articles')

    # 注意：SQLite对Text类型的索引需要在init_db()中手动创建表达式索引
    # 表级索引定义对Text类型支持有限，因此不在__table_args__中定义

    def __repr__(self):
        return f"<Article(kb_number={self.kb_number}, title={self.title[:50]}...)>"


class Product(Base):
    """产品模型"""
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), unique=True, nullable=False, index=True)

    # 多对多关系
    articles = relationship('Article', secondary=article_products, back_populates='products')

    def __repr__(self):
        return f"<Product(name={self.name})>"


# 数据库连接和会话管理
# 获取项目根目录（models.py 所在的目录）
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 数据库文件路径（使用绝对路径，避免工作目录问题）
DATABASE_PATH = os.path.join(BASE_DIR, "kb.db")
# 确保数据库文件所在目录存在
os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)

DATABASE_URL = f"sqlite:///{DATABASE_PATH}"
# 禁用WAL模式，使用传统DELETE模式（只生成单个.db文件）
engine = create_engine(
    DATABASE_URL, 
    connect_args={
        "check_same_thread": False,
        "timeout": 30  # 增加超时时间
    },
    pool_pre_ping=True,  # 连接前检查连接是否有效
    pool_size=20,  # 连接池大小
    max_overflow=40  # 最大溢出连接数
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """初始化数据库，创建所有表（禁用WAL模式）"""
    Base.metadata.create_all(bind=engine)
    # 禁用WAL模式，使用DELETE模式（只生成单个.db文件，不生成-shm和-wal文件）
    with engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=DELETE;"))
        conn.execute(text("PRAGMA synchronous=NORMAL;"))
        conn.execute(text("PRAGMA cache_size=10000;"))
        conn.execute(text("PRAGMA temp_store=MEMORY;"))
        
        # 创建额外的性能优化索引（如果不存在）
        # SQLite对Text类型的索引需要特殊处理，使用表达式索引
        try:
            # title搜索索引（使用COLLATE NOCASE支持大小写不敏感搜索）
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_title_search 
                ON articles(title COLLATE NOCASE)
            """))
            
            # updated_date索引（如果表级索引未创建）
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_updated_date 
                ON articles(updated_date)
            """))
            
            # created_at索引（如果表级索引未创建）
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_created_at 
                ON articles(created_at)
            """))
            
            # kb_number降序索引（用于分页排序优化）
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_kb_number_desc 
                ON articles(kb_number DESC)
            """))
            
            # 复合索引：kb_number + created_at（用于常见查询组合）
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_kb_created 
                ON articles(kb_number DESC, created_at DESC)
            """))
            
        except Exception as e:
            # 索引可能已存在，忽略错误
            print(f"索引创建警告: {e}")
        
        # 初始化FTS5全文索引
        init_fts5(conn)
        
        conn.commit()
    print("数据库初始化完成（DELETE模式，多表结构，已优化索引，FTS5全文搜索）")


def init_fts5(conn=None):
    """
    初始化FTS5全文索引表
    
    Args:
        conn: 数据库连接对象，如果为None则创建新连接
    """
    if conn is None:
        conn = engine.connect()
        should_close = True
    else:
        should_close = False
    
    try:
        # 检查是否已有数据
        result = conn.execute(text("SELECT COUNT(*) FROM articles")).scalar()
        
        # 创建FTS5虚拟表
        conn.execute(text("""
            CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
                kb_number UNINDEXED,
                title,
                content,
                content_rowid=id
            )
        """))
        
        # 如果已有数据但FTS5表为空，填充数据
        if result > 0:
            fts_count = conn.execute(text("SELECT COUNT(*) FROM articles_fts")).scalar()
            if fts_count == 0:
                print(f"正在填充FTS5全文索引（{result}条记录）...")
                conn.execute(text("""
                    INSERT INTO articles_fts(rowid, kb_number, title, content)
                    SELECT id, kb_number, title, COALESCE(content, '') FROM articles
                """))
                print("FTS5全文索引填充完成")
            elif fts_count < result:
                # FTS5表记录数少于articles表，需要补充数据
                print(f"检测到FTS5表不完整（{fts_count}/{result}），正在补充数据...")
                conn.execute(text("""
                    INSERT INTO articles_fts(rowid, kb_number, title, content)
                    SELECT id, kb_number, title, COALESCE(content, '') 
                    FROM articles 
                    WHERE id NOT IN (SELECT rowid FROM articles_fts)
                """))
                print("FTS5全文索引补充完成")
    except Exception as e:
        print(f"FTS5初始化警告: {e}")
    finally:
        if should_close:
            conn.close()


def check_fts5_status(db_session=None):
    """
    检查FTS5表状态
    
    Args:
        db_session: 数据库会话，如果为None则创建新会话
        
    Returns:
        dict: 包含FTS5状态信息的字典
    """
    if db_session is None:
        db = SessionLocal()
        should_close = True
    else:
        db = db_session
        should_close = False
    
    try:
        # 检查FTS5表是否存在
        try:
            fts_count = db.execute(text("SELECT COUNT(*) FROM articles_fts")).scalar()
            articles_count = db.query(func.count(Article.id)).scalar()
            return {
                'exists': True,
                'fts_count': fts_count,
                'articles_count': articles_count,
                'is_synced': fts_count == articles_count,
                'is_empty': fts_count == 0
            }
        except Exception:
            return {
                'exists': False,
                'fts_count': 0,
                'articles_count': db.query(func.count(Article.id)).scalar(),
                'is_synced': False,
                'is_empty': True
            }
    finally:
        if should_close:
            db.close()


def get_db():
    """获取数据库会话（用于依赖注入）"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session():
    """获取数据库会话（用于爬虫脚本）"""
    return SessionLocal()
