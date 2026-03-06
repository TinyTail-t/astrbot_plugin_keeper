from contextlib import contextmanager
from datetime import datetime
from typing import List, Optional, Dict, Any, Union, Tuple

from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime,
    Boolean, ForeignKey, Table, func, text, and_
)
from sqlalchemy.orm import (
    declarative_base, relationship, sessionmaker,
    joinedload, Session, Query
)

# ==================== 数据库配置 ====================

Base = declarative_base()

# 多对多关联表：记录 <-> 标签
record_tags = Table(
    'record_tags',
    Base.metadata,
    Column('record_id', Integer, ForeignKey('records.id', ondelete='CASCADE'), primary_key=True),
    Column('tag_id', Integer, ForeignKey('tags.id', ondelete='CASCADE'), primary_key=True)
)


# ==================== 模型定义 ====================

class User(Base):
    """用户模型"""
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(100), nullable=False, unique=True)  # 外部唯一标识符
    created_at = Column(DateTime, default=datetime.now)

    # 关系（用于级联删除）
    categories = relationship("Category", back_populates="user", cascade="all, delete-orphan")
    tags = relationship("Tag", back_populates="user", cascade="all, delete-orphan")
    records = relationship("Record", back_populates="user", cascade="all, delete-orphan")

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            # "user_id": self.user_id,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }


class Category(Base):
    """分类模型 - 树形结构"""
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    parent_id = Column(Integer, ForeignKey('categories.id', ondelete='SET NULL'), nullable=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(DateTime, default=datetime.now)

    # 自引用关系
    children = relationship(
        "Category",
        back_populates="parent",
        cascade="all, delete-orphan",
        lazy="select"
    )
    parent = relationship("Category", back_populates="children", remote_side=[id])

    # 关联的用户和记录
    user = relationship("User", back_populates="categories")
    records = relationship("Record", back_populates="category", cascade="all, delete-orphan")

    def _get_path(self) -> str:
        """获取分类路径，如：餐饮/外卖/午餐"""
        if self.parent:
            return f"{self.parent._get_path()}/{self.name}"
        return self.name

    def _get_all_children_ids(self) -> List[int]:
        """获取所有子分类ID（包括自身）"""
        ids = [self.id]
        for child in self.children:
            ids.extend(child._get_all_children_ids())
        return ids

    def to_dict(self, include_children: bool = False) -> Dict:
        result = {
            "id": self.id,
            "name": self.name,
            "parent_id": self.parent_id,
            # "user_id": self.user_id,
            "path": self._get_path(),
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
        if include_children:
            result["children"] = [c.to_dict(True) for c in self.children]
        return result


class Tag(Base):
    """标签模型"""
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    color = Column(String(7), default="#1890ff")
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    created_at = Column(DateTime, default=datetime.now)

    # 多对多关系
    records = relationship(
        "Record",
        secondary=record_tags,
        back_populates="tags"
    )
    user = relationship("User", back_populates="tags")

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "color": self.color,
            # "user_id": self.user_id,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }


class Record(Base):
    """记账记录模型"""
    __tablename__ = 'records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    time = Column(DateTime, nullable=False, default=datetime.now)
    description = Column(String(500), nullable=True)
    amount = Column(Float, nullable=False)
    is_expense = Column(Boolean, default=True)  # True=支出, False=收入
    category_id = Column(Integer, ForeignKey('categories.id', ondelete='SET NULL'), nullable=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 关系
    category = relationship("Category", back_populates="records")
    tags = relationship(
        "Tag",
        secondary=record_tags,
        back_populates="records",
        lazy="select"
    )
    user = relationship("User", back_populates="records")

    def to_dict(self, include_relations: bool = False) -> Dict:
        result = {
            "id": self.id,
            "time": self.time.isoformat() if self.time else None,
            "description": self.description,
            "amount": self.amount,
            "is_expense": self.is_expense,
            "category_id": self.category_id,
            # "user_id": self.user_id,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }
        if include_relations:
            result["category"] = self.category.to_dict() if self.category else None
            result["tags"] = [t.to_dict() for t in self.tags]
        return result


# ==================== 数据库管理类 ====================

class DatabaseManager:
    """数据库管理类 - 对外提供所有接口（多用户支持）"""

    def __init__(self, database_url: str = "sqlite:///accounting.db", echo: bool = False):
        self.engine = create_engine(database_url, echo=echo, future=True)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)

    @contextmanager
    def get_session(self) -> Session:
        """获取数据库会话的上下文管理器"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    # ==================== 用户接口 ====================

    def create_user(self, user_id: str) -> Dict:
        """创建用户"""
        with self.get_session() as session:
            user = User(user_id=user_id)
            session.add(user)
            session.flush()
            session.refresh(user)
            return user.to_dict()

    def get_user(self, user_id: str) -> Optional[Dict]:
        """根据外部标识符获取用户信息"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if user:
                return user.to_dict()
            return None

    def get_user_by_id(self, user_db_id: int) -> Optional[Dict]:
        """根据数据库内部ID获取用户信息"""
        with self.get_session() as session:
            user = session.get(User, user_db_id)
            if user:
                return user.to_dict()
            return None

    def update_user(self, user_id: str, **kwargs) -> Optional[Dict]:
        """更新用户信息（通常只允许修改外部标识符）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return None
            for key, value in kwargs.items():
                if hasattr(user, key):
                    setattr(user, key, value)
            session.flush()
            session.refresh(user)
            return user.to_dict()

    def delete_user(self, user_id: str) -> bool:
        """
        删除用户及其所有关联数据（分类、标签、记录）
        由于外键设置了 ondelete='CASCADE'，数据库会自动级联删除
        """
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return False
            session.delete(user)
            return True

    def list_users(self) -> List[Dict]:
        """列出所有用户"""
        with self.get_session() as session:
            users = session.query(User).all()
            return [u.to_dict() for u in users]

    # ==================== 分类接口 ====================

    def create_category(self, user_id: str, name: str, parent_id: Optional[int] = None) -> Dict:
        """创建分类（需指定所属用户）"""
        with self.get_session() as session:
            # 获取用户数据库ID
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                raise ValueError(f"User {user_id} not found")

            # 如果指定了父分类，需验证父分类属于同一用户
            if parent_id is not None:
                parent = session.query(Category).filter(
                    Category.id == parent_id,
                    Category.user_id == user.id
                ).first()
                if not parent:
                    raise ValueError("Parent category does not exist or does not belong to the user")

            category = Category(name=name, parent_id=parent_id, user_id=user.id)
            session.add(category)
            session.flush()
            session.refresh(category)
            return category.to_dict()

    def get_category(self, user_id: str, category_id: int) -> Optional[Dict]:
        """获取单个分类（需验证用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return None
            category = session.query(Category).filter(
                Category.id == category_id,
                Category.user_id == user.id
            ).first()
            if category:
                return category.to_dict()
            return None

    def get_category_tree(self, user_id: str, root_id: Optional[int] = None) -> List[Dict]:
        """获取分类树（仅限指定用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return []

            if root_id:
                root = session.query(Category).filter(
                    Category.id == root_id,
                    Category.user_id == user.id
                ).first()
                if not root:
                    return []
                result = [root.to_dict(include_children=True)]
                return result
            else:
                # 获取所有根分类
                roots = session.query(Category).filter(
                    Category.parent_id.is_(None),
                    Category.user_id == user.id
                ).all()
                result = [r.to_dict(include_children=True) for r in roots]
                return result

    def update_category(self, user_id: str, category_id: int, **kwargs) -> Optional[Dict]:
        """更新分类（需验证用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return None
            category = session.query(Category).filter(
                Category.id == category_id,
                Category.user_id == user.id
            ).first()
            if not category:
                return None
            for key, value in kwargs.items():
                if hasattr(category, key):
                    setattr(category, key, value)
            session.flush()
            session.refresh(category)
            return category.to_dict()

    def delete_category(self, user_id: str, category_id: int, force: bool = False) -> bool:
        """
        删除分类（需验证用户）
        force=True: 强制删除，将子分类上移或删除，关联记录设为NULL
        force=False: 如果有子分类或关联记录，禁止删除
        """
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return False
            category = session.query(Category).filter(
                Category.id == category_id,
                Category.user_id == user.id
            ).first()
            if not category:
                return False

            # 检查是否有子分类
            if category.children and not force:
                raise ValueError("该分类有子分类，无法删除")

            # 检查是否有关联记录
            record_count = session.query(Record).filter(
                Record.category_id == category_id,
                Record.user_id == user.id
            ).count()

            if record_count > 0 and not force:
                raise ValueError(f"该分类有 {record_count} 条关联记录，无法删除")

            if force:
                # 将子分类的parent_id设为NULL
                for child in category.children:
                    child.parent_id = None
                # 将关联记录的category_id设为NULL
                session.query(Record).filter(
                    Record.category_id == category_id,
                    Record.user_id == user.id
                ).update({"category_id": None})

            session.delete(category)
            return True

    def list_categories(self, user_id: str, parent_id: Optional[int] = None) -> List[Dict]:
        """列出分类（仅限指定用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return []
            query = session.query(Category).filter(Category.user_id == user.id).options(joinedload(Category.parent))
            if parent_id is not None:
                query = query.filter(Category.parent_id == parent_id)
            categories = query.all()
            return [c.to_dict() for c in categories]

    # ==================== 标签接口 ====================

    def create_tag(self, user_id: str, name: str, color: str = "#1890ff") -> Dict:
        """创建标签（需指定所属用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                raise ValueError(f"User {user_id} not found")
            tag = Tag(name=name, color=color, user_id=user.id)
            session.add(tag)
            session.flush()
            session.refresh(tag)
            return tag.to_dict()

    def get_tag(self, user_id: str, tag_id: int) -> Optional[Dict]:
        """获取单个标签（需验证用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return None
            tag = session.query(Tag).filter(
                Tag.id == tag_id,
                Tag.user_id == user.id
            ).first()
            if tag:
                return tag.to_dict()
            return None

    def update_tag(self, user_id: str, tag_id: int, **kwargs) -> Optional[Dict]:
        """更新标签（需验证用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return None
            tag = session.query(Tag).filter(
                Tag.id == tag_id,
                Tag.user_id == user.id
            ).first()
            if not tag:
                return None
            for key, value in kwargs.items():
                if hasattr(tag, key):
                    setattr(tag, key, value)
            session.flush()
            session.refresh(tag)
            return tag.to_dict()

    def delete_tag(self, user_id: str, tag_id: int) -> bool:
        """删除标签（需验证用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return False
            tag = session.query(Tag).filter(
                Tag.id == tag_id,
                Tag.user_id == user.id
            ).first()
            if not tag:
                return False
            session.delete(tag)
            return True

    def list_tags(self, user_id: str) -> List[Dict]:
        """列出所有标签（仅限指定用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return []
            tags = session.query(Tag).filter(Tag.user_id == user.id).all()
            return [tag.to_dict() for tag in tags]

    # ==================== 记录接口 - 单条操作 ====================

    def _validate_category_and_tags(self, session: Session, user_id: int,
                                    category_id: Optional[int], tag_ids: Optional[List[int]]):
        """验证分类和标签是否属于当前用户，并返回有效的分类和标签对象"""
        if category_id is not None:
            category = session.query(Category).filter(
                Category.id == category_id,
                Category.user_id == user_id
            ).first()
            if not category:
                raise ValueError(f"Category {category_id} does not exist or does not belong to the user")

        if tag_ids:
            tags = session.query(Tag).filter(
                Tag.id.in_(tag_ids),
                Tag.user_id == user_id
            ).all()
            if len(tags) != len(tag_ids):
                missing = set(tag_ids) - {t.id for t in tags}
                raise ValueError(f"Tags {missing} do not exist or do not belong to the user")
            return tags
        return []

    def create_record(
        self,
        user_id: str,
        *,
        time: datetime = None,
        description: str = None,
        amount: float,
        is_expense: bool,
        category_id: int = None,
        tag_ids: Optional[List[int]] = None
    ) -> Dict:
        """创建单条记录"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                raise ValueError(f"User {user_id} not found")

            # 验证分类和标签归属
            self._validate_category_and_tags(session, user.id, category_id, tag_ids)

            record = Record(
                time=time,
                description=description,
                amount=amount,
                is_expense=is_expense,
                category_id=category_id,
                user_id=user.id
            )

            if tag_ids:
                tags = session.query(Tag).filter(Tag.id.in_(tag_ids), Tag.user_id == user.id).all()
                record.tags = tags

            session.add(record)
            session.flush()
            session.refresh(record)

            # 预加载关系数据
            if record.category:
                session.refresh(record.category)
            for tag in record.tags:
                session.refresh(tag)

            result = record.to_dict(include_relations=True)
            return result

    def get_record(self, user_id: str, record_id: int) -> Optional[Dict]:
        """获取单条记录（需验证用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return None
            record = session.query(Record).options(
                joinedload(Record.category),
                joinedload(Record.tags)
            ).filter(
                Record.id == record_id,
                Record.user_id == user.id
            ).first()

            if not record:
                return None

            result = record.to_dict(include_relations=True)
            return result

    def update_record(self, user_id: str, record_id: int, **kwargs) -> Optional[Dict]:
        """更新记录（需验证用户和关联数据归属）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return None
            record = session.query(Record).filter(
                Record.id == record_id,
                Record.user_id == user.id
            ).first()
            if not record:
                return None

            # 处理普通字段
            tag_ids = kwargs.pop('tag_ids', None)
            new_category_id = kwargs.get('category_id', record.category_id)

            # 验证分类和标签归属（如果修改了相关字段）
            if new_category_id != record.category_id or tag_ids is not None:
                self._validate_category_and_tags(session, user.id, new_category_id, tag_ids)

            for key, value in kwargs.items():
                if hasattr(record, key) and key != 'tags':
                    setattr(record, key, value)

            # 处理标签关联
            if tag_ids is not None:
                if tag_ids:
                    tags = session.query(Tag).filter(Tag.id.in_(tag_ids), Tag.user_id == user.id).all()
                    record.tags = tags
                else:
                    record.tags = []

            record.updated_at = datetime.now()
            session.flush()
            session.refresh(record)

            # 重新加载关系
            session.refresh(record)
            if record.category:
                session.refresh(record.category)
            for tag in record.tags:
                session.refresh(tag)

            return record.to_dict(include_relations=True)

    def delete_record(self, user_id: str, record_id: int) -> bool:
        """删除单条记录（需验证用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return False
            record = session.query(Record).filter(
                Record.id == record_id,
                Record.user_id == user.id
            ).first()
            if not record:
                return False
            session.delete(record)
            return True

    # ==================== 记录接口 - 批量操作 ====================

    def create_records_batch(self, user_id: str, records_data: List[Dict]) -> List[Dict]:
        """批量创建记录（所有记录必须属于同一用户）"""
        results = []
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                raise ValueError(f"User {user_id} not found")

            for data in records_data:
                # 验证每条记录的分类和标签归属
                category_id = data.get('category_id')
                tag_ids = data.get('tag_ids', [])
                self._validate_category_and_tags(session, user.id, category_id, tag_ids)

                record = Record(
                    time=data.get('time', datetime.now()),
                    description=data.get('description', ''),
                    amount=data.get('amount', 0),
                    is_expense=data.get('is_expense', True),
                    category_id=category_id,
                    user_id=user.id
                )

                if tag_ids:
                    tags = session.query(Tag).filter(Tag.id.in_(tag_ids), Tag.user_id == user.id).all()
                    record.tags = tags

                session.add(record)
                session.flush()
                session.refresh(record)

                # 收集结果
                if record.category:
                    session.refresh(record.category)
                for tag in record.tags:
                    session.refresh(tag)

                results.append(record.to_dict(include_relations=True))

            return results

    def update_records_batch(self, user_id: str, updates: List[Tuple[int, Dict]]) -> List[Dict]:
        """
        批量更新记录
        updates: [(record_id, update_data), ...]
        所有记录必须属于同一用户
        """
        results = []
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                raise ValueError(f"User {user_id} not found")

            for record_id, data in updates:
                record = session.query(Record).filter(
                    Record.id == record_id,
                    Record.user_id == user.id
                ).first()
                if not record:
                    continue

                tag_ids = data.pop('tag_ids', None)
                new_category_id = data.get('category_id', record.category_id)

                # 验证分类和标签归属（如果修改）
                if new_category_id != record.category_id or tag_ids is not None:
                    self._validate_category_and_tags(session, user.id, new_category_id, tag_ids)

                for key, value in data.items():
                    if hasattr(record, key) and key != 'tags':
                        setattr(record, key, value)

                if tag_ids is not None:
                    if tag_ids:
                        tags = session.query(Tag).filter(Tag.id.in_(tag_ids), Tag.user_id == user.id).all()
                        record.tags = tags
                    else:
                        record.tags = []

                record.updated_at = datetime.now()
                session.flush()
                session.refresh(record)
                results.append(record.to_dict(include_relations=True))

            return results

    def delete_records_batch(self, user_id: str, record_ids: List[int]) -> int:
        """批量删除记录（仅删除属于该用户的记录）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return 0
            result = session.query(Record).filter(
                Record.id.in_(record_ids),
                Record.user_id == user.id
            ).delete(synchronize_session=False)
            return result

    # ==================== 记录接口 - 复杂查询 ====================

    def query_records(
        self,
        user_id: str,
        # 时间范围
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,

        # 金额范围
        min_amount: Optional[float] = None,
        max_amount: Optional[float] = None,

        # 类型
        is_expense: Optional[bool] = None,

        # 分类（支持单个或多个，支持包含子分类）
        category_id: Optional[Union[int, List[int]]] = None,
        include_subcategories: bool = False,

        # 标签（支持单个或多个，支持AND/OR模式）
        tag_ids: Optional[List[int]] = None,
        tag_match_mode: str = "OR",  # "OR" 或 "AND"

        # 文本搜索
        keyword: Optional[str] = None,

        # 排序
        order_by: str = "time",
        order_desc: bool = True,

        # 分页
        page: int = 1,
        page_size: int = 50,

        # 返回选项
        return_queryset: bool = False  # 返回查询对象而非结果，用于进一步处理
    ) -> Union[Dict, Query]:
        """
        复杂查询接口 - 支持多种条件组合（仅限指定用户）
        返回包含数据和分页信息的字典
        """
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return {
                    "data": [],
                    "pagination": {"page": page, "page_size": page_size, "total": 0, "total_pages": 0},
                    "summary": {"total_expense": 0, "total_income": 0}
                }

            query = session.query(Record).options(
                joinedload(Record.category),
                joinedload(Record.tags)
            ).filter(Record.user_id == user.id)

            # 时间过滤
            if start_time:
                query = query.filter(Record.time >= start_time)
            if end_time:
                query = query.filter(Record.time <= end_time)

            # 金额过滤
            if min_amount is not None:
                query = query.filter(Record.amount >= min_amount)
            if max_amount is not None:
                query = query.filter(Record.amount <= max_amount)

            # 类型过滤
            if is_expense is not None:
                query = query.filter(Record.is_expense == is_expense)

            # 分类过滤
            if category_id is not None:
                category_ids = []
                if isinstance(category_id, int):
                    category_ids = [category_id]
                else:
                    category_ids = category_id

                if include_subcategories:
                    # 获取所有子分类ID（确保属于当前用户）
                    all_ids = set()
                    for cid in category_ids:
                        cat = session.query(Category).filter(
                            Category.id == cid,
                            Category.user_id == user.id
                        ).first()
                        if cat:
                            all_ids.update(cat._get_all_children_ids())
                    category_ids = list(all_ids)

                query = query.filter(Record.category_id.in_(category_ids))

            # 标签过滤
            if tag_ids:
                if tag_match_mode == "AND":
                    # 必须包含所有指定标签（且标签属于当前用户）
                    for tag_id in tag_ids:
                        query = query.filter(
                            Record.tags.any(and_(Tag.id == tag_id, Tag.user_id == user.id))
                        )
                else:  # OR
                    # 包含任一指定标签
                    query = query.filter(
                        Record.tags.any(and_(Tag.id.in_(tag_ids), Tag.user_id == user.id))
                    )

            # 关键词搜索（描述）
            if keyword:
                query = query.filter(
                    Record.description.ilike(f"%{keyword}%")
                )

            # 排序
            order_column = getattr(Record, order_by, Record.time)
            if order_desc:
                query = query.order_by(order_column.desc())
            else:
                query = query.order_by(order_column.asc())

            if return_queryset:
                return query

            # 分页
            total = query.count()
            records = query.offset((page - 1) * page_size).limit(page_size).all()

            # 转换为字典
            data = [r.to_dict(include_relations=True) for r in records]

            return {
                "data": data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": (total + page_size - 1) // page_size
                },
                "summary": {
                    "total_expense": sum(
                        r.amount for r in records if r.is_expense
                    ),
                    "total_income": sum(
                        r.amount for r in records if not r.is_expense
                    )
                }
            }

    def get_statistics(
        self,
        user_id: str,
        group_by: str = "category",  # category, tag, month, day
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        is_expense: Optional[bool] = None
    ) -> List[Dict]:
        """统计接口（仅限指定用户）"""
        with self.get_session() as session:
            user = session.query(User).filter(User.user_id == user_id).first()
            if not user:
                return []

            query = session.query(Record).filter(Record.user_id == user.id)

            if start_time:
                query = query.filter(Record.time >= start_time)
            if end_time:
                query = query.filter(Record.time <= end_time)
            if is_expense is not None:
                query = query.filter(Record.is_expense == is_expense)

            if group_by == "category":
                # 按分类统计（仅限当前用户的分类）
                result = query.join(Category).filter(Category.user_id == user.id).group_by(
                    Category.id
                ).with_entities(
                    Category.id,
                    Category.name,
                    func.sum(Record.amount).label('total'),
                    func.count(Record.id).label('count')
                ).all()

                return [
                    {
                        "category_id": r[0],
                        "category_name": r[1],
                        "total_amount": float(r[2] or 0),
                        "record_count": r[3]
                    }
                    for r in result
                ]

            elif group_by == "tag":
                # 按标签统计（仅限当前用户的标签）
                result = session.query(
                    Tag.id,
                    Tag.name,
                    func.sum(Record.amount).label('total'),
                    func.count(Record.id).label('count')
                ).join(
                    record_tags, Tag.id == record_tags.c.tag_id
                ).join(
                    Record, Record.id == record_tags.c.record_id
                ).filter(
                    Record.id.in_(query.with_entities(Record.id)),
                    Tag.user_id == user.id
                ).group_by(Tag.id).all()

                return [
                    {
                        "tag_id": r[0],
                        "tag_name": r[1],
                        "total_amount": float(r[2] or 0),
                        "record_count": r[3]
                    }
                    for r in result
                ]

            elif group_by == "month":
                # 按月统计
                result = session.query(
                    func.strftime('%Y-%m', Record.time).label('month'),
                    func.sum(Record.amount).label('total'),
                    func.count(Record.id).label('count')
                ).filter(
                    Record.id.in_(query.with_entities(Record.id))
                ).group_by('month').all()

                return [
                    {
                        "month": r[0],
                        "total_amount": float(r[1] or 0),
                        "record_count": r[2]
                    }
                    for r in result
                ]

            return []

    # ==================== 自定义 SQL 接口 ====================

    def execute_sql(
        self,
        sql: str,
        params: Optional[Dict] = None,
        fetch: bool = True
    ) -> Union[List[Dict], int]:
        """
        执行自定义 SQL（注意：此接口不自动添加用户过滤，需手动在SQL中处理）
        返回查询结果或影响行数
        """
        with self.get_session() as session:
            result = session.execute(text(sql), params or {})

            if fetch:
                rows = result.mappings().all()
                return [dict(row) for row in rows]
            else:
                return result.rowcount

    def execute_sql_batch(self, sql_statements: List[str]) -> List[Any]:
        """批量执行多条 SQL 语句（不自动添加用户过滤）"""
        results = []
        with self.get_session() as session:
            for sql in sql_statements:
                result = session.execute(text(sql))
                try:
                    rows = result.mappings().all()
                    results.append([dict(row) for row in rows])
                except:
                    results.append(result.rowcount)
            return results