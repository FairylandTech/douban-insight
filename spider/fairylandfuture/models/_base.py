# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-11-28 11:57:26 UTC+08:00
"""

import re
import typing as t

from sqlalchemy import Column, Integer, DateTime, Boolean
from sqlalchemy import inspect
from sqlalchemy.orm import Mapped, DeclarativeBase, declared_attr, Session

from fairylandfuture import logger
from fairylandfuture.core.superclass.schema import BaseSchema
from fairylandfuture.utils import DateTimeUtils


class BaseModel(DeclarativeBase):
    __abstract__: bool = True

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True, comment="ID")
    created_at = Column(DateTime, default=DateTimeUtils.unzone_cst, nullable=False, comment="Create time")
    updated_at = Column(DateTime, default=DateTimeUtils.unzone_cst, onupdate=DateTimeUtils.unzone_cst, nullable=False, comment="Update time")
    existed = Column(Boolean, default=True, nullable=False, comment="Soft erase marker: 0=normal, 1=delete")

    @classmethod
    @declared_attr
    def __tablename__(cls) -> str:
        name = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()
        if name.endswith("_model"):
            name = name[:-6]
        return name

    @classmethod
    def from_schema(cls, schema: BaseSchema):
        ins = cls()
        for field, value in schema.to_dict().items():
            if hasattr(ins, field):
                setattr(ins, field, value)
        return ins

    def to_dict(self, exclude: t.Optional[t.Iterable[str]] = None) -> t.Dict[str, t.Any]:
        if exclude is None:
            exclude = set()

        return {column.name: getattr(self, column.name) for column in inspect(self.__class__).columns if column.name not in exclude}

    @classmethod
    def get_by_id(cls, session: Session, record_id: int) -> t.Optional["BaseModel"]:
        try:
            return session.query(cls).filter(cls.id == record_id, cls.existed == True).first()
        except Exception as err:
            logger.error(f"Failed to query {cls.__name__}, ID: {record_id}, error: {err}")
            return None

    @classmethod
    def get_all(cls, session: Session, limit: int = None, offset: int = None):
        try:
            query = session.query(cls).filter(cls.existed == True)

            if offset:
                query = query.offset(offset)
            if limit:
                query = query.limit(limit)

            return query.all()
        except Exception as err:
            logger.error(f"Failed to query {cls.__name__} list, error: {err}")
            return list()

    def refresh(self, session: Session) -> None:
        try:
            session.refresh(self)
        except Exception as err:
            logger.error(f"Failed to refresh {self.__class__.__name__} instance, error: {err}")
