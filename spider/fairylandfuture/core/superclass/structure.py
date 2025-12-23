# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2024-06-04 10:38:31 UTC+08:00
"""

import json
import typing as t
from dataclasses import dataclass, asdict, astuple, field, fields

from fairylandfuture import logger
from fairylandfuture.models import BaseModel


@dataclass(frozen=False)
class BaseStructure:

    @property
    def asdict(self) -> t.Dict[str, t.Any]:
        return asdict(self)

    @property
    def astuple(self) -> t.Tuple[t.Any, ...]:
        return astuple(self)

    @property
    def string(self) -> str:
        return json.dumps(self.asdict, separators=(",", ":"), ensure_ascii=False)

    def to_dict(self, /, *, ignorenone: bool = False) -> t.Dict[str, t.Any]:
        return {k: v for k, v in self.asdict.items() if v is not None} if ignorenone else self.asdict


@dataclass(frozen=True)
class BaseFrozenStructure:

    @property
    def asdict(self) -> t.Dict[str, t.Any]:
        return asdict(self)

    @property
    def astuple(self) -> t.Tuple[t.Any, ...]:
        return astuple(self)

    @property
    def string(self) -> str:
        return json.dumps(self.asdict, separators=(",", ":"), ensure_ascii=False)

    def to_dict(self, /, *, ignorenone: bool = False) -> t.Dict[str, t.Any]:
        return {k: v for k, v in self.asdict.items() if v is not None} if ignorenone else self.asdict

    @classmethod
    def from_model(cls, model: BaseModel):
        logger.debug(f"Converting model {model.__class__.__name__!r} to structure {cls.__name__!r}...")
        kwargs = {}
        model_dict = model.to_dict()
        for field in fields(cls):
            if field.name in model_dict:
                kwargs.update({field.name: model_dict.get(field.name)})

        return cls(**kwargs)


@dataclass
class BaseStructureTreeNode:
    id: t.Any
    parent_id: t.Any
    data: t.Dict[str, t.Any]
    children: t.List["BaseStructureTreeNode"] = field(default=None)

    def __post_init__(self):
        self.children = []

    def get_id(self) -> t.Any:
        return self.id

    def get_parent_id(self) -> t.Any:
        return self.parent_id

    def add_child(self, child: "BaseStructureTreeNode"):
        self.children.append(child)

    def get_children(self) -> t.List["BaseStructureTreeNode"]:
        return self.children

    def to_dict(self) -> t.Dict[str, t.Any]:
        result = {"id": self.id, "parent_id": self.parent_id, "data": self.data, "children": [child.to_dict() for child in self.children]}
        return result
