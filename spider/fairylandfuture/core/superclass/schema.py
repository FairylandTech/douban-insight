# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-11-26 12:54:27 UTC+08:00
"""

import datetime
import hashlib
import json
import typing as t
import uuid

from pydantic import BaseModel, ConfigDict, Field, field_serializer
from pydantic.alias_generators import to_camel

from fairylandfuture import logger
from fairylandfuture.enums import DateTimeEnum
from fairylandfuture.utils import DateTimeUtils


class PrimitiveSchema(BaseModel):

    model_config: t.ClassVar[ConfigDict] = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
        str_strip_whitespace=True,
        extra="ignore",
        validate_assignment=True,
    )

    def to_dict(self, /, *, exclude_fields: t.Optional[t.Iterable[str]] = None, exclude_none: bool = False, to_camel: bool = False) -> t.Dict[str, t.Any]:
        logger.debug(f"Serializing {self.__class__.__name__!r} to dict(mode=python)...")
        return self.model_dump(mode="python", exclude=set(exclude_fields) if exclude_fields else None, exclude_none=exclude_none, by_alias=to_camel)

    def to_serializable_dict(self, /, *, exclude_fields: t.Optional[t.Iterable[str]] = None, exclude_none: bool = False, to_camel: bool = False) -> t.Dict[str, t.Any]:
        logger.debug(f"Serializing {self.__class__.__name__!r} to dict(mode=json)...")
        return self.model_dump(mode="json", exclude=set(exclude_fields) if exclude_fields else None, exclude_none=exclude_none, by_alias=to_camel)

    def to_json_string(self, indent: int = 2) -> str:
        logger.debug(f"Serializing {self.__class__.__name__!r} to JSON string...")
        return self.model_dump_json(indent=indent, ensure_ascii=False)


class EntitySchema(PrimitiveSchema):

    id: t.Optional[int] = Field(description="ID")
    uuid: str = Field(default_factory=lambda: uuid.uuid4().hex, description="UUID", frozen=True)
    created_at: datetime.datetime = Field(default_factory=lambda: DateTimeUtils.unzone_cst(), description="Create Time", frozen=True)
    updated_at: datetime.datetime = Field(default_factory=lambda: DateTimeUtils.unzone_cst(), description="Update Time")
    existed: bool = Field(default=True, description="Is Existed Flag")

    @field_serializer("created_at", "updated_at", when_used="json")
    def __serialize_datetime(self, dt: datetime.datetime) -> str:
        return dt.strftime(DateTimeEnum.DATETIME.value)


class BaseSchema(EntitySchema):

    @property
    def hashcode(self) -> str:
        data = self.model_dump(mode="json", exclude={"id", "uuid", "created_at", "updated_at"})
        result = hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
        logger.debug(f"Calculating hashcode for {self.__class__.__name__!r} with result: {result!r}")
        return result

    def update(self, **kwargs) -> t.Self:
        flag = False
        frozen_fields = ("id", "uuid", "created_at")
        for field, value in kwargs.items():
            if field not in frozen_fields and hasattr(self, field) and getattr(self, field) != value:
                logger.debug(f"Updating field {field!r} of {self.__class__.__name__!r} from {getattr(self, field)!r} to {value!r}")
                setattr(self, field, value)
                flag = True

        if flag:
            self.updated_at = DateTimeUtils.unzone_cst()

        return self
