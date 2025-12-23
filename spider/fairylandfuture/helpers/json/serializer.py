# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-05-14 10:48:20 UTC+08:00
"""

import json
import typing as t

from fairylandfuture import logger
from fairylandfuture.helpers.json.encoder import JsonEncoder

ClazzType = t.TypeVar("ClazzType")
StrAny = t.TypeVar("StrAny", str, bytes, bytearray)


class JsonSerializerHelper:

    @classmethod
    def serialize(cls, value):
        logger.debug(f"Serializing value of type {type(value)}")
        return json.dumps(value, cls=JsonEncoder, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def deserialize(cls, value: t.Union[StrAny, t.Dict[str, t.Any]], clazz: t.Optional[t.Callable[..., ClazzType]] = None) -> ClazzType:
        logger.debug(f"Deserializing value of type {type(value)} to class {clazz}")
        if isinstance(value, t.Dict):
            if not clazz:
                return value
            logger.debug(f"Deserializing dict to class {clazz}")
            return clazz(**value)

        if not clazz:
            logger.debug("No class provided, deserializing to dict")
            return json.loads(value)

        logger.debug(f"Deserializing to class {clazz} using object_hook")
        return json.loads(value, object_hook=lambda x: clazz(**x))
