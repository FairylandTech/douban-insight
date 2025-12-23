# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-05-14 10:48:20 UTC+08:00
"""

import datetime
import decimal
import json

from fairylandfuture import logger
from fairylandfuture.core.superclass.structure import BaseFrozenStructure, BaseStructure
from fairylandfuture.enums import DateTimeEnum


class JsonEncoder(json.JSONEncoder):

    def default(self, o):
        if hasattr(o, "__dict__"):
            logger.debug(f"Serializing object of type {type(o)} using __dict__")
            return o.__dict__
        elif hasattr(o, "__slots__"):
            logger.debug(f"Serializing object of type {type(o)} using __slots__")
            return {slot: getattr(o, slot) for slot in o.__slots__}
        elif isinstance(o, datetime.datetime):
            logger.debug(f"Serializing datetime object: {o}")
            return o.strftime(DateTimeEnum.DATETIME.value)
        elif isinstance(o, datetime.date):
            logger.debug(f"Serializing date object: {o}")
            return o.strftime(DateTimeEnum.DATE.value)
        elif isinstance(o, datetime.time):
            logger.debug(f"Serializing time object: {o}")
            return o.strftime(DateTimeEnum.TIME.value)
        elif isinstance(o, decimal.Decimal):
            logger.debug(f"Serializing decimal object: {o}")
            return float(o)
        elif isinstance(o, (BaseStructure, BaseFrozenStructure)):
            logger.debug(f"Serializing structure object of type {type(o)} using to_dict()")
            return o.to_dict()

        logger.debug(f"Using super().default() for object of type {type(o)}")
        return super().default(o)
