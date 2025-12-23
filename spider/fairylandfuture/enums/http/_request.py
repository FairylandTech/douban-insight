# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-09-10 23:17:45 UTC+08:00
"""

from fairylandfuture.core.superclass.enumerate import BaseEnum


class HTTPRequestMethodEnum(BaseEnum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"
    TRACE = "TRACE"
    CONNECT = "CONNECT"

    @property
    def value(self) -> str:
        return super().value()
