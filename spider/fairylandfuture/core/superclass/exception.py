# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2024-08-10 18:32:58 UTC+08:00
"""

from fairylandfuture import logger


class BaseProgramException(Exception):

    def __init__(self, message: str = "Internal program error."):
        self.message = f"{self.__class__.__name__}: {message}"

    def __str__(self) -> str:
        logger.error(f"{self.__class__.__qualname__!r}: {self.message}")
        return self.message
