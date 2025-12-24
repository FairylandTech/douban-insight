# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 20:15:47 UTC+08:00
"""

import typing as t
from http.cookies import SimpleCookie

from fairylandlogger import Logger, LogManager


class DoubanUtils:
    logger: t.ClassVar["Logger"] = LogManager.get_logger("douban-spider-utils", "douban")

    @classmethod
    def load_cookies_from_file(cls, file_path: str) -> dict:
        """
        从文件加载 Cookie

        :param file_path: Cookie 文件路径
        :type file_path: str
        :return: Cookie 字典
        :rtype: dict
        """
        cookie = SimpleCookie()
        try:
            with open(file_path, "r", encoding="UTF-8") as f:
                cookie.load(f.read())
            return {k: m.value for k, m in cookie.items()}
        except FileNotFoundError as error:
            cls.logger.warning("Cookie 文件未找到")
            raise error
        except Exception as error:
            cls.logger.error(f"加载 Cookie 失败: {error}")
            raise error
