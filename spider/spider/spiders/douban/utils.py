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

    @classmethod
    def query_sql_clean(cls, query: str) -> str:
        """
        清理 SQL 语句中的多余空白字符

        :param query: 原始 SQL 语句
        :type query: str
        :return: 清理后的 SQL 语句
        :rtype: str
        """
        return " ".join(query.split())

    @classmethod
    def check_id_in_cache(cls, movie_id: str, cache_data: t.Set[str]):
        return movie_id in cache_data
