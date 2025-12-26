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

import requests
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

    @classmethod
    def get_proxy(cls, typed: int = 2) -> t.Optional[str]:
        response = requests.get(
            url=f"http://api.shenlongip.com/ip?key=1n28dz8g&protocol={typed}&mr=2&pattern=json&need=1111&count=1&sign=ab79686e9107b4f6b1ab6d8e25529091",
            timeout=10,
        )
        response.raise_for_status()
        data: t.Dict[str, int | t.List[t.Dict[str, int | str]]] = response.json()

        cls.logger.debug(f"代理IP响应数据: {data}")

        ip = data.get("data", [{}])[0].get("ip", "")
        port = data.get("data", [{}])[0].get("port", 0)

        if ip and port:
            proxies = {
                "http": f"http://{ip}:{port}",
                "https": f"https://{ip}:{port}",
                "socks5": f"socks5://{ip}:{port}",
            }

            if typed == 2:
                proxy = proxies.get("https", "")
            else:
                proxy = proxies.get("http", "")
            cls.logger.info(f"获取到代理IP: {proxy}")

            return proxy
        else:
            cls.logger.error("未能获取到有效的代理IP")
            return None
