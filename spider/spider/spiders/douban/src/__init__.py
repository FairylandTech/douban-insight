# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 19:03:59 UTC+08:00
"""

import typing as t

import scrapy
from fairylandlogger import Logger, LogManager

from spider.spiders.douban.cache import DoubanCacheManager, RedisManager
from spider.spiders.douban.database import PostgreSQLManager, DatabaseManager


class DoubanMovieSpiderBase(scrapy.Spider):
    """
    获取电影信息

    """

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-spider", "douban")
    cache: t.ClassVar["DoubanCacheManager"] = RedisManager
    database: t.ClassVar["DatabaseManager"] = PostgreSQLManager

    allowed_domains = ["douban.com", "m.douban.com"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "CONCURRENT_REQUESTS_PER_IP": 1,
        "CONCURRENT_ITEMS": 1,
        "REACTOR_THREADPOOL_MAXSIZE": 1,
        "DOWNLOAD_DELAY": 30,
        "AUTOTHROTTLE_ENABLED": False,
        "SCHEDULER_DEBUG": False,
    }
