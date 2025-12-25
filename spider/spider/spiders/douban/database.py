# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-25 19:35:42 UTC+08:00
"""

import typing as t

from fairylandlogger import LogManager, Logger

from spider.spiders.douban.config import DoubanConfig
from fairylandfuture.database.postgresql import PostgreSQLConnector


class DatabaseManager:
    _instance: "DatabaseManager"

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-spider-database", "douban")

    def __new__(cls, *args, **kwargs) -> "DatabaseManager":
        if not hasattr(cls, "_instance"):
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.config: t.Dict[str, t.Any] = DoubanConfig.load().get("postgresql", {})
        self.connector: "PostgreSQLConnector" = self.get_connector()

    def get_connector(self) -> "PostgreSQLConnector":
        connector = PostgreSQLConnector(
            host=self.config.get("host"),
            port=self.config.get("port"),
            database=self.config.get("database"),
            user=self.config.get("user"),
            password=self.config.get("password"),
        )

        self.Log.debug(f"数据库配置: {connector.dsn}")
        self.Log.info(f"数据库连接成功")

        return connector


PostgreSQLManager = DatabaseManager()
