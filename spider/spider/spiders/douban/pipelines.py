# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 00:34:45 UTC+08:00
"""

import typing as t

import scrapy
from fairylandlogger import LogManager, Logger
from itemadapter import ItemAdapter

from fairylandfuture.database.mysql import MySQLConnector, MySQLOperator
from fairylandfuture.structures.database import MySQLExecuteStructure
from spider.spiders.douban.config import DoubanConfig
from spider.spiders.douban.items import MovieInfoTiem
from spider.spiders.douban.dao import MovieDAO


class DoubanMoviePipeline:
    """豆瓣电影数据持久化 Pipeline - 负责电影和相关数据入库"""

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-pipeline", "douban")

    def __init__(self):
        self.config: t.Dict[str, t.Any] = DoubanConfig.load().get("mysql", {})
        self.__db_connector: t.Optional["MySQLConnector"] = None
        self.db: t.Optional["MySQLOperator"] = None

        self.movie_dao: t.Optional["MovieDAO"] = None

    def open_spider(self, spider):
        """爬虫启动时连接数据库"""
        try:
            self.__db_connector: "MySQLConnector" = MySQLConnector(
                host=self.config.get("host"),
                port=self.config.get("port"),
                database=self.config.get("database"),
                user=self.config.get("user"),
                password=self.config.get("password"),
            )
            self.Log.info("数据库连接成功")
            self.db: "MySQLOperator" = MySQLOperator(connector=self.__db_connector)

            self.movie_dao = MovieDAO(db=self.db)
        except Exception as err:
            self.Log.error(f"数据库连接失败: {err}")
            raise err

    def close_spider(self, spider):
        """爬虫关闭时断开数据库连接"""
        try:
            self.__db_connector.close()
            self.Log.info("数据库连接已断开")
        except Exception as err:
            self.Log.error(f"关闭数据库连接失败: {err}")

    def process_item(self, item: scrapy.Item, spider: scrapy.Spider) -> scrapy.Item:
        """处理数据项"""
        try:
            if isinstance(item, MovieInfoTiem):
                self.__process_movie_info(item)
        except Exception as err:
            self.Log.error(f"处理数据项失败: {err}")

        return item

    def __process_movie_info(self, item: "MovieInfoTiem"):
        item = ItemAdapter(item)

        movie_info = {
            "movie_id": item.get("movie_id"),
            "full_name": item.get("full_name"),
            "chinese_name": item.get("chinese_name"),
            "original_name": item.get("original_name"),
            "release_date": item.get("release_date"),
            "score": item.get("score"),
            "summary": item.get("summary"),
            "icon": item.get("icon"),
        }

        directors = item.get("directors", [])
        writers = item.get("writers", [])
        actors = item.get("actors", [])
        types = item.get("types", [])
        countries = item.get("countries", [])

        # 插入电影信息
        self.db.insert(
            MySQLExecuteStructure(
                query="""
                    insert into
                        tb_movie (movie_id, full_name, chinese_name, original_name, release_date, score, summary, icon)
                    values
                        (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                args=movie_info,
            )
        )
        self.Log.info(f"保存电影: {movie_info.get('full_name')} (ID: {movie_info.get('movie_id')})")

        # 插入导演、编剧、演员 到 tb_artist 并建立关系
        artist: t.List[t.Dict[str, str]] = []
        artist.extend(directors)
        artist.extend(writers)
        artist.extend(actors)
        artist = list({item.get("artist_id"): item for item in artist}.values())
