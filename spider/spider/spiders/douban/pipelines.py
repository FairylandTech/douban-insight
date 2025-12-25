# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 00:34:45 UTC+08:00
"""
import traceback
import typing as t

import scrapy
from fairylandlogger import LogManager, Logger
from itemadapter import ItemAdapter

from fairylandfuture.database.postgresql import PostgreSQLConnector, PostgreSQLOperator
from fairylandfuture.structures.database import MySQLExecuteStructure
from spider.spiders.douban.cache import RedisManager, DoubanCacheManager
from spider.spiders.douban.config import DoubanConfig
from spider.spiders.douban.dao import ArtistDAO
from spider.spiders.douban.items import MovieInfoTiem
from spider.spiders.douban.dao import MovieDAO
from spider.spiders.douban.structures import MovieStructure, MovieArtistStructure


class DoubanMoviePipeline:
    """豆瓣电影数据持久化 Pipeline - 负责电影和相关数据入库"""

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-pipeline", "douban")

    def __init__(self):
        self.config: t.Dict[str, t.Any] = DoubanConfig.load().get("postgresql", {})
        self.__db_connector: t.Optional["PostgreSQLConnector"] = None
        self.db: t.Optional["PostgreSQLOperator"] = None
        self.redis: "DoubanCacheManager" = RedisManager

        self.movie_dao: t.Optional["MovieDAO"] = None
        self.movie_artist_dao: t.Optional["ArtistDAO"] = None

    def open_spider(self, spider):
        """爬虫启动时连接数据库"""
        try:
            self.__db_connector: "PostgreSQLConnector" = PostgreSQLConnector(
                host=self.config.get("host"),
                port=self.config.get("port"),
                database=self.config.get("database"),
                user=self.config.get("user"),
                password=self.config.get("password"),
            )
            self.Log.info(f"数据库连接成功: {self.__db_connector.dsn}")
            self.db: "PostgreSQLOperator" = PostgreSQLOperator(connector=self.__db_connector)

            self.movie_dao = MovieDAO(db=self.db)
            self.movie_artist_dao = ArtistDAO(db=self.db)
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
                self.redis.mark_completed(item.get("movie_id"), {k: v for k, v in item.items()})
        except Exception as err:
            self.Log.error(f"处理数据项失败: {err}")
            self.Log.error(traceback.format_exc())

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
        movie_data = MovieStructure(**movie_info)
        self.movie_dao.insert_movie(movie_data)

        """
        # 插入导演、编剧、演员 到 tb_artist 并建立关系
        artists: t.List[t.Dict[str, str]] = []
        artists.extend([{"artist_id": director.get("artist_id"), "name": director.get("name"), "role": "director"} for director in directors])
        artists.extend([{"artist_id": writer.get("artist_id"), "name": writer.get("name"), "role": "writer"} for writer in writers])
        artists.extend([{"artist_id": actor.get("artist_id"), "name": actor.get("name"), "role": "actor"} for actor in actors])
        for artist in artists:
            role = artist.get("role")
            artist_data = MovieArtistStructure(artist_id=artist.get("artist_id"), name=artist.get("name"))
            # 插入艺术家信息
            self.movie_artist_dao.insert_artist(artist_data)
            # 建立电影与艺术家关系
            self.movie_artist_dao.insert_movie_artist_relation(role, movie_data.movie_id, artist_data.artist_id)
        """
