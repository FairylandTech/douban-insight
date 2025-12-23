# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import typing as t

from fairylandlogger import LogManager, Logger
from itemadapter import ItemAdapter

from spider.databases.models import (
    MySQLConnection,
    MovieDAO,
    ArtistDAO,
    MovieTypeDAO,
    MovieCountryDAO,
    MovieRelationDAO,
    MovieCommentDAO,
)
from spider.spiders.douban.items import ArtistItem, MovieCommentItem, MovieInfoTiem


class SpiderPipeline:
    def process_item(self, item, spider):
        return item


class DoubanMoviePipeline:
    """豆瓣电影数据持久化 Pipeline - 负责电影和相关数据入库"""

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-pipeline", "douban")

    def __init__(self):
        self.db = MySQLConnection()
        self.movie_dao = MovieDAO(self.db)
        self.artist_dao = ArtistDAO(self.db)
        self.movie_type_dao = MovieTypeDAO(self.db)
        self.movie_country_dao = MovieCountryDAO(self.db)
        self.movie_relation_dao = MovieRelationDAO(self.db)
        self.movie_comment_dao = MovieCommentDAO(self.db)

    def open_spider(self, spider):
        """爬虫启动时连接数据库"""
        try:
            self.db.connect()
            self.Log.info("数据库连接成功")
        except Exception as err:
            self.Log.error(f"数据库连接失败: {err}")
            raise err

    def close_spider(self, spider):
        """爬虫关闭时断开数据库连接"""
        try:
            self.db.disconnect()
            self.Log.info("数据库连接已断开")
        except Exception as err:
            self.Log.error(f"关闭数据库连接失败: {err}")

    def process_item(self, item, spider):
        """处理数据项"""
        try:
            if isinstance(item, MovieInfoTiem):
                self._process_movie_detail(item)
            elif isinstance(item, MovieCommentItem):
                self._process_movie_comment(item)
            elif isinstance(item, ArtistItem):
                self._process_artist(item)
        except Exception as err:
            self.Log.error(f"处理数据项失败: {err}")
            # 继续处理，不中断爬虫

        return item

    def _process_movie_detail(self, item: MovieInfoTiem):
        """处理电影详情数据，保存电影信息和相关人物及类型"""
        adapter = ItemAdapter(item)

        movie_data = {
            "movie_id": adapter.get("movie_id"),
            "full_name": adapter.get("full_name"),
            "chinese_name": adapter.get("chinese_name"),
            "original_name": adapter.get("original_name"),
            "release_date": adapter.get("release_date"),
            "score": adapter.get("score"),
            "summary": adapter.get("summary"),
        }

        # 1. 保存电影主信息
        movie_pk_id = self.movie_dao.insert_movie(movie_data)
        if not movie_pk_id:
            self.Log.error(f"保存电影失败: {movie_data.get('full_name')}")
            return

        # 2. 保存导演信息和关系
        directors = adapter.get("directors", [])
        if directors:
            for director in directors:
                if not isinstance(director, dict):
                    continue

                artist_data = {
                    "artist_id": director.get("artist_id"),
                    "name": director.get("name"),
                    "birthday": director.get("birthday"),
                    "photo": director.get("photo"),
                    "personage": director.get("personage"),
                }

                artist_pk_id = self.artist_dao.insert_artist(artist_data)
                if artist_pk_id:
                    self.movie_relation_dao.insert_director_relation(movie_pk_id, artist_pk_id)

        # 3. 保存编剧信息和关系
        writers = adapter.get("writers", [])
        if writers:
            for writer in writers:
                if not isinstance(writer, dict):
                    continue

                artist_data = {
                    "artist_id": writer.get("artist_id"),
                    "name": writer.get("name"),
                    "birthday": writer.get("birthday"),
                    "photo": writer.get("photo"),
                    "personage": writer.get("personage"),
                }

                artist_pk_id = self.artist_dao.insert_artist(artist_data)
                if artist_pk_id:
                    self.movie_relation_dao.insert_writer_relation(movie_pk_id, artist_pk_id)

        # 4. 保存演员信息和关系
        actors = adapter.get("actors", [])
        if actors:
            for actor in actors:
                if not isinstance(actor, dict):
                    continue

                artist_data = {
                    "artist_id": actor.get("artist_id"),
                    "name": actor.get("name"),
                    "birthday": actor.get("birthday"),
                    "photo": actor.get("photo"),
                    "personage": actor.get("personage"),
                }

                artist_pk_id = self.artist_dao.insert_artist(artist_data)
                if artist_pk_id:
                    self.movie_relation_dao.insert_actor_relation(movie_pk_id, artist_pk_id)

        # 5. 保存电影类型关系
        genres = adapter.get("genres", [])
        if genres:
            for genre in genres:
                if not isinstance(genre, str):
                    continue

                type_info = self.movie_type_dao.get_type_by_name(genre)
                if type_info:
                    self.movie_type_dao.insert_movie_type_relation(movie_pk_id, type_info["id"])

        # 6. 保存电影国家/地区关系
        countries = adapter.get("countries", [])
        if countries:
            for country in countries:
                if not isinstance(country, str):
                    continue

                country_info = self.movie_country_dao.get_country_by_name(country)
                if country_info:
                    self.movie_country_dao.insert_movie_country_relation(movie_pk_id, country_info["id"])

        self.Log.info(f"电影详情保存完成: {movie_data.get('full_name')} (ID: {movie_data.get('movie_id')})")

    def _process_movie_comment(self, item: MovieCommentItem):
        """处理电影评论数据，保存到数据库"""
        adapter = ItemAdapter(item)

        comment_data = {
            "movie_id": adapter.get("movie_id"),
            "content": adapter.get("content"),
            "rating": adapter.get("rating"),
        }

        comment_id = self.movie_comment_dao.insert_comment(comment_data)
        if comment_id:
            self.Log.debug(f"评论保存完成: movie_id={comment_data.get('movie_id')}")
        else:
            self.Log.warning(f"评论保存失败: movie_id={comment_data.get('movie_id')}")

    def _process_artist(self, item: ArtistItem):
        """处理艺术家数据（备用方法）"""
        adapter = ItemAdapter(item)

        artist_data = {
            "artist_id": adapter.get("artist_id"),
            "name": adapter.get("name"),
            "birthday": adapter.get("birthday"),
            "photo": adapter.get("photo"),
            "personage": adapter.get("personage"),
        }

        self.artist_dao.insert_artist(artist_data)
        self.Log.debug(f"艺术家保存完成: {artist_data.get('name')}")
