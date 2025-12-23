# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 00:45:14 UTC+08:00
"""

import typing as t

from fairylandlogger import LogManager, Logger

from fairylandfuture.database.mysql import MySQLOperator


class MovieDAO:
    """电影数据访问对象"""

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-dao", "douban")

    def __init__(self, db: "MySQLOperator"):
        self.db = db

    def insert_movie(self, movie_data: dict) -> int:
        """插入电影信息"""

        with self.db.get_cursor() as cursor:
            try:
                cursor.execute(
                    sql,
                    (
                        movie_data.get("movie_id"),
                        movie_data.get("full_name"),
                        movie_data.get("chinese_name"),
                        movie_data.get("original_name"),
                        movie_data.get("release_date"),
                        movie_data.get("score"),
                        movie_data.get("summary"),
                    ),
                )
                self.Log.info(f"保存电影: {movie_data.get('full_name')} (ID: {movie_data.get('movie_id')})")

                # 获取电影的自增ID
                cursor.execute("SELECT id FROM tb_movie WHERE movie_id = %s", (movie_data.get("movie_id"),))
                result = cursor.fetchone()
                return result["id"] if result else None
            except Exception as err:
                self.Log.error(f"保存电影失败: {err}")
                raise err

    def get_movie_by_movie_id(self, movie_id: str) -> t.Optional[dict]:
        """根据豆瓣电影ID获取电影"""
        sql = "SELECT id, movie_id FROM tb_movie WHERE movie_id = %s AND deleted = 0"

        with self.db.get_cursor() as cursor:
            cursor.execute(sql, (movie_id,))
            return cursor.fetchone()


class ArtistDAO:
    """演员数据访问对象"""

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-dao", "douban")

    def __init__(self, db: MySQLConnection):
        self.db = db

    def insert_artist(self, artist_data: dict) -> int:
        """插入或更新演员信息，返回 artist 表的自增ID"""
        sql = """
            INSERT INTO tb_artist 
            (artist_id, name, birthday, photo, personage)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            birthday = VALUES(birthday),
            photo = VALUES(photo),
            personage = VALUES(personage),
            updated_at = NOW()
        """

        with self.db.get_cursor() as cursor:
            try:
                cursor.execute(
                    sql,
                    (
                        artist_data.get("artist_id"),
                        artist_data.get("name"),
                        artist_data.get("birthday"),
                        artist_data.get("photo"),
                        artist_data.get("personage"),
                    ),
                )
                self.Log.debug(f"保存艺术家: {artist_data.get('name')}")

                # 获取艺术家的自增ID
                cursor.execute("SELECT id FROM tb_artist WHERE artist_id = %s", (artist_data.get("artist_id"),))
                result = cursor.fetchone()
                return result["id"] if result else None
            except Exception as err:
                self.Log.error(f"保存艺术家失败: {err}")
                raise err

    def get_artist_by_artist_id(self, artist_id: str) -> t.Optional[dict]:
        """根据艺术家ID获取艺术家"""
        sql = "SELECT id, artist_id FROM tb_artist WHERE artist_id = %s AND deleted = 0"

        with self.db.get_cursor() as cursor:
            cursor.execute(sql, (artist_id,))
            return cursor.fetchone()


class MovieTypeDAO:
    """电影类型数据访问对象"""

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-dao", "douban")

    def __init__(self, db: MySQLConnection):
        self.db = db

    def get_type_by_name(self, type_name: str) -> t.Optional[dict]:
        """根据类型名称获取类型"""
        sql = "SELECT id, name FROM tb_movie_type WHERE name = %s AND deleted = 0"

        with self.db.get_cursor() as cursor:
            cursor.execute(sql, (type_name,))
            return cursor.fetchone()

    def insert_movie_type_relation(self, movie_id: int, type_id: int):
        """插入电影类型关系"""
        sql = """
            INSERT IGNORE INTO tb_movie_type_relation 
            (movie_id, type_id)
            VALUES (%s, %s)
        """

        with self.db.get_cursor() as cursor:
            try:
                cursor.execute(sql, (movie_id, type_id))
                self.Log.debug(f"保存电影类型关系: movie_id={movie_id}, type_id={type_id}")
            except Exception as err:
                self.Log.error(f"保存电影类型关系失败: {err}")
                raise err


class MovieCountryDAO:
    """电影国家数据访问对象"""

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-dao", "douban")

    def __init__(self, db: MySQLConnection):
        self.db = db

    def get_country_by_name(self, country_name: str) -> t.Optional[dict]:
        """根据国家名称获取国家"""
        sql = "SELECT id, name FROM tb_movie_country WHERE name = %s AND deleted = 0"

        with self.db.get_cursor() as cursor:
            cursor.execute(sql, (country_name,))
            return cursor.fetchone()

    def insert_movie_country_relation(self, movie_id: int, country_id: int):
        """插入电影国家关系"""
        sql = """
            INSERT IGNORE INTO tb_movie_country_relation 
            (movie_id, country_id)
            VALUES (%s, %s)
        """

        with self.db.get_cursor() as cursor:
            try:
                cursor.execute(sql, (movie_id, country_id))
                self.Log.debug(f"保存电影国家关系: movie_id={movie_id}, country_id={country_id}")
            except Exception as err:
                self.Log.error(f"保存电影国家关系失败: {err}")
                raise err


class MovieRelationDAO:
    """电影人物关系数据访问对象"""

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-dao", "douban")

    def __init__(self, db: MySQLConnection):
        self.db = db

    def insert_director_relation(self, movie_id: int, artist_id: int):
        """插入电影导演关系"""
        sql = """
            INSERT IGNORE INTO tb_movie_director_artist_relation 
            (movie_id, artist_id)
            VALUES (%s, %s)
        """

        with self.db.get_cursor() as cursor:
            try:
                cursor.execute(sql, (movie_id, artist_id))
                self.Log.debug(f"保存电影导演关系: movie_id={movie_id}, artist_id={artist_id}")
            except Exception as err:
                self.Log.error(f"保存电影导演关系失败: {err}")
                raise err

    def insert_writer_relation(self, movie_id: int, artist_id: int):
        """插入电影编剧关系"""
        sql = """
            INSERT IGNORE INTO tb_movie_writer_artist_relation 
            (movie_id, artist_id)
            VALUES (%s, %s)
        """

        with self.db.get_cursor() as cursor:
            try:
                cursor.execute(sql, (movie_id, artist_id))
                self.Log.debug(f"保存电影编剧关系: movie_id={movie_id}, artist_id={artist_id}")
            except Exception as err:
                self.Log.error(f"保存电影编剧关系失败: {err}")
                raise err

    def insert_actor_relation(self, movie_id: int, artist_id: int):
        """插入电影演员关系"""
        sql = """
            INSERT IGNORE INTO tb_movie_actor_artist_relation 
            (movie_id, artist_id)
            VALUES (%s, %s)
        """

        with self.db.get_cursor() as cursor:
            try:
                cursor.execute(sql, (movie_id, artist_id))
                self.Log.debug(f"保存电影演员关系: movie_id={movie_id}, artist_id={artist_id}")
            except Exception as err:
                self.Log.error(f"保存电影演员关系失败: {err}")
                raise err


class MovieCommentDAO:
    """电影评论数据访问对象"""

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-dao", "douban")

    def __init__(self, db: MySQLConnection):
        self.db = db

    def insert_comment(self, comment_data: dict) -> int:
        """插入电影评论，返回自增ID"""
        sql = """
            INSERT INTO tb_movie_comment 
            (movie_id, content, rating)
            VALUES (%s, %s, %s)
        """

        with self.db.get_cursor() as cursor:
            try:
                # 获取电影的自增ID
                movie_sql = "SELECT id FROM tb_movie WHERE movie_id = %s AND deleted = 0"
                cursor.execute(movie_sql, (comment_data.get("movie_id"),))
                movie_result = cursor.fetchone()

                if not movie_result:
                    self.Log.warning(f"电影不存在: {comment_data.get('movie_id')}")
                    return None

                movie_pk_id = movie_result["id"]

                cursor.execute(
                    sql,
                    (
                        movie_pk_id,
                        comment_data.get("content"),
                        comment_data.get("rating"),
                    ),
                )
                self.Log.debug(f"保存电影评论: movie_id={comment_data.get('movie_id')}")

                # 获取评论的自增ID
                comment_id = cursor.lastrowid
                return comment_id
            except Exception as err:
                self.Log.error(f"保存电影评论失败: {err}")
                raise err
