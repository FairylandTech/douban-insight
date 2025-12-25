# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 00:45:14 UTC+08:00
"""
import traceback
import typing as t

from fairylandlogger import LogManager, Logger

from fairylandfuture.database.postgresql import PostgreSQLOperator
from fairylandfuture.structures.database import MySQLExecuteStructure, PostgreSQLExecuteStructure
from spider.spiders.douban.structures import MovieStructure, MovieArtistStructure
from spider.spiders.douban.utils import DoubanUtils

Log: "Logger" = LogManager.get_logger("douban-dao", "douban")


class MovieDAO:
    """电影数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
        self.db = db

    def insert_movie(self, movie_data: "MovieStructure"):
        query = """
                insert into
                    movie.tb_movie (movie_id, full_name, chinese_name, original_name, release_date, score, summary, icon)
                values
                    (%(movie_id)s, %(full_name)s, %(chinese_name)s, %(original_name)s, %(release_date)s, %(score)s, %(summary)s, %(icon)s)
                on conflict (movie_id) do update
                    set movie_id = excluded.movie_id,
                        full_name = excluded.full_name,
                        chinese_name = excluded.chinese_name,
                        original_name = excluded.original_name,
                        release_date = excluded.release_date,
                        score = excluded.score,
                        summary = excluded.summary,
                        icon = excluded.icon,
                        updated_at = now()
                returning id;
                """
        vars = movie_data.to_dict()
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"插入电影信息, Query: {query}, Vars: {vars}")

        execute = PostgreSQLExecuteStructure(query, vars)

        try:
            result = self.db.insert(execute)
            Log.info(f"插入电影信息, BD Result: {result}")
            Log.info(f"保存电影: {movie_data.full_name} ({movie_data.movie_id})")
        except Exception as err:
            Log.error(f"保存电影失败: {err}")
            raise err


class ArtistDAO:
    """演员数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
        self.db = db

    def get_artist_by_artist_id(self, artist_id: str):
        query = """
                select id, artist_id, name
                from tb_artist
                where artist_id = %(artist_id)s
                  and deleted is false
                """
        args = {"artist_id": artist_id}
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"查询演员信息, Query: {query}, Args: {args}")

        execute = MySQLExecuteStructure(query, args)

        result = self.db.select(execute)
        Log.debug(f"查询结果: {result}")

        if isinstance(result, bool) or not result:
            return None

        if len(result) == 1:
            result = result[0]
        else:
            result = None

        return result

    def insert_artist(self, artist_data: "MovieArtistStructure"):
        existing_artist = self.get_artist_by_artist_id(artist_data.artist_id)
        if existing_artist:
            Log.info(f"艺术家已存在: {artist_data.name} ({artist_data.artist_id})")
            return

        query = """
                insert into
                    tb_artist (artist_id, name)
                values
                    (%(artist_id)s, %(name)s)
                """
        args = artist_data.to_dict()
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"插入演员信息, Query: {query}, Args: {args}")

        execute = MySQLExecuteStructure(query, args)
        try:
            result = self.db.insert(execute)
            Log.info(f"插入演员信息, BD Result: {result}")
            Log.info(f"保存艺术家: {artist_data.name}")
        except Exception as err:
            Log.error(f"保存艺术家失败: {err}")
            Log.error(traceback.format_exc())
            raise err

    def insert_movie_artist_relation(self, typed: str, movie_id: str, artist_id: str):
        if typed == "director":
            query = """
                    insert into
                        tb_movie_director_artist_relation (movie_id, artist_id)
                    values
                        (%(movie_id)s, %(artist_id)s)
                    """
        elif typed == "writer":
            query = """
                    insert into
                        tb_movie_writer_artist_relation (movie_id, artist_id)
                    values
                        (%(movie_id)s, %(artist_id)s)
                    """
        else:  # actor
            query = """
                    insert into
                        tb_movie_actor_artist_relation (movie_id, artist_id)
                    values
                        (%(movie_id)s, %(artist_id)s) \
                    """
        args = {"movie_id": movie_id, "artist_id": artist_id}
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"插入电影-艺术家关系, Query: {query}, Args: {args}")

        execute = MySQLExecuteStructure(query, args)

        try:
            result = self.db.insert(execute)
            Log.info(f"插入电影-艺术家关系, BD Result: {result}")
            Log.info(f"保存电影-艺术家关系: movie_id={movie_id}, artist_id={artist_id}, type={typed}")
        except Exception as err:
            Log.error(f"保存电影-艺术家关系失败: {err}")
            raise err


class MovieTypeDAO:
    """电影类型数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
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
                Log.debug(f"保存电影类型关系: movie_id={movie_id}, type_id={type_id}")
            except Exception as err:
                Log.error(f"保存电影类型关系失败: {err}")
                raise err


class MovieCountryDAO:
    """电影国家数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
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
                Log.debug(f"保存电影国家关系: movie_id={movie_id}, country_id={country_id}")
            except Exception as err:
                Log.error(f"保存电影国家关系失败: {err}")
                raise err


class MovieRelationDAO:
    """电影人物关系数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
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
                Log.debug(f"保存电影导演关系: movie_id={movie_id}, artist_id={artist_id}")
            except Exception as err:
                Log.error(f"保存电影导演关系失败: {err}")
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
                Log.debug(f"保存电影编剧关系: movie_id={movie_id}, artist_id={artist_id}")
            except Exception as err:
                Log.error(f"保存电影编剧关系失败: {err}")
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
                Log.debug(f"保存电影演员关系: movie_id={movie_id}, artist_id={artist_id}")
            except Exception as err:
                Log.error(f"保存电影演员关系失败: {err}")
                raise err


class MovieCommentDAO:
    """电影评论数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
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
                    Log.warning(f"电影不存在: {comment_data.get('movie_id')}")
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
                Log.debug(f"保存电影评论: movie_id={comment_data.get('movie_id')}")

                # 获取评论的自增ID
                comment_id = cursor.lastrowid
                return comment_id
            except Exception as err:
                Log.error(f"保存电影评论失败: {err}")
                raise err
