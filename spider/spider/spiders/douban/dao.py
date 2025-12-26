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
from collections import namedtuple

from fairylandlogger import LogManager, Logger

from fairylandfuture.database.postgresql import PostgreSQLOperator
from fairylandfuture.structures.database import PostgreSQLExecuteStructure
from spider.spiders.douban.structures import MovieStructure, MovieArtistStructure
from spider.spiders.douban.utils import DoubanUtils

Log: "Logger" = LogManager.get_logger("douban-dao", "douban")


class MovieDAO:
    """电影数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
        self.db = db

    def get_movie_id_all(self):
        query = """
                select movie_id
                from movie.tb_movie
                where deleted is false;
                """
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"查询所有电影ID, Query: {query}, Vars: {{}}")
        execute = PostgreSQLExecuteStructure(query, {})
        MovieRow = namedtuple("MovieRow", ("movie_id",))
        result: t.Tuple[MovieRow, ...] = self.db.select(execute)

        if isinstance(result, t.Sequence) and len(result) > 0:
            return [row.movie_id for row in result]
        else:
            return []

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
        params = movie_data.to_dict()
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"插入电影信息, Query: {query}, Params: {params}")

        execute = PostgreSQLExecuteStructure(query, params)

        try:
            result = self.db.insert(execute)
            Log.info(f"插入电影信息, BD Result: {result}")
            Log.info(f"保存电影: {movie_data.full_name} ({movie_data.movie_id})")
        except Exception as error:
            Log.error(f"保存电影失败: {error}")
            raise error


class ArtistDAO:
    """演员数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
        self.db = db

    def insert_artist(self, artist_data: "MovieArtistStructure"):
        query = """
                insert into
                    movie.tb_artist (artist_id, name)
                values
                    (%(artist_id)s, %(name)s)
                on conflict (artist_id) do update
                    set artist_id = excluded.artist_id,
                        name = excluded.name,
                        updated_at = now()
                returning id;
                """
        params = artist_data.to_dict()
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"插入演员信息, Query: {query}, Params: {params}")

        execute = PostgreSQLExecuteStructure(query, params)
        try:
            result = self.db.insert(execute)
            Log.info(f"插入演员信息, BD Result: {result}")
            Log.info(f"保存艺术家: {artist_data.name}")
            return result
        except Exception as error:
            Log.error(f"保存艺术家失败: {error}")
            Log.error(traceback.format_exc())
            raise error

    def insert_movie_artist_relation(self, typed: str, movie_id: str, artist_id: int):
        if typed == "director":
            query = """
                    insert into
                        movie.tb_movie_director_artist_relation (movie_id, artist_id)
                    values
                        (%(movie_id)s, %(artist_id)s)
                    on conflict (movie_id, artist_id) do update
                        set movie_id = excluded.movie_id,
                            artist_id = excluded.artist_id,
                            updated_at = now()
                    returning id;
                    """
        elif typed == "writer":
            query = """
                    insert into
                        movie.tb_movie_writer_artist_relation (movie_id, artist_id)
                    values
                        (%(movie_id)s, %(artist_id)s)
                    on conflict (movie_id, artist_id) do update
                        set movie_id = excluded.movie_id,
                            artist_id = excluded.artist_id,
                            updated_at = now()
                    returning id;
                    """
        else:  # actor
            query = """
                    insert into
                        movie.tb_movie_actor_artist_relation (movie_id, artist_id)
                    values
                        (%(movie_id)s, %(artist_id)s)
                    on conflict (movie_id, artist_id) do update
                        set movie_id = excluded.movie_id,
                            artist_id = excluded.artist_id,
                            updated_at = now()
                    returning id;
                    """
        params = {"movie_id": movie_id, "artist_id": artist_id}
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"插入电影-{typed}关系, Query: {query}, Params: {params}")

        execute = PostgreSQLExecuteStructure(query, params)

        try:
            result = self.db.insert(execute)
            Log.info(f"插入电影-艺术家关系, BD Result: {result}")
            Log.info(f"保存电影-艺术家关系: movie_id={movie_id}, artist_id={artist_id}, type={typed}")
        except Exception as error:
            Log.error(f"保存电影-艺术家关系失败: {error}")
            raise error


class MovieTypeDAO:
    """电影类型数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
        self.db = db

    def get_all_types(self):
        query = """
                select id, name
                from movie.tb_movie_type
                where deleted is false
                order by id;
                """
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"查询所有电影类型, Query: {query}, Vars: {{}}")
        execute = PostgreSQLExecuteStructure(query, {})
        MovieTypeRow = namedtuple("MovieTypeRow", ("id", "name"))
        result: t.Tuple[MovieTypeRow, ...] = self.db.select(execute)

        if isinstance(result, t.Sequence) and len(result) > 0:
            return [{"id": row.id, "name": row.name} for row in result]
        else:
            return []

    def get_id_by_name(self, type_name: str) -> t.Optional[int]:
        query = """
                select id
                from movie.tb_movie_type
                where name = %(type_name)s
                  and deleted is false;
                """
        params = {"type_name": type_name}
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"查询电影类型ID, Query: {query}, Params: {params}")

        execute = PostgreSQLExecuteStructure(query, params)

        try:
            MovieTypeRow = namedtuple("MovieTypeRow", ("id",))
            result: t.Tuple[MovieTypeRow, ...] = self.db.select(execute)
            Log.info(f"查询电影类型ID, BD Result: {result}")
            if result and len(result) > 0:
                return result[0].id
            else:
                return None
        except Exception as error:
            Log.error(f"查询电影类型ID失败: {error}")
            raise error

    def insert_movie_type_relation(self, movie_id: str, type_name: str):
        """插入电影类型关系"""
        query = """
                with type_lookup as (select id as type_id
                                     from movie.tb_movie_type
                                     where name = %(type_name)s
                                       and deleted is false
                                     )
                insert
                into
                    movie.tb_movie_type_relation (movie_id, type_id)
                select %(movie_id)s, type_id
                from type_lookup
                on conflict (movie_id, type_id) do update
                    set movie_id = excluded.movie_id,
                        type_id = excluded.type_id,
                        updated_at = now()
                returning id;
                """
        params = {"movie_id": movie_id, "type_name": type_name}
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"插入电影类型关系, Query: {query}, Params: {params}")

        execute = PostgreSQLExecuteStructure(query, params)

        try:
            result = self.db.execute(execute)
            Log.info(f"插入电影类型关系, BD Result: {result}")
            Log.info(f"保存电影类型关系: movie_id={movie_id}, type_name={type_name}")
        except Exception as error:
            Log.error(f"保存电影类型关系失败: {error}")
            raise error


class MovieCountryDAO:
    """电影国家数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
        self.db = db

    def insert_movie_country_relation(self, movie_id: str, country_name: str):
        query = """
                with country_upsert as (
                    insert into movie.tb_movie_country (name)
                        values (%(country_name)s)
                        on conflict (name) do update
                            set updated_at = now()
                        returning id
                    )
                insert
                into
                    movie.tb_movie_country_relation (movie_id, country_id)
                select %(movie_id)s, id
                from country_upsert
                on conflict (movie_id, country_id) do update
                    set updated_at = now()
                returning id;
                """
        params = {"movie_id": movie_id, "country_name": country_name}
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"插入电影国家关系, Query: {query}, Params: {params}")

        execute = PostgreSQLExecuteStructure(query, params)

        try:
            result = self.db.execute(execute)
            Log.info(f"插入电影国家关系, BD Result: {result}")
            Log.info(f"保存电影国家关系: movie_id={movie_id}, country_name={country_name}")
        except Exception as error:
            Log.error(f"保存电影国家关系失败: {error}")


class MovieCommentDAO:
    """电影评论数据访问对象"""

    def __init__(self, db: "PostgreSQLOperator"):
        self.db = db

    def insert_comment(self, comment_data: dict):
        query = """
                insert into
                    movie.tb_movie_comment (movie_id, comment_id, content)
                values
                    (%(movie_id)s, %(comment_id)s, %(content)s)
                on conflict (comment_id) do update
                    set movie_id = excluded.movie_id,
                        content = excluded.content,
                        updated_at = now()
                returning id;
                """
        query = DoubanUtils.query_sql_clean(query)
        Log.debug(f"插入电影评论, Query: {query}, Params: {comment_data}")

        execute = PostgreSQLExecuteStructure(query, comment_data)
        try:
            result = self.db.insert(execute)
            Log.info(f"插入电影评论, DB Result: {result}")
            return result
        except Exception as error:
            Log.error(f"保存电影评论失败: {error}")
            Log.error(traceback.format_exc())
            raise error
