# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 20:15:10 UTC+08:00
"""

import typing as t
from typing import AsyncIterator, Any, Iterable

import scrapy
from fairylandlogger import LogManager, Logger

from spider.spiders.douban.cache import RedisManager, DoubanCacheManager
from spider.spiders.douban.database import PostgreSQLManager, DatabaseManager
from spider.spiders.douban.src import DoubanMovieSpiderBase


class DoubanMovieShortCommentSpider(DoubanMovieSpiderBase):
    """
    获取电影短评

    """

    name = "douban-movie-short-comment"

    def __init__(self):
        super().__init__()

        self.page = 1
        self.size = 20

        self.start_index = 0

    def start_requests(self) -> Iterable[Any]:
        self.Log.info("开始获取电影短评")

        # 从缓存中获取电影 ID 列表
        movie_ids = self.cache.get_db_movie_ids()

        for movie_id in self.cache.get_db_movie_ids():
            self.Log.info(f"开始获取电影 {movie_id} 的短评")
            # 从缓存中获取已完成的电影短评任务
            # 如果任务已完成, 则跳过, 如果没有完成, 则继续获取评论
            for sort in ["new_score", "time"]:
                # 添加获取评论的任务, 任务状态 Padding
                yield from self.__request_movie_comment(movie_id=movie_id, start=0, limit=20)

    def __request_movie_comment(self, movie_id: str, start: int, limit: int, sort: str) -> Iterable[Any]:
        url = f"https://movie.douban.com/subject/{movie_id}/comments?start={start}&limit={limit}&status=P&sort=new_score"

        yield scrapy.Request(
            url=url,
            method="GET",
            headers=self.headers,
            cookies=self.cookies,
            cb_kwargs={"movie_id": movie_id, "start": start, "limit": limit},
        )
