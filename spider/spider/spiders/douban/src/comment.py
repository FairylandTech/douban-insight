# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 20:15:10 UTC+08:00
"""

from typing import Any, Iterable

import fake_useragent
import scrapy

from fairylandfuture.database.postgresql import PostgreSQLOperator
from spider.spiders.douban.dao import MovieDAO
from spider.spiders.douban.items import MovieCommentItem
from spider.spiders.douban.src import DoubanMovieSpiderBase
from spider.spiders.douban.structures import MovieTask
from spider.spiders.douban.utils import DoubanUtils


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

        self.headers = {
            "User-Agent": fake_useragent.FakeUserAgent(os="Windows").random,
            "Referer": "https://movie.douban.com/explore",
        }
        self.cookies = DoubanUtils.load_cookies_from_file("config/douban.cookies")

        self.movie_dao = MovieDAO(PostgreSQLOperator(self.database.connector))

    def start_requests(self) -> Iterable[Any]:
        self.Log.info("开始获取电影短评")

        # 从缓存中获取电影 ID 列表
        movie_ids = self.cache.get_db_movie_ids()
        # 获取已完成的电影 ID 列表
        completed_movie_ids = self.cache.get_druable_comment_completed()

        for movie_id in movie_ids:
            self.Log.info(f"开始获取电影 {movie_id} 的短评")
            if movie_id in completed_movie_ids:
                self.Log.info(f"电影 {movie_id} 的短评已完成，跳过")
                continue

            self.Log.info(f"保存电影 {movie_id} 的短评任务到缓存")
            self.cache.save_comment_task(MovieTask(movie_id=movie_id))
            for sort in ["new_score", "time"]:
                yield from self.__request_movie_comment(movie_id=movie_id, start=self.start_index, limit=self.size, sort=sort)

    def __request_movie_comment(self, movie_id: str, start: int, limit: int, sort: str) -> Iterable[Any]:
        url = f"https://movie.douban.com/subject/{movie_id}/comments?start={start}&limit={limit}&status=P&sort={sort}"

        if start == 0:
            self.cache.mark_comment_processing(movie_id)
        yield scrapy.Request(
            url=url,
            method="GET",
            headers=self.headers,
            cookies=self.cookies,
            cb_kwargs={"movie_id": movie_id, "start": start, "limit": limit, "sort": sort},
            callback=self.parse,
        )

    def parse(self, response: scrapy.http.Response, **kwargs):
        movie_id = kwargs.get("movie_id")
        start = kwargs.get("start")
        limit = kwargs.get("limit")
        sort = kwargs.get("sort")

        # 解析评论
        comment_items = response.css(".comment-item")
        if not comment_items:
            self.Log.info(f"电影 {movie_id} 分类 {sort} 已无更多评论")
            self.Log.info(f"电影 {movie_id} 分类 {sort} 全部评论获取完成")
            # 仅在最后一个 sort 完成时才标记电影完成
            self.__mark_sort_completed(movie_id, sort)
            return

        for item in comment_items:
            # 提取数据
            comment_id = item.attrib.get("data-cid")
            content = item.css(".short::text").get()

            yield MovieCommentItem(
                movie_id=movie_id,
                comment_id=comment_id,
                content=content,
            )

        # 检查是否有下一页
        next_page = response.css("a.next::attr(href)").get()
        if next_page:
            new_start = start + limit
            yield from self.__request_movie_comment(movie_id, new_start, limit, sort)
        else:
            self.Log.info(f"电影 {movie_id} 分类 {sort} 全部评论获取完成")
            self.__mark_sort_completed(movie_id, sort)

    def __mark_sort_completed(self, movie_id: str, sort: str):
        """标记某个 sort 类型完成，当两个 sort 都完成时标记电影任务完成"""
        # 获取已完成的 sort 类型集合
        completed_sorts = self.cache.get(f"douban:movie:comment:completed_sorts:{movie_id}") or ""
        completed_sorts_set = set(completed_sorts.split(",")) if completed_sorts else set()
        completed_sorts_set.add(sort)

        # 保存已完成的 sort 类型
        self.cache.set(f"douban:movie:comment:completed_sorts:{movie_id}", ",".join(completed_sorts_set))

        # 当两个 sort 都完成时，标记电影任务为已完成
        if len(completed_sorts_set) >= 2:  # 已完成 new_score 和 time 两种排序
            self.Log.info(f"电影 {movie_id} 所有短评分类已完成")
            self.cache.save_druable_comment_completed(movie_id)
            self.cache.mark_comment_completed(movie_id, None)
            self.cache.delete(f"douban:movie:comment:completed_sorts:{movie_id}")
