# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-22 21:43:28 UTC+08:00
"""
import json
import typing as t
from http.cookies import SimpleCookie
from urllib.parse import urlencode

import fake_useragent
import scrapy
from fairylandlogger import LogManager, Logger

from spider.enums import SpiderStatus
from spider.spiders.douban.cache import DoubanCacheManager
from spider.spiders.douban.structure import MovieTask


class DoubanMovieSpider(scrapy.Spider):
    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-spider", "douban")
    Cache: t.ClassVar["DoubanCacheManager"] = DoubanCacheManager()

    name = "douban-movie"
    allowed_domains = ["douban.com", "m.douban.com"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "User-Agent": fake_useragent.UserAgent().random,
            "Referer": "https://movie.douban.com/explore",
        }
        self.cookies = self._load_cookies_from_file("config/douban.cookies")

    def _load_cookies_from_file(self, file_path: str) -> dict:
        cookie = SimpleCookie()
        try:
            with open(file_path, "r", encoding="UTF-8") as f:
                cookie.load(f.read())
            return {k: m.value for k, m in cookie.items()}
        except FileNotFoundError as error:
            self.Log.warning("Cookie 文件未找到")
            raise error
        except Exception as error:
            self.Log.error(f"加载 Cookie 失败: {error}")
            raise error

    def start_requests(self):
        recommend_start_index: str | None = self.Cache.get("douban:movie:recommend:start")
        self.Log.info(f"推荐电影起始索引: {recommend_start_index}")
        url = "https://m.douban.com/rexxar/api/v2/movie/recommend"
        params = {
            "refresh": "0",
            "start": "0" if not recommend_start_index else recommend_start_index,
            "count": "20",
            "selected_categories": {},
            "uncollect": False,
            "score_range": "0,10",
            "tags": "",
            "ck": "kfPA",
        }

        headers = self.headers.copy()
        headers.update(
            {
                "authority": "m.douban.com",
                "accept": "application/json, text/plain, */*",
                "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                "cache-control": "no-cache",
                "origin": "https://movie.douban.com",
                "pragma": "no-cache",
                "priority": "u=1, i",
                "referer": "https://movie.douban.com/explore",
                "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
            }
        )

        url = f"{url}?{urlencode(params, doseq=True)}"
        self.Log.info(f"获取电影ID: URL={url}, Headers={headers}")

        yield scrapy.Request(
            method="GET",
            url=url,
            headers=headers,
            cookies=self.cookies,
            callback=self.parse_movie_api,
            dont_filter=True,
        )

    def parse_movie_api(self, response: scrapy.http.Response):
        self.Log.debug(f"电影API响应状态码: {response.status}")
        self.Log.debug(f"电影API响应内容: {response.text}")
        data: t.Dict[str, t.Any] = json.loads(response.text)
        self.Cache.set("douban:movie:recommend:start", str(data.get("start") + data.get("count")))

        for item in data.get("items", []):
            item: t.Dict[str, t.Any]
            self.Log.debug(f"处理数据项: {item}")
            if item.get("type") != "movie":
                self.Log.warning(f"跳过非电影类型: {item.get('type')}, 数据: {item}")
                continue

            movie_id = item.get("id")
            movie_name = item.get("title")
            movie_url = f"https://movie.douban.com/subject/{movie_id}"
            self.Log.info(f"处理电影: {movie_name} ID: {movie_id}, URL: {movie_url}")

            task = MovieTask(
                movie_id=movie_id,
                status=SpiderStatus.PENDING,
            )
            self.Cache.save_task(task)

            yield scrapy.Request(
                url=movie_url,
                headers=self.headers,
                cookies=self.cookies,
                callback=self.parse_movie,
                cb_kwargs={"movie_id": movie_id},
            )

    def parse_movie(self, response: scrapy.http.Response, movie_id: str):
        pass
