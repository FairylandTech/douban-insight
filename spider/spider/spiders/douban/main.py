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

import fake_useragent
import scrapy
from fairylandlogger import LogManager, Logger

from spider.spiders.douban.cache import DoubanCacheManager


class DoubanMovieSpider(scrapy.Spider):
    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-spider", "douban")
    Cache: t.ClassVar["DoubanCacheManager"] = DoubanCacheManager()

    name = "douban-movie"
    allowed_domains = ["douban.com", "m.douban.com"]

    api_url = "https://m.douban.com/rexxar/api/v2/subject/recent_hot/movie?start=0&limit=20&ck=kfPA"

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
        yield scrapy.Request(
            url=self.api_url,
            headers=self.headers,
            cookies=self.cookies,
            callback=self.parse_api,
            dont_filter=True,
        )

    def parse_api(self, response: scrapy.http.Response):
        data = json.loads(response.text)
        items = data.get("items", [])
        ids = [i.get("id") for i in items if i.get("id")]
        self.Log.info(f"获取到 {len(ids)} 个电影 ID：{ids}")

        for sid in ids:
            movie_url = f"https://movie.douban.com/subject/{sid}/"
            yield scrapy.Request(
                url=movie_url,
                headers=self.headers,
                cookies=self.cookies,
                callback=self.parse_movie,
                cb_kwargs={"sid": sid},
            )

    def parse_movie(self, response: scrapy.http.Response, sid: str):
        title = response.css("#content h1 span::text").get() or ""
        year = response.css("#content h1 span.year::text").re_first(r"\d{4}") or ""
        rating = response.css("strong.ll.rating_num::text").get() or ""
        info = " ".join(response.css("#info *::text").getall()).strip()

        yield {
            "id": sid,
            "title": title.strip(),
            "year": year.strip(),
            "rating": rating.strip(),
            "info": info,
            "url": response.url,
        }
