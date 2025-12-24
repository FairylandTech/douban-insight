# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 19:04:20 UTC+08:00
"""

import datetime
import json
import traceback
import typing as t
from urllib.parse import urlencode

import fake_useragent
import scrapy
from fairylandlogger import LogManager, Logger

from fairylandfuture.helpers.json.serializer import JsonSerializerHelper
from spider.enums import SpiderStatus
from spider.spiders.douban.cache import DoubanCacheManager, RedisManager
from spider.spiders.douban.items import MovieInfoTiem
from spider.spiders.douban.structure import MovieTask
from spider.spiders.douban.utils import DoubanUtils


class DoubanMovieSpider(scrapy.Spider):
    """
    获取电影信息和评论

    """

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-spider", "douban")
    Cache: t.ClassVar["DoubanCacheManager"] = RedisManager

    name = "douban-movie-info"
    allowed_domains = ["douban.com", "m.douban.com"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "User-Agent": fake_useragent.FakeUserAgent(os="Windows").random,
            "Referer": "https://movie.douban.com/explore",
        }
        self.cookies = DoubanUtils.load_cookies_from_file("config/douban.cookies")

    def start_requests(self):
        """
        处理缓存任务或获取新的电影ID

        :return:
        :rtype:
        """
        tasks: t.List["MovieTask"] = self.Cache.get_tasks()
        self.Log.info(f"缓存中待处理任务数量: {len(tasks)}")

        # 处理缓存中的任务
        exec_cache_task = False
        for task in tasks:
            if task.status != SpiderStatus.COMPLETED:
                exec_cache_task = True
                self.Log.info(f"继续处理任务(电影信息/解析电影信息): {task.movie_id}")
                yield from self.__request_movie_info(task.movie_id)

        # 如果没有待处理任务, 则获取新的电影ID列表
        if not exec_cache_task:
            self.Log.info("没有待处理任务，开始获取新的电影ID列表")
            yield from self.__request_movie_ids()

    def __request_movie_ids(self):
        """
        请求电影ID列表

        :return: Scrapy 请求生成器
        :rtype: t.Generator[scrapy.Request, t.Any, None]
        """
        recommend_start_index: str | None = self.Cache.get("douban:movie:recommend:start")
        start_index = recommend_start_index if recommend_start_index else "0"

        self.Log.info(f"获取推荐电影列表，起始索引: {start_index}")

        url = "https://m.douban.com/rexxar/api/v2/movie/recommend"
        params = {
            "refresh": "0",
            "start": start_index,
            "count": "1",
            "selected_categories": {},
            "uncollect": False,
            "score_range": "0,10",
            "tags": "",
            "ck": "kfPA",
        }

        url_with_params = f"{url}?{urlencode(params, doseq=True)}"

        self.Log.info(f"请求电影ID列表: {url_with_params}")

        yield scrapy.Request(
            method="GET",
            url=url_with_params,
            headers=self.made_headers(),
            cookies=self.cookies,
            callback=self.__parse_movie_ids,
            dont_filter=True,
            # errback=self._handle_error,
        )

    def __parse_movie_ids(self, response: scrapy.http.Response):
        """
        解析电影ID列表

        :param response: 电影ID API响应
        :type response: scrapy.http.Response
        :return: 生成器，包含电影信息请求
        :rtype: t.Generator[scrapy.Request, t.Any, None]
        """
        self.Log.debug(f"电影ID API响应状态码: {response.status}")

        try:
            data: t.Dict[str, t.Any] = json.loads(response.text)

            # 更新起始索引
            current_start = data.get("start", 0)
            count = data.get("count", 0)
            next_start = current_start + count
            self.Cache.set("douban:movie:recommend:start", str(next_start))
            self.Log.info(f"已更新起始索引: {next_start}")

            # 处理电影列表
            items = data.get("items", [])
            self.Log.info(f"获取到 {len(items)} 条数据")

            for item in items:
                if item.get("type") != "movie":
                    self.Log.warning(f"跳过非电影类型: {item.get('type')}")
                    continue

                movie_id: str = item.get("id")
                movie_name: str = item.get("title")
                self.Log.info(f"处理电影: {movie_name} (ID: {movie_id})")

                task = MovieTask(
                    movie_id=movie_id,
                    status=SpiderStatus.PENDING,
                )
                self.Cache.save_task(task)
                self.Log.info(f"创建新任务成功: {task}")

                yield from self.__request_movie_info(movie_id)
        except json.JSONDecodeError as e:
            self.Log.error(f"解析电影ID列表失败: {e}")
        except Exception as e:
            self.Log.error(f"处理电影ID列表时出错: {e}")

    def __request_movie_info(self, movie_id: str) -> t.Generator[scrapy.Request, t.Any, None]:
        """
        请求电影信息页面

        :param movie_id: 电影ID
        :type movie_id: str
        :return: Scrapy 请求生成器
        :rtype: scrapy.Request
        """
        movie_id = "35419153"
        movie_url = f"https://movie.douban.com/subject/{movie_id}/"
        self.Log.info(f"请求电影信息: ID={movie_id}, URL={movie_url}")

        # self.Cache.mark_processing(movie_id)

        yield scrapy.Request(
            url=movie_url,
            headers=self.headers,
            cookies=self.cookies,
            callback=self.__parse_movie_info,
            cb_kwargs={"movie_id": movie_id},
            dont_filter=True,
            # errback=self._handle_error,
            meta={"movie_id": movie_id},
        )

    def __parse_movie_info(self, response: scrapy.http.Response, movie_id: str):
        """
        解析电影信息页面

        :param response: 页面响应
        :type response: scrapy.http.Response
        :param movie_id: 电影ID
        :type movie_id: str
        :return:
        :rtype:
        """
        self.Log.info(f"解析电影信息: ID={movie_id}, Status={response.status}")

        try:
            full_name = self.__extract_full_name(response)
            chinese_name, original_name = self.separate_movie_name(full_name)
            release_date = self.__extract_release_date(response)
            score = self.__extract_score(response)
            directors = self.__extract_directors(response)
            writers = self.__extract_writers(response)
            actors = self.__extract_actors(response)
            types = self.__extract_types(response)
            countries = self.__extract_countries(response)
            summary = self.__extract_summary(response)
            icon = self.__extract_icon(response)

            item = MovieInfoTiem()
            item.update(
                movie_id=movie_id,
                full_name=full_name,
                chinese_name=chinese_name,
                original_name=original_name,
                release_date=release_date,
                score=score,
                directors=directors,
                writers=writers,
                actors=actors,
                types=types,
                countries=countries,
                summary=summary,
                icon=icon,
            )

            # self.Cache.mark_parsed(movie_id)

            self.Log.info(f"成功解析电影信息: ID={movie_id}, Data={JsonSerializerHelper.serialize(item)}")

            # yield item

        except Exception as error:
            self.Log.error(f"解析电影信息失败: ID={movie_id}, Error={error}")
            self.Log.error(traceback.format_exc())
            # self.Cache.mark_failed(movie_id, str(error))

    def made_headers(self) -> t.Dict[str, str]:
        """
        生成请求电影ID列表的请求头

        :return: 请求头
        :rtype: dict
        """
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

        return headers

    @staticmethod
    def separate_movie_name(full_name: str) -> t.Tuple[str, str]:
        """
        分割电影名称为中文名和其他名称

        :param full_name: 完整电影名称
        :type full_name: str
        :return: 中文名和其他名称的元组
        :rtype: tuple
        """
        parts = full_name.split()
        if not parts:
            return "", ""
        chinese = parts[0]
        other = " ".join(parts[1:]) if len(parts) > 1 else ""
        return chinese, other

    def __wrapper_css(self, css: scrapy.selector.SelectorList) -> str:
        """
        CSS 选择器包装器，提取文本并去除多余空白

        :param css: CSS 选择器
        :type css: scrapy.selector.SelectorList
        :return: 文本内容
        :rtype: str
        """
        return css.get(default="").strip() if css else ""

    def __extract_full_name(self, response: scrapy.http.Response) -> str:
        """
        提取电影完整名称

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 完整的电影名称
        :rtype: str
        """
        try:
            return self.__wrapper_css(response.css("""h1 span[property="v:itemreviewed"]::text"""))
        except Exception as error:
            raise error

    def __extract_release_date(self, response: scrapy.http.Response) -> datetime.date:
        """
        提取电影上映日期 (最早的日期)

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 上映日期
        :rtype: datetime.date
        """
        try:
            content = self.__wrapper_css(response.css("""span[property="v:initialReleaseDate"]::text"""))
            return datetime.datetime.strptime(content[:10], "%Y-%m-%d").date()
        except Exception as error:
            raise error

    def __extract_score(self, response: scrapy.http.Response) -> float | str:
        """
        提取电影评分

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 电影评分
        :rtype: float | str
        """
        score = self.__wrapper_css(response.css("""strong.rating_num::text"""))
        try:
            return float(score)
        except Exception as err:
            self.Log.error(f"解析电影评分失败: {err}")
            raise err

    def __extract_directors(self, response: scrapy.http.Response) -> t.List[t.Dict[str, str]]:
        """
        获取导演列表 (返回包含artist_id和name的字典列表)

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 导演列表
        :rtype: list
        """
        writers = []

        directors = response.css("""#info a[rel="v:directedBy"]::text""").getall()
        urls = response.css("""#info a[rel="v:directedBy"]::attr(href)""").getall()
        for director, url in zip(directors, urls):
            artist_id = url.strip("/").split("/")[-1] if url else None
            writers.append(
                {
                    "artist_id": artist_id,
                    "name": director.strip(),
                }
            )

        self.Log.info(f"提取导演: {len(writers)} 人")
        return writers

    def __extract_writers(self, response: scrapy.http.Response) -> t.List[t.Dict[str, str]]:
        """
        提取编剧列表

        :param response:  页面响应
        : type response: scrapy.http.Response
        :return: 编剧列表
        :rtype: list
        """
        writers = []

        writer_section = response.xpath('//div[@id="info"]//span[@class="pl" and contains(text(), "编剧")]/following-sibling::span[@class="attrs"][1]')

        if writer_section:
            writer_elements = writer_section.css("a")

            for elem in writer_elements:
                name = elem.css("::text").get()
                url = elem.css("::attr(href)").get()

                artist_id = url.strip("/").split("/")[-1] if url else None
                writers.append(
                    {
                        "artist_id": artist_id,
                        "name": name.strip() if name else name,
                    }
                )

        self.Log.info(f"提取编剧: {len(writers)} 人")
        return writers

    def __extract_actors(self, response: scrapy.http.Response) -> t.List[t.Dict[str, str]]:
        """
        提取演员列表

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 演员列表
        :rtype: list
        """
        actors = []

        actor_names = response.css("""#info a[rel="v:starring"]::text""").getall()
        actor_urls = response.css("""#info a[rel="v:starring"]::attr(href)""").getall()

        for name, url in zip(actor_names, actor_urls):
            artist_id = url.strip("/").split("/")[-1] if url else None
            actors.append(
                {
                    "artist_id": artist_id,
                    "name": name.strip(),
                }
            )

        self.Log.info(f"提取演员: {len(actors)} 人")
        return actors

    def __extract_types(self, response: scrapy.http.Response) -> t.List[str]:
        """
        提取电影类型

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 电影类型列表
        :rtype: list
        """
        return response.css('span[property="v:genre"]::text').getall()

    def __extract_countries(self, response: scrapy.http.Response) -> t.List[str]:
        """
        提取电影国家/地区

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 国家/地区列表
        :rtype: list
        """
        countries = []
        info_section = response.css("div#info")
        info_text = info_section.css("::text").getall()
        for i, text in enumerate(info_text):
            if "制片国家" in text or "地区" in text:
                if i + 1 < len(info_text):
                    country_text = info_text[i + 1].strip()
                    countries = [c.strip() for c in country_text.split("/") if c.strip()]
                break
        return countries

    def __extract_summary(self, response: scrapy.http.Response) -> str:
        """
        提取电影简介

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 电影简介
        :rtype: str
        """
        summary = response.css('span[property="v:summary"]::text').getall()
        return "".join(summary).strip() if summary else ""

    def __extract_icon(self, response: scrapy.http.Response) -> str:
        """
        提取封面图片URL

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 封面图片URL
        :rtype: str
        """
        return response.css("div#mainpic img::attr(src)").get(default="")
