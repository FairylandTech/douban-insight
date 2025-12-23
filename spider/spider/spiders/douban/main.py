# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-22 21:43:28 UTC+08:00
"""

import datetime
import json
import traceback
import typing as t
from http.cookies import SimpleCookie
from urllib.parse import urlencode

import scrapy
from fairylandlogger import LogManager, Logger
from scrapy.http import Response
from twisted.python.failure import Failure

from spider.enums import SpiderStatus
from spider.spiders.douban.cache import DoubanCacheManager
from spider.spiders.douban.items import MovieCommentItem, MovieInfoTiem
from spider.spiders.douban.structure import MovieTask

import fake_useragent


class DoubanMovieSpider(scrapy.Spider):
    """
    获取电影信息和评论

    """

    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-spider", "douban")
    Cache: t.ClassVar["DoubanCacheManager"] = DoubanCacheManager()

    name = "douban-movie"
    allowed_domains = ["douban.com", "m.douban.com"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers = {
            "User-Agent": fake_useragent.FakeUserAgent(os="Windows").random,
            "Referer": "https://movie.douban.com/explore",
        }
        self.cookies = self._load_cookies_from_file("config/douban.cookies")

    def _load_cookies_from_file(self, file_path: str) -> dict:
        """
        从文件加载 Cookie

        :param file_path: Cookie 文件路径
        :type file_path: str
        :return: Cookie 字典
        :rtype: dict
        """
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
        """
        处理缓存任务或获取新的电影ID

        :return:
        :rtype:
        """
        tasks: t.List["MovieTask"] = self.Cache.get_tasks()
        self.Log.info(f"缓存中待处理任务数量: {len(tasks)}")

        # 处理缓存中的任务
        has_pending = False
        for task in tasks:
            if task.status == SpiderStatus.PENDING or task.status == SpiderStatus.PROCESSING:
                has_pending = True
                self.Log.info(f"继续处理 PENDING 任务(电影信息/解析电影信息): {task.movie_id}")
                yield from self.__request_movie_info(task.movie_id)

            elif task.status == SpiderStatus.PARSED_INFO:
                self.Log.info(f"继续处理 PARSING 任务(电影短评): {task.movie_id}")
                yield from self.__request_movie_comments(task.movie_id)

        # 如果没有待处理任务, 则获取新的电影ID列表
        if not has_pending:
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

        url_with_params = f"{url}?{urlencode(params, doseq=True)}"

        self.Log.info(f"请求电影ID列表: {url_with_params}")

        yield scrapy.Request(
            method="GET",
            url=url_with_params,
            headers=headers,
            cookies=self.cookies,
            callback=self.__parse_movie_ids,
            dont_filter=True,
            errback=self._handle_error,
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
            errback=self._handle_error,
            meta={"movie_id": movie_id},
        )

    def __parse_movie_info(self, response: Response, movie_id: str):
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

            self.Cache.mark_parsed_info(movie_id)

            yield item
            # yield from self.__request_movie_comments(movie_id)

        except Exception as error:
            self.Log.error(f"解析电影信息失败: ID={movie_id}, Error={error}")
            self.Log.error(traceback.format_exc())
            self.Cache.mark_failed(movie_id, str(error))

    def __request_movie_comments(self, movie_id: str, start: int = 0):
        """
        请求电影评论页面

        :param movie_id: 电影ID
        :type movie_id: str
        :param start: 评论起始索引
        :type start: int
        :return:
        :rtype:
        """
        comments_url = f"https://movie.douban.com/subject/{movie_id}/comments"
        params = {
            "start": start,
            "limit": 20,
            "status": "P",
            "sort": "new_score",
        }

        url_with_params = f"{comments_url}?{urlencode(params)}"
        self.Log.info(f"请求电影评论: ID={movie_id}, start={start}")

        # 更新状态为 PARSED_COMMENT
        if start == 0:
            self.Cache.mark_parsed_comment(movie_id)

        yield scrapy.Request(
            url=url_with_params,
            headers=self.headers,
            cookies=self.cookies,
            callback=self.__parse_movie_comments,
            cb_kwargs={"movie_id": movie_id, "start": start},
            dont_filter=True,
            errback=self._handle_error,
            meta={"movie_id": movie_id},
        )

    def __parse_movie_comments(self, response: Response, movie_id: str, start: int = 0):
        """
        解析电影评论页面

        :param response: 页面响应
        :type response: scrapy.http.Response
        :param movie_id:
        :type movie_id:
        :param start:
        :type start:
        :return:
        :rtype:
        """
        self.Log.info(f"解析电影评论: ID={movie_id}, start={start}, Status={response.status}")

        try:
            # 提取评论列表
            comments = response.css("div.comment-item")
            self.Log.info(f"找到 {len(comments)} 条评论")

            for comment in comments:
                item = MovieCommentItem()
                item["movie_id"] = movie_id
                item["comment_id"] = comment.css("::attr(data-cid)").get()
                item["username"] = comment.css("span.comment-info a::text").get()
                item["rating"] = self.__extract_comment_rating(comment)
                item["content"] = comment.css("p.comment-content span.short::text").get()
                item["useful_count"] = comment.css("span.votes::text").get()
                item["comment_time"] = comment.css("span.comment-time::attr(title)").get()

                yield item

            # 判断是否需要继续翻页
            has_next = response.css("div.paginator a.next").get() is not None

            if has_next and start < 200:  # 限制最多爬取200条评论
                next_start = start + 20
                self.Log.info(f"继续获取下一页评论: start={next_start}")
                yield from self.__request_movie_comments(movie_id, next_start)
            else:
                # 所有评论处理完成，标记任务为 COMPLETED
                self.Log.info(f"电影 {movie_id} 所有数据处理完成")
                self.Cache.mark_completed(movie_id)

        except Exception as e:
            self.Log.error(f"解析电影评论失败: ID={movie_id}, Error={e}")
            self.Cache.mark_failed(movie_id, str(e))

    @staticmethod
    def separate_movie_name(full_name) -> t.Tuple[str, str]:
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

    def __extract_release_date(self, response: Response) -> datetime.date:
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

    def __extract_score(self, response: Response) -> float | str:
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

    def __extract_directors(self, response: Response) -> t.List[t.Dict[str, str]]:
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

    def __extract_writers(self, response: Response) -> t.List[t.Dict[str, str]]:
        """
        提取编剧列表

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 编剧列表
        :rtype: list
        """
        writers = []

        writer_elements = response.css("""#info span.pl:contains("编剧") ~ span.attrs a""")
        writer_names = [elem.css("::text").get() for elem in writer_elements]
        writer_urls = [elem.css("::attr(href)").get() for elem in writer_elements]

        for name, url in zip(writer_names, writer_urls):
            artist_id = url.strip("/").split("/")[-1] if url else None
            writers.append(
                {
                    "artist_id": artist_id,
                    "name": name.strip() if name else name,
                }
            )

        self.Log.info(f"提取编剧: {len(writers)} 人")
        return writers

    def __extract_actors(self, response: Response) -> t.List[t.Dict[str, str]]:
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

    def __extract_types(self, response: Response) -> t.List[str]:
        """
        提取电影类型

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 电影类型列表
        :rtype: list
        """
        return response.css('span[property="v:genre"]::text').getall()

    def __extract_countries(self, response: Response) -> list:
        """提取电影国家/地区"""
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

    def __extract_summary(self, response: Response) -> str:
        """提取电影简介"""
        summary = response.css('span[property="v:summary"]::text').getall()
        return "".join(summary).strip() if summary else ""

    def __extract_icon(self, response: Response) -> str:
        """提取封面图片URL"""
        return response.css("div#mainpic img::attr(src)").get(default="")

    def _handle_error(self, failure: Failure):
        """统一错误处理"""
        movie_id = failure.request.meta.get("movie_id")
        self.Log.error(f"请求失败: {failure.request.url}, Error: {failure.value}")

        if movie_id:
            task = self.Cache.get_task(movie_id)
            if task and task.retry_count < task.max_retries:
                self.Log.warning(f"任务 {movie_id} 失败，将重试 ({task.retry_count + 1}/{task.max_retries})")
                self.Cache.mark_failed(movie_id, str(failure.value))
            else:
                self.Log.error(f"任务 {movie_id} 超过最大重试次数，标记为失败")
                self.Cache.mark_failed(movie_id, str(failure.value))
