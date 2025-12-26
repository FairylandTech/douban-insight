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
import unicodedata

from fairylandfuture.database.postgresql import PostgreSQLOperator
from fairylandfuture.helpers.json.serializer import JsonSerializerHelper
from spider.enums import SpiderStatus
from spider.spiders.douban.dao import MovieDAO, MovieTypeDAO
from spider.spiders.douban.items import MovieInfoTiem
from spider.spiders.douban.src import DoubanMovieSpiderBase
from spider.spiders.douban.structures import MovieTask
from spider.spiders.douban.utils import DoubanUtils


class DoubanMovieSpider(DoubanMovieSpiderBase):
    """
    获取电影信息

    """

    name = "douban-movie-info"

    def __init__(self):
        super().__init__()

        # 最大页数，默认 10, 每页固定条数 20
        self.max_pages, self.count_per_page = 26, 20

        self.headers = {
            "User-Agent": fake_useragent.FakeUserAgent(os="Windows").random,
            "Referer": "https://movie.douban.com/explore",
        }
        self.cookies = DoubanUtils.load_cookies_from_file("config/douban.cookies")

        self.movie_dao = MovieDAO(PostgreSQLOperator(self.database.connector))
        self.movie_type_dao = MovieTypeDAO(PostgreSQLOperator(self.database.connector))

    def start_requests(self):
        # self.cache.clean_completed_tasks()
        # 先同步数据库已有ID到缓存
        db_movie_ids = self.movie_dao.get_movie_id_all()
        self.Log.info(f"数据库中已存在的电影ID数量: {len(db_movie_ids)}")
        if db_movie_ids:
            self.cache.save_db_movie_ids(db_movie_ids)

        # 先处理缓存中的任务
        tasks: t.List["MovieTask"] = self.cache.get_tasks()
        tasks = [task for task in tasks if task.status != SpiderStatus.COMPLETED]
        if tasks:
            self.Log.info(f"缓存中待处理任务数量: {len(tasks)}")
            for task in tasks:
                self.Log.info(f"处理缓存任务: ID={task.movie_id}, Status={task.status}")
                yield from self.__request_movie_info(task.movie_id)

        types = self.movie_type_dao.get_all_types()
        self.Log.info(f"电影类型列表: {types}")
        for typed in types:
            type_id = typed.get("id")
            type_name = typed.get("name")
            self.Log.info(f"开始处理电影类型: ID={type_id}, Name={type_name}")
            # 分页拉取推荐列表
            # 读取缓存中的 start（偏移量），换算为页码；固定每页 20 条
            start = int(self.cache.get(f"douban:movie:recommend:start:{type_id}") or "0")
            count = self.count_per_page
            page = start // count if count > 0 else 0
            # 限制最大页数为 10 页（或传入的 max_pages），若已达上限则不再请求
            max_start = self.max_pages * count
            if start >= max_start:
                self.Log.info(f"已达到最大分页限制: start={start} >= max_start={max_start}，停止请求。")
                continue

            yield from self.__request_movie_id(start, count, page, type_id, type_name)

    def __request_movie_id(self, start: int, count: int, page: int, type_id: int, type_name: str):
        """
        请求电影推荐列表

        :param start: 开始索引 (绝对偏移量)
        :param count: 每次请求数量 (固定 20)
        :param page: 当前页码 (用于日志和下一页计算)
        :param type_id: 电影类型ID
        :param type_name: 电影类型名称
        """
        # 使用传入的 start 作为绝对偏移量；不强制归一化为 page*count
        effective_start = start
        # 最大 start 限制：10 页 * 20 条 = 200（或由 max_pages 决定）
        max_start = self.max_pages * count
        if effective_start >= max_start:
            self.Log.info(f"达到最大页数限制: start={effective_start} >= max_start={max_start}，停止分页。")
            return

        url = "https://m.douban.com/rexxar/api/v2/movie/recommend"
        params = {
            "refresh": "0",
            "start": str(effective_start),
            "count": str(count),
            "selected_categories": json.dumps({"类型": f"{type_name}"}, ensure_ascii=False, separators=(",", ":")),
            "uncollect": False,
            "score_range": "0,10",
            "tags": f"{type_name}",
            "ck": "A_Ee",
        }
        url_with_params = f"{url}?{urlencode(params, doseq=True)}"
        self.Log.info(f"请求电影ID列表: start={effective_start}, count={count}, page={page+1}, type={type_name}, max_pages={self.max_pages}")
        yield scrapy.Request(
            method="GET",
            url=url_with_params,
            headers=self.made_headers(),
            cookies=self.cookies,
            callback=self.__parse_movie_id,
            dont_filter=True,
            meta={"start": effective_start, "count": count, "page": page, "type_id": type_id, "type_name": type_name},
        )

    def __parse_movie_id(self, response: scrapy.http.Response):
        self.Log.debug(f"电影ID API响应状态码: {response.status}")
        try:
            data: t.Dict[str, t.Any] = json.loads(response.text)
            items = data.get("items", []) or []
            total = data.get("total")
            start = response.meta.get("start")
            count = response.meta.get("count")
            page = response.meta.get("page")
            type_id = response.meta.get("type_id")
            type_name = response.meta.get("type_name")

            self.Log.info(f"获取到 {len(items)} 条数据，start={start}, total={total}, type={type_name}")

            if not items:
                self.Log.info(f"当前页为空，停止分页: type={type_name}")
                self.cache.set(f"douban:movie:recommend:start:{type_id}", str(start))
                return

            for item in items:
                if item.get("type") != "movie":
                    self.Log.warning(f"跳过非电影类型: {item.get('type')}")
                    continue

                movie_id: str = item.get("id")
                movie_name: str = item.get("title")
                if DoubanUtils.check_id_in_cache(movie_id, self.cache.get_db_movie_ids()):
                    self.Log.info(f"电影ID已存在于数据库，跳过: {movie_id}")
                    continue

                task = MovieTask(movie_id=movie_id, status=SpiderStatus.PENDING)
                self.cache.save_task(task)
                yield from self.__request_movie_info(movie_id)

            # 计算下一页：固定每页 20 条；start = (page+1) * count
            next_page = (page or 0) + 1
            next_start = next_page * count
            # 如果当前返回数量等于请求数量，且尚未达到最大限制，则推进到下一页的 start
            # 否则保持当前 start 以避免越界
            self.cache.set(f"douban:movie:recommend:start:{type_id}", str(next_start))

            # 是否继续分页：以最大页数为上限
            max_start = self.max_pages * count  # 例如 10 * 20 = 200
            should_continue = True
            if not items:
                should_continue = False
                self.Log.info(f"当前页为空，停止分页: type={type_name}")
            elif next_start >= max_start:
                should_continue = False
                self.Log.info(f"达到最大页数限制: next_start={next_start} >= max_start={max_start}，停止分页: type={type_name}")
            elif total is not None and next_start >= total:
                should_continue = False
                self.Log.info(f"已达到总数限制 total={total}，停止分页: type={type_name}")
            elif len(items) < count:
                should_continue = False
                self.Log.info(f"返回数量小于请求数量，视为最后一页，停止分页: type={type_name}")

            if should_continue:
                yield from self.__request_movie_id(next_start, count, next_page, type_id, type_name)

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

        self.cache.mark_processing(movie_id)

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

            self.cache.mark_parsed(movie_id)

            self.Log.info(f"成功解析电影信息: ID={movie_id}, Data={JsonSerializerHelper.serialize(item)}")

            yield item
        except Exception as error:
            self.Log.error(f"解析电影信息失败: ID={movie_id}, Error={error}")
            self.Log.error(traceback.format_exc())
            self.cache.mark_failed(movie_id, str(error))

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

    @staticmethod
    def remove_control_chars(text: str) -> str:
        return "".join(ch for ch in text if not unicodedata.category(ch).startswith("C"))

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
        self.Log.info("提取电影完整名称")
        try:
            return self.remove_control_chars(self.__wrapper_css(response.css("""h1 span[property="v:itemreviewed"]::text""")))
        except Exception as error:
            raise error

    def __extract_release_date(self, response: scrapy.http.Response) -> t.Optional[datetime.date]:
        """
        提取电影上映日期 (依次尝试多个日期，直到解析成功)

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 上映日期
        :rtype: datetime.date
        """
        self.Log.info("提取电影上映日期")

        # 获取所有上映日期文本
        date_texts = response.css("""span[property="v:initialReleaseDate"]::text""").getall()
        self.Log.info(f"提取到所有上映日期: {date_texts}")

        if not date_texts:
            raise ValueError("未找到上映日期信息")

        for i, date_text in enumerate(date_texts, 1):
            try:
                date_text = date_text.strip()
                self.Log.info(f"尝试解析第{i}个日期: {date_text}")

                if not date_text:
                    continue

                # 提取括号前的日期部分
                if "(" in date_text:
                    date_part = date_text.split("(")[0].strip()
                else:
                    date_part = date_text

                # 根据日期部分的格式进行解析
                if len(date_part) == 4 and date_part.isdigit():
                    # 只有年份，如"2025"
                    year = int(date_part)
                    parsed_date = datetime.date(year, 1, 1)
                elif len(date_part) >= 8:  # 完整日期格式，至少"2025-1-1"
                    # 标准化日期格式
                    parts = date_part.split("-")
                    if len(parts) == 3:
                        year = parts[0]
                        month = parts[1].zfill(2)
                        day = parts[2].zfill(2)
                        date_str = f"{year}-{month}-{day}"
                        parsed_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
                    else:
                        raise ValueError(f"日期格式不正确: {date_part}")
                else:
                    raise ValueError(f"无法识别的日期格式: {date_part}")

                self.Log.info(f"第{i}个日期解析成功: {parsed_date}")
                return parsed_date

            except Exception as e:
                self.Log.warning(f"第{i}个日期解析失败: {date_text}, 错误: {e}")
                continue

        # 如果所有日期都解析失败
        error_msg = f"所有上映日期解析都失败: {date_texts}"
        self.Log.error(error_msg)
        return None

    def __extract_score(self, response: scrapy.http.Response) -> float | str:
        """
        提取电影评分

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 电影评分
        :rtype: float | str
        """
        self.Log.info("提取电影评分")
        score = self.__wrapper_css(response.css("""strong.rating_num::text"""))
        try:
            return float(score)
        except Exception as err:
            self.Log.error(f"解析电影评��失败: {err}")
            raise err

    def __extract_directors(self, response: scrapy.http.Response) -> t.List[t.Dict[str, str]]:
        """
        获取导演列表 (返回包含artist_id和name的字典列表)

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 导演列表
        :rtype: list
        """
        self.Log.info("提取导演列表")
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
        self.Log.info("提取编剧列表")
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
        self.Log.info("提取演员列表")
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
        self.Log.info("提取电影类型")
        return response.css('span[property="v:genre"]::text').getall()

    def __extract_countries(self, response: scrapy.http.Response) -> t.List[str]:
        """
        提取电影国家/地区

        :param response: 页面响应
        :type response: scrapy.http.Response
        :return: 国家/地区列表
        :rtype: list
        """
        self.Log.info("提取电影国家/地区")
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
        self.Log.info("提取电影简介")
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
        self.Log.info("提取封面图片URL")
        return self.__wrapper_css(response.css("div#mainpic img::attr(src)"))
