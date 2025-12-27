# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-27 03:26:17 UTC+08:00
"""

import random
import time
import traceback
from http.cookies import SimpleCookie
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from fairylandlogger import Logger, LogManager
from fake_useragent import FakeUserAgent

from fairylandfuture.database.postgresql import PostgreSQLOperator
from spider.spiders.douban.cache import RedisManager
from spider.spiders.douban.dao import MovieCommentDAO
from spider.spiders.douban.database import PostgreSQLManager


class DoubanMovieCommentFetcher:
    logger: Logger = LogManager.get_logger("douban-comment-fetcher", "douban")

    def __init__(self):
        self.cookies = self._load_cookies("config/douban.cookies")
        # self.session = requests.Session()

        # 数据库配置
        db_operator = PostgreSQLOperator(PostgreSQLManager.connector)
        self.comment_dao = MovieCommentDAO(db_operator)

        self.cache = RedisManager

        # 爬取配置
        self.page_size = 20
        self.delay = random.randint(30, 300)

    def _load_cookies(self, file_path: str) -> dict:
        """从文件加载 Cookie"""
        cookie = SimpleCookie()
        try:
            with open(file_path, "r", encoding="UTF-8") as f:
                cookie.load(f.read())
            return {k: m.value for k, m in cookie.items()}
        except FileNotFoundError:
            self.logger.warning(f"Cookie 文件未找到: {file_path}")
            return {}
        except Exception as error:
            self.logger.error(f"加载 Cookie 失败: {error}")
            return {}

    def get_movie_ids(self) -> List[str]:
        """从数据库获取所有电影 ID"""
        self.logger.info("开始从数据库获取电影 ID 列表")
        movie_ids = self.cache.get_db_movie_ids()
        self.logger.info(f"获取到 {len(movie_ids)} 个电影 ID")
        return movie_ids

    def request_with_retry(self, url: str, movie_id: str):
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "referer": f"https://movie.douban.com/subject/{movie_id}/?from=showing",
            "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": FakeUserAgent(os="Windows").random,
        }
        return requests.get(url=url, headers=headers, cookies=self.cookies, timeout=30, verify=False)

    def fetch_comments(self, movie_id: str, sort: str = "new_score") -> List[Dict]:
        """获取单个电影的短评"""
        self.logger.info(f"开始获取电影 {movie_id} 的短评（排序方式: {sort}）")

        all_comments = []
        start = 0

        while True:
            url = f"https://movie.douban.com/subject/{movie_id}/comments?start={start}&limit={self.page_size}&status=P&sort={sort}&ck=UaKp"

            try:
                response = None
                while True:
                    self.logger.info(f"请求 URL: {url}")
                    response = self.request_with_retry(url, movie_id)
                    if response.status_code == 301:
                        self.logger.warning(f"请求被重定向，可能需要更新 Cookie 或使用代理")
                    if response.status_code != 200:
                        self.logger.error(f"请求失败，状态码: {response.status_code}")
                        dalay = self.delay * random.randint(1, 3)
                        self.logger.info(f"等待 {dalay} 秒后重试...")
                        time.sleep(dalay)
                        response = self.request_with_retry(url, movie_id)
                    else:
                        break

                # 解析 HTML
                self.logger.info(f"response request header: {response.request.headers}")
                soup = BeautifulSoup(response.text, "html.parser")
                comment_items = soup.select(".comment-item")

                if not comment_items:
                    self.logger.info(f"电影 {movie_id} 排序 {sort} 已无更多评论")
                    break

                # 提取评论数据
                page_comments = []
                for item in comment_items:
                    comment_id = item.get("data-cid")
                    content_tag = item.select_one(".short")
                    content = content_tag.get_text(strip=True) if content_tag else ""

                    if comment_id and content:
                        comment_data = {
                            "movie_id": movie_id,
                            "comment_id": comment_id,
                            "content": content,
                        }
                        self.logger.debug(f"提取到的数据: {comment_data}")
                        page_comments.append(comment_data)
                        all_comments.append(comment_data)

                self.logger.info(f"本页获取到 {len(page_comments)} 条评论")

                # 检查是否有下一页
                next_link = soup.select_one("a.next")
                if not next_link or "href" not in next_link.attrs:
                    self.logger.info(f"电影 {movie_id} 排序 {sort} 评论获取完成")
                    break

                start += self.page_size

                # 延迟
                self.logger.info(f"等待 {self.delay} 秒后继续...")
                time.sleep(self.delay)

            except requests.RequestException as error:
                self.logger.error(f"请求失败: {error}")
                self.logger.error(traceback.format_exc())
                break
            except Exception as error:
                self.logger.error(f"解析失败: {error}")
                self.logger.error(traceback.format_exc())
                break

        self.logger.info(f"电影 {movie_id} 排序 {sort} 共获取 {len(all_comments)} 条评论")
        return all_comments

    def save_comments(self, comments: List[Dict]) -> None:
        """保存评论到数据库"""
        self.logger.info(f"开始保存 {len(comments)} 条评论到数据库")

        success_count = 0
        for comment in comments:
            try:
                PostgreSQLManager.ping()
                self.comment_dao.insert_comment(comment)
                success_count += 1
            except Exception as error:
                self.logger.error(f"保存评论失败: {error}")

        self.logger.info(f"成功保存 {success_count}/{len(comments)} 条评论")

    def run(self):
        """运行爬虫"""
        self.logger.info("=" * 50)
        self.logger.info("豆瓣电影短评获取器启动")
        self.logger.info("=" * 50)

        # 获取电影 ID 列表
        movie_ids = self.get_movie_ids()

        if not movie_ids:
            self.logger.warning("没有找到电影 ID，退出程序")
            return

        # 遍历每个电影
        total_movies = len(movie_ids)
        for index, movie_id in enumerate(movie_ids, start=1):
            self.logger.info(f"\n{'=' * 50}")
            self.logger.info(f"处理电影 [{index}/{total_movies}]: {movie_id}")
            self.logger.info(f"{'=' * 50}")
            if movie_id in self.cache.get_druable_comment_completed():
                self.logger.info(f"电影 {movie_id} 的评论已完成，跳过")
                continue

            # 获取两种排序方式的评论
            for sort in ["new_score", "time"]:
                comments = self.fetch_comments(movie_id, sort)

                if comments:
                    self.save_comments(comments)

                # 排序之间的延迟
                if sort == "new_score":
                    time.sleep(self.delay)

            self.logger.info(f"电影 {movie_id} 的所有评论获取完成")
            self.cache.save_druable_comment_completed(movie_id)

        self.logger.info("\n" + "=" * 50)
        self.logger.info("所有电影评论获取完成")
        self.logger.info("=" * 50)


def main():
    """主函数"""
    fetcher = DoubanMovieCommentFetcher()
    fetcher.run()


if __name__ == "__main__":
    main()
