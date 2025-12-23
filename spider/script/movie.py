# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-22 20:48:18 UTC+08:00
"""

import typing as t
from http.cookies import SimpleCookie

import fake_useragent
import requests


class MovieInfoSpider:

    def __init__(self, session: t.Optional[requests.Session] = None) -> None:
        self.session = session or requests.Session()

    def spider(self):
        pass


def main():
    headers = {
        "User-Agent": fake_useragent.UserAgent().random,
        "Referer": "https://movie.douban.com/explore",
    }

    session = requests.Session()
    cookies = SimpleCookie()

    with open("douban.cookies", "r", encoding="UTF-8") as strema:
        cookies.load(strema.read())

    session.cookies.update({key: morsel.value for key, morsel in cookies.items()})

    params = {
        "refresh": "0",
        "start": "20",
        "count": "20",
        # "selected_categories": {"类型": "喜剧"},
        "selected_categories": {},
        "uncollect": False,
        "score_range": "0,10",
        "tags": "",
        "ck": "kfPA",
    }

    response = session.get(
        url = "https://m.douban.com/rexxar/api/v2/movie/recommend",
        params=params,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()

    print(response.json())

    ids = [i.get("id") for i in response.json().get("items")]
    print(ids)
    # 存入 redis 任务状态为 padding

    # 获取电影信息
    # movie_url = f"https://movie.douban.com/subject/{ids[0]}/"
    # response2 = session.get(
    #     url=movie_url,
    #     headers=headers,
    #     timeout=30,
    # )
    # response2.raise_for_status()
    # print(response2.text)
    # redis 任务 任务状态为 processing

    spider = MovieInfoSpider(session)
    spider.spider()


if __name__ == "__main__":
    main()
    pass
