# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-22 20:48:18 UTC+08:00
"""
import json
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
        "User-Agent": fake_useragent.FakeUserAgent(os="Windows").random,
        # "Referer": "https://movie.douban.com/explore",
        "Referer": "https://movie.douban.com/subject/35419153/comments?limit=20&status=P&sort=new_score",
    }

    headers = headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "priority": "u=0, i",
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": fake_useragent.FakeUserAgent(os="Windows").random,
    }

    session = requests.Session()
    cookies = SimpleCookie()

    with open("douban.cookies", "r", encoding="UTF-8") as strema:
        cookies.load(strema.read())

    session.cookies.update({key: morsel.value for key, morsel in cookies.items()})

    params = {
        "refresh": "0",
        "start": "500",
        "count": "20",
        "selected_categories": json.dumps({"类型": "动作"}, ensure_ascii=False, separators=(",", ":")),
        "uncollect": False,
        "score_range": "0,10",
        "tags": "动作",
        "ck": "A_Ee",
    }

    # response = session.get(
    #     url="https://m.douban.com/rexxar/api/v2/movie/recommend",
    #     params=params,
    #     headers=headers,
    #     timeout=30,
    # )

    params = {
        "percent_type": "",
        # "start": "16920",
        "start": "400",
        "limit": "20",
        "status": "P",
        "sort": "new_score",
        # "comments_only": "1",
        # "ck": "A_Ee",
    }
    # https://movie.douban.com/subject/35419153/comments?start=380&limit=20&status=P&sort=new_score
    page = 1
    size = 20
    movie_id = "35419153"
    response = session.get(
        url=f"https://movie.douban.com/subject/{movie_id}/comments?start={page * size}&limit={size}&status=P&sort=new_score",
        # params=params,
        headers=headers,
        timeout=30,
        verify=False,
    )
    response.raise_for_status()

    # print(response.json())

    print(response.json().get("html"))

    # print(response.json().get("items"))
    #
    # ids = [i.get("id") for i in response.json().get("items")]
    # print(ids)
    # 存入 cache 任务状态为 padding

    # 获取电影信息
    # movie_url = f"https://movie.douban.com/subject/{ids[0]}/"
    # response2 = session.get(
    #     url=movie_url,
    #     headers=headers,
    #     timeout=30,
    # )
    # response2.raise_for_status()
    # print(response2.text)
    # cache 任务 任务状态为 processing

    spider = MovieInfoSpider(session)
    spider.spider()


if __name__ == "__main__":
    main()
    pass
