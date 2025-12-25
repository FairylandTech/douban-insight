# coding: UTF-8

import typing as t
import scrapy

import requests
from fairylandlogger import Logger, LogManager


class SpiderProxyMiddleware:
    Log: t.ClassVar["Logger"] = LogManager.get_logger("spider-middleware", "scrapy")

    def process_request(self, request: scrapy.Request, spider: scrapy.Spider):
        self.Log.debug(f"获取代理处理请求: {request.url}")
        if request.url.startswith("http://"):
            proxy = self.__get_proxy(typed=1)
            if proxy:
                request.meta["proxy"] = proxy
        elif request.url.startswith("https://"):
            proxy = self.__get_proxy(typed=2)
            if proxy:
                request.meta["proxy"] = proxy
        else:
            self.Log.warning(f"无法识别请求协议: {request.url}")

    def __get_proxy(self, typed: int) -> t.Optional[str]:
        response = requests.get(
            url=f"http://api.shenlongip.com/ip?key=d9y2e6o6&protocol={typed}&mr=1&pattern=json&need=1111&count=1&sign=ab79686e9107b4f6b1ab6d8e25529091",
            timeout=10,
        )
        response.raise_for_status()
        data: t.Dict[str, int | t.List[t.Dict[str, int | str]]] = response.json()

        self.Log.debug(f"代理IP响应数据: {data}")

        ip = data.get("data", [{}])[0].get("ip", "")
        port = data.get("data", [{}])[0].get("port", 0)

        if ip and port:
            proxies = {
                "http": f"http://{ip}:{port}",
                "https": f"https://{ip}:{port}",
                "socks5": f"socks5://{ip}:{port}",
            }

            if typed == 2:
                proxy = proxies.get("https", "")
            else:
                proxy = proxies.get("http", "")
            self.Log.info(f"获取到代理IP: {proxy}")

            return proxy

            # test_response = requests.get("http://myip.ipip.net/", proxies=proxies, timeout=5)
            # if test_response.status_code != 200:
            #     self.Log.error(f"代理IP测试失败，状态码: {test_response.status_code}")
            #     return self.__get_proxy(typed=typed)
            # else:
            #     self.Log.info(f"代理IP测试成功: {test_response.text.strip()}")
            #     return proxy
        else:
            self.Log.error("未能获取到有效的代理IP")
            return None
