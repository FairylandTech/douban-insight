# Scrapy settings for spider project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import logging

from scrapy.utils.log import configure_logging as scrapy_configure_logging

scrapy_configure_logging(install_root_handler=False)

for name in ("scrapy", "twisted", "w3lib", "urllib3", "py.warnings", "asyncio"):
    logging.getLogger(name).propagate = False

BOT_NAME = "spider"

LOG_ENABLED = True
LOG_LEVEL = "DEBUG"

ADDONS = {}

ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS_PER_DOMAIN = 1

DOWNLOAD_DELAY = 5
DOWNLOAD_TIMEOUT = 5
RANDOMIZE_DOWNLOAD_DELAY = True

AUTOTHROTTLE_ENABLED = True

RETRY_ENABLED = True
RETRY_TIMES = 2
RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 403, 404, 429]

FEED_EXPORT_ENCODING = "UTF-8"

REDIRECT_ENABLED = False

SPIDER_MODULES = [
    "spider.spiders",
    # "spider.spiders.douban",
]
NEWSPIDER_MODULE = "spider.spiders"

ITEM_PIPELINES = {
    # 处理豆瓣电影数据的 Pipeline
    "spider.spiders.douban.pipelines.DoubanMoviePipeline": 0,
}

DOWNLOADER_MIDDLEWARES = {
    # 添加代理中间件
    "spider.middlewares.SpiderProxyMiddleware": 0,
}
