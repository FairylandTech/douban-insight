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

LOG_ENABLED = True
LOG_LEVEL = "DEBUG"

BOT_NAME = "spider"

SPIDER_MODULES = [
    "spider.spiders",
    "spider.spiders.douban",
]
NEWSPIDER_MODULE = "spider.spiders"

ADDONS = {}

ROBOTSTXT_OBEY = True

CONCURRENT_REQUESTS_PER_DOMAIN = 1

DOWNLOAD_DELAY = 1

FEED_EXPORT_ENCODING = "UTF-8"

ITEM_PIPELINES = {
    # 处理豆瓣电影数据的 Pipeline
    "spider.spiders.douban.pipelines.DoubanMoviePipeline": 300,
}
