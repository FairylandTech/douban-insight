# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MovieInfoTiem(scrapy.Item):
    """电影信息数据项"""

    # 电影ID(豆瓣电影ID) String
    movie_id = scrapy.Field()
    # 电影名称 String
    full_name = scrapy.Field()
    # 中文名称 String
    chinese_name = scrapy.Field()
    # 其他名称 String
    original_name = scrapy.Field()
    # 上映日期 Date
    release_date = scrapy.Field()
    # 评分 Float
    score = scrapy.Field()
    # 导演 List<String>
    directors = scrapy.Field()
    # 编剧 List<String>
    writers = scrapy.Field()
    # 演员 List<String>
    actors = scrapy.Field()
    # 电影类型 List<String>
    types = scrapy.Field()
    # 制片国家/地区 List<String>
    countries = scrapy.Field()
    # 电影简介 String
    summary = scrapy.Field()
    # 电影海报 URL String
    icon = scrapy.Field()


class ArtistItem(scrapy.Item):
    """演员/导演/编剧数据项"""

    # 艺术家ID String
    artist_id = scrapy.Field()
    # 艺术家姓名 String
    name = scrapy.Field()


class MovieCommentItem(scrapy.Item):
    """电影评论数据项"""

    movie_id = scrapy.Field()  # 电影ID（豆瓣ID）
    comment_id = scrapy.Field()  # 评论ID
    content = scrapy.Field()  # 评论内容
    rating = scrapy.Field()  # 评分 (1-5)
    username = scrapy.Field()  # 用户名（仅用于日志）
    useful_count = scrapy.Field()  # 有用数（仅用于日志）
    comment_time = scrapy.Field()  # 评论时间（仅用于日志）
