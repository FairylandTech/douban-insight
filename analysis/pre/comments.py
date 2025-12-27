# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-27 18:00:35 UTC+08:00
"""

import typing as t
import re
import jieba

from pathlib import Path

from fairylandlogger import LogManager


class TestProProcessor:
    logger = LogManager.get_logger()

    def __init__(self, stopwords: str | Path):
        self.stopwords = self.__load_stopwords(stopwords)

    def __load_stopwords(self, stopwords: str | Path) -> set[str]:
        stopword_set = set()
        with open(stopwords, "r", encoding="UTF-8") as f:
            for line in f:
                stopword_set.add(line.strip())

        stopword_set.update(("电影", "这个", "一个", "没有", "什么", "感觉", "知道", "觉得", "还是", "就是", "片", "片子"))
        return stopword_set

    def clean(self, text: str) -> str:
        text = re.sub(r"[^\u4e00-\u9fa5a-zA-Z0-9]", "", text)
        return text.strip()

    def cut(self, text: str) -> list[str]:
        cleaned_text = self.clean(text)
        words = jieba.lcut(text)
        return [word for word in words if word not in self.stopwords and len(word) > 1]
