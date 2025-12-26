# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 21:15:06 UTC+08:00
"""

import datetime
import time
import typing as t
from dataclasses import dataclass, field

from fairylandfuture.core.superclass.structure import BaseFrozenStructure, BaseStructure
from spider.enums import SpiderStatus


@dataclass(frozen=False)
class MovieTask(BaseStructure):
    movie_id: str
    status: SpiderStatus = SpiderStatus.PENDING
    create_time: float = field(default_factory=time.time)
    update_time: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3
    error_msg: t.Optional[str] = None
    data: t.Optional[dict] = None


@dataclass(frozen=True)
class MovieStructure(BaseFrozenStructure):
    """豆瓣电影数据结构"""

    movie_id: str
    full_name: str
    chinese_name: str
    original_name: str
    release_date: t.Union[datetime.date, str]
    score: float
    summary: str
    icon: str


@dataclass(frozen=True)
class MovieArtistStructure(BaseFrozenStructure):
    """豆瓣电影演员数据结构"""

    artist_id: str
    name: str
