# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-24 21:15:06 UTC+08:00
"""

import datetime
import typing as t
from dataclasses import dataclass

from fairylandfuture.core.superclass.structure import BaseFrozenStructure


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
