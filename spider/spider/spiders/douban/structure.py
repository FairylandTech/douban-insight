# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-22 22:01:25 UTC+08:00
"""

import time
import typing as t
from dataclasses import dataclass, field

from spider.enums import SpiderStatus


@dataclass
class MovieTask:
    movie_id: str
    status: SpiderStatus = SpiderStatus.PENDING
    create_time: float = field(default_factory=time.time)
    update_time: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3
    error_msg: t.Optional[str] = None
    data: t.Optional[dict] = None
