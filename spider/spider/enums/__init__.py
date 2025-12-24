# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-22 21:50:58 UTC+08:00
"""

import typing as t
from enum import Enum


class SpiderStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    PARSED = "parsed"
    COMPLETED = "completed"
    FAILED = "failed"
