# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-22 23:05:57 UTC+08:00
"""

import typing as t

import yaml


class DoubanConfig:

    @classmethod
    def load(cls) -> t.Dict[str, t.Any]:
        with open("config/application.yaml", "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        return config.get("douban", {})
