# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-22 21:47:13 UTC+08:00
"""

import typing as t

from redis import Redis


class RedisCacheManager:
    _instance: "RedisCacheManager"

    def __new__(cls, *args, **kwargs) -> "RedisCacheManager":
        if not hasattr(cls, "_instance"):
            cls._instance = super(RedisCacheManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, client: Redis, prefix: str = "spider:cache"):
        self.redis = client
        self.prefix = prefix

    def _get_key(self, key: str) -> str:
        return f"{self.prefix}:{key}"

    def get(self, key: str) -> t.Any:
        redis_key = self._get_key(key)
        redis_value: bytes = self.redis.get(name=redis_key)
        return redis_value.decode("UTF-8") if redis_value else None

    def set(self, key: str, value: t.Any, expire: int | None = None) -> None:
        redis_key = self._get_key(key)
        self.redis.set(name=redis_key, value=value, ex=expire)

    def delete(self, key: str) -> None:
        redis_key = self._get_key(key)
        self.redis.delete(redis_key)
