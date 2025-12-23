# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2025-12-22 22:06:34 UTC+08:00
"""
import json
import typing as t
import time
from dataclasses import asdict

from fairylandlogger import LogManager, Logger
from redis import Redis

from spider.cache import RedisCacheManager
from spider.enums import SpiderStatus
from spider.spiders.douban.config import DoubanConfig
from spider.spiders.douban.structure import MovieTask


class DoubanCacheManager(RedisCacheManager):
    Log: t.ClassVar["Logger"] = LogManager.get_logger("douban-spider-redis", "douban")

    def __init__(self):
        super().__init__(client=self._create_redis_client())

    def _create_redis_client(self) -> "Redis":
        config: t.Dict[str, str] = DoubanConfig.load().get("redis")
        self.Log.debug(f"Redis 配置: {config}")

        client = Redis(
            host=config.get("host"),
            port=config.get("port"),
            db=config.get("db"),
            password=config.get("password"),
        )

        try:
            client.ping()
            self.Log.info("成功连接到 Redis 服务器")
            return client
        except Exception as err:
            self.Log.error(f"连接到 Redis 服务器失败: {err}")
            raise err

    def save_task(self, task: "MovieTask"):
        try:
            key = f"douban:movie:task:{task.movie_id}"
            task.update_time = time.time()

            task_data = asdict(task)
            task_data.update(status=task.status.value)
            self.Log.info(f"任务数据: {task_data}")

            self.Log.info(f"保存任务 {key} 到缓存")
            self.set(key=key, value=json.dumps(task_data, ensure_ascii=False, separators=(",", ":")))
            return True
        except Exception as err:
            self.Log.error(f"保存任务 {task.movie_id} 失败: {err}")
            return False

    def get_task(self, movie_id: str) -> t.Optional["MovieTask"]:
        key = self._get_key(f"douban:movie:task:{movie_id}")
        self.Log.info(f"从缓存获取任务 {key}")
        data = self.redis.get(key)

        if not data:
            self.Log.warning(f"任务 {movie_id} 不存在于缓存")
            return None

        try:
            task_data = json.loads(data)
            task_data["status"] = SpiderStatus(task_data["status"])
            self.Log.info(f"{movie_id} 任务数据: {task_data}")
            return MovieTask(**task_data)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"解析任务数据失败 {movie_id}: {e}")
            return None

    def mark_processing(self, movie_id: str) -> bool:
        self.Log.info(f"标记任务 {movie_id} 为处理中")
        task = self.get_task(movie_id)
        task.status = SpiderStatus.PROCESSING
        return self.save_task(task)

    def mark_parsed_info(self, movie_id: str) -> bool:
        self.Log.info(f"标记任务 {movie_id} 为信息已解析")
        task = self.get_task(movie_id)
        task.status = SpiderStatus.PARSED_INFO
        return self.save_task(task)

    def mark_parsed_comment(self, movie_id: str) -> bool:
        task = self.get_task(movie_id)
        task.status = SpiderStatus.PARSED_COMMENT
        return self.save_task(task)

    def mark_completed(self, movie_id: str, data: dict = None) -> bool:
        task = self.get_task(movie_id)
        if not task:
            task = MovieTask(movie_id=movie_id)

        task.status = SpiderStatus.COMPLETED
        task.data = data
        return self.save_task(task)

    def mark_failed(self, movie_id: str, error_msg: str) -> bool:
        task = self.get_task(movie_id)
        if not task:
            task = MovieTask(movie_id=movie_id)

        task.status = SpiderStatus.FAILED
        task.error_msg = error_msg
        task.retry_count += 1
        return self.save_task(task)

    def get_stats(self) -> dict:
        pattern = self._get_key("task", "*")
        keys = self.redis.keys(pattern)

        stats = {"total": 0, "pending": 0, "processing": 0, "completed": 0, "failed": 0}

        for key in keys:
            task = self.get_task(key.split(":")[-1])
            if task:
                stats["total"] += 1
                stats[task.status.value] += 1

        return stats

    def get_tasks(self):
        pattern = self._get_key("douban:movie:task:*")
        keys: t.List[bytes] = self.redis.keys(pattern)
        self.Log.info(f"获取所有任务，匹配模式: {pattern}")

        tasks: t.List["MovieTask"] = []
        for key in keys:
            key: bytes
            value: bytes = self.redis.get(key.decode("UTF-8"))
            if not value:
                self.Log.warning(f"任务 {key.decode('UTF-8')} 数据为空, 跳过")
                continue

            value_asdict: t.Dict[str, t.Any] = json.loads(value)
            task = MovieTask(
                movie_id=value_asdict.get("movie_id"),
                status=SpiderStatus(value_asdict.get("status")),
                create_time=value_asdict.get("create_time"),
                update_time=value_asdict.get("update_time"),
                retry_count=value_asdict.get("retry_count"),
                max_retries=value_asdict.get("max_retries"),
                error_msg=value_asdict.get("error_msg"),
                data=value_asdict.get("data"),
            )
            tasks.append(task)

        return tasks
