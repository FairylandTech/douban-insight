# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2024-07-03 22:53:43 UTC+08:00
"""

import threading
import typing as t


class SingletonMeta(type):
    """
    Singleton Metaclass.

    Thread-safe impl of Singleton.
    """

    __instance: t.Dict[type, object] = {}

    def __init__(cls, name, bases, namespace, **kwargs):
        super().__init__(name, bases, namespace, **kwargs)

        cls._lock: threading.RLock = threading.RLock()

    def __call__(cls, *args, **kwargs):
        if cls not in cls.__instance:
            with cls._lock:
                if cls not in cls.__instance:
                    instance = super().__call__(*args, **kwargs)
                    cls.__instance.update({cls: instance})

        return cls.__instance.get(cls)
