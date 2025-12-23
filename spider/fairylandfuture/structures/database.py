# coding: UTF-8
"""
@software: PyCharm
@author: Lionel Johnson
@contact: https://fairy.host
@organization: https://github.com/FairylandFuture
@datetime: 2024-06-27 00:07:43 UTC+08:00
"""

import typing as t
from dataclasses import dataclass, field

from fairylandfuture.core.superclass.structure import BaseFrozenStructure


@dataclass(frozen=True)
class MySQLExecuteStructure(BaseFrozenStructure):
    query: str
    args: t.Optional[t.Union[t.Sequence, t.MutableSequence, t.Mapping, t.MutableMapping]] = field(default=None)


@dataclass(frozen=True)
class PostgreSQLExecuteStructure(BaseFrozenStructure):
    query: str
    vars: t.Optional[t.Union[t.Sequence, t.MutableSequence, t.Mapping, t.MutableMapping]] = field(default=None)


@dataclass(frozen=True)
class ElasticsearchBulkParamStructure(BaseFrozenStructure):
    index: str
    id: str
    content: t.Union[t.Mapping[str, t.Any], t.MutableMapping[str, t.Any]]
