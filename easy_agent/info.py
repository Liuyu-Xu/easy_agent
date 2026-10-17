"""Immutable information packets passed between nodes."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from types import MappingProxyType
from typing import Any, Generic, TypeVar
from uuid import uuid4

from .errors import _safe_error, InfoError

T = TypeVar("T")


class FrozenDict(Mapping[str, Any]):
    """A recursively immutable, JSON-compatible mapping."""

    __slots__ = ("_lookup",)

    def __init__(self, items: tuple[tuple[str, Any], ...]) -> None:
        self._lookup = dict(items)

    def __getitem__(self, key: str) -> Any:
        return self._lookup[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._lookup)

    def __len__(self) -> int:
        return len(self._lookup)

    def __repr__(self) -> str:
        return repr(self._lookup)


def _freeze(value: Any, path: str = "data") -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        raise _safe_error(InfoError, f"{path} 的浮点数必须是有限值")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        frozen: list[tuple[str, Any]] = []
        for key, item in value.items():
            if not isinstance(key, str):
                raise _safe_error(InfoError, f"{path} 的字典键必须是字符串，收到 {type(key).__name__}")
            frozen.append((key, _freeze(item, f"{path}.<field>")))
        return FrozenDict(tuple(frozen))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item, f"{path}[]") for item in value)
    raise _safe_error(InfoError,
        f"{path} 包含当前版本不支持的类型 {type(value).__name__}；"
        "Info 目前只接受 JSON 兼容数据"
    )


def _thaw(value: Any) -> Any:
    if isinstance(value, FrozenDict):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _hash_data(value: Any) -> str:
    encoded = json.dumps(
        _thaw(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


class Info(Generic[T]):
    """A business value plus immutable framework provenance.

    Public construction accepts business data and optional schema labels. Provenance is
    only populated by the runtime through ``_runtime_info``.
    """

    __slots__ = (
        "_data",
        "_id",
        "_schema_id",
        "_schema_version",
        "_parent_ids",
        "_origin_kind",
        "_run_id",
        "_producer_node_id",
        "_producer_execution_id",
        "_created_at",
        "_content_hash",
        "_labels",
        "_sealed",
    )

    def __init__(
        self,
        data: T,
        *,
        schema_id: str | None = None,
        schema_version: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        object.__setattr__(self, "_sealed", False)
        object.__setattr__(self, "_data", _freeze(data))
        object.__setattr__(self, "_id", uuid4().hex)
        object.__setattr__(self, "_schema_id", schema_id)
        object.__setattr__(self, "_schema_version", schema_version)
        object.__setattr__(self, "_parent_ids", ())
        object.__setattr__(self, "_origin_kind", "user")
        object.__setattr__(self, "_run_id", None)
        object.__setattr__(self, "_producer_node_id", None)
        object.__setattr__(self, "_producer_execution_id", None)
        object.__setattr__(self, "_created_at", datetime.now(timezone.utc))
        object.__setattr__(self, "_content_hash", _hash_data(self._data))
        object.__setattr__(self, "_labels", MappingProxyType(dict(labels or {})))
        object.__setattr__(self, "_sealed", True)

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("Info 是不可变对象")
        object.__setattr__(self, name, value)

    @classmethod
    def _runtime_info(
        cls,
        data: Any,
        *,
        schema_id: str | None,
        schema_version: str | None,
        labels: Mapping[str, str] | None,
        parent_ids: tuple[str, ...],
        origin_kind: str,
        run_id: str,
        producer_node_id: str | None,
        producer_execution_id: str | None,
    ) -> "Info[Any]":
        item = cls(
            _thaw(data),
            schema_id=schema_id,
            schema_version=schema_version,
            labels=labels,
        )
        object.__setattr__(item, "_parent_ids", tuple(parent_ids))
        object.__setattr__(item, "_origin_kind", origin_kind)
        object.__setattr__(item, "_run_id", run_id)
        object.__setattr__(item, "_producer_node_id", producer_node_id)
        object.__setattr__(item, "_producer_execution_id", producer_execution_id)
        return item

    @property
    def data(self) -> T:
        return self._data  # type: ignore[return-value]

    @property
    def text(self) -> str:
        if not isinstance(self._data, str):
            raise _safe_error(InfoError, "只有字符串 Info 才能通过 .text 读取")
        return self._data

    def get(self, key: str, default: Any = None) -> Any:
        if not isinstance(self._data, FrozenDict):
            raise _safe_error(InfoError, "只有字典 Info 才能通过 .get() 读取键")
        return self._data.get(key, default)

    @property
    def id(self) -> str:
        return self._id

    @property
    def schema_id(self) -> str | None:
        return self._schema_id

    @property
    def schema_version(self) -> str | None:
        return self._schema_version

    @property
    def parent_ids(self) -> tuple[str, ...]:
        return self._parent_ids

    @property
    def origin_kind(self) -> str:
        return self._origin_kind

    @property
    def run_id(self) -> str | None:
        return self._run_id

    @property
    def producer_node_id(self) -> str | None:
        return self._producer_node_id

    @property
    def producer_execution_id(self) -> str | None:
        return self._producer_execution_id

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def content_hash(self) -> str:
        return self._content_hash

    @property
    def labels(self) -> Mapping[str, str]:
        return self._labels

    def to_dict(self, *, include_data: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self.id,
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "parent_ids": list(self.parent_ids),
            "origin_kind": self.origin_kind,
            "run_id": self.run_id,
            "producer_node_id": self.producer_node_id,
            "producer_execution_id": self.producer_execution_id,
            "created_at": self.created_at.isoformat(),
            "content_hash": self.content_hash,
            "labels": dict(self.labels),
        }
        if include_data:
            result["data"] = _thaw(self.data)
        return result

    def __repr__(self) -> str:
        return f"Info(id={self.id!r}, data={self.data!r})"


def plain_data(info: Info[Any]) -> Any:
    """Internal conversion for logs, routing and schema checks."""

    return _thaw(info.data)
