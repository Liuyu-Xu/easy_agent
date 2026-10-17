"""Node definitions and their sole editable routing declarations."""

from __future__ import annotations

from dataclasses import dataclass
import inspect
import os
from types import MappingProxyType
from typing import Any, Mapping, TYPE_CHECKING
from uuid import uuid4
import weakref

from .errors import _safe_error, GraphValidationError, OwnershipError, UnsupportedFeatureError
from .info import Info
from .config import _resolve_attempts, _UNSET, _validate_config_value, normalize_env_file
from ._definition import DEFINITION_LOCK, definition_locked, editor_identity

if TYPE_CHECKING:
    from .graph import Graph, _EndRef


_NO_DEFAULT = object()
_DEFINITION_FIELDS = frozenset({
    "env_file", "model", "provider", "base_url", "prompt", "role", "temperature",
    "max_output_tokens", "input_token_budget", "tools", "input_schema",
    "output_schema", "timeout", "max_attempts", "retry", "max_parallelism",
    "required_permissions", "api_key_env", "_node_id", "_name",
    "_owner_ref", "_owner_identity", "_outgoing", "_route_rule",
    "_receive_rule", "tool_name", "description", "side_effect",
})


@dataclass(frozen=True)
class _RouteRule:
    field: str
    cases: tuple[tuple[Any, str | None], ...]
    expected_type: type
    default_target_id: str | None | object = _NO_DEFAULT


@dataclass(frozen=True)
class _ReceiveRule:
    mode: str
    source_ids: tuple[str, ...]


class Node:
    _declaration_fields = _DEFINITION_FIELDS

    def __init__(
        self,
        *,
        env_file: str | os.PathLike[str] | None = None,
        model: str | None = None,
        provider: str | None = None,
        base_url: str | None = None,
        prompt: str | None = None,
        role: str | None = None,
        temperature: float | None = None,
        max_output_tokens: int | None = None,
        input_token_budget: int | None = None,
        tools: list[Any] | tuple[Any, ...] | None = None,
        input_schema: type | None = None,
        output_schema: type | None = None,
        timeout: float | None = None,
        max_attempts: int | None | object = _UNSET,
        retry: int | None | object = _UNSET,
        max_parallelism: int | None = None,
        required_permissions: set[str] | frozenset[str] | None = None,
        api_key_env: str | None = None,
    ) -> None:
        object.__setattr__(self, "_initializing", True)
        object.__setattr__(self, "_definition_leases", 0)
        object.__setattr__(self, "_definition_editor", None)
        resolved_attempts = _resolve_attempts(max_attempts, retry, default=None)
        self.env_file = env_file
        self.model = model
        self.provider = provider
        self.base_url = base_url
        self.prompt = prompt
        self.role = role
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.input_token_budget = input_token_budget
        self.tools = tools or ()
        self.input_schema = input_schema
        self.output_schema = output_schema
        self.timeout = timeout
        object.__setattr__(self, "retry", None if retry is _UNSET else retry)
        object.__setattr__(self, "max_attempts", resolved_attempts)
        self.max_parallelism = max_parallelism
        self.required_permissions = frozenset(required_permissions or ())
        self.api_key_env = api_key_env
        self._node_id = uuid4().hex
        self._name: str | None = None
        self._owner_ref: weakref.ReferenceType[Graph] | None = None
        self._owner_identity: str | None = None
        self._outgoing: list[str] = []
        self._route_rule: _RouteRule | None = None
        self._receive_rule: _ReceiveRule | None = None
        object.__setattr__(self, "_initializing", False)

    def __setattr__(self, name: str, value: Any) -> None:
        # User iterators are evaluated before locking. Eligibility is checked
        # again with the actual write, so a run cannot slip through conversion.
        if name == "env_file":
            value = normalize_env_file(value)
        elif name == "tools":
            value = tuple(value or ())
        elif name == "required_permissions":
            value = frozenset(value or ())
        with DEFINITION_LOCK:
            self._assign_definition(name, value)

    def _assign_definition(self, name: str, value: Any) -> None:
        if not getattr(self, "_initializing", True):
            self._ensure_mutable()
            if name == "tools" and self.owner is not None:
                self.owner._touch_nodes(*(tool for tool in value if isinstance(tool, Node)))
        if name in {"input_schema", "output_schema"}:
            if value is not None and not isinstance(value, type):
                raise TypeError(f"{name} 只接受 Python 类型或 Pydantic v2 模型类")
        else:
            _validate_config_value(name, value)
        if name == "max_attempts":
            object.__setattr__(self, "retry", None)
        elif name == "retry":
            object.__setattr__(self, "max_attempts",
                               _resolve_attempts(_UNSET, value, default=None))
        object.__setattr__(self, name, value)

    @definition_locked
    def __delattr__(self, name: str) -> None:
        if not getattr(self, "_initializing", True):
            self._ensure_mutable()
        if name in self._declaration_fields or name in {"_initializing", "_definition_leases", "_definition_editor"}:
            raise AttributeError(f"Node 的框架字段 {name} 不能删除；请使用允许的赋值清空配置")
        object.__delattr__(self, name)

    def _ensure_mutable(self) -> None:
        if self._definition_editor is not None and self._definition_editor[1] != editor_identity():
            raise _safe_error(GraphValidationError, "声明正由另一执行上下文编辑")
        if self._definition_leases:
            raise _safe_error(GraphValidationError, "工具声明仍被运行或清理资源使用，不能修改")
        owner = self.owner
        if owner is not None:
            owner._ensure_definition_mutable()
            owner._touch_nodes(self)

    def _declaration_state(self) -> dict[str, Any]:
        return {key: (list(value) if key == "_outgoing" else value)
                for key, value in self.__dict__.items() if key in self._declaration_fields}

    def _restore_declaration(self, state: Mapping[str, Any]) -> None:
        for key in self._declaration_fields - state.keys():
            self.__dict__.pop(key, None)
        for key, value in state.items():
            object.__setattr__(self, key, list(value) if key == "_outgoing" else value)

    @property
    def id(self) -> str:
        return self._node_id

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def owner(self) -> "Graph | None":
        return self._owner_ref() if self._owner_ref is not None else None

    def _check_binding(self, graph: "Graph", name: str) -> None:
        if self._definition_editor is not None and self._definition_editor[0] != graph.id:
            raise _safe_error(OwnershipError, "候选声明仍属于另一 Graph 的编辑事务")
        if self._owner_identity is not None:
            if self.owner is graph and self._name == name:
                return
            raise _safe_error(OwnershipError, "同一个 Node 实例只能以一个名称绑定到一个 Graph")

    def _bind(self, graph: "Graph", name: str) -> None:
        self._check_binding(graph, name)
        self._owner_identity = graph.id
        self._owner_ref = weakref.ref(graph)
        self._name = name

    def _require_owner(self) -> "Graph":
        owner = self.owner
        if owner is None:
            raise _safe_error(OwnershipError, "Node 必须先通过 Graph.involve 或属性赋值注册再设置连接")
        return owner

    def send_to(self, *targets: "Node") -> None:
        if not targets:
            raise _safe_error(GraphValidationError, "send_to 至少需要一个目标节点")
        self._set_downstreams(targets, append=True)

    @definition_locked
    def downstreams(self) -> tuple[Mapping[str, Any], ...]:
        """Return an immutable snapshot of every declared downstream relation."""

        owner = self._require_owner()
        result: list[Mapping[str, Any]] = []
        for edge in owner._port_edges:
            if edge.source == self.id:
                result.append(MappingProxyType({
                    "target_id": edge.target, "target_name": owner._target_name(edge.target),
                    "kind": "port", "output_port": edge.output, "input_port": edge.input,
                }))
        for target_id in self._outgoing:
            result.append(
                MappingProxyType(
                    {
                        "target_id": target_id,
                        "target_name": owner._target_name(target_id),
                        "kind": "send",
                    }
                )
            )
        if self._route_rule is not None:
            for value, target_id in self._route_rule.cases:
                result.append(
                    MappingProxyType(
                        {
                            "target_id": target_id,
                            "target_name": owner._target_name(target_id),
                            "kind": "route",
                            "field": self._route_rule.field,
                            "case": value,
                        }
                    )
                )
            if self._route_rule.default_target_id is not _NO_DEFAULT:
                target_id = self._route_rule.default_target_id
                result.append(
                    MappingProxyType(
                        {
                            "target_id": target_id,
                            "target_name": owner._target_name(target_id),
                            "kind": "route_default",
                            "field": self._route_rule.field,
                        }
                    )
                )
        return tuple(result)

    def set_downstreams(self, targets: list["Node"] | tuple["Node", ...]) -> None:
        self._set_downstreams(tuple(targets))

    @definition_locked
    def _set_downstreams(self, targets: tuple["Node", ...], *, append: bool = False) -> None:
        self._ensure_mutable()
        owner = self._require_owner()
        if hasattr(self, "_hub_contract") or any(hasattr(target, "_hub_contract") for target in targets):
            raise _safe_error(GraphValidationError, "连接 Hub 必须通过 involve 显式指定端口")
        if self._route_rule is not None:
            raise _safe_error(GraphValidationError, "同一个节点不能同时声明普通下游和 route")
        normalized: list[str] = list(self._outgoing) if append else []
        for target in targets:
            if not isinstance(target, Node):
                raise TypeError("set_downstreams 目标必须是 Node")
            if target.owner is not owner or owner._nodes_by_id.get(target.id) is not target:
                raise _safe_error(OwnershipError, "连接目标必须已经注册到同一个 Graph")
            if target.id not in normalized:
                normalized.append(target.id)
        for target_id in set(self._outgoing) - set(normalized):
            owner._validate_edge_removal(self.id, target_id)
        self._outgoing[:] = normalized

    @definition_locked
    def remove_downstream(self, target: "Node") -> None:
        self._ensure_mutable()
        owner = self._require_owner()
        if not isinstance(target, Node) or target.owner is not owner:
            raise _safe_error(OwnershipError, "连接目标必须属于同一个 Graph")
        if target.id not in self._outgoing:
            raise _safe_error(GraphValidationError, "指定的普通下游不存在")
        owner._validate_edge_removal(self.id, target.id)
        self._outgoing.remove(target.id)

    def route(
        self, *, field: str, cases: Mapping[Any, "Node | _EndRef"],
        default: "Node | _EndRef | object" = _NO_DEFAULT,
    ) -> None:
        self._route(field=field, cases=dict(cases), default=default)

    @definition_locked
    def _route(
        self,
        *,
        field: str,
        cases: Mapping[Any, "Node | _EndRef"],
        default: "Node | _EndRef | object" = _NO_DEFAULT,
        replace: bool = False,
    ) -> None:
        self._ensure_mutable()
        owner = self._require_owner()
        if self._outgoing:
            raise _safe_error(GraphValidationError, "同一个节点不能同时声明 send_to 和 route")
        if self._route_rule is not None and not replace:
            raise _safe_error(GraphValidationError, "节点已经声明 route")
        if not field or not isinstance(field, str):
            raise _safe_error(GraphValidationError, "route.field 必须是非空字符串")
        if not cases:
            raise _safe_error(GraphValidationError, "route.cases 不能为空")
        keys = list(cases.keys())
        expected_type = type(keys[0])
        if expected_type not in {str, int, float, bool}:
            raise _safe_error(GraphValidationError,
                "当前版本 route.cases 的键只支持字符串、整数、浮点数和布尔值"
            )
        if any(type(key) is not expected_type for key in keys):
            raise _safe_error(GraphValidationError, "当前版本 route.cases 的键必须具有同一精确类型")

        normalized: list[tuple[Any, str | None]] = []
        for key, target in cases.items():
            normalized.append((key, self._target_id(owner, target)))
        default_id: str | None | object = _NO_DEFAULT
        if default is not _NO_DEFAULT:
            default_id = self._target_id(owner, default)
        current = _RouteRule(
            field=field,
            cases=tuple(normalized),
            expected_type=expected_type,
            default_target_id=default_id,
        )
        if replace and self._route_rule is not None:
            old_targets = {target for _, target in self._route_rule.cases}
            old_targets.add(self._route_rule.default_target_id)
            new_targets = {target for _, target in current.cases}
            new_targets.add(current.default_target_id)
            for target in old_targets - new_targets - {_NO_DEFAULT, None}:
                owner._validate_edge_removal(self.id, target)
        self._route_rule = current

    def set_route(
        self, *, field: str, cases: Mapping[Any, "Node | _EndRef"],
        default: "Node | _EndRef | object" = _NO_DEFAULT,
    ) -> None:
        self._route(field=field, cases=dict(cases), default=default, replace=True)

    @definition_locked
    def clear_route(self) -> None:
        self._ensure_mutable()
        owner = self._require_owner()
        if self._route_rule is None:
            raise _safe_error(GraphValidationError, "节点没有 route 可清除")
        for _, target_id in self._route_rule.cases:
            if target_id is not None:
                owner._validate_edge_removal(self.id, target_id)
        if self._route_rule.default_target_id not in {_NO_DEFAULT, None}:
            owner._validate_edge_removal(
                self.id, self._route_rule.default_target_id  # type: ignore[arg-type]
            )
        self._route_rule = None

    def receive(self, *, mode: str, sources: list["Node"] | tuple["Node", ...]) -> None:
        self._receive(mode=mode, sources=tuple(sources))

    @definition_locked
    def _receive(self, *, mode: str, sources: tuple["Node", ...], replace: bool = False) -> None:
        self._ensure_mutable()
        owner = self._require_owner()
        if self._receive_rule is not None and not replace:
            raise _safe_error(GraphValidationError, "节点已经声明 receive")
        if mode != "all":
            raise _safe_error(UnsupportedFeatureError,
                "v0.3 的 sequence 历史上下文接口和错误类型尚未定稿；"
                "当前仅实现 mode='all'，详见 v0.3-issues.md"
            )
        if not sources:
            raise _safe_error(GraphValidationError, "receive.sources 不能为空")
        ids: list[str] = []
        for source in sources:
            if not isinstance(source, Node):
                raise TypeError("receive 来源必须是 Node")
            if (
                source.owner is not owner
                or owner._nodes_by_id.get(source.id) is not source
            ):
                raise _safe_error(OwnershipError, "receive 来源必须注册在同一个 Graph")
            if source.id in ids:
                raise _safe_error(GraphValidationError, "receive.sources 不能重复")
            ids.append(source.id)
        self._receive_rule = _ReceiveRule(mode=mode, source_ids=tuple(ids))

    def set_receive(
        self, *, mode: str, sources: list["Node"] | tuple["Node", ...]
    ) -> None:
        self._receive(mode=mode, sources=tuple(sources), replace=True)

    @definition_locked
    def clear_receive(self) -> None:
        self._ensure_mutable()
        if self._receive_rule is None:
            raise _safe_error(GraphValidationError, "节点没有 receive 可清除")
        self._receive_rule = None

    def _target_id(self, owner: "Graph", target: "Node | _EndRef | object") -> str | None:
        if owner._is_end(target):
            return None
        if not isinstance(target, Node):
            raise TypeError("route 目标必须是同图 Node 或 graph.END")
        if (
            target.owner is not owner
            or owner._nodes_by_id.get(target.id) is not target
        ):
            raise _safe_error(OwnershipError, "route 目标必须已经注册到同一个 Graph")
        return target.id

    async def forward(self, info: Info) -> Info:
        from ._api import call_model

        return await call_model(info, self.id)

    def logs(
        self,
        *,
        run_id: str | None = None,
        node_execution_id: str | None = None,
        event_type: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        owner = self._require_owner()
        return owner.logs(
            run_id=run_id,
            node=self,
            node_execution_id=node_execution_id,
            event_type=event_type,
            offset=offset,
            limit=limit,
        )

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.forward)
