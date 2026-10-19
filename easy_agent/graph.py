"""Graph ownership, validation, scheduling, routing and log views."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from hashlib import sha256
import inspect
import os
from pathlib import Path
from random import uniform
from threading import Lock
from time import perf_counter
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from .config import (
    _resolve_attempts,
    _UNSET,
    _validate_config_value,
    _validate_int,
    normalize_env_file,
)
from .errors import (
    _safe_error,
    EasyAgentError,
    GraphValidationError,
    InfoError,
    NodeExecutionError,
    NodeTimeoutError,
    OwnershipError,
    PermissionDeniedError,
    ResultError,
    RoutingError,
    UnsupportedFeatureError,
)
from .events import EventStore
from .info import FrozenDict, Info, plain_data
from .node import Node, _NO_DEFAULT, _ReceiveRule, _RouteRule
from .hub import HubNode, _Port
from .tools import Tool
from ._definition import DEFINITION_LOCK, definition_locked, editor_identity
from ._blocking import bind_blocking
from ._api import ModelAttempt, ModelDeadlineExpired, bind_model_attempt
from ._plan import _Edge, _EndRef
from ._compiler import _CompilerMixin
from ._toolcall import _ToolCallMixin
from ._runtime import (
    _AttemptDeadlineExpired,
    _Outcome,
    _Retry,
    _RunState,
    _WorkItem,
)


def _remap_ids(ids: Any, old_id: str, new_id: str) -> list[str]:
    return [new_id if item == old_id else item for item in ids]


def _remap_route(rule: _RouteRule, old_id: str, new_id: str) -> _RouteRule:
    return _RouteRule(
        field=rule.field,
        cases=tuple(
            (value, new_id if target == old_id else target)
            for value, target in rule.cases
        ),
        expected_type=rule.expected_type,
        default_target_id=(
            new_id if rule.default_target_id == old_id else rule.default_target_id
        ),
    )


def _remap_receive(rule: _ReceiveRule, old_id: str, new_id: str) -> _ReceiveRule:
    return _ReceiveRule(
        mode=rule.mode,
        source_ids=tuple(
            new_id if item == old_id else item for item in rule.source_ids
        ),
    )


class Graph(_CompilerMixin, _ToolCallMixin):
    """Owns nodes and executes the workflow they declare."""

    VERSION = "0.3"

    def __init__(
        self,
        *,
        env_file: str | os.PathLike[str] | None = None,
        execution: str = "sequential",
        provider: str | None = None,
        model: str | None = None,
        base_url: str | None = None,
        api_key_env: str | object = _UNSET,
        temperature: float | None = None,
        max_output_tokens: int | None = None,
        input_token_budget: int | None = None,
        timeout: float | None = None,
        max_attempts: int | object = _UNSET,
        retry: int | object = _UNSET,
        max_concurrency: int = 1,
        log_dir: str | os.PathLike[str] | None = None,
        allowed_tools: set[str] | frozenset[str] | None = None,
        allowed_permissions: set[str] | frozenset[str] | None = None,
    ) -> None:
        if execution not in {"sequential", "async", "multiprocess", "hybrid", "auto"}:
            raise ValueError(f"未知 execution：{execution}")
        _validate_int("max_concurrency", max_concurrency)
        resolved_max_attempts = _resolve_attempts(max_attempts, retry, default=3)
        for field_name, field_value in (
            ("provider", provider), ("model", model), ("base_url", base_url),
            ("api_key_env", None if api_key_env is _UNSET else api_key_env),
            ("timeout", timeout), ("temperature", temperature),
            ("max_output_tokens", max_output_tokens),
            ("input_token_budget", input_token_budget),
        ):
            _validate_config_value(field_name, field_value)

        object.__setattr__(self, "_api_key_env_explicit", api_key_env is not _UNSET)
        if api_key_env is _UNSET:
            api_key_env = "EASY_AGENT_API_KEY"
        object.__setattr__(self, "_graph_id", uuid4().hex)
        object.__setattr__(self, "_nodes", {})
        object.__setattr__(self, "_nodes_by_id", {})
        object.__setattr__(self, "_involved", False)
        object.__setattr__(self, "_port_edges", ())
        object.__setattr__(self, "_start_ids", None)
        object.__setattr__(self, "_end_ids", None)
        object.__setattr__(self, "_max_steps", None)
        object.__setattr__(self, "_end_ref", _EndRef(self._graph_id))
        object.__setattr__(self, "_execution", execution)
        object.__setattr__(self, "_env_file", normalize_env_file(env_file) if env_file else None)
        object.__setattr__(
            self,
            "_graph_defaults",
            {
                "provider": provider,
                "model": model,
                "base_url": base_url,
                "api_key_env": api_key_env,
                "temperature": 0 if temperature is None else temperature,
                "max_output_tokens": (
                    10000 if max_output_tokens is None else max_output_tokens
                ),
                "input_token_budget": input_token_budget,
                "timeout": 180.0 if timeout is None else timeout,
                "max_attempts": resolved_max_attempts,
            },
        )
        object.__setattr__(self, "_max_concurrency", max_concurrency)
        object.__setattr__(self, "_allowed_tools", frozenset(allowed_tools or ()))
        object.__setattr__(
            self, "_allowed_permissions", frozenset(allowed_permissions or ())
        )
        resolved_log_dir = (
            Path(log_dir).resolve()
            if log_dir is not None
            else Path.cwd() / ".easy_agent" / "logs"
        )
        object.__setattr__(self, "_event_store", EventStore(resolved_log_dir))
        object.__setattr__(self, "_last_run_id", None)
        object.__setattr__(self, "_last_run_lock", Lock())
        object.__setattr__(self, "_definition_lock", DEFINITION_LOCK)
        object.__setattr__(self, "_active_runs", 0)
        object.__setattr__(self, "_edit_depth", 0)
        object.__setattr__(self, "_edit_owner", None)
        object.__setattr__(self, "_edit_journal", None)
        object.__setattr__(self, "_cleanups", {})

    @definition_locked
    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        self._ensure_definition_mutable()
        if name in {"run", "arun", "involve"}:
            raise _safe_error(OwnershipError, f"Graph.{name} 是保留方法，不能被实例属性或节点覆盖")
        if name == "entry":
            raise AttributeError("entry 已移除；请使用 start 和 end 节点列表")
        if name in {"start", "end"}:
            self._set_boundary(name, value)
            return
        if name == "max_steps":
            self._set_max_steps(value)
            return
        descriptor = inspect.getattr_static(type(self), name, None)
        if isinstance(descriptor, property) and descriptor.fset is None:
            raise AttributeError(f"property '{name}' has no setter")
        if isinstance(value, Node):
            self._register_node(name, value)
            object.__setattr__(self, name, value)
            return
        if name in self._nodes:
            self._remove_registration(name)
        object.__setattr__(self, name, value)

    @definition_locked
    def __delattr__(self, name: str) -> None:
        if not name.startswith("_"):
            self._ensure_definition_mutable()
        if name in self._nodes:
            self._remove_registration(name)
        object.__delattr__(self, name)

    def _ensure_definition_mutable(self) -> None:
        lock = getattr(self, "_definition_lock", None)
        if lock is None:
            return
        with lock:
            if self._edit_depth and self._edit_owner != editor_identity():
                raise _safe_error(GraphValidationError, "Graph 正由另一执行上下文编辑")
            if self._active_runs or self._cleanups:
                raise _safe_error(GraphValidationError,
                    "Graph 正在运行；运行期间只能查询日志，不能修改图或节点定义"
                )
            for tool in self._tool_declarations():
                editor = tool._definition_editor
                if editor is not None and editor != (self.id, editor_identity()):
                    raise _safe_error(GraphValidationError, "共享工具声明正由另一事务编辑")

    def _begin_run(self) -> tuple[Tool, ...]:
        with self._definition_lock:
            if self._edit_depth:
                raise _safe_error(GraphValidationError, "Graph 正在编辑，不能开始运行")
            if self._cleanups:
                raise _safe_error(GraphValidationError, "Graph 仍有未退出的清理资源，不能开始新运行")
            tools = self._tool_declarations()
            if any(tool._definition_editor is not None for tool in tools):
                raise _safe_error(GraphValidationError, "共享工具仍在编辑事务中，不能开始运行")
            for tool in tools:
                object.__setattr__(tool, "_definition_leases", tool._definition_leases + 1)
            object.__setattr__(self, "_active_runs", self._active_runs + 1)
            return tools

    def _end_run(self, tools: tuple[Tool, ...]) -> None:
        with self._definition_lock:
            object.__setattr__(self, "_active_runs", self._active_runs - 1)
            for tool in tools:
                object.__setattr__(tool, "_definition_leases", tool._definition_leases - 1)

    def _tool_declarations(self) -> tuple[Tool, ...]:
        found: dict[int, Tool] = {}
        pending = list(self._nodes.values())
        seen: set[int] = set()
        while pending:
            node = pending.pop()
            if id(node) in seen:
                continue
            seen.add(id(node))
            if isinstance(node, Tool):
                found[id(node)] = node
            pending.extend(tool for tool in node.tools if isinstance(tool, Tool))
        return tuple(found.values())

    @property
    def id(self) -> str:
        return self._graph_id

    @property
    def END(self) -> _EndRef:
        return self._end_ref

    @property
    @definition_locked
    def start(self) -> list[Node] | None:
        return None if self._start_ids is None else [self._nodes_by_id[item] for item in self._start_ids]

    @property
    @definition_locked
    def end(self) -> list[Node] | None:
        return None if self._end_ids is None else [self._nodes_by_id[item] for item in self._end_ids]

    @property
    @definition_locked
    def max_steps(self) -> int | None:
        return self._max_steps

    def _downstream_specs(self, node: Node) -> list[dict[str, Any]]:
        """Every outgoing relation of one node, in declaration order.

        A node declares either port edges, plain targets, or a route — never a
        mix — so the grouped view below cannot reorder like-for-like entries.
        """

        specs: list[dict[str, Any]] = [
            {
                "target_id": edge.target,
                "target_name": self._target_name(edge.target),
                "kind": "port",
                "output_port": edge.output,
                "input_port": edge.input,
            }
            for edge in self._port_edges
            if edge.source == node.id
        ]
        specs.extend(
            {
                "target_id": target_id,
                "target_name": self._target_name(target_id),
                "kind": "send",
            }
            for target_id in node._outgoing
        )
        rule = node._route_rule
        if rule is not None:
            specs.extend(
                {
                    "target_id": target_id,
                    "target_name": self._target_name(target_id),
                    "kind": "route",
                    "field": rule.field,
                    "case": value,
                }
                for value, target_id in rule.cases
            )
            if rule.default_target_id is not _NO_DEFAULT:
                specs.append(
                    {
                        "target_id": rule.default_target_id,
                        "target_name": self._target_name(rule.default_target_id),
                        "kind": "route_default",
                        "field": rule.field,
                    }
                )
        return specs

    @property
    @definition_locked
    def relations(self) -> Mapping[str, Mapping[str, tuple[Mapping[str, Any], ...]]]:
        result: dict[str, Mapping[str, tuple[Mapping[str, Any], ...]]] = {}
        for source_name, source in self._nodes.items():
            grouped: dict[str, list[Mapping[str, Any]]] = {}
            for spec in self._downstream_specs(source):
                detail = {
                    key: spec[key]
                    for key in ("kind", "output_port", "input_port", "field", "case")
                    if key in spec
                }
                grouped.setdefault(spec["target_name"], []).append(
                    MappingProxyType(detail)
                )
            result[source_name] = MappingProxyType(
                {key: tuple(items) for key, items in grouped.items()}
            )
        return MappingProxyType(result)

    def _target_name(self, target_id: str | None) -> str:
        if target_id is None:
            return "__END__"
        target = self._nodes_by_id.get(target_id)
        return target.name if target is not None and target.name else f"<missing:{target_id}>"

    def _is_end(self, target: object) -> bool:
        return isinstance(target, _EndRef) and target.graph_id == self.id

    def _set_boundary(self, name: str, value: list[Node] | None) -> None:
        if value is None:
            object.__setattr__(self, f"_{name}_ids", None)
            return
        if not isinstance(value, list):
            raise TypeError(f"{name} 必须是 Node 列表")
        if not value or len({id(node) for node in value}) != len(value):
            raise _safe_error(GraphValidationError, f"{name} 不能为空或包含重复节点")
        if any(not isinstance(node, Node) or node.owner is not self
               or self._nodes_by_id.get(node.id) is not node for node in value):
            raise _safe_error(OwnershipError, f"{name} 必须包含当前 Graph 中已注册的 Node")
        object.__setattr__(self, f"_{name}_ids", tuple(node.id for node in value))

    def _set_max_steps(self, value: int | None) -> None:
        if value is not None:
            _validate_int("max_steps", value)
        object.__setattr__(self, "_max_steps", value)

    def _register_node(self, name: str, node: Node) -> None:
        if not name or name.startswith("_"):
            raise _safe_error(OwnershipError, "节点属性名必须是公开的非空名称")
        old = self._nodes.get(name)
        if old is node:
            return
        node._check_binding(self, name)
        if old is not None:
            incident = bool(
                self._references_to(old.id, excluding=old.id)
                or old._outgoing
                or old._route_rule is not None
                or old._receive_rule is not None
            )
            if incident:
                raise _safe_error(GraphValidationError,
                    f"节点 {name} 存在连接或入口引用；请使用 "
                    "graph.replace_node(..., migrate_edges=True)"
                )
        self._touch_nodes(node)
        node._bind(self, name)
        if old is not None:
            self._nodes_by_id.pop(old.id, None)
        self._nodes[name] = node
        self._nodes_by_id[node.id] = node

    def _remove_registration(self, name: str) -> None:
        node = self._nodes[name]
        if self._edit_depth == 0:
            references = self._references_to(node.id, excluding=node.id)
            if references:
                raise _safe_error(GraphValidationError,
                    f"节点 {name} 仍被引用，不能删除：{references}"
                )
        self._nodes.pop(name)
        self._nodes_by_id.pop(node.id, None)
        for boundary in ("start", "end"):
            ids = getattr(self, f"_{boundary}_ids")
            if ids is not None:
                object.__setattr__(self, f"_{boundary}_ids", tuple(item for item in ids if item != node.id))

    def _references_to(self, target_id: str, *, excluding: str | None = None) -> list[str]:
        references = [name for name in ("start", "end")
                      if target_id in (getattr(self, f"_{name}_ids") or ())]
        references.extend("Hub 端口连接" for edge in self._port_edges
                          if target_id in (edge.source, edge.target))
        for source_name, source in self._nodes.items():
            if source.id == excluding:
                continue
            if target_id in source._outgoing:
                references.append(f"{source_name}.send_to")
            if source._route_rule is not None:
                if any(item == target_id for _, item in source._route_rule.cases):
                    references.append(f"{source_name}.route")
                if source._route_rule.default_target_id == target_id:
                    references.append(f"{source_name}.route.default")
            if (
                source._receive_rule is not None
                and target_id in source._receive_rule.source_ids
            ):
                references.append(f"{source_name}.receive")
        return references

    def _validate_edge_removal(self, source_id: str, target_id: str) -> None:
        if self._edit_depth:
            return
        target = self._nodes_by_id.get(target_id)
        if (
            target is not None
            and target._receive_rule is not None
            and source_id in target._receive_rule.source_ids
        ):
            source_name = self._target_name(source_id)
            target_name = self._target_name(target_id)
            raise _safe_error(GraphValidationError,
                f"不能删除 {source_name} -> {target_name}；目标 receive 仍依赖该连接"
            )

    @contextmanager
    def edit(self):
        """Atomically edit a graph definition and validate it on commit."""

        with self._definition_lock:
            self._ensure_definition_mutable()
            if self._edit_depth:
                raise _safe_error(GraphValidationError, "graph.edit() 不支持嵌套使用")
            snapshot = self._definition_snapshot()
            object.__setattr__(self, "_edit_journal", {})
            object.__setattr__(self, "_edit_owner", editor_identity())
            try:
                self._touch_nodes(*self._nodes.values())
            except BaseException:
                object.__setattr__(self, "_edit_journal", None)
                object.__setattr__(self, "_edit_owner", None)
                raise
            object.__setattr__(self, "_edit_depth", 1)
        try:
            yield self
            with self._definition_lock:
                self.validate()
        except BaseException:
            with self._definition_lock:
                self._restore_definition(snapshot)
            raise
        finally:
            with self._definition_lock:
                for node, _ in self._edit_journal.values():
                    object.__setattr__(node, "_definition_editor", None)
                object.__setattr__(self, "_edit_depth", 0)
                object.__setattr__(self, "_edit_journal", None)
                object.__setattr__(self, "_edit_owner", None)

    def involve(self, *, nodes: list[Node], relations: list[tuple[Node | _Port, Node | _Port]],
                start: list[Node], end: list[Node]) -> None:
        """Assemble atomically; start/end order defines the input/output positions."""
        if any(not isinstance(value, list) for value in (nodes, relations, start, end)):
            raise TypeError("nodes、relations、start 和 end 必须是列表")
        members = tuple(nodes)
        starts, ends = tuple(start), tuple(end)
        edges = []
        for pair in relations:
            if not isinstance(pair, tuple) or len(pair) != 2:
                raise TypeError("relations 每项必须是 (source, target) 二元组")
            edges.append(tuple(pair))
        with self._definition_lock:
            self._ensure_definition_mutable()
            if self._nodes or self._involved:
                raise _safe_error(GraphValidationError, "involve 仅用于空 Graph 的一次完整装配")
            if not members or any(not isinstance(node, Node) for node in members):
                raise _safe_error(GraphValidationError, "nodes 必须包含至少一个 Node")
            identities = {id(node) for node in members}
            if len(identities) != len(members) or len({node.id for node in members}) != len(members):
                raise _safe_error(OwnershipError, "nodes 不能重复包含同一个节点身份")
            for boundary in (starts, ends):
                if not boundary or len({id(node) for node in boundary}) != len(boundary):
                    raise _safe_error(GraphValidationError, "start 与 end 不能为空或包含重复节点")
                if any(id(node) not in identities for node in boundary):
                    raise _safe_error(OwnershipError, "start 与 end 必须属于 nodes")
            for node in members:
                if node._owner_identity is not None:
                    raise _safe_error(OwnershipError, "involve 节点必须尚未注册到 Graph")
                if node._outgoing or node._route_rule is not None or node._receive_rule is not None:
                    raise _safe_error(GraphValidationError, "involve 不接收已有连线、route 或 receive 的节点")
            seen = set()
            port_edges = []
            ordinary = []
            for source, target in edges:
                source_node = source.node if isinstance(source, _Port) else source
                target_node = target.node if isinstance(target, _Port) else target
                if id(source_node) not in identities or id(target_node) not in identities:
                    raise _safe_error(OwnershipError, "关系两端必须属于 nodes")
                if ((isinstance(source, HubNode) or isinstance(target, HubNode))
                        or (isinstance(source, _Port) and source.direction != "output")
                        or (isinstance(target, _Port) and target.direction != "input")):
                    raise _safe_error(GraphValidationError, "Hub 关系必须按输出端到输入端连接，不能省略端口")
                edge = _Edge(source_node.id, target_node.id,
                             source.key if isinstance(source, _Port) else None,
                             target.key if isinstance(target, _Port) else None)
                if edge in seen:
                    raise _safe_error(GraphValidationError, "relations 不能包含重复边")
                seen.add(edge)
                (port_edges if isinstance(source, _Port) or isinstance(target, _Port) else ordinary).append(edge)
            with self.edit():
                for node in members:
                    self._register_node(f"node_{node.id}", node)
                object.__setattr__(self, "_port_edges", tuple(port_edges))
                for edge in ordinary:
                    self._nodes_by_id[edge.source].send_to(self._nodes_by_id[edge.target])
                self.start = list(starts)
                self.end = list(ends)
            object.__setattr__(self, "_involved", True)

    def _touch_nodes(self, *nodes: Node) -> None:
        """Check the entire declaration set before acquiring any edit ownership."""
        journal = self._edit_journal
        if journal is None:
            return
        pending = list(nodes)
        candidates = {}
        while pending:
            node = pending.pop()
            if id(node) in journal or id(node) in candidates:
                continue
            if node._owner_identity is not None and node.owner is not self:
                raise _safe_error(OwnershipError, "不能编辑另一 Graph 所属的节点声明")
            if node._definition_leases:
                raise _safe_error(GraphValidationError, "声明仍被运行或清理资源使用，不能编辑")
            editor = node._definition_editor
            if editor is not None and editor != (self.id, self._edit_owner):
                raise _safe_error(GraphValidationError, "候选声明正由另一事务编辑")
            candidates[id(node)] = (node, node._declaration_state())
            pending.extend(tool for tool in node.tools if isinstance(tool, Tool))
        for identity, (node, state) in candidates.items():
            journal[identity] = (node, state)
            object.__setattr__(node, "_definition_editor", (self.id, self._edit_owner))

    def _definition_snapshot(self) -> dict[str, Any]:
        return {
            "nodes": dict(self._nodes),
            "nodes_by_id": dict(self._nodes_by_id),
            "port_edges": self._port_edges,
            "start_ids": self._start_ids,
            "end_ids": self._end_ids,
            "max_steps": self._max_steps,
            "public_attrs": {key: value for key, value in self.__dict__.items()
                             if not key.startswith("_")},
        }

    def _restore_definition(self, snapshot: Mapping[str, Any]) -> None:
        for node, state in self._edit_journal.values():
            node._restore_declaration(state)
        restored_nodes = dict(snapshot["nodes"])
        for name in tuple(self.__dict__):
            if not name.startswith("_"):
                self.__dict__.pop(name, None)
        for name, value in snapshot["public_attrs"].items():
            object.__setattr__(self, name, value)
        object.__setattr__(self, "_nodes", restored_nodes)
        object.__setattr__(self, "_nodes_by_id", dict(snapshot["nodes_by_id"]))
        object.__setattr__(self, "_port_edges", snapshot["port_edges"])
        object.__setattr__(self, "_start_ids", snapshot["start_ids"])
        object.__setattr__(self, "_end_ids", snapshot["end_ids"])
        object.__setattr__(self, "_max_steps", snapshot["max_steps"])

    @definition_locked
    def replace_node(
        self, name: str, new_node: Node, *, migrate_edges: bool = False
    ) -> None:
        if not isinstance(new_node, Node):
            raise TypeError("new_node 必须是 Node")
        self._ensure_definition_mutable()
        with (nullcontext(self) if self._edit_depth else self.edit()):
            if name not in self._nodes:
                raise _safe_error(GraphValidationError, f"节点 {name} 不存在")
            old = self._nodes[name]
            if old is new_node:
                return
            if isinstance(old, HubNode) or isinstance(new_node, HubNode) or any(
                    old.id in (edge.source, edge.target) for edge in self._port_edges):
                raise _safe_error(UnsupportedFeatureError, "端口关联节点替换协议尚未定稿；请重新装配图，见 Hub 问题文件")
            if new_node._outgoing or new_node._route_rule or new_node._receive_rule:
                raise _safe_error(GraphValidationError, "替换候选已有拓扑声明，不能猜测合并")
            incident = bool(
                self._references_to(old.id, excluding=old.id)
                or old._outgoing
                or old._route_rule is not None
                or old._receive_rule is not None
            )
            if incident and not migrate_edges:
                raise _safe_error(GraphValidationError,
                    f"节点 {name} 存在连接或入口引用；迁移时必须显式设置 migrate_edges=True"
                )
            new_node._check_binding(self, name)
            self._touch_nodes(new_node)
            new_node._bind(self, name)
            if migrate_edges:
                object.__setattr__(
                    new_node, "_outgoing",
                    _remap_ids(old._outgoing, old.id, new_node.id),
                )
                if old._route_rule is not None:
                    object.__setattr__(
                        new_node, "_route_rule",
                        _remap_route(old._route_rule, old.id, new_node.id),
                    )
                if old._receive_rule is not None:
                    object.__setattr__(
                        new_node, "_receive_rule",
                        _remap_receive(old._receive_rule, old.id, new_node.id),
                    )
                for source in self._nodes.values():
                    if source is old:
                        continue
                    source._outgoing[:] = _remap_ids(
                        source._outgoing, old.id, new_node.id
                    )
                    if source._route_rule is not None:
                        source._route_rule = _remap_route(
                            source._route_rule, old.id, new_node.id
                        )
                    if source._receive_rule is not None:
                        source._receive_rule = _remap_receive(
                            source._receive_rule, old.id, new_node.id
                        )
                for boundary in ("start", "end"):
                    ids = getattr(self, f"_{boundary}_ids")
                    if ids is not None:
                        object.__setattr__(
                            self, f"_{boundary}_ids",
                            tuple(_remap_ids(ids, old.id, new_node.id)),
                        )
            self._nodes[name] = new_node
            self._nodes_by_id.pop(old.id, None)
            self._nodes_by_id[new_node.id] = new_node
            object.__setattr__(self, name, new_node)

    def validate(self) -> None:
        self._compile(
            max_steps_override=None,
            max_concurrency_override=None,
            for_run=False,
        )

    def run(
        self,
        inputs: list[Info[Any]],
        *,
        max_steps: int | None = None,
        max_concurrency: int | None = None,
    ) -> list[Info[Any]]:
        """Execute once; inputs and outputs follow the declared boundary order."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.arun(
                    inputs,
                    max_steps=max_steps,
                    max_concurrency=max_concurrency,
                )
            )
        raise RuntimeError(
            "检测到正在运行的异步事件循环；请使用 await graph.arun(inputs)"
        )

    __call__ = run

    async def arun(
        self,
        inputs: list[Info[Any]],
        *,
        max_steps: int | None = None,
        max_concurrency: int | None = None,
    ) -> list[Info[Any]]:
        """Execute asynchronously through the same ordered boundary protocol."""
        if not isinstance(inputs, list) or any(not isinstance(info, Info) for info in inputs):
            raise TypeError("Graph 输入必须是 Info 列表")
        initial = tuple(inputs)
        run_id = None
        try:
            if self._execution in {"multiprocess", "hybrid", "auto"}:
                raise _safe_error(UnsupportedFeatureError,
                    f"execution={self._execution!r} 尚不支持；当前仅支持 sequential 和 async"
                )
            tools = self._begin_run()
            try:
                run_id = uuid4().hex
                return await self._arun_frozen(
                    initial,
                    run_id=run_id,
                    max_steps=max_steps,
                    max_concurrency=max_concurrency,
                )
            finally:
                self._end_run(tools)
        except Exception as exc:
            raise _safe_error(
                EasyAgentError, "Graph 运行失败", run_id=run_id, cause=exc,
            ) from None

    async def _arun_frozen(
        self,
        inputs: tuple[Info[Any], ...],
        *,
        run_id: str,
        max_steps: int | None = None,
        max_concurrency: int | None = None,
    ) -> list[Info[Any]]:
        state: _RunState | None = None
        self._event_store.start_run(run_id)
        with self._last_run_lock:
            object.__setattr__(self, "_last_run_id", run_id)

        try:
            plan = self._compile(
                max_steps_override=max_steps,
                max_concurrency_override=max_concurrency,
                for_run=True,
            )
            if len(inputs) != len(plan.start_ids):
                raise _safe_error(GraphValidationError,
                    f"输入数量 {len(inputs)} 与开始节点数量 {len(plan.start_ids)} 不匹配")
            self._event_store.append(
                run_id,
                "GraphRunStarted",
                data={
                    "graph_id": self.id,
                    "graph_version": self.VERSION,
                    "topology_hash": plan.topology_hash,
                    "execution": self._execution,
                    "config": plan.config.log_view(),
                    "allowed_tools": sorted(self._allowed_tools),
                    "allowed_permissions": sorted(self._allowed_permissions),
                },
            )
            for secret_name, source in plan.config.secret_sources.items():
                self._event_store.append(
                    run_id,
                    "SecretReferenceResolved",
                    category="audit",
                    data={
                        "secret_reference": secret_name,
                        "source": source,
                        "resolved": secret_name in plan.config.secret_values,
                    },
                )
            for info in inputs:
                self._event_store.append(
                    run_id, "InfoConsumed", info_ids=[info.id],
                    data={"content_hash": info.content_hash, "schema_id": info.schema_id,
                          "origin_kind": info.origin_kind},
                )
            state = _RunState(plan=plan, run_id=run_id)
            result = await self._run_plan(plan, inputs, run_id, state)
            state.status = "SUCCEEDED"
            self._event_store.append(
                run_id,
                "GraphRunSucceeded",
                info_ids=[info.id for info in result],
                data={"steps": state.steps},
            )
            return result
        except asyncio.CancelledError:
            if state is None or not state.terminal_logged:
                self._event_store.append(run_id, "GraphRunCancelled")
            raise
        except Exception as exc:
            error = _safe_error(EasyAgentError, "Graph 运行失败", run_id=run_id, cause=exc)
            if isinstance(error, PermissionDeniedError):
                self._event_store.append(
                    run_id,
                    "PermissionDenied",
                    category="audit",
                    data={"error_type": type(error).__name__},
                )
            if state is None or not state.terminal_logged:
                self._event_store.append(
                    run_id, "GraphRunFailed", data={"error_type": type(error).__name__},
                )
            raise error from None
        finally:
            if state is not None and state.blocking_pool is not None:
                # Never wait for a blocked business thread; its definition lease
                # is released by the future callback once it really returns.
                state.blocking_pool.shutdown(wait=False)
                state.blocking_pool = None

    async def _run_plan(
        self, plan: _Plan, inputs: tuple[Info[Any], ...], run_id: str, state: _RunState
    ) -> list[Info[Any]]:
        initial_group = uuid4().hex if len(plan.start_ids) > 1 else None
        ready: deque[_WorkItem] = deque(
            _WorkItem(node_id=node_id, info=info, branch_group=initial_group)
            for node_id, info in zip(plan.start_ids, inputs)
        )

        active: set[asyncio.Task] = set()
        task_nodes: dict[asyncio.Task, str] = {}
        active_by_node: dict[str, int] = {node_id: 0 for node_id in plan.nodes}
        run_limit = 1 if self._execution == "sequential" else plan.config.max_concurrency

        def completed(task):
            state.completion_sequence += 1
            state.completions.put_nowait((state.completion_sequence, task))

        watchdog = asyncio.create_task(self._stall_watchdog(state))
        try:
            while ready or active or state.delayed:
                self._release_delayed(state, ready)
                queued = len(ready)
                for _ in range(queued):
                    if len(active) >= run_limit:
                        break
                    item = ready.popleft()
                    node_plan = plan.nodes[item.node_id]
                    node_limit = node_plan.max_parallelism or plan.config.max_concurrency
                    if active_by_node[item.node_id] >= node_limit:
                        ready.append(item)
                        continue
                    task = asyncio.create_task(self._execute_item(state, item))
                    state.owned_tasks.add(task)
                    task.add_done_callback(completed)
                    active.add(task)
                    task_nodes[task] = item.node_id
                    active_by_node[item.node_id] += 1
                if not active and not state.delayed:
                    raise _safe_error(GraphValidationError,
                        "存在就绪任务但没有可取得的执行名额", run_id=run_id
                    )
                # A due retry must wake the coordinator even while another
                # branch is active. Waiting never occupies a business slot.
                delay = (
                    max(0.0, min(entry.ready_at for entry in state.delayed) - perf_counter())
                    if state.delayed else None
                )
                try:
                    sequence, task = await asyncio.wait_for(state.completions.get(), timeout=delay)
                except asyncio.TimeoutError:
                    continue
                active.remove(task)
                node_id = task_nodes.pop(task)
                active_by_node[node_id] -= 1
                try:
                    outcome = task.result()
                finally:
                    state.owned_tasks.discard(task)
                if isinstance(outcome, _Retry):
                    # Backoff does not hold a concurrency slot.
                    state.delayed.append(outcome)
                    continue
                outcome = self._commit_outcome(state, outcome, sequence)
                await self._dispatch_outcome(state, outcome, ready)
        except asyncio.CancelledError:
            self._stop_run(state, "CANCELLED")
            raise
        except BaseException as exc:
            self._stop_run(state, "FAILED", exc)
            raise
        finally:
            watchdog.cancel()
            await asyncio.gather(watchdog, return_exceptions=True)
            # A stopped coordinator must also observe failures already queued
            # as done; pending-only cleanup leaves sibling failures unhandled.
            for task in tuple(state.owned_tasks):
                if task.done():
                    if not task.cancelled():
                        task.exception()
                    state.owned_tasks.discard(task)
                    state.cancel_requested.discard(task)
            pending = {task for task in state.owned_tasks if not task.done()}
            with self._definition_lock:
                blocked = {
                    key for key, future in state.blocking_futures.items()
                    if not future.done()
                }
            if pending or blocked:
                self._retain_cleanup(state)
                for task in pending:
                    if task not in state.cancel_requested:
                        state.cancel_requested.add(task)
                        task.cancel()
                # Finite cleanup and forceful isolation remain specification
                # dependencies. Shield keeps repeated cancellation from erasing
                # actual ownership; done callbacks release leases on exit.
                if pending:
                    await asyncio.shield(
                        asyncio.gather(*pending, return_exceptions=True)
                    )

        if state.join_buffers:
            waiting = [
                {
                    "node": plan.names_by_id[node_id],
                    "branch_group": branch_group,
                    "received_count": len(values),
                }
                for (node_id, branch_group), values in state.join_buffers.items()
            ]
            raise _safe_error(GraphValidationError,
                f"汇合输入不完整：{waiting}", run_id=run_id
            )
        candidates = state.candidates
        if plan.end_ids is not None:
            by_node: dict[str, Info[Any]] = {}
            for node_id, terminal in candidates:
                if node_id not in plan.end_ids or node_id in by_node:
                    raise _safe_error(ResultError, "结束节点未声明或重复产生输出；请显式汇合", run_id=run_id)
                by_node[node_id] = terminal
            if set(by_node) != set(plan.end_ids):
                raise _safe_error(ResultError, "结束节点输出不完整", run_id=run_id)
            terminals = [by_node[node_id] for node_id in plan.end_ids]
        elif len(candidates) != 1:
            raise _safe_error(ResultError,
                f"未声明 end 时必须产生一个结果候选，实际为 {len(candidates)} 个",
                run_id=run_id,
            )
        else:
            terminals = [candidates[0][1]]
        results = []
        for terminal in terminals:
            published = Info._runtime_info(
                terminal.data, schema_id=terminal.schema_id,
                schema_version=terminal.schema_version, labels=terminal.labels,
                parent_ids=(terminal.id,), origin_kind="graph_output", run_id=run_id,
                producer_node_id=None, producer_execution_id=None,
            )
            self._event_store.append(
                run_id, "InfoPublished", info_ids=[published.id, terminal.id],
                data={"content_hash": published.content_hash, "schema_id": published.schema_id},
            )
            results.append(published)
        return results

    async def _execute_item(
        self, state: _RunState, item: _WorkItem
    ) -> _Outcome | _Retry:
        node_plan = state.plan.nodes[item.node_id]
        execution_id = item.execution_id or uuid4().hex
        attempt = item.attempt
        if attempt == 1:
            for event in ("NodeReady", "NodeQueued"):
                self._event_store.append(
                    state.run_id, event, category="node", span_id=execution_id,
                    node_id=node_plan.node_id, node_execution_id=execution_id,
                    info_ids=[item.info.id], delivery_id=item.delivery_id,
                    cause_event_id=item.cause_event_id,
                    data={"node_name": node_plan.name},
                )
        if state.status != "RUNNING":
            raise asyncio.CancelledError()
        if node_plan.tool is not None:
            self._check_tool_admission(node_plan.tool, node_plan.name)
        state.consume_step()
        started_at = perf_counter()
        timeout = node_plan.effective_config.get("timeout")
        deadline = started_at + float(timeout) if timeout is not None else None
        state.active_attempts[execution_id] = attempt
        started = self._event_store.append(
            state.run_id, "NodeAttemptStarted", category="node",
            span_id=execution_id, node_id=item.node_id,
            node_execution_id=execution_id, attempt=attempt,
            info_ids=[item.info.id], delivery_id=item.delivery_id,
            data={"node_name": node_plan.name},
        )
        business: asyncio.Task | None = None
        try:
            normalized_input = self._validate_schema(
                item.info, node_plan.input_schema, "输入", state.run_id
            )
            if normalized_input.content_hash != item.info.content_hash:
                # The public provenance of a transformed input is not yet
                # defined. Reject it explicitly rather than silently passing
                # the original, incorrect value to user code.
                raise _safe_error(UnsupportedFeatureError,
                    "输入 Schema 改变了业务数据；派生 Info 来源协议尚未定稿",
                    run_id=state.run_id,
                )
            self._check_deadline(deadline)

            tool_runner = None
            if node_plan.tools:
                async def tool_runner(call, _plan=node_plan, _exec=execution_id,
                                      _attempt=attempt):
                    return await self._run_internal_tool(
                        state, _plan, _exec, _attempt, call
                    )

            def register_blocking(future: Future) -> None:
                self._register_blocking_future(
                    state, future, execution_id=execution_id, attempt=attempt
                )

            async def call_forward():
                context = ModelAttempt(
                    run_id=state.run_id, node_id=item.node_id,
                    node_execution_id=execution_id, attempt=attempt,
                    config=node_plan.effective_config,
                    api_key=state.plan.config.secret_values.get(
                        node_plan.effective_config.get("_secret_id")
                    ),
                    deadline=deadline, output_schema=node_plan.output_schema,
                    event_store=self._event_store, task=asyncio.current_task(),
                    hub_contract=node_plan.hub_contract,
                    tool_runner=tool_runner,
                    tool_specs=(
                        [descriptor.spec for descriptor in node_plan.tools]
                        if node_plan.tools else None
                    ),
                )
                with bind_model_attempt(context), bind_blocking(
                    self._blocking_pool(state), register_blocking
                ):
                    if inspect.iscoroutinefunction(node_plan.forward):
                        return await node_plan.forward(item.info)
                    # Blocking business runs off the event loop so one node
                    # cannot freeze sibling deadlines. The thread cannot be
                    # killed; the attempt is abandoned and its result rejected.
                    future = self._register_blocking_future(
                        state, self._blocking_pool(state).submit(node_plan.forward, item.info),
                        execution_id=execution_id, attempt=attempt,
                    )
                    return await asyncio.wrap_future(future)

            business = asyncio.create_task(call_forward())
            state.owned_tasks.add(business)
            remaining = None if deadline is None else max(0, deadline - perf_counter())
            done, _ = await asyncio.wait({business}, timeout=remaining)
            if not done:
                # Cancellation is left to the single cleanup path: issuing one
                # here and another there would interrupt a node that swallows
                # cancellation to finish its own cleanup.
                raise _AttemptDeadlineExpired()
            result = business.result()
            if state.status != "RUNNING":
                raise state.terminal_error or asyncio.CancelledError()
            self._check_deadline(deadline)
            if not isinstance(result, Info):
                raise _safe_error(NodeExecutionError,
                    f"节点 {node_plan.name} 的 forward() 必须返回 Info",
                    run_id=state.run_id,
                )
            normalized_output = self._validate_schema(
                result, node_plan.output_schema, "输出", state.run_id
            )
            self._check_deadline(deadline)
            output = self._formalize_output(
                input_info=item.info, returned=result,
                normalized_info=normalized_output, run_id=state.run_id,
                node_id=item.node_id, node_execution_id=execution_id,
            )
            self._check_deadline(deadline)
            return _Outcome(
                item=item, node_plan=node_plan, output=output,
                node_execution_id=execution_id, event_id=started["event_id"],
                attempt=attempt, started_at=started_at, deadline=deadline,
            )
        except (_AttemptDeadlineExpired, ModelDeadlineExpired):
            with self._definition_lock:
                thread_running = any(
                    key[:2] == (execution_id, attempt) and not future.done()
                    for key, future in state.blocking_futures.items()
                )
            error = _safe_error(NodeTimeoutError,
                f"节点 {node_plan.name} 第 {attempt} 次尝试超过 "
                f"{float(timeout):g} 秒；本次 Graph 运行停止", run_id=state.run_id
            )
            self._stop_run(state, "FAILED", error)
            self._event_store.append(
                state.run_id, "NodeTimedOut", category="node",
                span_id=execution_id, node_id=item.node_id,
                node_execution_id=execution_id, attempt=attempt,
                info_ids=[item.info.id],
                data={"timeout_seconds": timeout,
                      "duration_ms": round((perf_counter() - started_at) * 1000, 3),
                      "deadline_enforced": not thread_running,
                      "blocking_thread_still_running": thread_running},
            )
            raise error from None
        except asyncio.CancelledError:
            self._event_store.append(
                state.run_id, "NodeCancelled", category="node",
                span_id=execution_id, node_id=item.node_id,
                node_execution_id=execution_id, attempt=attempt,
                info_ids=[item.info.id],
                data={"duration_ms": round((perf_counter() - started_at) * 1000, 3)},
            )
            raise
        except Exception as exc:
            error = _safe_error(
                NodeExecutionError, "节点执行失败", run_id=state.run_id, cause=exc,
            )
            duration = round((perf_counter() - started_at) * 1000, 3)
            max_attempts = int(node_plan.effective_config.get("max_attempts") or 1)
            retryable = bool(getattr(error, "_easy_agent_retryable", False))
            if retryable and attempt < max_attempts and state.status == "RUNNING":
                state.active_attempts.pop(execution_id, None)
                delay = self._backoff_delay(attempt)
                self._event_store.append(
                    state.run_id, "NodeAttemptFailed", category="node",
                    span_id=execution_id, node_id=item.node_id,
                    node_execution_id=execution_id, attempt=attempt,
                    info_ids=[item.info.id],
                    data={"error_type": type(error).__name__,
                          "duration_ms": duration,
                          "next_attempt": attempt + 1,
                          "retry_delay_seconds": round(delay, 3)},
                )
                return _Retry(
                    item=replace(item, execution_id=execution_id,
                                 attempt=attempt + 1),
                    ready_at=perf_counter() + delay,
                    delay=delay,
                )
            self._event_store.append(
                state.run_id, "NodeFailed", category="node",
                span_id=execution_id, node_id=item.node_id,
                node_execution_id=execution_id, attempt=attempt,
                info_ids=[item.info.id],
                data={"error_type": type(error).__name__,
                      "duration_ms": duration,
                      "attempts_used": attempt},
            )
            raise error from None

        finally:
            # An abandoned thread keeps its task tracked so cleanup can hold the
            # definition lease until it really returns.
            if business is not None and business.done():
                state.owned_tasks.discard(business)

    def _blocking_pool(self, state: _RunState) -> ThreadPoolExecutor:
        if state.blocking_pool is None:
            state.blocking_pool = ThreadPoolExecutor(
                max_workers=max(1, state.plan.config.max_concurrency),
                thread_name_prefix="easy-agent-blocking",
            )
        return state.blocking_pool

    def _register_blocking_future(
        self, state: _RunState, future: Future, *, execution_id: str, attempt: int,
    ) -> Future:
        """Track a blocking future until it finishes, then forget it.

        Registering before awaiting keeps the definition lease alive while the
        thread still reads node declarations.
        """

        key = (execution_id, attempt, uuid4().hex)
        with self._definition_lock:
            state.blocking_futures[key] = future

        def forget(done: Future) -> None:
            with self._definition_lock:
                if state.blocking_futures.get(key) is done:
                    state.blocking_futures.pop(key, None)

        future.add_done_callback(forget)
        return future

    @staticmethod
    def _backoff_delay(attempt: int) -> float:
        """Full-jitter exponential backoff; first retry waits 0-0.5s."""

        return uniform(0.0, min(8.0, 0.5 * (2 ** (attempt - 1))))

    async def _stall_watchdog(self, state: _RunState) -> None:
        """Report event-loop stalls so unexplained timeouts stay attributable."""

        interval, threshold = 0.05, 0.1
        expected = perf_counter() + interval
        try:
            while state.status == "RUNNING":
                await asyncio.sleep(interval)
                now = perf_counter()
                stall = now - expected
                if stall >= threshold:
                    try:
                        self._event_store.append(
                            state.run_id, "EventLoopBlocked",
                            data={
                                "stall_ms": round(stall * 1000, 3),
                                "in_flight": sorted(state.active_attempts),
                            },
                        )
                    except Exception:
                        pass  # diagnostics must never fail a run
                expected = now + interval
        except asyncio.CancelledError:
            raise

    @staticmethod
    def _check_deadline(deadline: float | None) -> None:
        if deadline is not None and perf_counter() >= deadline:
            raise _AttemptDeadlineExpired()

    def _commit_outcome(self, state: _RunState, outcome: _Outcome, sequence: int) -> _Outcome:
        if state.status != "RUNNING":
            if state.terminal_error is not None:
                raise state.terminal_error
            raise asyncio.CancelledError()
        if state.active_attempts.get(outcome.node_execution_id) != outcome.attempt:
            raise _safe_error(GraphValidationError, "候选结果的 attempt 已失效", run_id=state.run_id)
        try:
            self._check_deadline(outcome.deadline)
        except _AttemptDeadlineExpired:
            error = _safe_error(NodeTimeoutError, "候选输出提交超过尝试期限", run_id=state.run_id)
            self._stop_run(state, "FAILED", error)
            self._event_store.append(
                state.run_id, "NodeTimedOut", category="node",
                node_id=outcome.item.node_id,
                node_execution_id=outcome.node_execution_id, attempt=outcome.attempt,
                data={"completion_sequence": sequence},
            )
            raise error from None
        output, item = outcome.output, outcome.item
        if output is not item.info:
            self._event_store.append(
                state.run_id, "InfoProduced", category="node",
                node_id=item.node_id, node_execution_id=outcome.node_execution_id,
                span_id=outcome.node_execution_id, attempt=outcome.attempt,
                info_ids=[output.id, item.info.id], cause_event_id=outcome.event_id,
                data={"content_hash": output.content_hash, "schema_id": output.schema_id},
            )
        succeeded = self._event_store.append(
            state.run_id, "NodeSucceeded", category="node",
            node_id=item.node_id, node_execution_id=outcome.node_execution_id,
            span_id=outcome.node_execution_id, attempt=outcome.attempt,
            info_ids=[output.id], cause_event_id=outcome.event_id,
            data={"duration_ms": round((perf_counter() - outcome.started_at) * 1000, 3),
                  "completion_sequence": sequence},
        )
        del state.active_attempts[outcome.node_execution_id]
        return replace(outcome, event_id=succeeded["event_id"])

    def _stop_run(self, state: _RunState, status: str, error: BaseException | None = None) -> None:
        if state.status != "RUNNING":
            return
        state.status = status
        state.terminal_error = error
        state.active_attempts.clear()
        # Resource protection also survives a failed terminal log write.
        self._retain_cleanup(state)
        self._event_store.append(
            state.run_id,
            "GraphRunCancelled" if status == "CANCELLED" else "GraphRunFailed",
            data={} if error is None else {"error_type": type(error).__name__},
        )
        state.terminal_logged = True

    def _retain_cleanup(self, state: _RunState) -> None:
        with self._definition_lock:
            pending = {task for task in state.owned_tasks if not task.done()}
            blocked = {
                key: future for key, future in state.blocking_futures.items()
                if not future.done()
            }
            entry = self._cleanups.get(state.run_id)
            if entry is None:
                if not pending and not blocked:
                    return
                tools = self._tool_declarations()
                for tool in tools:
                    object.__setattr__(
                        tool, "_definition_leases", tool._definition_leases + 1
                    )
                entry = {"tasks": set(), "futures": {}, "tools": tools}
                self._cleanups[state.run_id] = entry
            new_tasks = pending - entry["tasks"]
            new_futures = {
                key: future for key, future in blocked.items()
                if key not in entry["futures"]
            }
            entry["tasks"].update(pending)
            entry["futures"].update(blocked)

        for task in new_tasks:
            task.add_done_callback(
                lambda done, run=state: self._cleanup_task_exited(run, done)
            )
        for key, future in new_futures.items():
            future.add_done_callback(
                lambda done, run=state, name=key: self._cleanup_future_exited(
                    run, name, done
                )
            )

    def _cleanup_task_exited(self, state: _RunState, task: asyncio.Task) -> None:
        if not task.cancelled():
            task.exception()  # Retrieve failures even after repeated cancellation.
        with self._definition_lock:
            state.owned_tasks.discard(task)
            state.cancel_requested.discard(task)
            entry = self._cleanups.get(state.run_id)
            if entry is None:
                return
            entry["tasks"].discard(task)
            self._release_cleanup(state, entry)

    def _cleanup_future_exited(
        self, state: _RunState, key: tuple[str, int, str], future: Future
    ) -> None:
        with self._definition_lock:
            if state.blocking_futures.get(key) is future:
                state.blocking_futures.pop(key, None)
            entry = self._cleanups.get(state.run_id)
            if entry is None:
                return
            entry["futures"].pop(key, None)
            self._release_cleanup(state, entry)

    def _release_cleanup(self, state: _RunState, entry: Mapping[str, Any]) -> None:
        if entry["tasks"] or entry["futures"]:
            return
        self._cleanups.pop(state.run_id, None)
        for tool in entry["tools"]:
            object.__setattr__(
                tool, "_definition_leases", tool._definition_leases - 1
            )

    @staticmethod
    def _release_delayed(state: _RunState, ready: deque) -> None:
        if not state.delayed:
            return
        now = perf_counter()
        due = sorted(
            (entry for entry in state.delayed if entry.ready_at <= now),
            key=lambda entry: entry.ready_at,
        )
        if not due:
            return
        remaining = [entry for entry in state.delayed if entry.ready_at > now]
        state.delayed = remaining
        for entry in due:
            ready.append(entry.item)

    def _formalize_output(
        self,
        *,
        input_info: Info[Any],
        returned: Info[Any],
        normalized_info: Info[Any],
        run_id: str,
        node_id: str,
        node_execution_id: str,
    ) -> Info[Any]:
        if returned is input_info and normalized_info.content_hash == returned.content_hash:
            return input_info
        return Info._runtime_info(
            normalized_info.data,
            schema_id=returned.schema_id,
            schema_version=returned.schema_version,
            labels=returned.labels,
            parent_ids=(input_info.id,),
            origin_kind="node_output",
            run_id=run_id,
            producer_node_id=node_id,
            producer_execution_id=node_execution_id,
        )

    def _validate_schema(
        self,
        info: Info[Any],
        schema: type | None,
        direction: str,
        run_id: str,
    ) -> Info[Any]:
        if schema is None:
            return info
        data = info.data
        model_validate = getattr(schema, "model_validate", None)
        model_json_schema = getattr(schema, "model_json_schema", None)
        if callable(model_validate) and callable(model_json_schema):
            try:
                model = model_validate(plain_data(info), strict=True)
                normalized = model.model_dump(mode="json")
                # Reuse Info's own JSON and finite-number constraints; do not
                # run user validators a second time during submission.
                return Info(normalized)
            except Exception:
                raise _safe_error(InfoError,
                    f"{direction}数据不符合 Pydantic 模型 {schema.__name__}",
                    run_id=run_id,
                ) from None
        if schema is dict:
            valid = isinstance(data, Mapping)
        elif schema is list:
            valid = isinstance(data, tuple)
        elif schema in {str, int, float, bool}:
            valid = type(data) is schema
        else:
            valid = isinstance(data, schema)
        if not valid:
            raise _safe_error(InfoError,
                f"{direction}数据不符合 {schema.__name__}，实际为 {type(data).__name__}",
                run_id=run_id,
            )
        return info

    async def _dispatch_outcome(
        self, state: _RunState, outcome: _Outcome, ready: deque[_WorkItem]
    ) -> None:
        if state.status != "RUNNING":
            raise state.terminal_error or asyncio.CancelledError()
        node_plan = outcome.node_plan
        if node_plan.route_rule is not None:
            target_id = self._select_route(state, outcome)
            if target_id is None:
                state.candidates.append((node_plan.node_id, outcome.output))
            else:
                await self._deliver(
                    state,
                    source_id=node_plan.node_id,
                    target_id=target_id,
                    info=outcome.output,
                    branch_group=outcome.item.branch_group,
                    cause_event_id=outcome.event_id,
                    ready=ready,
                )
            return

        if not node_plan.outgoing:
            state.candidates.append((node_plan.node_id, outcome.output))
            return

        branch_group = outcome.item.branch_group
        if len(node_plan.outgoing) > 1:
            branch_group = uuid4().hex
        port_infos = {}
        if node_plan.hub_contract is not None:
            for index, key in enumerate(node_plan.hub_contract.output_keys):
                port_infos[index] = Info._runtime_info(
                    outcome.output.data[key], schema_id=None, schema_version=None, labels=None,
                    parent_ids=(outcome.output.id,), origin_kind="hub_output", run_id=state.run_id,
                    producer_node_id=node_plan.node_id,
                    producer_execution_id=outcome.node_execution_id,
                )
            self._event_store.append(
                state.run_id, "HubOutputsPrepared", node_id=node_plan.node_id,
                node_execution_id=outcome.node_execution_id,
                info_ids=[info.id for info in port_infos.values()],
                data={"parent_info_id": outcome.output.id,
                      "ports": list(node_plan.hub_contract.output_keys)},
            )
        for edge in node_plan.deliveries:
            await self._deliver(
                state,
                source_id=node_plan.node_id,
                target_id=edge.target,
                info=outcome.output if edge.output is None else port_infos[edge.output],
                branch_group=branch_group,
                cause_event_id=outcome.event_id,
                ready=ready,
                input_port=edge.input, output_port=edge.output,
            )

    def _select_route(self, state: _RunState, outcome: _Outcome) -> str | None:
        rule = outcome.node_plan.route_rule
        assert rule is not None
        data = outcome.output.data
        if not isinstance(data, FrozenDict):
            raise _safe_error(RoutingError,
                f"节点 {outcome.node_plan.name} 的 route 要求字典 Info",
                run_id=state.run_id,
            )
        if rule.field not in data:
            raise _safe_error(RoutingError,
                f"路由字段 {rule.field!r} 不存在", run_id=state.run_id
            )
        value = data[rule.field]
        if type(value) is not rule.expected_type:
            raise _safe_error(RoutingError,
                f"路由字段 {rule.field!r} 类型应为 {rule.expected_type.__name__}，"
                f"实际为 {type(value).__name__}",
                run_id=state.run_id,
            )
        target_id: str | None | object = _NO_DEFAULT
        matched_case_type: str | None = None
        for case, candidate in rule.cases:
            if type(value) is type(case) and value == case:
                target_id = candidate
                matched_case_type = type(case).__name__
                break
        if target_id is _NO_DEFAULT:
            target_id = rule.default_target_id
        if target_id is _NO_DEFAULT:
            raise _safe_error(RoutingError,
                f"路由字段 {rule.field!r} 没有匹配分支且未设置 default",
                run_id=state.run_id,
            )
        target_name = "__END__" if target_id is None else state.plan.names_by_id[target_id]
        self._event_store.append(
            state.run_id,
            "RouteSelected",
            node_id=outcome.node_plan.node_id,
            node_execution_id=outcome.node_execution_id,
            info_ids=[outcome.output.id],
            cause_event_id=outcome.event_id,
            data={
                "field": rule.field,
                "matched_type": matched_case_type,
                "target": target_name,
                "used_default": matched_case_type is None,
            },
        )
        return target_id  # type: ignore[return-value]

    async def _deliver(
        self,
        state: _RunState,
        *,
        source_id: str,
        target_id: str,
        info: Info[Any],
        branch_group: str | None,
        cause_event_id: str,
        ready: deque[_WorkItem],
        input_port: str | None = None,
        output_port: int | None = None,
    ) -> None:
        if state.status != "RUNNING":
            raise state.terminal_error or asyncio.CancelledError()
        delivery_id = uuid4().hex
        state.reception_sequence += 1
        source_name = state.plan.names_by_id[source_id]
        target_name = state.plan.names_by_id[target_id]
        requested = self._event_store.append(
            state.run_id,
            "InfoDeliveryRequested",
            node_id=source_id,
            info_ids=[info.id],
            delivery_id=delivery_id,
            cause_event_id=cause_event_id,
            fork_group_id=branch_group,
            data={
                "source": source_name,
                "target": target_name,
                "branch_group": branch_group,
                "input_port": input_port, "output_port": output_port,
            },
        )
        received = self._event_store.append(
            state.run_id,
            "InfoDeliveryReceived",
            node_id=target_id,
            info_ids=[info.id],
            delivery_id=delivery_id,
            cause_event_id=requested["event_id"],
            fork_group_id=branch_group,
            data={
                "source": source_name,
                "target": target_name,
                "branch_group": branch_group,
                "reception_sequence": state.reception_sequence,
                "input_port": input_port, "output_port": output_port,
            },
        )

        target_plan = state.plan.nodes[target_id]
        join = target_plan.join
        if join is None:
            ready.append(
                _WorkItem(
                    node_id=target_id,
                    info=info,
                    branch_group=branch_group,
                    delivery_id=delivery_id,
                    cause_event_id=received["event_id"],
                )
            )
            return

        if join.automatic:
            branch_group = state.run_id
        if branch_group is None:
            raise _safe_error(GraphValidationError,
                f"汇合节点 {target_name} 收到没有分叉组的输入",
                run_id=state.run_id,
            )
        key = (target_id, branch_group)
        if key in state.completed_joins:
            raise _safe_error(GraphValidationError,
                f"汇合节点 {target_name} 收到已完成分叉组的迟到输入",
                run_id=state.run_id,
            )
        bucket = state.join_buffers.setdefault(key, {})
        token = input_port if input_port is not None else source_id
        if token not in dict(join.sources):
            raise _safe_error(GraphValidationError, "输入不属于声明的汇合来源", run_id=state.run_id)
        if token in bucket:
            raise _safe_error(GraphValidationError,
                f"汇合节点 {target_name} 在同一分叉组收到重复来源 {source_name}",
                run_id=state.run_id,
            )
        bucket[token] = info
        if set(bucket) != {source for source, _ in join.sources}:
            return

        ordered = [bucket[source] for source, _ in join.sources]
        cache_key = (branch_group, tuple((label, bucket[source].id) for source, label in join.sources))
        combined = state.joined_infos.get(cache_key)
        reused = combined is not None
        if combined is None:
            combined = Info._runtime_info(
                {label: plain_data(bucket[source]) for source, label in join.sources},
                schema_id=None, schema_version=None, labels=None,
                parent_ids=tuple(item.id for item in ordered), origin_kind="join",
                run_id=state.run_id, producer_node_id=None, producer_execution_id=None,
            )
            state.joined_infos[cache_key] = combined
        del state.join_buffers[key]
        state.completed_joins.add(key)
        joined = self._event_store.append(
            state.run_id,
            "InfoJoined",
            node_id=target_id,
            info_ids=[combined.id, *combined.parent_ids],
            delivery_id=delivery_id,
            cause_event_id=received["event_id"],
            fork_group_id=branch_group,
            data={
                "target": target_name,
                "sources": [label for _, label in join.sources],
                "branch_group": branch_group,
                "reused": reused,
            },
        )
        ready.append(
            _WorkItem(
                node_id=target_id,
                info=combined,
                branch_group=branch_group,
                delivery_id=delivery_id,
                cause_event_id=joined["event_id"],
            )
        )


    def logs(
        self,
        *,
        run_id: str | None = None,
        node: Node | None = None,
        node_execution_id: str | None = None,
        event_type: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        selected_run = run_id or self._last_run_id
        if selected_run is None:
            return []
        if node is not None and node.owner is not self:
            raise _safe_error(OwnershipError, "只能查询当前 Graph 所属 Node 的日志")
        return self._event_store.query(
            run_id=selected_run,
            category="node" if node is not None else "graph",
            node_id=node.id if node is not None else None,
            node_execution_id=node_execution_id,
            event_type=event_type,
            offset=offset,
            limit=limit,
        )

    def audit(
        self,
        *,
        run_id: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        selected_run = run_id or self._last_run_id
        if selected_run is None:
            return []
        return self._event_store.query(
            run_id=selected_run,
            category="audit",
            offset=offset,
            limit=limit,
        )

    @definition_locked
    def to_dict(self) -> dict[str, Any]:
        nodes: list[dict[str, Any]] = []
        for name, node in self._nodes.items():
            route: dict[str, Any] | None = None
            if node._route_rule is not None:
                route = {
                    "field": node._route_rule.field,
                    "cases": [
                        {"value": value, "target": self._target_name(target_id)}
                        for value, target_id in node._route_rule.cases
                    ],
                    "default": (
                        None
                        if node._route_rule.default_target_id is _NO_DEFAULT
                        else self._target_name(
                            node._route_rule.default_target_id  # type: ignore[arg-type]
                        )
                    ),
                    "has_default": node._route_rule.default_target_id is not _NO_DEFAULT,
                }
            receive = None
            if node._receive_rule is not None:
                receive = {
                    "mode": node._receive_rule.mode,
                    "sources": [
                        self._target_name(item) for item in node._receive_rule.source_ids
                    ],
                }
            nodes.append(
                {
                    "name": name,
                    "id": node.id,
                    "class": f"{type(node).__module__}.{type(node).__qualname__}",
                    "config": {
                        "env_file": str(node.env_file) if node.env_file is not None else None,
                        "model": node.model,
                        "provider": node.provider,
                        "base_url": node.base_url,
                        "prompt": node.prompt,
                        "role": node.role,
                        "temperature": node.temperature,
                        "max_output_tokens": node.max_output_tokens,
                        "input_token_budget": node.input_token_budget,
                        "timeout": node.timeout,
                        "retry": node.retry,
                        "max_attempts": node.max_attempts,
                        "max_parallelism": node.max_parallelism,
                        "api_key_env": node.api_key_env,
                        "tools": [
                            (
                                tool.tool_name
                                if isinstance(tool, Tool)
                                else getattr(tool, "__name__", None)
                            )
                            for tool in node.tools
                        ],
                        "required_permissions": sorted(node.required_permissions),
                    },
                    "send_to": [self._target_name(item) for item in node._outgoing],
                    "route": route,
                    "receive": receive,
                    "hub": None if not isinstance(node, HubNode) else {
                        "input_count": node.input_count, "output_count": node.output_count,
                        "input_keys": list(node.input_keys), "rules": list(node.rules),
                        "template_version": 1,
                        "prompt_hash": sha256(node._hub_contract.prompt.encode("utf-8")).hexdigest(),
                    },
                }
            )
        return {
            "graph": {
                "id": self.id,
                "version": self.VERSION,
                "execution": self._execution,
                "start": None if self._start_ids is None else [self._target_name(item) for item in self._start_ids],
                "end": None if self._end_ids is None else [self._target_name(item) for item in self._end_ids],
                "max_steps": self._max_steps,
                "max_concurrency": self._max_concurrency,
                "log_dir": str(self._event_store.directory),
                "defaults": {
                    key: value
                    for key, value in self._graph_defaults.items()
                    if key != "api_key_env"
                },
                "api_key_env": self._graph_defaults["api_key_env"],
                "allowed_tools": sorted(self._allowed_tools),
                "allowed_permissions": sorted(self._allowed_permissions),
            },
            "nodes": nodes,
            "port_relations": [
                {"source": self._target_name(edge.source), "target": self._target_name(edge.target),
                 "output_port": edge.output, "input_port": edge.input}
                for edge in self._port_edges
            ],
        }
