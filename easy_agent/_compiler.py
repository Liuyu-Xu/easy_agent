"""Compilation of graph definitions into an immutable execution plan.

Compilation resolves configuration and tool declarations under the definition
lock, then builds the plan. Business forward methods are not invoked here.
"""

from __future__ import annotations

from collections import deque
from dataclasses import replace
from hashlib import sha256
import inspect
import json
from types import MappingProxyType
from typing import Any, Mapping

from .config import ConfigSnapshot, _ConfigResolver, _validate_int
from .errors import (
    _safe_error,
    ConfigurationError,
    GraphValidationError,
    OwnershipError,
    PermissionDeniedError,
    ResultError,
    UnsupportedFeatureError,
)
from .hub import HubNode
from .node import Node, _NO_DEFAULT
from .tools import Tool
from ._api import validate_model_config
from ._definition import definition_locked
from ._plan import (
    _Edge,
    _JoinPlan,
    _NodePlan,
    _Plan,
    _ToolDescriptor,
    _compile_function,
)


class _CompilerMixin:
    """Turns a graph definition into a validated plan."""

    @definition_locked
    def _compile(
        self,
        *,
        max_steps_override: int | None,
        max_concurrency_override: int | None,
        for_run: bool,
    ) -> _Plan:
        if not self._nodes:
            raise _safe_error(GraphValidationError, "Graph 至少需要一个 Node")
        for name, value in (("运行覆盖 max_steps", max_steps_override),
                            ("运行覆盖 max_concurrency", max_concurrency_override)):
            if value is not None:
                _validate_int(name, value, error_type=ConfigurationError)

        names_by_id = {node.id: name for name, node in self._nodes.items()}
        node_plans: dict[str, _NodePlan] = {}
        incoming: dict[str, list[str]] = {node.id: [] for node in self._nodes.values()}

        resolver = _ConfigResolver(
            self._graph_defaults, self._env_file, self._api_key_env_explicit,
            tuple(self._nodes.values()),
        )
        public_values = resolver.public_values
        self._validate_ports()
        port_outgoing: dict[str, list[_Edge]] = {}
        for edge in self._port_edges:
            port_outgoing.setdefault(edge.source, []).append(edge)

        for name, node in self._nodes.items():
            if node.owner is not self or node.name != name:
                raise _safe_error(OwnershipError, f"节点 {name} 的所有权或注册名称不一致")

            effective = resolver.resolve(node)
            hub_contract = node._hub_contract if isinstance(node, HubNode) else None
            if hub_contract is not None:
                if getattr(node.forward, "__func__", None) is not Node.forward:
                    raise _safe_error(UnsupportedFeatureError, "首版 Hub 使用内置模型执行，不支持覆盖 forward")
                effective = {**effective, "prompt": hub_contract.prompt, "role": None}
            standalone_tool = self._describe_tool(node) if isinstance(node, Tool) else None
            if standalone_tool is not None:
                self._check_tool_admission(standalone_tool, name)

            if not node.required_permissions.issubset(self._allowed_permissions):
                missing = sorted(node.required_permissions - self._allowed_permissions)
                raise _safe_error(PermissionDeniedError,
                    f"节点 {name} 请求了未授权权限：{missing}"
                )
            tool_names: list[str] = []
            tool_descriptors: list[_ToolDescriptor] = []
            for tool in node.tools:
                if isinstance(tool, Tool):
                    if tool.owner is not None:
                        raise _safe_error(GraphValidationError,
                            f"Tool 实例 {tool.tool_name} 已注册为图节点，不能同时作为内部工具"
                        )
                    tool_name = tool.tool_name
                elif inspect.isfunction(tool) or inspect.iscoroutinefunction(tool):
                    tool_name = getattr(tool, "__name__", None)
                    if tool_name == "<lambda>":
                        raise _safe_error(GraphValidationError,
                            f"节点 {name} 的 lambda 工具必须通过显式适配器提供稳定名称"
                        )
                else:
                    raise _safe_error(UnsupportedFeatureError,
                        f"节点 {name} 包含未配置适配器的第三方工具 "
                        f"{type(tool).__name__}"
                    )
                if not isinstance(tool_name, str) or not tool_name:
                    raise _safe_error(GraphValidationError, f"节点 {name} 包含没有有效 name 的工具")
                if tool_name in tool_names:
                    raise _safe_error(GraphValidationError,
                        f"节点 {name} 包含重名工具：{tool_name}"
                    )
                tool_names.append(tool_name)
                descriptor = self._describe_tool(tool, internal=True)
                self._check_tool_admission(descriptor, name)
                tool_descriptors.append(descriptor)
            public_values[f"nodes.{name}.tools"] = ",".join(tool_names)

            if getattr(node.forward, "__func__", None) is Node.forward:
                validate_model_config(
                    effective, output_schema=node.output_schema,
                    api_key=resolver.secret_values.get(effective['_secret_id']),
                    require_secret=for_run or node.env_file is not None,
                    hub_contract=hub_contract,
                )

            if effective.get("prompt") is not None:
                public_values[f"nodes.{name}.prompt_hash"] = sha256(
                    effective["prompt"].encode("utf-8")
                ).hexdigest()
            if node.role is not None:
                public_values[f"nodes.{name}.role"] = node.role

            deliveries = tuple(_Edge(node.id, target) for target in node._outgoing) + tuple(port_outgoing.get(node.id, ()))
            for target_id in dict.fromkeys(edge.target for edge in deliveries):
                if target_id not in names_by_id:
                    raise _safe_error(GraphValidationError,
                        f"节点 {name} 指向已删除或未注册的目标 {target_id}"
                    )
                incoming[target_id].append(node.id)
            if node._route_rule is not None:
                for _, target_id in node._route_rule.cases:
                    if target_id is not None:
                        if target_id not in names_by_id:
                            raise _safe_error(GraphValidationError,
                                f"节点 {name} 的 route 指向已删除目标 {target_id}"
                            )
                        incoming[target_id].append(node.id)
                default_id = node._route_rule.default_target_id
                if default_id is not _NO_DEFAULT and default_id is not None:
                    if default_id not in names_by_id:
                        raise _safe_error(GraphValidationError,
                            f"节点 {name} 的 default route 指向已删除目标 {default_id}"
                        )
                    incoming[default_id].append(node.id)

            node_plans[node.id] = _NodePlan(
                node_id=node.id,
                name=name,
                outgoing=tuple(dict.fromkeys(edge.target for edge in deliveries)),
                route_rule=node._route_rule,
                receive_rule=node._receive_rule,
                effective_config=MappingProxyType(effective),
                input_schema=node.input_schema,
                output_schema=node.output_schema,
                max_parallelism=node.max_parallelism,
                forward=node.forward,
                tools=tuple(tool_descriptors),
                tool=standalone_tool,
                deliveries=deliveries,
                hub_contract=hub_contract,
            )

        start_ids = self._resolve_start(node_plans, incoming)
        reachable = self._reachable(start_ids, node_plans)
        if reachable != set(node_plans):
            missing = sorted(names_by_id[item] for item in set(node_plans) - reachable)
            raise _safe_error(GraphValidationError, f"存在从入口不可达的节点：{missing}")

        has_cycle, cyclic_components = self._cycle_components(node_plans)
        roots = {node_id for node_id, sources in incoming.items() if not sources}
        if not has_cycle and set(start_ids) != roots:
            raise _safe_error(GraphValidationError, "start 必须覆盖全部零入度节点")
        if self._end_ids is not None:
            if has_cycle or any(node.route_rule is not None for node in node_plans.values()):
                raise _safe_error(UnsupportedFeatureError, "显式 start/end 当前仅支持无条件无环图")
            leaves = {node_id for node_id, node in node_plans.items() if not node.outgoing}
            if set(self._end_ids) != leaves:
                raise _safe_error(GraphValidationError, "end 必须覆盖全部终点")
        if has_cycle:
            if self._start_ids is None or len(start_ids) != 1:
                raise _safe_error(GraphValidationError, "包含循环的 Graph 必须显式设置单个 graph.start")
            if max_steps_override is None and self._max_steps is None:
                raise _safe_error(GraphValidationError, "包含循环的 Graph 必须显式设置 graph.max_steps")
            if len(cyclic_components) != 1:
                raise _safe_error(UnsupportedFeatureError, "当前版本只支持一个有界单路径反馈循环")
            component = cyclic_components[0]
            for node_id in component:
                plan_node = node_plans[node_id]
                if len(plan_node.outgoing) > 1 or plan_node.receive_rule is not None:
                    raise _safe_error(UnsupportedFeatureError, "当前版本不支持并行或汇合参与循环")
                internal_targets = [
                    target
                    for target in self._all_targets(plan_node)
                    if target is not None and target in component
                ]
                if len(internal_targets) > 1:
                    raise _safe_error(UnsupportedFeatureError, "当前版本循环在每个节点只允许一个回传方向")

        simple = not has_cycle and not any(node.route_rule is not None for node in node_plans.values())
        if simple:
            if self._end_ids is None and sum(not node.outgoing for node in node_plans.values()) != 1:
                raise _safe_error(ResultError, "多个终点必须显式声明 end")
        else:
            if any(node.hub_contract is not None for node in node_plans.values()):
                raise _safe_error(UnsupportedFeatureError, "Hub 仅支持固定无条件 DAG")
            self._validate_joins_and_broadcasts(node_plans, incoming, names_by_id, start_ids)
        node_plans = self._prepare_joins(node_plans, incoming, simple)
        if not self._has_possible_result(node_plans, reachable):
            raise _safe_error(ResultError, "Graph 没有可到达的结果出口")

        resolved_max_steps = (
            max_steps_override
            or self._max_steps
            or max(100, len(node_plans) * 10)
        )
        resolved_concurrency = max_concurrency_override or self._max_concurrency
        config = ConfigSnapshot(
            public_values=MappingProxyType(public_values),
            secret_values=MappingProxyType(resolver.secret_values),
            secret_sources=MappingProxyType(resolver.secret_sources),
            max_steps=resolved_max_steps,
            max_concurrency=resolved_concurrency,
        )

        export = self.to_dict()
        topology_hash = sha256(
            json.dumps(export, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return _Plan(
            topology_hash=topology_hash,
            nodes=MappingProxyType(node_plans),
            names_by_id=MappingProxyType(names_by_id),
            start_ids=start_ids,
            end_ids=self._end_ids,
            config=config,
        )
    def _validate_ports(self) -> None:
        inputs, outputs = {}, {}
        for edge in self._port_edges:
            if edge.source not in self._nodes_by_id or edge.target not in self._nodes_by_id:
                raise _safe_error(GraphValidationError, "端口关系引用了未注册节点")
            source, target = self._nodes_by_id[edge.source], self._nodes_by_id[edge.target]
            if isinstance(source, HubNode) and isinstance(target, HubNode):
                raise _safe_error(UnsupportedFeatureError, "Hub 串联协议尚未定稿")
            if edge.input is not None:
                if not isinstance(target, HubNode) or edge.input not in target.input_keys:
                    raise _safe_error(GraphValidationError, "输入端口不存在")
                inputs.setdefault(target.id, []).append(edge.input)
            if edge.output is not None:
                if not isinstance(source, HubNode) or type(edge.output) is not int or not 0 <= edge.output < source.output_count:
                    raise _safe_error(GraphValidationError, "输出端口不存在")
                outputs.setdefault(source.id, []).append(edge.output)
        for node in self._nodes.values():
            if not isinstance(node, HubNode):
                if any(isinstance(self._nodes_by_id.get(target), HubNode) for target in node._outgoing):
                    raise _safe_error(GraphValidationError, "连接 Hub 必须显式指定端口")
                continue
            if node._outgoing or node._route_rule is not None or node._receive_rule is not None:
                raise _safe_error(GraphValidationError, "Hub 不能混用普通连接、route 或 receive")
            if node.id in (self._start_ids or ()) or node.id in (self._end_ids or ()):
                raise _safe_error(UnsupportedFeatureError, "Hub 首版仅作为中间节点")
            ports_in, ports_out = inputs.get(node.id, []), outputs.get(node.id, [])
            if sorted(ports_in) != sorted(node.input_keys) or sorted(ports_out) != list(range(node.output_count)):
                raise _safe_error(GraphValidationError, "Hub 端口必须各绑定一次，不能重复或悬空")
            targets = [edge.target for edge in self._port_edges if edge.source == node.id]
            if len(set(targets)) != len(targets):
                raise _safe_error(UnsupportedFeatureError, "同一 Hub 多个输出连接同一目标的协议尚未定稿")
    def _prepare_joins(self, nodes: dict[str, _NodePlan], incoming: dict[str, list[str]],
                       simple: bool) -> dict[str, _NodePlan]:
        result = {}
        for node_id, plan in nodes.items():
            receive = plan.receive_rule
            if simple and receive is not None and set(receive.source_ids) != set(incoming[node_id]):
                raise _safe_error(GraphValidationError, "简单 DAG 的 receive 必须包含全部直接上游")
            if plan.hub_contract is not None:
                sources = tuple((key, key) for key in plan.hub_contract.input_keys)
            elif receive is not None:
                sources = tuple((source, nodes[source].name) for source in receive.source_ids)
            elif simple and len(incoming[node_id]) > 1:
                sources = tuple((source, nodes[source].name) for source in nodes if source in incoming[node_id])
            else:
                sources = ()
            result[node_id] = replace(plan, join=_JoinPlan(sources, simple) if sources else None)
        if simple:
            # Each supported path has at most one multi-input convergence.
            # Nested batches remain a separate public contract.
            remaining = {node: len(set(sources)) for node, sources in incoming.items()}
            ready = deque(node for node, count in remaining.items() if count == 0)
            depth = dict.fromkeys(nodes, 0)
            while ready:
                node = ready.popleft()
                merging = result[node].join is not None and len(result[node].join.sources) > 1
                if merging and depth[node]:
                    raise _safe_error(UnsupportedFeatureError, "嵌套多输入汇合协议尚未定稿，见 Hub 问题文件")
                for target in result[node].outgoing:
                    depth[target] = max(depth[target], depth[node] + int(merging))
                    remaining[target] -= 1
                    if remaining[target] == 0:
                        ready.append(target)
        return result
    def _describe_tool(self, tool: Any, *, internal: bool = False) -> _ToolDescriptor:
        name = tool.tool_name if isinstance(tool, Tool) else tool.__name__
        effect = getattr(tool, "side_effect", False)
        if not isinstance(effect, bool):
            raise _safe_error(GraphValidationError, f"工具 {name} 的 side_effect 必须是 bool")
        parameters = ()
        if isinstance(tool, Tool):
            schema = {"type": "object"}
            if internal and tool.input_schema not in {None, dict}:
                try:
                    if not callable(getattr(tool.input_schema, "model_validate", None)):
                        raise ValueError()
                    schema = tool.input_schema.model_json_schema()
                    if schema.get("type") != "object":
                        raise ValueError()
                except Exception:
                    raise _safe_error(UnsupportedFeatureError,
                        f"内部工具 {name} 的输入 Schema 必须接受 JSON 对象"
                    ) from None
            spec = {
                "type": "function",
                "function": {
                    "name": name,
                    "description": tool.description,
                    "parameters": schema,
                },
            }
            kind = "tool"
        else:
            parameters, spec = _compile_function(tool, name)
            kind = "function"
        return _ToolDescriptor(
            name=name,
            required_permissions=frozenset(getattr(tool, "required_permissions", ())),
            side_effect=effect,
            callable=tool.forward if isinstance(tool, Tool) else tool,
            kind=kind,
            spec=spec,
            parameters=parameters,
            input_schema=tool.input_schema if isinstance(tool, Tool) else None,
            output_schema=tool.output_schema if isinstance(tool, Tool) else None,
        )
    def _check_tool_admission(self, tool: _ToolDescriptor, node_name: str) -> None:
        if tool.name not in self._allowed_tools:
            raise _safe_error(PermissionDeniedError, f"节点 {node_name} 使用了未授权工具：{tool.name}")
        if not tool.required_permissions.issubset(self._allowed_permissions):
            missing = sorted(tool.required_permissions - self._allowed_permissions)
            raise _safe_error(PermissionDeniedError, f"工具 {tool.name} 请求了未授权权限：{missing}")
        if tool.side_effect:
            raise _safe_error(UnsupportedFeatureError,
                f"副作用工具 {tool.name} 尚不支持；当前仅允许 side_effect=False 的工具"
            )
    def _resolve_start(
        self, nodes: Mapping[str, _NodePlan], incoming: Mapping[str, list[str]]
    ) -> tuple[str, ...]:
        if self._start_ids is not None:
            if not self._start_ids or not set(self._start_ids).issubset(nodes):
                raise _safe_error(GraphValidationError, "显式开始节点已被删除")
            return self._start_ids
        roots = [node_id for node_id, sources in incoming.items() if not sources]
        if len(roots) != 1:
            raise _safe_error(GraphValidationError,
                f"无法自动识别唯一入口，发现 {len(roots)} 个零入度节点；请设置 graph.start"
            )
        return (roots[0],)
    def _reachable(self, start_ids: tuple[str, ...], nodes: Mapping[str, _NodePlan]) -> set[str]:
        seen: set[str] = set()
        stack = list(start_ids)
        while stack:
            node_id = stack.pop()
            if node_id in seen:
                continue
            seen.add(node_id)
            for target in self._all_targets(nodes[node_id]):
                if target is not None:
                    stack.append(target)
        return seen
    def _all_targets(self, node: _NodePlan) -> tuple[str | None, ...]:
        if node.route_rule is not None:
            targets: list[str | None] = [target for _, target in node.route_rule.cases]
            if node.route_rule.default_target_id is not _NO_DEFAULT:
                targets.append(node.route_rule.default_target_id)  # type: ignore[arg-type]
            return tuple(targets)
        return tuple(node.outgoing)
    def _cycle_components(
        self, nodes: Mapping[str, _NodePlan]
    ) -> tuple[bool, list[set[str]]]:
        index = 0
        stack: list[str] = []
        on_stack: set[str] = set()
        indices: dict[str, int] = {}
        low: dict[str, int] = {}
        components: list[set[str]] = []

        def visit(node_id: str) -> None:
            nonlocal index
            indices[node_id] = index
            low[node_id] = index
            index += 1
            stack.append(node_id)
            on_stack.add(node_id)
            for target in self._all_targets(nodes[node_id]):
                if target is None:
                    continue
                if target not in indices:
                    visit(target)
                    low[node_id] = min(low[node_id], low[target])
                elif target in on_stack:
                    low[node_id] = min(low[node_id], indices[target])
            if low[node_id] == indices[node_id]:
                component: set[str] = set()
                while True:
                    item = stack.pop()
                    on_stack.remove(item)
                    component.add(item)
                    if item == node_id:
                        break
                components.append(component)

        for node_id in nodes:
            if node_id not in indices:
                visit(node_id)

        cyclic: list[set[str]] = []
        for component in components:
            if len(component) > 1:
                cyclic.append(component)
            else:
                node_id = next(iter(component))
                if node_id in self._all_targets(nodes[node_id]):
                    cyclic.append(component)
        return bool(cyclic), cyclic
    def _validate_joins_and_broadcasts(
        self,
        nodes: Mapping[str, _NodePlan],
        incoming: Mapping[str, list[str]],
        names_by_id: Mapping[str, str],
        start_ids: tuple[str, ...],
    ) -> None:
        joins_by_broadcaster: dict[str, str] = {}
        forks = [(node.node_id, node.outgoing) for node in nodes.values()
                 if node.route_rule is None and len(node.outgoing) > 1]
        if len(start_ids) > 1:
            forks.append(("__START__", start_ids))
        for node_id, plan in nodes.items():
            receive = plan.receive_rule
            if receive is None:
                continue
            if not set(receive.source_ids).issubset(set(incoming[node_id])):
                missing = [
                    names_by_id[source]
                    for source in receive.source_ids
                    if source not in incoming[node_id]
                ]
                raise _safe_error(GraphValidationError,
                    f"汇合节点 {plan.name} 缺少来源连接：{missing}"
                )
            source_set = frozenset(receive.source_ids)
            for source_id in receive.source_ids:
                source_plan = nodes[source_id]
                if (
                    source_plan.route_rule is not None
                    or source_plan.receive_rule is not None
                    or source_plan.outgoing != (node_id,)
                ):
                    raise _safe_error(UnsupportedFeatureError,
                        f"汇合来源 {source_plan.name} 必须无条件且仅发送到 {plan.name}"
                    )

            broadcasters: list[str] = []
            for fork_id, branches in forks:
                terminals: list[str] = []
                for branch_start in branches:
                    terminal = self._linear_branch_terminal(
                        branch_start=branch_start,
                        join_id=node_id,
                        expected_sources=source_set,
                        nodes=nodes,
                    )
                    if terminal is None:
                        break
                    terminals.append(terminal)
                if (
                    len(terminals) == len(branches)
                    and len(terminals) == len(source_set)
                    and frozenset(terminals) == source_set
                ):
                    broadcasters.append(fork_id)

            if len(broadcasters) != 1:
                raise _safe_error(UnsupportedFeatureError,
                    f"汇合节点 {plan.name} 必须对应唯一的无条件线性多步骤分叉；"
                    f"实际识别到 {len(broadcasters)} 个"
                )
            broadcaster_id = broadcasters[0]
            if broadcaster_id in joins_by_broadcaster:
                raise _safe_error(UnsupportedFeatureError, "同一次广播不能声明多个结构化汇合")
            joins_by_broadcaster[broadcaster_id] = node_id

        for plan in nodes.values():
            if len(plan.outgoing) <= 1:
                continue
            if self._end_ids is None and plan.node_id not in joins_by_broadcaster:
                raise _safe_error(ResultError,
                    f"广播节点 {plan.name} 的并行分支没有显式、结构化的 all 汇合"
                )
    def _linear_branch_terminal(
        self,
        *,
        branch_start: str,
        join_id: str,
        expected_sources: frozenset[str],
        nodes: Mapping[str, _NodePlan],
    ) -> str | None:
        """Return the declared join source for one unconditional linear branch."""

        current = branch_start
        seen: set[str] = set()
        while current not in seen:
            seen.add(current)
            if current in expected_sources:
                source = nodes[current]
                if (
                    source.route_rule is None
                    and source.receive_rule is None
                    and source.outgoing == (join_id,)
                ):
                    return current
                return None
            if current == join_id:
                return None
            item = nodes[current]
            if (
                item.route_rule is not None
                or item.receive_rule is not None
                or len(item.outgoing) != 1
            ):
                return None
            current = item.outgoing[0]
        return None
    def _has_possible_result(
        self, nodes: Mapping[str, _NodePlan], reachable: set[str]
    ) -> bool:
        for node_id in reachable:
            node = nodes[node_id]
            if node.route_rule is None and not node.outgoing:
                return True
            if node.route_rule is not None:
                if any(target is None for _, target in node.route_rule.cases):
                    return True
                if node.route_rule.default_target_id is None:
                    return True
        return False
