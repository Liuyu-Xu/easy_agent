"""Plan data structures and tool-declaration schema construction.

Compiled once per run and treated as immutable afterwards. Nothing here reads
graph state; the compiler builds these objects and the runtime only consumes
them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
from types import UnionType
from typing import Any, Mapping, Union, get_args, get_origin, get_type_hints

from .config import ConfigSnapshot
from .errors import _safe_error, GraphValidationError, UnsupportedFeatureError
from .hub import _HubContract
from .node import _ReceiveRule, _RouteRule


_JSON_TYPES: dict[Any, str] = {
    str: "string", int: "integer", float: "number", bool: "boolean",
    dict: "object", list: "array",
}


@dataclass(frozen=True)
class _EndRef:
    graph_id: str


@dataclass(frozen=True)
class _ToolDescriptor:
    name: str
    required_permissions: frozenset[str]
    side_effect: bool
    callable: Any = field(default=None, compare=False)
    kind: str = "function"
    spec: Any = field(default=None, compare=False)
    parameters: tuple[_ToolParameter, ...] = ()
    input_schema: type | None = None
    output_schema: type | None = None


@dataclass(frozen=True)
class _ToolParameter:
    name: str
    value_type: type
    nullable: bool
    required: bool

    @property
    def schema(self) -> dict[str, Any]:
        kind = _JSON_TYPES[self.value_type]
        return {"type": [kind, "null"] if self.nullable else kind}

    def accepts(self, value: Any) -> bool:
        if value is None:
            return self.nullable
        if self.value_type is float:
            # JSON Schema number includes integers, but not booleans.
            return type(value) in {int, float}
        return type(value) is self.value_type


def _compile_function(tool: Any, name: str) -> tuple[tuple[_ToolParameter, ...], dict]:
    """Freeze one contract for both provider declarations and local calls."""

    try:
        hints = get_type_hints(tool)
    except (NameError, TypeError, ValueError):
        raise _safe_error(UnsupportedFeatureError,
            f"函数工具 {name} 的类型标注无法在运行前解析，请使用可解析的类型"
        ) from None
    signature = inspect.signature(tool)
    parameters = []
    for parameter_name, parameter in signature.parameters.items():
        if parameter.kind not in {
            inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY,
        }:
            raise _safe_error(UnsupportedFeatureError,
                f"函数工具 {name} 的参数 {parameter_name} 必须支持具名调用；"
                "首版不支持位置专用参数和可变参数"
            )
        annotation = hints.get(parameter_name, inspect.Parameter.empty)
        if annotation is inspect.Parameter.empty:
            raise _safe_error(GraphValidationError,
                f"函数工具 {name} 的参数 {parameter_name} 缺少类型标注"
            )
        nullable = False
        if get_origin(annotation) in {UnionType, Union}:
            arguments = get_args(annotation)
            optional = [item for item in arguments if item is not type(None)]
            if len(optional) == 1 and len(arguments) == 2:
                annotation, nullable = optional[0], True
        if annotation not in _JSON_TYPES:
            raise _safe_error(UnsupportedFeatureError,
                f"函数工具 {name} 的参数 {parameter_name} 使用了无法导出 JSON Schema 的类型；"
                "首版仅支持 str/int/float/bool/list/dict 及其 Optional"
            )
        parameters.append(_ToolParameter(
            parameter_name, annotation, nullable,
            parameter.default is inspect.Parameter.empty,
        ))
    return tuple(parameters), {
        "type": "function",
        "function": {
            "name": name,
            "description": inspect.getdoc(tool) or "",
            "parameters": {
                "type": "object",
                "properties": {p.name: p.schema for p in parameters},
                "required": [p.name for p in parameters if p.required],
                "additionalProperties": False,
            },
        },
    }


@dataclass(frozen=True)
class _Edge:
    source: str
    target: str
    output: int | None = None
    input: str | None = None


@dataclass(frozen=True)
class _JoinPlan:
    sources: tuple[tuple[str, str], ...]
    automatic: bool


@dataclass(frozen=True)
class _NodePlan:
    node_id: str
    name: str
    outgoing: tuple[str, ...]
    route_rule: _RouteRule | None
    receive_rule: _ReceiveRule | None
    effective_config: Mapping[str, Any]
    input_schema: type | None
    output_schema: type | None
    max_parallelism: int | None
    forward: Any
    tools: tuple[_ToolDescriptor, ...]
    tool: _ToolDescriptor | None
    deliveries: tuple[_Edge, ...]
    hub_contract: _HubContract | None
    join: _JoinPlan | None = None


@dataclass(frozen=True)
class _Plan:
    topology_hash: str
    nodes: Mapping[str, _NodePlan]
    names_by_id: Mapping[str, str]
    start_ids: tuple[str, ...]
    end_ids: tuple[str, ...] | None
    config: ConfigSnapshot
