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


def _json_schema(annotation: Any, *, tool: str, parameter: str) -> dict[str, Any]:
    """Map a supported annotation to JSON Schema; reject anything else."""

    if annotation in _JSON_TYPES:
        return {"type": _JSON_TYPES[annotation]}
    origin = get_origin(annotation)
    if origin is UnionType or origin is Union:
        arguments = get_args(annotation)
        optional = [item for item in arguments if item is not type(None)]
        if len(optional) == 1 and len(arguments) == 2:
            return _json_schema(optional[0], tool=tool, parameter=parameter)
    raise _safe_error(UnsupportedFeatureError,
        f"函数工具 {tool} 的参数 {parameter} 使用了无法导出 JSON Schema 的类型；"
        "首版仅支持 str/int/float/bool/list/dict 及其 Optional"
    )


def _function_parameters(tool: Any) -> dict[str, Any]:
    name = getattr(tool, "__name__", "<unnamed>")
    try:
        hints = get_type_hints(tool)
    except (NameError, TypeError, ValueError):
        raise _safe_error(UnsupportedFeatureError,
            f"函数工具 {name} 的类型标注无法在运行前解析，请使用可解析的类型"
        ) from None
    signature = inspect.signature(tool)
    properties: dict[str, Any] = {}
    required: list[str] = []
    for parameter_name, parameter in signature.parameters.items():
        annotation = hints.get(parameter_name, inspect.Parameter.empty)
        if annotation is inspect.Parameter.empty:
            raise _safe_error(GraphValidationError,
                f"函数工具 {name} 的参数 {parameter_name} 缺少类型标注"
            )
        properties[parameter_name] = _json_schema(
            annotation, tool=name, parameter=parameter_name
        )
        if parameter.default is inspect.Parameter.empty:
            required.append(parameter_name)
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def _function_spec(tool: Any, name: str) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": inspect.getdoc(tool) or "",
            "parameters": _function_parameters(tool),
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
