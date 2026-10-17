"""Execution of model-requested tools inside the parent attempt.

Tools are sub-activities: they share the attempt deadline and output budget,
never become graph nodes, and never trigger routing or delivery.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from time import perf_counter
from typing import Any, Mapping, get_type_hints

from .errors import EasyAgentError
from .info import Info, plain_data
from ._plan import _NodePlan, _ToolDescriptor
from ._runtime import _RunState


class _ToolArgumentError(Exception):
    """A model-supplied tool call did not match the declared signature."""


class _ToolCallMixin:
    """Runs tools requested by the model as attempt sub-activities."""

    async def _run_internal_tool(
        self, state: _RunState, node_plan: "_NodePlan", execution_id: str,
        attempt: int, call: Mapping[str, Any],
    ) -> str:
        """Execute one model-requested tool as a sub-activity of the attempt."""

        name = call.get("name")
        call_id = call.get("id")
        started = perf_counter()
        self._event_store.append(
            state.run_id, "ToolCallStarted", category="node",
            span_id=execution_id, node_id=node_plan.node_id,
            node_execution_id=execution_id, attempt=attempt,
            tool_call_id=call_id,
            data={"tool": name, "node_name": node_plan.name},
        )
        descriptor = next(
            (item for item in node_plan.tools if item.name == name), None
        )
        if descriptor is None:
            return self._tool_failure(
                state, node_plan, execution_id, attempt, call_id, name,
                started, "unknown_tool",
            )
        # Admission problems (permission, side effect) abort the attempt;
        # they are not recoverable by the model.
        self._check_tool_admission(descriptor, node_plan.name)
        try:
            arguments = self._parse_tool_arguments(call.get("arguments"))
        except _ToolArgumentError:
            return self._tool_failure(
                state, node_plan, execution_id, attempt, call_id, name,
                started, "invalid_arguments",
            )
        try:
            value = await self._invoke_tool(state, descriptor, arguments)
        except asyncio.CancelledError:
            raise
        except _ToolArgumentError:
            return self._tool_failure(
                state, node_plan, execution_id, attempt, call_id, name,
                started, "invalid_arguments",
            )
        except EasyAgentError:
            raise
        except Exception:
            return self._tool_failure(
                state, node_plan, execution_id, attempt, call_id, name,
                started, "tool_error",
            )
        try:
            payload = json.dumps(
                plain_data(value) if isinstance(value, Info) else value,
                ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError):
            return self._tool_failure(
                state, node_plan, execution_id, attempt, call_id, name,
                started, "unserializable_result",
            )
        self._event_store.append(
            state.run_id, "ToolCallFinished", category="node",
            span_id=execution_id, node_id=node_plan.node_id,
            node_execution_id=execution_id, attempt=attempt,
            tool_call_id=call_id,
            data={"tool": name, "node_name": node_plan.name,
                  "duration_ms": round((perf_counter() - started) * 1000, 3)},
        )
        return payload
    def _tool_failure(
        self, state: _RunState, node_plan: "_NodePlan", execution_id: str,
        attempt: int, call_id: Any, name: Any, started: float, reason: str,
    ) -> str:
        self._event_store.append(
            state.run_id, "ToolCallFailed", category="node",
            span_id=execution_id, node_id=node_plan.node_id,
            node_execution_id=execution_id, attempt=attempt,
            tool_call_id=call_id,
            data={"tool": name, "node_name": node_plan.name, "reason": reason,
                  "duration_ms": round((perf_counter() - started) * 1000, 3)},
        )
        return json.dumps(
            {"error": reason}, ensure_ascii=False, separators=(",", ":")
        )
    @staticmethod
    def _parse_tool_arguments(raw: Any) -> dict[str, Any]:
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except (ValueError, RecursionError):
                raise _ToolArgumentError() from None
        if not isinstance(raw, Mapping) or any(
            not isinstance(key, str) for key in raw
        ):
            raise _ToolArgumentError()
        return dict(raw)
    async def _invoke_tool(
        self, state: _RunState, descriptor: "_ToolDescriptor",
        arguments: dict[str, Any],
    ) -> Any:
        target = descriptor.callable
        if descriptor.kind == "tool":
            info = Info(arguments)
            if inspect.iscoroutinefunction(target.forward):
                return await target.forward(info)
            return await asyncio.wrap_future(self._register_blocking_future(
                state, self._blocking_pool(state).submit(target.forward, info)
            ))
        parameters = inspect.signature(target).parameters
        missing = [
            name for name, parameter in parameters.items()
            if parameter.default is inspect.Parameter.empty
            and name not in arguments
        ]
        unknown = [name for name in arguments if name not in parameters]
        if missing or unknown:
            raise _ToolArgumentError()
        try:
            hints = get_type_hints(target)
        except (NameError, TypeError, ValueError):
            # Postponed annotations that cannot be resolved are not guessed at;
            # argument names are still validated strictly.
            hints = {}
        for name, value in arguments.items():
            annotation = hints.get(name, inspect.Parameter.empty)
            if annotation is inspect.Parameter.empty:
                continue
            if not self._value_matches(annotation, value):
                raise _ToolArgumentError()
        if inspect.iscoroutinefunction(target):
            return await target(**arguments)
        return await asyncio.wrap_future(self._register_blocking_future(
            state, self._blocking_pool(state).submit(target, **arguments)
        ))
    @staticmethod
    def _value_matches(schema: Any, value: Any) -> bool:
        if schema is dict:
            return isinstance(value, Mapping)
        if schema is list:
            return isinstance(value, (list, tuple))
        if schema in {str, int, float, bool}:
            return type(value) is schema
        try:
            return isinstance(value, schema)
        except TypeError:
            return False
