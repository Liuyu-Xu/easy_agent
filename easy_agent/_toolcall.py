"""Execution of model-requested tools inside the parent attempt.

Tools are sub-activities: they share the attempt deadline and output budget,
never become graph nodes, and never trigger routing or delivery.
"""

from __future__ import annotations

import asyncio
import inspect
import json
from time import perf_counter
from typing import Any, Mapping

from .errors import EasyAgentError, InfoError, UnsupportedFeatureError, _safe_error
from .info import Info, plain_data
from ._plan import _NodePlan, _ToolDescriptor
from ._runtime import _RunState
from ._json import load_object


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
            value = await self._invoke_tool(state, descriptor, arguments, execution_id, attempt)
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
        try:
            return load_object(raw)
        except (TypeError, ValueError, RecursionError):
            raise _ToolArgumentError() from None

    async def _invoke_tool(
        self, state: _RunState, descriptor: "_ToolDescriptor",
        arguments: dict[str, Any], execution_id: str, attempt: int,
    ) -> Any:
        target = descriptor.callable
        if descriptor.kind == "tool":
            info = Info(arguments)
            try:
                normalized = self._validate_schema(info, descriptor.input_schema, "输入", state.run_id)
            except InfoError:
                raise _ToolArgumentError() from None
            if normalized.content_hash != info.content_hash:
                raise _safe_error(UnsupportedFeatureError,
                    "输入 Schema 改变了业务数据；派生 Info 来源协议尚未定稿"
                )
            args, kwargs = (info,), {}
        else:
            parameters = {p.name: p for p in descriptor.parameters}
            if any(p.required and p.name not in arguments for p in descriptor.parameters):
                raise _ToolArgumentError()
            if any(name not in parameters or not parameters[name].accepts(value)
                   for name, value in arguments.items()):
                raise _ToolArgumentError()
            args, kwargs = (), arguments
        if inspect.iscoroutinefunction(target):
            value = await target(*args, **kwargs)
        else:
            value = await asyncio.wrap_future(self._register_blocking_future(
                state, self._blocking_pool(state).submit(target, *args, **kwargs),
                execution_id=execution_id, attempt=attempt,
            ))
        if descriptor.kind == "tool":
            if not isinstance(value, Info):
                raise TypeError()
            try:
                value = self._validate_schema(value, descriptor.output_schema, "输出", state.run_id)
            except InfoError:
                raise ValueError() from None
        return value
