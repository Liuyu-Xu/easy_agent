"""Minimal non-streaming DeepSeek adapter and private per-task context."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import json
from time import perf_counter
from typing import Any, Mapping
from urllib.parse import urlsplit

import httpx

from .errors import (
    _safe_error,
    ConfigurationError,
    ExecutionLimitError,
    NodeExecutionError,
    UnsupportedFeatureError,
)
from .info import Info, plain_data
from .hub import _HubContract


@dataclass(frozen=True)
class ModelAttempt:
    run_id: str
    node_id: str
    node_execution_id: str
    attempt: int
    config: Mapping[str, Any]
    api_key: str | None = field(repr=False)
    deadline: float | None
    output_schema: type | None
    event_store: Any = field(repr=False)
    task: asyncio.Task = field(repr=False)
    hub_contract: _HubContract | None = None
    tool_runner: Any = field(default=None, repr=False)
    tool_specs: Any = field(default=None, repr=False)


_ATTEMPT: ContextVar[ModelAttempt | None] = ContextVar("easy_agent_model_attempt", default=None)


class ModelDeadlineExpired(Exception):
    """Private signal for a deadline verified against the framework clock."""


@contextmanager
def bind_model_attempt(context: ModelAttempt):
    token = _ATTEMPT.set(context)
    try:
        yield
    finally:
        _ATTEMPT.reset(token)


def endpoint_url(base_url: Any) -> str:
    if not isinstance(base_url, str) or not base_url.strip():
        raise _safe_error(ConfigurationError, "内置模型节点缺少有效 base_url")
    try:
        parsed = urlsplit(base_url)
        _ = parsed.port
        local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        if (not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment
                or parsed.scheme not in {"https", "http"}
                or (parsed.scheme == "http" and not local)):
            raise ValueError()
    except ValueError:
        raise _safe_error(ConfigurationError, "模型 base_url 必须是无认证信息的 HTTPS 地址；HTTP 仅允许本地测试") from None
    normalized = base_url.rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    return normalized + "/chat/completions"


def validate_model_config(
    config: Mapping[str, Any],
    *,
    output_schema: type | None,
    api_key: str | None,
    require_secret: bool,
    hub_contract: _HubContract | None = None,
) -> None:
    if config.get("provider") not in {None, "deepseek"}:
        raise _safe_error(UnsupportedFeatureError, "最小内置适配器仅支持 provider='deepseek'")
    if output_schema not in {None, str} and not (hub_contract is not None and output_schema is dict):
        raise _safe_error(UnsupportedFeatureError, "内置适配器仅支持字符串输出和固定 Hub 输出契约")
    if not isinstance(config.get("model"), str) or not config["model"].strip():
        raise _safe_error(ConfigurationError, "内置模型节点缺少有效 model")
    endpoint_url(config.get("base_url"))
    temperature = config.get("temperature")
    if not 0 <= temperature <= 2:
        raise _safe_error(ConfigurationError, "内置模型 temperature 必须是 0 到 2 的有限数值")
    if require_secret and (
        not isinstance(api_key, str) or not api_key
        or api_key != api_key.strip() or "\r" in api_key or "\n" in api_key
    ):
        raise _safe_error(ConfigurationError, "内置模型节点缺少有效 API 密钥，请检查 api_key_env 和 env_file")


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(retries=0),
        follow_redirects=False,
    )


def _event(context: ModelAttempt, event_type: str, data: dict[str, Any]) -> None:
    context.event_store.append(
        context.run_id, event_type, category="model",
        span_id=context.node_execution_id, node_id=context.node_id,
        node_execution_id=context.node_execution_id, attempt=context.attempt,
        data=data,
    )


_RETRYABLE_HTTP_STATUS = frozenset({408, 409, 425, 429})
_MAX_TOOL_ROUNDS = 64


def _failure(message: str, *, retryable: bool = False) -> NodeExecutionError:
    """Build a safe adapter error and record whether retrying is allowed."""

    error = _safe_error(NodeExecutionError, message)
    error._easy_agent_retryable = retryable
    return error


def _usage(payload: dict[str, Any], maximum: int) -> dict[str, int | None] | None:
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None
    safe_usage: dict[str, int | None] = {}
    for name in ("prompt_tokens", "completion_tokens", "total_tokens"):
        value = usage.get(name)
        safe_usage[name] = value if type(value) is int and value >= 0 else None
    details = usage.get("completion_tokens_details")
    reasoning = details.get("reasoning_tokens") if isinstance(details, dict) else None
    safe_usage["reasoning_tokens"] = (
        reasoning if type(reasoning) is int and reasoning >= 0 else None
    )
    completion = safe_usage["completion_tokens"]
    if completion is not None and completion > maximum:
        raise _failure("模型报告的输出 Token 用量超过本轮剩余预算")
    return safe_usage


def _tool_calls(message: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = message.get("tool_calls")
    if not raw:
        return []
    if not isinstance(raw, list):
        raise _failure("模型响应中的 tool_calls 结构非法")
    calls: list[dict[str, Any]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict) or item.get("type") not in {None, "function"}:
            raise _failure("模型响应中的 tool_calls 结构非法")
        function = item.get("function")
        if not isinstance(function, dict):
            raise _failure("模型响应中的 tool_calls 缺少 function")
        name = function.get("name")
        if not isinstance(name, str) or not name.strip():
            raise _failure("模型响应中的工具名称非法")
        arguments = function.get("arguments", "")
        if isinstance(arguments, dict):
            arguments = json.dumps(arguments, ensure_ascii=False, sort_keys=True)
        if not isinstance(arguments, str):
            raise _failure("模型响应中的工具参数非法")
        call_id = item.get("id")
        if not isinstance(call_id, str) or not call_id:
            call_id = f"call_{index}"
        calls.append({"id": call_id, "name": name, "arguments": arguments})
    return calls


def _parse_payload(payload: Any, maximum: int) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise _failure("模型响应不是合法的 Chat Completions 对象")
    choices = payload.get("choices")
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        raise _failure("模型响应必须包含一个 choice")
    choice = choices[0]
    message = choice.get("message")
    if not isinstance(message, dict):
        raise _failure("模型响应缺少 message")
    usage = _usage(payload, maximum)
    reported = payload.get("model")
    calls = _tool_calls(message)
    finish_reason = choice.get("finish_reason")
    safe_model = reported if isinstance(reported, str) and reported else None
    if calls:
        if finish_reason != "tool_calls":
            raise _failure("模型未正常完成工具请求，拒绝执行截断或中止调用")
        content = message.get("content")
        return {
            "kind": "tools",
            "calls": calls,
            "content": content if isinstance(content, str) else "",
            "usage": usage,
            "reported_model": safe_model,
            "finish_reason": finish_reason,
        }
    if finish_reason != "stop":
        raise _failure("模型未正常完成文本输出，拒绝发布截断或中止结果")
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise _failure("模型响应缺少有效文本正文")
    return {
        "kind": "final",
        "calls": [],
        "content": content,
        "usage": usage,
        "reported_model": safe_model,
        "finish_reason": finish_reason,
    }


async def _post(context: ModelAttempt, body: dict[str, Any], remaining: float | None):
    try:
        async with _client() as client:
            return await client.post(
                endpoint_url(context.config["base_url"]), json=body,
                headers={"Authorization": "Bearer " + context.api_key},
                timeout=remaining,
            )
    except httpx.TimeoutException:
        if context.deadline is not None and perf_counter() >= context.deadline:
            raise ModelDeadlineExpired() from None
        raise _failure("模型 HTTP 阶段超时", retryable=True) from None
    except httpx.HTTPError:
        raise _failure("模型 HTTP 传输失败", retryable=True) from None


async def call_model(info: Info, node_id: str) -> Info:
    context = _ATTEMPT.get()
    if context is None or context.node_id != node_id or context.task is not asyncio.current_task():
        raise _safe_error(UnsupportedFeatureError, "内置 Node.forward 必须由 Graph 的当前尝试调用")
    config = context.config
    validate_model_config(
        config, output_schema=context.output_schema,
        api_key=context.api_key, require_secret=True,
        hub_contract=context.hub_contract,
    )
    system = "\n\n".join(config[name] for name in ("role", "prompt") if config.get(name))
    messages: list[dict[str, Any]] = []
    if system:
        messages.append({"role": "system", "content": system})
    user = info.data if isinstance(info.data, str) else json.dumps(
        plain_data(info), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    messages.append({"role": "user", "content": user})
    budget = int(config["max_output_tokens"])
    rounds = 0
    while True:
        rounds += 1
        if rounds > _MAX_TOOL_ROUNDS:
            raise _safe_error(ExecutionLimitError, "内部工具轮次超过框架安全上限")
        remaining = None if context.deadline is None else context.deadline - perf_counter()
        if remaining is not None and remaining <= 0:
            raise ModelDeadlineExpired()
        if budget <= 0:
            raise _failure("本次尝试的输出预算已耗尽")
        body = {
            "model": config["model"], "messages": messages,
            "temperature": config["temperature"],
            "max_tokens": budget,
            "stream": False, "thinking": {"type": "disabled"},
        }
        if context.tool_specs:
            body["tools"] = list(context.tool_specs)
        _event(context, "ModelRequestStarted", {
            "requested_model": config["model"], "thinking": "disabled",
            "stream": False, "max_output_tokens": budget, "round": rounds,
        })
        response = await _post(context, body, remaining)
        if response.status_code != 200:
            retryable = (
                response.status_code in _RETRYABLE_HTTP_STATUS
                or response.status_code >= 500
            )
            raise _failure(
                f"模型 API 返回 HTTP {response.status_code}", retryable=retryable
            ) from None
        try:
            payload = response.json()
        except (ValueError, UnicodeError):
            raise _failure("模型响应不是合法 JSON") from None
        parsed = _parse_payload(payload, budget)
        completion = (parsed["usage"] or {}).get("completion_tokens")
        if completion is not None:
            budget -= completion
        _event(context, "ModelRequestFinished", {
            "requested_model": config["model"],
            "reported_model": parsed["reported_model"],
            "finish_reason": parsed["finish_reason"],
            "usage": parsed["usage"],
            "round": rounds,
        })
        if parsed["kind"] == "final":
            return Info(
                parsed["content"] if context.hub_contract is None
                else context.hub_contract.parse(parsed["content"])
            )
        if context.tool_runner is None:
            raise _safe_error(UnsupportedFeatureError, "该节点不具备内部工具执行入口")
        if completion is None:
            raise _failure("模型请求续轮但未报告用量，无法核算输出预算")
        if budget <= 0:
            raise _failure("本次尝试的输出预算已耗尽，拒绝执行无法续轮的工具调用")
        _event(context, "ModelToolRound", {
            "round": rounds, "tool_count": len(parsed["calls"]),
        })
        messages.append({
            "role": "assistant",
            "content": parsed["content"],
            "tool_calls": [
                {"id": call["id"], "type": "function",
                 "function": {"name": call["name"], "arguments": call["arguments"]}}
                for call in parsed["calls"]
            ],
        })
        for call in parsed["calls"]:
            tool_content = await context.tool_runner(call)
            messages.append({
                "role": "tool", "tool_call_id": call["id"], "content": tool_content,
            })

