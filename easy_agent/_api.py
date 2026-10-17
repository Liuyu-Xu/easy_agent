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

from .errors import _safe_error, ConfigurationError, NodeExecutionError, UnsupportedFeatureError
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
    has_tools: bool
    event_store: Any = field(repr=False)
    task: asyncio.Task = field(repr=False)
    hub_contract: _HubContract | None = None


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
    has_tools: bool,
    api_key: str | None,
    require_secret: bool,
    hub_contract: _HubContract | None = None,
) -> None:
    if config.get("provider") not in {None, "deepseek"}:
        raise _safe_error(UnsupportedFeatureError, "最小内置适配器仅支持 provider='deepseek'")
    if has_tools:
        raise _safe_error(UnsupportedFeatureError, "最小 API 适配器不执行内部工具循环；详见 v0.3-api-minimal.md")
    if output_schema not in {None, str} and not (hub_contract is not None and output_schema is dict):
        raise _safe_error(UnsupportedFeatureError, "最小 API 适配器仅支持字符串输出；详见 v0.3-api-minimal.md")
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


def _parse_response(payload: Any, maximum: int) -> tuple[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        raise _safe_error(NodeExecutionError, "模型响应不是合法的 Chat Completions 对象")
    choices = payload.get("choices")
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        raise _safe_error(NodeExecutionError, "模型响应必须包含一个 choice")
    choice = choices[0]
    message = choice.get("message")
    if not isinstance(message, dict):
        raise _safe_error(NodeExecutionError, "模型响应缺少 message")
    if message.get("tool_calls") or choice.get("finish_reason") == "tool_calls":
        raise _safe_error(UnsupportedFeatureError, "模型返回了最小适配器不支持的内部工具请求")
    if choice.get("finish_reason") != "stop":
        raise _safe_error(NodeExecutionError, "模型未正常完成文本输出，拒绝发布截断或中止结果")
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise _safe_error(NodeExecutionError, "模型响应缺少有效文本正文")
    usage = payload.get("usage")
    safe_usage: dict[str, int | None] | None = None
    if isinstance(usage, dict):
        safe_usage = {}
        for name in ("prompt_tokens", "completion_tokens", "total_tokens"):
            value = usage.get(name)
            safe_usage[name] = value if type(value) is int and value >= 0 else None
        details = usage.get("completion_tokens_details")
        reasoning = details.get("reasoning_tokens") if isinstance(details, dict) else None
        safe_usage["reasoning_tokens"] = reasoning if type(reasoning) is int and reasoning >= 0 else None
        completion = safe_usage["completion_tokens"]
        if completion is not None and completion > maximum:
            raise _safe_error(NodeExecutionError, "模型报告的输出 Token 用量超过尝试上限")
    reported = payload.get("model")
    return content, {
        "reported_model": reported if isinstance(reported, str) and reported else None,
        "finish_reason": "stop",
        "usage": safe_usage,
    }


async def call_model(info: Info, node_id: str) -> Info:
    context = _ATTEMPT.get()
    if context is None or context.node_id != node_id or context.task is not asyncio.current_task():
        raise _safe_error(UnsupportedFeatureError, "内置 Node.forward 必须由 Graph 的当前尝试调用")
    config = context.config
    validate_model_config(
        config, output_schema=context.output_schema,
        has_tools=context.has_tools, api_key=context.api_key, require_secret=True,
        hub_contract=context.hub_contract,
    )
    system = "\n\n".join(config[name] for name in ("role", "prompt") if config.get(name))
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    user = info.data if isinstance(info.data, str) else json.dumps(
        plain_data(info), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    messages.append({"role": "user", "content": user})
    body = {
        "model": config["model"], "messages": messages,
        "temperature": config["temperature"],
        "max_tokens": config["max_output_tokens"],
        "stream": False, "thinking": {"type": "disabled"},
    }
    remaining = None if context.deadline is None else context.deadline - perf_counter()
    if remaining is not None and remaining <= 0:
        raise ModelDeadlineExpired()
    _event(context, "ModelRequestStarted", {
        "requested_model": config["model"], "thinking": "disabled",
        "stream": False, "max_output_tokens": config["max_output_tokens"],
    })
    try:
        async with _client() as client:
            response = await client.post(
                endpoint_url(config["base_url"]), json=body,
                headers={"Authorization": "Bearer " + context.api_key},
                timeout=remaining,
            )
    except httpx.TimeoutException:
        if context.deadline is not None and perf_counter() >= context.deadline:
            raise ModelDeadlineExpired() from None
        raise _safe_error(NodeExecutionError, "模型 HTTP 阶段超时；未自动重试") from None
    except httpx.HTTPError:
        raise _safe_error(NodeExecutionError, "模型 HTTP 传输失败；未自动重试") from None
    if response.status_code != 200:
        raise _safe_error(NodeExecutionError, f"模型 API 返回 HTTP {response.status_code}；未自动重试") from None
    try:
        payload = response.json()
    except (ValueError, UnicodeError):
        raise _safe_error(NodeExecutionError, "模型响应不是合法 JSON") from None
    content, metadata = _parse_response(payload, config["max_output_tokens"])
    _event(context, "ModelRequestFinished", {
        "requested_model": config["model"], **metadata,
    })
    return Info(content if context.hub_contract is None else context.hub_contract.parse(content))
