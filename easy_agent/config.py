"""Explicit .env loading and per-run configuration snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
import math
import os
from pathlib import Path
from typing import Any, Mapping, TYPE_CHECKING
import warnings

from dotenv.parser import parse_stream

from .errors import _safe_error, ConfigurationError

if TYPE_CHECKING:
    from .node import Node


_UNSET = object()


def _validate_int(name: str, value: Any, *, minimum: int = 1,
                  error_type: type[Exception] | None = None) -> None:
    invalid_type = not isinstance(value, int) or isinstance(value, bool)
    if invalid_type or value < minimum:
        message = f"{name} 必须为{'正' if minimum == 1 else '非负'}整数"
        if error_type is ConfigurationError:
            raise _safe_error(ConfigurationError, message)
        raise (error_type or (TypeError if invalid_type else ValueError))(message)


def _validate_config_value(name: str, value: Any) -> None:
    """Validate declarations independently of a model provider."""
    if value is None:
        return
    if name in {"max_output_tokens", "input_token_budget", "max_parallelism", "max_attempts"}:
        _validate_int(name, value)
    elif name in {"timeout", "temperature"}:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} 必须是数值或 None")
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite or (name == "timeout" and value <= 0):
            raise ValueError(f"{name} 必须是{'正的' if name == 'timeout' else ''}有限数值")
    elif name in {"model", "provider", "base_url", "api_key_env", "prompt", "role"}:
        if not isinstance(value, str):
            raise TypeError(f"{name} 必须是字符串或 None")


def _resolve_attempts(max_attempts: Any, retry: Any, *, default: int | None) -> int | None:
    if max_attempts is not _UNSET and retry is not _UNSET:
        raise ValueError("max_attempts 与 retry 不能同时提供")
    if retry is not _UNSET:
        if retry is None and default is None:
            return None
        _validate_int("retry", retry, minimum=0)
        warnings.warn("retry 已弃用；请改用表示总机会数的 max_attempts",
                      DeprecationWarning, stacklevel=3)
        return retry + 1
    value = default if max_attempts is _UNSET else max_attempts
    if value is not None or default is not None:
        _validate_int("max_attempts", value)
    return value


DEFAULT_ENV_KEYS = {
    "provider": "EASY_AGENT_PROVIDER",
    "model": "EASY_AGENT_MODEL",
    "base_url": "EASY_AGENT_BASE_URL",
    "api_key": "EASY_AGENT_API_KEY",
}

LLM_ENV_KEYS = {
    "provider": "EASY_AGENT_PROVIDER",
    "model": "LLM_MODEL_ID",
    "base_url": "LLM_BASE_URL",
    "api_key": "LLM_API_KEY",
}


def select_env_keys(
    env_file_values: Mapping[str, str], *, include_environment: bool = True,
) -> Mapping[str, str]:
    """Select one account vocabulary without combining account settings."""
    def present(keys: Mapping[str, str]) -> bool:
        return any(
            (include_environment and keys[name] in os.environ)
            or keys[name] in env_file_values
            for name in ("model", "base_url", "api_key")
        )

    standard, compatible = present(DEFAULT_ENV_KEYS), present(LLM_ENV_KEYS)
    if standard and compatible:
        raise _safe_error(ConfigurationError, "EASY_AGENT_* 与 LLM_* 账户配置不能同时出现")
    return LLM_ENV_KEYS if compatible else DEFAULT_ENV_KEYS


def normalize_env_file(path: str | os.PathLike[str] | None) -> Path | None:
    if path is None:
        return None
    try:
        resolved = Path(path).resolve()
        exists = resolved.is_file()
    except (OSError, UnicodeError):
        raise _safe_error(ConfigurationError, "无法解析 env_file 路径") from None
    if not exists:
        raise _safe_error(ConfigurationError, f"指定的 env_file 不存在：{resolved}")
    return resolved


def read_selected_env(path: Path | None, keys: set[str]) -> dict[str, str]:
    if path is None:
        return {}
    try:
        if not path.is_file():
            raise _safe_error(ConfigurationError, f"指定的 env_file 不存在：{path}")
        content = path.read_text(encoding="utf-8")
        bindings = tuple(parse_stream(StringIO(content)))
        invalid_lines = [
            binding.original.line for binding in bindings if binding.error
        ]
        if invalid_lines:
            rendered = ", ".join(str(line) for line in invalid_lines)
            raise _safe_error(ConfigurationError,
                f"env_file 包含非法语法：{path}，行号：{rendered}"
            )

        seen: dict[str, int] = {}
        duplicates: list[tuple[str, int, int]] = []
        for binding in bindings:
            if binding.key is None:
                continue
            if binding.key in seen:
                duplicates.append(
                    (binding.key, seen[binding.key], binding.original.line)
                )
            else:
                seen[binding.key] = binding.original.line
        if duplicates:
            rendered = ", ".join(
                f"{key}（第 {first}/{again} 行）"
                for key, first, again in duplicates
            )
            raise _safe_error(ConfigurationError,
                f"env_file 包含重复键：{path}：{rendered}"
            )

    except ConfigurationError:
        raise
    except (OSError, UnicodeError):
        raise _safe_error(ConfigurationError, f"无法读取 env_file：{path}") from None
    return {binding.key: binding.value for binding in bindings
            if binding.key in keys and binding.value is not None}


@dataclass(frozen=True)
class ConfigSnapshot:
    public_values: Mapping[str, str | int | float | None]
    secret_values: Mapping[str, str]
    secret_sources: Mapping[str, str]
    max_steps: int
    max_concurrency: int

    def log_view(self) -> dict[str, object]:
        return {
            "public_values": dict(self.public_values),
            "secret_references": {
                name: {"source": source, "resolved": name in self.secret_values}
                for name, source in self.secret_sources.items()
            },
            "max_steps": self.max_steps,
            "max_concurrency": self.max_concurrency,
        }


def resolve_key(
    key: str,
    *,
    explicit: str | None,
    env_file_values: Mapping[str, str],
) -> tuple[str | None, str]:
    if explicit is not None:
        return explicit, "graph"
    if key in os.environ:
        return os.environ[key], "os.environ"
    if key in env_file_values:
        return env_file_values[key], "env_file"
    return None, "missing"


class _ConfigResolver:
    """One compilation's file cache and isolated node account snapshots."""

    def __init__(self, defaults: Mapping[str, Any], env_file: Path | None,
                 explicit_key: bool, nodes: tuple[Node, ...]) -> None:
        self.defaults = defaults
        self.env_file = env_file
        self.explicit_key = explicit_key
        self.keys = set(DEFAULT_ENV_KEYS.values()) | set(LLM_ENV_KEYS.values())
        if defaults['api_key_env']:
            self.keys.add(str(defaults['api_key_env']))
        self.keys.update(node.api_key_env for node in nodes if node.api_key_env)
        self.files = {}
        self.legacy = None
        self.public_values = {}
        self.secret_values = {}
        self.secret_sources = {}

    def _read(self, path: Path | None) -> dict[str, str]:
        if path not in self.files:
            self.files[path] = read_selected_env(path, self.keys)
        return self.files[path]

    def _legacy_account(self):
        if self.legacy is None:
            values = self._read(self.env_file)
            selected = select_env_keys(values)
            account = {}
            for key in ('provider', 'model', 'base_url'):
                value, source = resolve_key(
                    selected[key], explicit=self.defaults[key], env_file_values=values,
                )
                if value is None and key in ('model', 'base_url'):
                    value = {'model': 'deepseek-v4-flash',
                             'base_url': 'https://api.deepseek.com'}[key]
                    source = 'library_default'
                account[key] = value
                self.public_values[f'graph.{key}'] = value
                self.public_values[f'graph.{key}_source'] = source
            reference = self.defaults['api_key_env'] if self.explicit_key else selected['api_key']
            self.legacy = account, values, reference
        return self.legacy

    def resolve(self, node: Node) -> dict[str, Any]:
        scoped = node.env_file is not None
        if scoped:
            values = self._read(node.env_file)
            selected = select_env_keys(values, include_environment=False)
            account = {key: values.get(selected[key]) for key in ('provider', 'model', 'base_url')}
            reference = node.api_key_env or selected['api_key']
            secret = values.get(reference)
            source = 'env_file' if reference in values else 'missing'
            secret_id = f'{node.id}:{reference}'
        else:
            account, values, default_reference = self._legacy_account()
            reference = node.api_key_env or default_reference
            secret, source = (resolve_key(reference, explicit=None, env_file_values=values)
                              if reference else (None, 'missing'))
            secret_id = reference
        effective = {}
        for key in ('provider', 'model', 'base_url'):
            local = getattr(node, key)
            effective[key] = local if local is not None else account[key]
            self.public_values[f'nodes.{node.name}.{key}'] = effective[key]
            self.public_values[f'nodes.{node.name}.{key}_source'] = (
                'node' if local is not None else (
                    ('env_file' if selected[key] in values else 'missing') if scoped else 'graph'
                )
            )
        if scoped:
            if any(not isinstance(effective[key], str) or not effective[key].strip()
                   for key in ('model', 'base_url')) or not secret:
                raise _safe_error(ConfigurationError, '节点 env_file 账户必须提供 model、base_url 和所选密钥')
            self.public_values[f'nodes.{node.name}.env_file'] = str(node.env_file)
        for key in ('temperature', 'max_output_tokens', 'input_token_budget', 'timeout', 'max_attempts'):
            local = getattr(node, key)
            effective[key] = local if local is not None else self.defaults[key]
            self.public_values[f'nodes.{node.name}.{key}'] = effective[key]
        if effective['input_token_budget'] is not None:
            self.public_values[f'nodes.{node.name}.input_token_budget_status'] = 'accepted_not_applied'
        if reference:
            self.secret_sources[secret_id] = source
        if secret is not None:
            self.secret_values[secret_id] = secret
        effective.update(api_key_env=reference, _secret_id=secret_id,
                         prompt=node.prompt, role=node.role)
        try:
            for key, value in effective.items():
                _validate_config_value(key, value)
        except (TypeError, ValueError):
            raise _safe_error(ConfigurationError, f"节点配置 {key} 的类型或数值无效") from None
        return effective
