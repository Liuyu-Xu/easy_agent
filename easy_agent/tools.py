"""Tool nodes participating in the same Info flow as model nodes."""

from __future__ import annotations

import inspect
from typing import Any

from .errors import _safe_error, UnsupportedFeatureError
from .info import Info
from .config import _UNSET
from .node import Node


class Tool(Node):
    """A graph node with a compatibility bridge for legacy ``invoke`` methods.

    New subclasses may override ``forward`` directly.  Existing subclasses that
    only override ``invoke`` continue to work through the default bridge.
    """

    side_effect = False

    def __init__(
        self,
        *,
        tool_name: str | None = None,
        description: str | None = None,
        side_effect: bool | object = _UNSET,
        **node_config: Any,
    ) -> None:
        super().__init__(**node_config)
        if side_effect is _UNSET:
            side_effect = type(self).side_effect
        resolved_name = tool_name or type(self).__name__
        self.tool_name = resolved_name
        self.description = description or inspect.getdoc(type(self)) or resolved_name
        self.side_effect = side_effect

    def __setattr__(self, name: str, value: Any) -> None:
        if name in {"tool_name", "description"}:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} 必须是非空字符串")
        elif name == "side_effect" and not isinstance(value, bool):
            raise TypeError("side_effect 必须是 bool")
        super().__setattr__(name, value)

    async def forward(self, info: Info) -> Info:
        legacy = getattr(self, "invoke", None)
        if not callable(legacy):
            raise _safe_error(UnsupportedFeatureError,
                "Tool 必须覆盖 forward(info)；旧版 invoke(info)仅作兼容"
            )
        result = legacy(info)
        if inspect.isawaitable(result):
            result = await result
        return result
