"""Per-attempt registry for blocking business work.

A blocking call must run off the event loop *and* stay tracked, otherwise a
graph definition could be edited while the thread still reads it. The executor
is provided by the running attempt through a context variable so that
``Tool.forward`` can reach it without importing the graph module.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable

_REGISTRY: ContextVar[
    tuple[ThreadPoolExecutor, Callable[[Future], None]] | None
] = ContextVar("easy_agent_blocking", default=None)


@contextmanager
def bind_blocking(
    pool: ThreadPoolExecutor, register: Callable[[Future], None]
):
    token = _REGISTRY.set((pool, register))
    try:
        yield
    finally:
        _REGISTRY.reset(token)


def submit_blocking(function: Any, *args: Any, **kwargs: Any) -> Future | None:
    """Submit to the current attempt's pool, or return None when unbound."""

    current = _REGISTRY.get()
    if current is None:
        return None
    pool, register = current
    future = pool.submit(function, *args, **kwargs)
    register(future)
    return future
