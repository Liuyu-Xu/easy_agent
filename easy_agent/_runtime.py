"""Per-run runtime state: work items, retries, outcomes and the run record.

These objects belong to exactly one run and are never shared across runs.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field

from .errors import _safe_error, ExecutionLimitError
from .info import Info
from ._plan import _NodePlan, _Plan


@dataclass(frozen=True)
class _WorkItem:
    node_id: str
    info: Info[Any]
    branch_group: str | None = None
    delivery_id: str | None = None
    cause_event_id: str | None = None
    execution_id: str | None = None
    attempt: int = 1


@dataclass(frozen=True)
class _Retry:
    """A retryable attempt failure returned to the coordinator for backoff."""

    item: _WorkItem
    ready_at: float
    delay: float


@dataclass(frozen=True)
class _Outcome:
    item: _WorkItem
    node_plan: _NodePlan
    output: Info[Any]
    node_execution_id: str
    event_id: str
    attempt: int
    started_at: float
    deadline: float | None


class _AttemptDeadlineExpired(Exception):
    """Raised only by the framework deadline supervisor."""

    def __init__(self, *, thread_running: bool = False) -> None:
        super().__init__()
        self.thread_running = thread_running


@dataclass
class _RunState:
    plan: _Plan
    run_id: str
    steps: int = 0
    candidates: list[tuple[str, Info[Any]]] = field(default_factory=list)
    join_buffers: dict[tuple[str, str], dict[str, Info[Any]]] = field(
        default_factory=dict
    )
    completed_joins: set[tuple[str, str]] = field(default_factory=set)
    joined_infos: dict[tuple, Info[Any]] = field(default_factory=dict)
    status: str = "RUNNING"
    terminal_logged: bool = False
    terminal_error: BaseException | None = None
    owned_tasks: set[asyncio.Task] = field(default_factory=set)
    cancel_requested: set[asyncio.Task] = field(default_factory=set)
    blocking_pool: ThreadPoolExecutor | None = None
    blocking_futures: dict[str, Future] = field(default_factory=dict)
    delayed: list[_Retry] = field(default_factory=list)
    active_attempts: dict[str, int] = field(default_factory=dict)
    completions: asyncio.Queue = field(default_factory=asyncio.Queue)
    completion_sequence: int = 0
    reception_sequence: int = 0

    def consume_step(self) -> None:
        self.steps += 1
        if self.steps > self.plan.config.max_steps:
            raise _safe_error(ExecutionLimitError,
                f"运行超过 max_steps={self.plan.config.max_steps}",
                run_id=self.run_id,
            )
