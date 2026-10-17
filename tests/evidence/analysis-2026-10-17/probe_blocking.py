"""Acceptance probe for PF-1 (blocking execution) after the fix.

Scenario 1-2 use a *sync* forward, which now runs on the blocking pool.
Scenario 3 keeps a blocking call inside a coroutine: it cannot be fixed
statically, so the framework records an EventLoopBlocked event instead.
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path

from easy_agent import Graph, Info, Node

TMP = Path(tempfile.mkdtemp(prefix="ea-block-"))


class Start(Node):
    def forward(self, info: Info) -> Info:
        return Info("go")


class SyncSlow(Node):
    def forward(self, info: Info) -> Info:
        time.sleep(0.3)
        return Info("A")


class AsyncTimed(Node):
    async def forward(self, info: Info) -> Info:
        started = time.perf_counter()
        await asyncio.sleep(0.05)
        info_elapsed["value"] = round(time.perf_counter() - started, 3)
        return Info("B")


class AsyncBlocking(Node):
    async def forward(self, info: Info) -> Info:
        await asyncio.sleep(0)
        time.sleep(0.2)
        return Info("A")


def build(*, slow, fast, slow_timeout, fast_timeout, log):
    graph = Graph(execution="async", max_concurrency=2, timeout=5, log_dir=log)
    begin = Start()
    graph.begin, graph.slow, graph.fast = begin, slow, fast
    graph.begin.send_to(graph.slow)
    graph.begin.send_to(graph.fast)
    graph.start = [begin]
    graph.end = [graph.slow, graph.fast]
    if slow_timeout is not None:
        slow.timeout = slow_timeout
    if fast_timeout is not None:
        fast.timeout = fast_timeout
    return graph


info_elapsed: dict[str, float] = {}

# scenario 1: no per-node deadline; is the sibling still delayed?
graph = build(slow=SyncSlow(), fast=AsyncTimed(), slow_timeout=None,
              fast_timeout=None, log=TMP / "s1")
graph.run([Info("x")])
print("s1_sync_block_sibling_elapsed =", info_elapsed.get("value"))

# scenario 2: sibling deadline 0.1s; is it still mis-charged?
info_elapsed.clear()
graph = build(slow=SyncSlow(), fast=AsyncTimed(), slow_timeout=5,
              fast_timeout=0.1, log=TMP / "s2")
error = None
try:
    graph.run([Info("x")])
except Exception as exc:  # noqa: BLE001 - observation only
    error = type(exc).__name__
print("s2_sibling_deadline_error =", error)
print("s2_sibling_elapsed =", info_elapsed.get("value"))

# scenario 3: blocking inside a coroutine is a documented limitation
graph = Graph(log_dir=TMP / "s3", timeout=5)
graph.node = AsyncBlocking()
result = graph.run([Info("x")])[0]
blocks = [
    event for event in graph._event_store.query(run_id=result.run_id, limit=200)
    if event["event_type"] == "EventLoopBlocked"
]
print("s3_async_block_stall_events =", len(blocks))
print("s3_stall_ms =", blocks[0]["data"]["stall_ms"] if blocks else None)
raise SystemExit(0)
