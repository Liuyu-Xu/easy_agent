"""Current deterministic fault schedules; historical audit evidence stays unchanged."""

import asyncio

from easy_agent import Graph, Info, LoggingError, Node


class Identity(Node):
    def forward(self, info):
        return info


async def unobserved_errors(directory):
    entered = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()
    captured = []

    class InspectGraph(Graph):
        def _stop_run(self, state, *args):
            captured.append(state)
            return super()._stop_run(state, *args)

    class Fail(Node):
        def __init__(self, index):
            super().__init__()
            self.index = index

        async def forward(self, info):
            entered[self.index].set()
            await release.wait()
            raise ValueError("controlled simultaneous failure")

    graph = InspectGraph(execution="async", max_concurrency=2, log_dir=directory)
    graph.source, graph.left, graph.right, graph.join = Identity(), Fail(0), Fail(1), Identity()
    graph.source.send_to(graph.left, graph.right)
    graph.left.send_to(graph.join)
    graph.right.send_to(graph.join)
    graph.join.receive(mode="all", sources=[graph.left, graph.right])
    task = asyncio.create_task(graph.arun([Info("start")]))
    try:
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in entered)), 2)
    finally:
        release.set()
    try:
        await task
    except BaseException:
        pass
    owned = captured[0].owned_tasks
    unobserved = [task for task in owned if getattr(task, "_log_traceback", False)]
    result = {"unretrieved_done_failures": len(unobserved),
              "pending_tasks": sum(not task.done() for task in owned)}
    # The audit itself consumes remaining failures to leave no warning leak.
    for task in owned:
        if task.done() and not task.cancelled():
            task.exception()
    return result


async def timeout_log_failure(directory, fail_log):
    timeout_seen = asyncio.Event()
    fast_ready = asyncio.Event()
    late_success = []

    class HeldQueue(asyncio.Queue):
        def __init__(self):
            super().__init__()
            self.reads = 0

        async def get(self):
            self.reads += 1
            if self.reads == 2:
                # Hold observation of the already completed sibling until the
                # independent deadline supervisor has observed the timeout.
                await timeout_seen.wait()
            return await super().get()

    class InspectGraph(Graph):
        async def _run_plan(self, plan, initial_info, run_id, state):
            state.completions = HeldQueue()
            return await super()._run_plan(plan, initial_info, run_id, state)

        async def _execute_item(self, state, item):
            if item.node_id == self.slow.id:
                await fast_ready.wait()
            outcome = await super()._execute_item(state, item)
            if item.node_id == self.fast.id:
                fast_ready.set()
            return outcome

    class Fast(Node):
        async def forward(self, info):
            return Info("ready sibling")

    class Slow(Node):
        async def forward(self, info):
            await asyncio.sleep(10)
            return info

    graph = InspectGraph(execution="async", max_concurrency=2, log_dir=directory)
    graph.source, graph.fast, graph.slow, graph.join = Identity(), Fast(timeout=1), Slow(timeout=0.01), Identity()
    graph.source.send_to(graph.fast, graph.slow)
    graph.fast.send_to(graph.join)
    graph.slow.send_to(graph.join)
    graph.join.receive(mode="all", sources=[graph.fast, graph.slow])
    append = graph._event_store.append

    def audit_append(run_id, event_type, **kwargs):
        if event_type == "NodeTimedOut":
            timeout_seen.set()
            if fail_log:
                raise LoggingError("controlled deadline log failure", run_id=run_id)
        if event_type == "NodeSucceeded" and kwargs.get("node_id") == graph.fast.id and timeout_seen.is_set():
            late_success.append(event_type)
        return append(run_id, event_type, **kwargs)

    graph._event_store.append = audit_append
    try:
        await graph.arun([Info("start")])
        caller_error = None
    except BaseException as exc:
        caller_error = type(exc).__name__
    return {"fail_log": fail_log, "caller_error": caller_error,
            "sibling_success_after_timeout_observed": len(late_success)}
