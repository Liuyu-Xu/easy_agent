"""Read-only behavioral audit of the first-stage implementation.

This script uses isolated Graph instances, temporary logs and releasable tasks.
It does not fix or replace production implementations and is not discovered as
a normal project test. Its JSON results record actual observations.
"""

import asyncio
import json
from pathlib import Path
import sys
import tempfile
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from easy_agent import Graph, Node, Tool, Info, LoggingError


class Identity(Node):
    def forward(self, info):
        return info


class ReadTool(Tool):
    def forward(self, info):
        return info


def error_type(call):
    try:
        call()
    except BaseException as exc:
        return type(exc).__name__
    return None


async def shared_editor_during_run(directory):
    entered, release = asyncio.Event(), asyncio.Event()

    class Waiting(Node):
        async def forward(self, info):
            entered.set()
            await release.wait()
            return info

    tool = ReadTool(tool_name="read")
    running = Graph(execution="async", allowed_tools={"read"}, log_dir=directory)
    editor = Graph(allowed_tools={"read"}, log_dir=directory)
    running.node, editor.node = Waiting(tools=[tool]), Identity(tools=[tool])
    task = asyncio.create_task(running.arun(Info("start")))
    await entered.wait()
    try:
        with editor.edit():
            release.set()
            try:
                await task
                caller_error = None
            except BaseException as exc:
                caller_error = type(exc).__name__
    finally:
        release.set()
        if not task.done():
            await asyncio.gather(task, return_exceptions=True)
    return {
        "caller_error": caller_error,
        "succeeded_log_count": len(running.logs(event_type="GraphRunSucceeded")),
        "active_runs_after_exit": running._active_runs,
        "tool_leases_after_exit": tool._definition_leases,
        "edit_after_exit_error": error_type(lambda: setattr(running.node, "prompt", "after")),
    }


def foreign_rollback(directory):
    graph, other = Graph(log_dir=directory), Graph(log_dir=directory)
    graph.node, other.node = Identity(), Identity(prompt="before")
    try:
        with graph.edit():
            bind_error = error_type(lambda: setattr(graph, "foreign", other.node))
            foreign_marked = other.node._definition_editor is not None
            other.node.prompt = "legitimate other-graph change"
            raise RuntimeError("abort first graph")
    except RuntimeError:
        pass
    return {"bind_error": bind_error, "foreign_marked_after_rejection": foreign_marked,
            "other_prompt_after_rollback": other.node.prompt}


def deleted_tools(directory):
    graph = Graph(log_dir=directory)
    graph.node = Identity()
    deletion = error_type(lambda: del_tools(graph.node))
    return {"delete_error": deletion,
            "restore_error": error_type(lambda: setattr(graph.node, "tools", ())),
            "remove_node_error": error_type(lambda: del_node(graph)),
            "export_error": error_type(graph.to_dict)}


def del_tools(node):
    del node.tools


def del_node(graph):
    del graph.node


def node_attribute_collision(directory):
    class Cached(Node):
        def __init__(self):
            super().__init__()
            self._nodes = {"business": "cache"}

        def forward(self, info):
            return info

    graph = Graph(log_dir=directory)
    graph.source, graph.target = Cached(), Identity()
    return {"send_to_error": error_type(lambda: graph.source.send_to(graph.target)),
            "downstreams_error": error_type(graph.source.downstreams)}


def unrelated_graph_lock(directory):
    graph, other = Graph(log_dir=directory), Graph(log_dir=directory)
    graph.node, other.node = Identity(), Identity()
    started, finished = threading.Event(), threading.Event()

    def read_other():
        started.set()
        other.to_dict()
        finished.set()

    thread = threading.Thread(target=read_other)
    with graph.edit():
        thread.start()
        if not started.wait(2):
            raise RuntimeError("probe thread did not start")
        blocked_during_edit = not finished.wait(0.05)
    thread.join(2)
    if thread.is_alive():
        raise RuntimeError("probe thread was not released")
    return {"unrelated_reader_blocked": blocked_during_edit,
            "reader_finished_after_edit": finished.is_set()}


async def retained_tasks(directory):
    observed = []

    class InspectGraph(Graph):
        def _commit_outcome(self, state, outcome, sequence):
            observed.append((state.steps, len(state.owned_tasks),
                             sum(task.done() for task in state.owned_tasks)))
            return super()._commit_outcome(state, outcome, sequence)

    class Count(Node):
        async def forward(self, info):
            await asyncio.sleep(0)
            return Info({"count": info.data["count"] + 1,
                         "done": info.data["count"] + 1 == 10})

    graph = InspectGraph(execution="async", log_dir=directory)
    graph.node = Count()
    graph.entry, graph.max_steps = graph.node, 20
    graph.node.route(field="done", cases={True: graph.END}, default=graph.node)
    await graph.arun(Info({"count": 0, "done": False}))
    return {"last_steps_owned_done": observed[-1], "owned_task_counts": [v[1] for v in observed]}


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
    task = asyncio.create_task(graph.arun(Info("start")))
    try:
        await asyncio.gather(*(event.wait() for event in entered))
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
        await graph.arun(Info("start"))
        caller_error = None
    except BaseException as exc:
        caller_error = type(exc).__name__
    return {"fail_log": fail_log, "caller_error": caller_error,
            "sibling_success_after_timeout_observed": len(late_success)}


async def main():
    with tempfile.TemporaryDirectory() as directory:
        result = {
            "shared_editor_during_run": await shared_editor_during_run(directory),
            "foreign_rollback": foreign_rollback(directory),
            "deleted_tools": deleted_tools(directory),
            "node_attribute_collision": node_attribute_collision(directory),
            "unrelated_graph_lock": unrelated_graph_lock(directory),
            "retained_tasks": await retained_tasks(directory),
            "unobserved_errors": await unobserved_errors(directory),
            "timeout_log_control": await timeout_log_failure(directory, False),
            "timeout_log_failure": await timeout_log_failure(directory, True),
        }
    print(json.dumps({"python": sys.version.split()[0], "observations": result}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
