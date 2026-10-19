"""Read-only audit probes. No real credentials or external HTTP requests.

This file records current behavior rather than changing library code. Run from
the repository root using the common environment's Python.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import sys
import tempfile
import threading
import time
from typing import get_type_hints
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from easy_agent import Graph, Info, Node, Tool
from easy_agent.errors import NodeExecutionError, _safe_error
from easy_agent._runtime import _WorkItem
import httpx


def final(tokens=3):
    return {
        "model": "audit-model",
        "choices": [{"message": {"content": "done"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 2, "completion_tokens": tokens,
                  "total_tokens": tokens + 2},
    }


def tool_call(name, arguments, *, tokens=3, finish="tool_calls"):
    payload = final(tokens)
    payload["choices"] = [{
        "message": {"content": None, "tool_calls": [{
            "id": "audit-call", "type": "function",
            "function": {"name": name, "arguments": arguments},
        }]}, "finish_reason": finish,
    }]
    return payload


@contextmanager
def setup(**overrides):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        env = path / "fake.env"
        env.write_text("AUDIT_FAKE_KEY=not-a-real-credential\n", encoding="utf-8")
        options = dict(
            env_file=env, model="audit-model", provider="deepseek",
            api_key_env="AUDIT_FAKE_KEY", execution="async",
            timeout=1, max_attempts=1, max_output_tokens=32,
            log_dir=path / "logs",
        )
        options.update(overrides)
        yield Graph(**options)


def events(graph):
    return graph._event_store.query(run_id=graph._last_run_id, limit=2000)


def mock_run(graph, payloads):
    requests = []

    async def handler(request):
        # Retain request JSON only; never retain Authorization headers.
        requests.append(json.loads(request.content))
        return httpx.Response(200, json=payloads[len(requests) - 1])

    def factory():
        return httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                 follow_redirects=False)

    with patch("easy_agent._api._client", factory):
        output = graph.run([Info("audit")])
    return requests, [item.text for item in output]


def retry_timer():
    unlocked = asyncio.Event()
    attempts = []

    class Start(Node):
        async def forward(self, info):
            return Info("start")

    class Flaky(Node):
        async def forward(self, info):
            attempts.append(time.perf_counter())
            if len(attempts) == 1:
                error = _safe_error(NodeExecutionError, "audit transient failure")
                error._easy_agent_retryable = True
                raise error
            unlocked.set()
            return Info("retried")

    class Waiting(Node):
        async def forward(self, info):
            await unlocked.wait()
            return Info("unlocked")

    with setup(max_attempts=2, max_concurrency=2) as graph:
        graph.begin, graph.flaky, graph.waiting = Start(), Flaky(), Waiting(timeout=0.25)
        graph.begin.send_to(graph.flaky)
        graph.begin.send_to(graph.waiting)
        graph.start, graph.end = [graph.begin], [graph.flaky, graph.waiting]
        started = time.perf_counter()
        try:
            with patch.object(Graph, "_backoff_delay", staticmethod(lambda attempt: 0.01)):
                graph.run([Info("audit")])
            status = "succeeded"
        except Exception as error:
            status = type(error).__name__
        return dict(status=status, flaky_attempts=len(attempts),
                    elapsed_ms=round((time.perf_counter() - started) * 1000),
                    scheduled_retry_ms=10, sibling_timeout_ms=250)


def cumulative_budget():
    called = []

    def add(a: int, b: int) -> int:
        called.append([a, b])
        return a + b

    with setup(allowed_tools={"add"}, max_output_tokens=10) as graph:
        graph.llm = Node(tools=[add])
        requests, output = mock_run(graph, [
            tool_call("add", '{"a":1,"b":2}', tokens=7), final(6),
        ])
        return dict(status="succeeded", request_limits=[r["max_tokens"] for r in requests],
                    reported_total_completion_tokens=13, attempt_limit=10,
                    output=output, tool_calls=called)


def finish_reason():
    outcomes = []
    for finish in ("length", "content_filter"):
        called = []

        def add(a: int, b: int) -> int:
            called.append([a, b])
            return a + b

        with setup(allowed_tools={"add"}) as graph:
            graph.llm = Node(tools=[add])
            requests, output = mock_run(graph, [
                tool_call("add", '{"a":1,"b":2}', finish=finish), final(),
            ])
            logged = [e["data"]["finish_reason"] for e in events(graph)
                      if e["event_type"] == "ModelRequestFinished"]
            outcomes.append(dict(actual_finish_reason=finish, tool_calls=called,
                                 requests=len(requests), output=output,
                                 logged_finish_reasons=logged))
    return outcomes


def argument_json():
    seen = []

    def add(a: int, b: int) -> int:
        seen.append([a, b])
        return a + b

    with setup(allowed_tools={"add"}) as graph:
        graph.llm = Node(tools=[add])
        mock_run(graph, [tool_call("add", '{"a":1,"a":9,"b":2}'), final()])
    nan_seen = []

    def inspect_number(value: float) -> str:
        nan_seen.append(math.isnan(value))
        return "checked"

    with setup(allowed_tools={"inspect_number"}) as graph:
        graph.llm = Node(tools=[inspect_number])
        mock_run(graph, [tool_call("inspect_number", '{"value":NaN}'), final()])
    return dict(duplicate_key_actual_arguments=seen,
                nonfinite_value_reached_function=nan_seen)


def parameter_contract():
    records = []

    def number(value: float) -> str:
        records.append("number executed")
        return "ok"

    def optional(value: int | None = None) -> str:
        records.append([type(value).__name__, value])
        return "ok"

    def positional(value: int, /) -> str:
        records.append("positional executed")
        return "ok"

    outcomes = []
    for target, raw in ((number, '{"value":1}'), (optional, '{"value":null}'),
                        (optional, '{"value":true}'), (positional, '{"value":1}')):
        with setup(allowed_tools={target.__name__}) as graph:
            graph.llm = Node(tools=[target])
            graph.validate()
            requests, output = mock_run(graph, [tool_call(target.__name__, raw), final()])
            outcomes.append(dict(tool=target.__name__, arguments=raw,
                                 declared=requests[0]["tools"][0]["function"]["parameters"],
                                 tool_message=requests[1]["messages"][-1]["content"],
                                 output=output))
    return dict(cases=outcomes, executed=records)


def tool_object_schema():
    entered = []

    class Restricted(Tool):
        async def forward(self, info):
            entered.append(type(info.data).__name__)
            return Info("not-an-integer")

    with setup(allowed_tools={"Restricted"}) as graph:
        graph.llm = Node(tools=[Restricted(input_schema=str, output_schema=int)])
        requests, output = mock_run(graph, [
            tool_call("Restricted", '{"value":1}'), final(),
        ])
        result = dict(entered_with=list(entered),
                      tool_message=requests[1]["messages"][-1]["content"],
                      declared=requests[0]["tools"][0]["function"]["parameters"],
                      output=output, graph_node_controls=[])
    for direction, data in (("input", {"value": 1}), ("output", "valid-input")):
        with setup(allowed_tools={"Restricted"}) as graph:
            graph.restricted = Restricted(input_schema=str, output_schema=int)
            try:
                graph.run([Info(data)])
                status = "succeeded"
            except Exception as error:
                status = type(error).__name__
            result["graph_node_controls"].append(dict(direction=direction, status=status))
    return result


def blocking_timeout_metadata():
    entered, release = threading.Event(), threading.Event()

    def slow(value: str) -> str:
        entered.set()
        release.wait(2)
        return value

    with setup(allowed_tools={"slow"}, timeout=0.15) as graph:
        graph.llm = Node(tools=[slow])
        try:
            mock_run(graph, [tool_call("slow", '{"value":"x"}')])
            status = "succeeded"
        except Exception as error:
            status = type(error).__name__
        finally:
            timed = [e["data"] for e in events(graph) if e["event_type"] == "NodeTimedOut"]
            result = dict(status=status, entered=entered.is_set(),
                          retained_cleanup=bool(graph._cleanups),
                          thread_not_released=not release.is_set(), timeout_events=timed)
            release.set()
            deadline = time.perf_counter() + 2
            while graph._cleanups and time.perf_counter() < deadline:
                time.sleep(0.005)
            result["cleanup_released_after_thread_exit"] = not bool(graph._cleanups)
        return result


def runtime_type_hints():
    try:
        get_type_hints(_WorkItem)
        return dict(status="succeeded")
    except Exception as error:
        return dict(status=type(error).__name__, message=str(error))


def internal_tool_steps():
    called = []

    def echo(value: int) -> int:
        called.append(value)
        return value

    payload = tool_call("echo", '{"value":1}')
    calls = payload["choices"][0]["message"]["tool_calls"]
    calls.extend([
        {"id": "audit-call-2", "type": "function",
         "function": {"name": "echo", "arguments": '{"value":2}'}},
        {"id": "audit-call-3", "type": "function",
         "function": {"name": "echo", "arguments": '{"value":3}'}},
    ])
    with setup(allowed_tools={"echo"}) as graph:
        graph.max_steps = 1
        graph.llm = Node(tools=[echo])
        requests, output = mock_run(graph, [payload, final()])
        return dict(max_steps=1, internal_tools_executed=called,
                    model_requests=len(requests), output=output)


if __name__ == "__main__":
    probes = (retry_timer, cumulative_budget, finish_reason, argument_json,
              parameter_contract, tool_object_schema, blocking_timeout_metadata,
              runtime_type_hints, internal_tool_steps)
    result = {"python": sys.version.split()[0], "external_http_requests": 0,
              "probes": {}, "source_sha256": {}}
    for probe in probes:
        result["probes"][probe.__name__] = probe()
        print(probe.__name__ + ": recorded")
    for path in sorted((ROOT / "easy_agent").glob("*.py")):
        result["source_sha256"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    target = Path(__file__).with_name("probe-result.json")
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
                      encoding="utf-8")
    print("Evidence: " + str(target.relative_to(ROOT)))
