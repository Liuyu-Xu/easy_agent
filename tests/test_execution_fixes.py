"""Regression for the three priority fixes: blocking, retry, tool loop."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import httpx

from easy_agent import (
    Graph,
    GraphValidationError,
    Info,
    Node,
    NodeExecutionError,
    NodeTimeoutError,
    Tool,
    UnsupportedFeatureError,
)
from easy_agent.errors import _safe_error

SECRET = "test-execution-secret-never-log"


def response(content="OK", **changes):
    payload = {
        "model": "reported-model",
        "choices": [
            {"message": {"role": "assistant", "content": content},
             "finish_reason": "stop"}
        ],
        "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
    }
    payload.update(changes)
    return payload


def tool_response(name="add", arguments='{"a":1,"b":2}', call_id="call-1"):
    return {
        "model": "reported-model",
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {"id": call_id, "type": "function",
                         "function": {"name": name, "arguments": arguments}}
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 4, "completion_tokens": 3, "total_tokens": 7},
    }


class FixTestCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.logs = self.directory / "logs"

    def graph(self, **kwargs):
        defaults = dict(log_dir=self.logs, timeout=2)
        defaults.update(kwargs)
        return Graph(**defaults)

    def api_graph(self, **kwargs):
        env = self.directory / "api.env"
        if not env.exists():
            env.write_text(f"FIX_TEST_KEY={SECRET}\n", encoding="utf-8")
        defaults = dict(
            env_file=env, model="requested-model", provider="deepseek",
            api_key_env="FIX_TEST_KEY", max_output_tokens=32, timeout=2,
            execution="async", log_dir=self.logs,
        )
        defaults.update(kwargs)
        return Graph(**defaults)

    @contextmanager
    def transport(self, handler):
        def factory():
            return httpx.AsyncClient(
                transport=httpx.MockTransport(handler), follow_redirects=False
            )

        with patch("easy_agent._api._client", factory):
            yield

    def events(self, graph, run_id=None):
        return graph._event_store.query(
            run_id=run_id or graph._last_run_id, limit=2000
        )


class Start(Node):
    def forward(self, info):
        return Info("start")


def retryable_error(message="transient"):
    error = _safe_error(NodeExecutionError, message)
    error._easy_agent_retryable = True
    return error


# --------------------------------------------------------------------- PF-1
class BlockingExecutionTests(FixTestCase):
    def test_sync_node_runs_off_the_event_loop(self):
        observed = {}

        class SlowSync(Node):
            def forward(self, info):
                time.sleep(0.2)
                return Info("slow")

        class Timed(Node):
            async def forward(self, info):
                started = time.perf_counter()
                await asyncio.sleep(0.05)
                observed["elapsed"] = time.perf_counter() - started
                return Info("fast")

        graph = self.graph(execution="async", max_concurrency=2)
        graph.begin, graph.slow, graph.fast = Start(), SlowSync(), Timed()
        graph.begin.send_to(graph.slow)
        graph.begin.send_to(graph.fast)
        graph.start = [graph.begin]
        graph.end = [graph.slow, graph.fast]
        outputs = graph.run([Info("x")])
        self.assertEqual([item.text for item in outputs], ["slow", "fast"])
        self.assertLess(observed["elapsed"], 0.15)

    def test_sibling_deadline_is_not_charged_to_a_blocked_loop(self):
        class SlowSync(Node):
            def forward(self, info):
                time.sleep(0.3)
                return Info("slow")

        class Timed(Node):
            async def forward(self, info):
                await asyncio.sleep(0.05)
                return Info("fast")

        slow, fast = SlowSync(), Timed()
        slow.timeout, fast.timeout = 5, 0.1
        graph = self.graph(execution="async", max_concurrency=2)
        graph.begin, graph.slow, graph.fast = Start(), slow, fast
        graph.begin.send_to(graph.slow)
        graph.begin.send_to(graph.fast)
        graph.start = [graph.begin]
        graph.end = [graph.slow, graph.fast]
        outputs = graph.run([Info("x")])
        self.assertEqual([item.text for item in outputs], ["slow", "fast"])

    def test_abandoned_blocking_thread_keeps_definition_lease(self):
        entered, release = threading.Event(), threading.Event()

        class Stuck(Node):
            def forward(self, info):
                entered.set()
                release.wait(5)
                return Info("late")

        graph = self.graph(timeout=0.05)
        graph.node = Stuck()
        with self.assertRaises(NodeTimeoutError):
            graph.run([Info("x")])
        self.assertTrue(entered.is_set())
        self.assertTrue(graph._cleanups)
        with self.assertRaises(GraphValidationError):
            graph.node.prompt = "changed"
        release.set()
        deadline = time.time() + 5
        while graph._cleanups and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(graph._cleanups, {})
        graph.node.prompt = "changed"

    def test_event_loop_stall_is_recorded(self):
        class BlockingAsync(Node):
            async def forward(self, info):
                await asyncio.sleep(0)
                time.sleep(0.2)
                return Info("late")

        graph = self.graph(timeout=5)
        graph.node = BlockingAsync()
        result = graph.run([Info("x")])[0]
        blocked = [
            event for event in self.events(graph, result.run_id)
            if event["event_type"] == "EventLoopBlocked"
        ]
        self.assertTrue(blocked)
        self.assertGreaterEqual(blocked[0]["data"]["stall_ms"], 100)

    def test_timed_out_sync_attempt_records_enforcement(self):
        entered, release = threading.Event(), threading.Event()

        class Stuck(Node):
            def forward(self, info):
                entered.set()
                release.wait(5)
                return Info("late")

        graph = self.graph(timeout=0.3)
        graph.node = Stuck()
        try:
            with self.assertRaises(NodeTimeoutError) as raised:
                graph.run([Info("x")])
            self.assertTrue(entered.is_set())
            timed_out = graph.node.logs(
                run_id=raised.exception.run_id, event_type="NodeTimedOut"
            )
            self.assertEqual(len(timed_out), 1)
            self.assertFalse(timed_out[0]["data"]["deadline_enforced"])
            self.assertTrue(timed_out[0]["data"]["blocking_thread_still_running"])
        finally:
            release.set()

    def test_async_timeout_still_cancels_without_a_thread(self):
        class Slow(Node):
            async def forward(self, info):
                await asyncio.sleep(5)
                return Info("late")

        graph = self.graph(timeout=0.05)
        graph.node = Slow()
        with self.assertRaises(NodeTimeoutError) as raised:
            graph.run([Info("x")])
        timed_out = graph.node.logs(
            run_id=raised.exception.run_id, event_type="NodeTimedOut"
        )
        self.assertEqual(len(timed_out), 1)
        self.assertTrue(timed_out[0]["data"]["deadline_enforced"])
        self.assertFalse(timed_out[0]["data"]["blocking_thread_still_running"])


# --------------------------------------------------------------------- PF-2
class RetryTests(FixTestCase):
    def test_retryable_status_is_retried_until_success(self):
        calls = []

        async def handler(request):
            calls.append(1)
            if len(calls) == 1:
                return httpx.Response(500, text=SECRET)
            return httpx.Response(200, json=response("OK"))

        graph = self.api_graph()
        graph.llm = Node()
        with patch.object(Graph, "_backoff_delay", staticmethod(lambda attempt: 0.0)):
            with self.transport(handler):
                result = graph.run([Info("x")])[0]
        self.assertEqual(result.text, "OK")
        self.assertEqual(len(calls), 2)
        attempts = graph.llm.logs(
            run_id=result.run_id, event_type="NodeAttemptStarted"
        )
        self.assertEqual([event["attempt"] for event in attempts], [1, 2])

    def test_non_retryable_status_is_not_retried(self):
        calls = []

        async def handler(request):
            calls.append(1)
            return httpx.Response(401, text=SECRET)

        graph = self.api_graph()
        graph.llm = Node()
        with self.assertRaises(NodeExecutionError) as raised:
            with self.transport(handler):
                graph.run([Info("x")])
        self.assertEqual(len(calls), 1)
        self.assertIn("401", str(raised.exception))
        self.assertNotIn(SECRET, str(raised.exception))

    def test_framework_deadline_stops_without_retrying(self):
        calls = []

        async def handler(request):
            calls.append(1)
            await asyncio.sleep(0.3)
            return httpx.Response(200, json=response())

        graph = self.api_graph(timeout=0.05)
        graph.llm = Node()
        with self.assertRaises(NodeTimeoutError):
            with self.transport(handler):
                graph.run([Info("x")])
        self.assertEqual(len(calls), 1)

    def test_max_attempts_one_disables_retry(self):
        calls = []

        async def handler(request):
            calls.append(1)
            return httpx.Response(500, text=SECRET)

        graph = self.api_graph(max_attempts=1)
        graph.llm = Node()
        with self.assertRaises(NodeExecutionError):
            with self.transport(handler):
                graph.run([Info("x")])
        self.assertEqual(len(calls), 1)

    def test_backoff_releases_the_concurrency_slot(self):
        order = []

        class Flaky(Node):
            def forward(self, info):
                order.append("A")
                if order.count("A") == 1:
                    raise retryable_error()
                return Info("A")

        class Other(Node):
            def forward(self, info):
                order.append("B")
                return Info("B")

        graph = self.graph(execution="async", max_concurrency=1, max_attempts=2)
        graph.begin = Start()
        graph.a, graph.b = Flaky(), Other()
        graph.begin.send_to(graph.a)
        graph.begin.send_to(graph.b)
        graph.start = [graph.begin]
        graph.end = [graph.a, graph.b]
        with patch.object(Graph, "_backoff_delay", staticmethod(lambda attempt: 0.1)):
            outputs = graph.run([Info("x")])
        self.assertEqual([item.text for item in outputs], ["A", "B"])
        self.assertEqual(order, ["A", "B", "A"])

    def test_non_retryable_custom_failure_still_has_one_attempt(self):
        calls = []

        class Boom(Node):
            def forward(self, info):
                calls.append(1)
                raise ConnectionError("unclassified")

        graph = self.graph(max_attempts=3)
        graph.node = Boom()
        with self.assertRaises(NodeExecutionError) as raised:
            graph.run([Info("x")])
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            len(graph.node.logs(
                run_id=raised.exception.run_id, event_type="NodeAttemptStarted")),
            1,
        )


# --------------------------------------------------------------------- PF-3
class ToolLoopTests(FixTestCase):
    def test_model_calls_a_function_tool(self):
        calls = []
        requests = []

        def add(a: int, b: int) -> int:
            calls.append((a, b))
            return a + b

        payloads = [tool_response(), response("5")]

        async def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json=payloads.pop(0))

        graph = self.api_graph(allowed_tools={"add"})
        graph.llm = Node(tools=[add])
        with self.transport(handler):
            result = graph.run([Info("x")])[0]
        self.assertEqual(result.text, "5")
        self.assertEqual(calls, [(1, 2)])
        second = requests[1]["messages"]
        self.assertEqual(second[-1]["role"], "tool")
        self.assertEqual(second[-1]["tool_call_id"], "call-1")
        self.assertEqual(second[-1]["content"], "3")
        self.assertEqual(requests[1]["max_tokens"], 32 - 3)

    def test_unknown_tool_name_is_returned_to_the_model(self):
        requests = []
        payloads = [tool_response(name="missing"), response("done")]

        async def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json=payloads.pop(0))

        graph = self.api_graph(allowed_tools={"add"})

        def add(a: int, b: int) -> int:
            return a + b

        graph.llm = Node(tools=[add])
        with self.transport(handler):
            result = graph.run([Info("x")])[0]
        self.assertEqual(result.text, "done")
        self.assertEqual(
            json.loads(requests[1]["messages"][-1]["content"]),
            {"error": "unknown_tool"},
        )

    def test_invalid_arguments_are_returned_to_the_model(self):
        requests = []
        payloads = [tool_response(arguments='{"a":"1","b":2}'), response("done")]

        async def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json=payloads.pop(0))

        graph = self.api_graph(allowed_tools={"add"})

        def add(a: int, b: int) -> int:
            return a + b

        graph.llm = Node(tools=[add])
        with self.transport(handler):
            result = graph.run([Info("x")])[0]
        self.assertEqual(result.text, "done")
        self.assertEqual(
            json.loads(requests[1]["messages"][-1]["content"]),
            {"error": "invalid_arguments"},
        )

    def test_tool_exception_is_returned_without_leaking_text(self):
        requests = []
        payloads = [tool_response(name="explode"), response("done")]

        async def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json=payloads.pop(0))

        def explode(a: int, b: int) -> int:
            raise ValueError(SECRET)

        graph = self.api_graph(allowed_tools={"explode"})
        graph.llm = Node(tools=[explode])
        with self.transport(handler):
            result = graph.run([Info("x")])[0]
        self.assertEqual(result.text, "done")
        content = requests[1]["messages"][-1]["content"]
        self.assertEqual(json.loads(content), {"error": "tool_error"})
        self.assertNotIn(SECRET, content)

    def test_tool_round_without_usage_stops_the_attempt(self):
        payload = tool_response()
        del payload["usage"]

        async def handler(request):
            return httpx.Response(200, json=payload)

        def add(a: int, b: int) -> int:
            return a + b

        graph = self.api_graph(allowed_tools={"add"})
        graph.llm = Node(tools=[add])
        with self.assertRaises(NodeExecutionError) as raised:
            with self.transport(handler):
                graph.run([Info("x")])
        self.assertIn("用量", str(raised.exception))

    def test_side_effect_tool_is_still_rejected(self):
        class Writer(Tool):
            side_effect = True

        graph = self.api_graph(allowed_tools={"Writer"})
        graph.llm = Node(tools=[Writer()])
        with self.assertRaises(UnsupportedFeatureError):
            graph.validate()

    def test_tool_call_is_not_a_graph_node(self):
        payloads = [tool_response(), response("5")]

        async def handler(request):
            return httpx.Response(200, json=payloads.pop(0))

        def add(a: int, b: int) -> int:
            return a + b

        graph = self.api_graph(allowed_tools={"add"})
        graph.llm = Node(tools=[add])
        with self.transport(handler):
            result = graph.run([Info("x")])[0]
        events = self.events(graph, result.run_id)
        tools = [
            event for event in events
            if event["event_type"] in {"ToolCallStarted", "ToolCallFinished"}
        ]
        self.assertEqual(len(tools), 2)
        self.assertTrue(all(event["node_id"] == graph.llm.id for event in tools))
        self.assertEqual(len(graph._nodes), 1)


    def test_blocking_function_tool_keeps_the_definition_lease(self):
        entered, release = threading.Event(), threading.Event()

        def slow_tool(value: str) -> str:
            entered.set()
            release.wait(5)
            return value

        payloads = [
            tool_response(name="slow_tool", arguments='{"value":"x"}'),
            response("done"),
        ]

        async def handler(request):
            return httpx.Response(200, json=payloads.pop(0))

        graph = self.api_graph(allowed_tools={"slow_tool"}, timeout=0.3)
        graph.llm = Node(tools=[slow_tool])
        try:
            with self.assertRaises(NodeTimeoutError):
                with self.transport(handler):
                    graph.run([Info("x")])
            self.assertTrue(entered.wait(2))
            self.assertTrue(graph._cleanups)
            with self.assertRaises(GraphValidationError):
                graph.llm.prompt = "early"
        finally:
            release.set()


class ToolDeclarationTests(FixTestCase):
    def test_tools_are_declared_to_the_provider(self):
        requests = []
        payloads = [tool_response(), response("5")]

        def add(a: int, b: int) -> int:
            """Add two integers."""
            return a + b

        async def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json=payloads.pop(0))

        graph = self.api_graph(allowed_tools={"add"})
        graph.llm = Node(tools=[add])
        with self.transport(handler):
            graph.run([Info("x")])
        declared = requests[0]["tools"]
        self.assertEqual(len(declared), 1)
        function = declared[0]["function"]
        self.assertEqual(function["name"], "add")
        self.assertEqual(function["description"], "Add two integers.")
        self.assertEqual(
            function["parameters"],
            {
                "type": "object",
                "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                "required": ["a", "b"],
                "additionalProperties": False,
            },
        )

    def test_optional_parameter_is_declared_and_not_required(self):
        requests = []
        payloads = [response("ok")]

        def greet(name: str, times: int | None = None) -> str:
            return name * (times or 1)

        async def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json=payloads.pop(0))

        graph = self.api_graph(allowed_tools={"greet"})
        graph.llm = Node(tools=[greet])
        with self.transport(handler):
            graph.run([Info("x")])
        parameters = requests[0]["tools"][0]["function"]["parameters"]
        self.assertEqual(parameters["properties"]["times"], {"type": "integer"})
        self.assertEqual(parameters["required"], ["name"])

    def test_unsupported_tool_annotation_is_rejected_before_running(self):
        from datetime import datetime

        def stamp(when: datetime) -> str:
            return when.isoformat()

        graph = self.api_graph(allowed_tools={"stamp"})
        graph.llm = Node(tools=[stamp])
        with self.assertRaises(UnsupportedFeatureError):
            graph.validate()


class LegacyToolTests(FixTestCase):
    def test_legacy_tool_does_not_block_the_event_loop(self):
        marks = {}

        class Start(Node):
            async def forward(self, info):
                marks["t0"] = time.perf_counter()
                return Info("go")

        class Legacy(Tool):
            def invoke(self, info):
                time.sleep(0.2)
                return info

        class Timed(Node):
            async def forward(self, info):
                await asyncio.sleep(0.05)
                marks["done"] = time.perf_counter() - marks["t0"]
                return Info("B")

        graph = self.graph(execution="async", max_concurrency=2,
                           allowed_tools={"Legacy"})
        graph.begin, graph.legacy, graph.timed = Start(), Legacy(), Timed()
        graph.begin.send_to(graph.legacy)
        graph.begin.send_to(graph.timed)
        graph.start = [graph.begin]
        graph.end = [graph.legacy, graph.timed]
        graph.run([Info("x")])
        self.assertLess(marks["done"], 0.15)


if __name__ == "__main__":
    unittest.main()
