"""Regression for the 2026-10-19 review: scheduling and tool boundaries."""

from __future__ import annotations

import asyncio
from dataclasses import fields
import json
import threading
import time
from typing import get_type_hints
from unittest.mock import patch

import httpx

from easy_agent import Info, Node, NodeExecutionError, NodeTimeoutError, Tool, UnsupportedFeatureError
from easy_agent import _runtime
from test_execution_fixes import FixTestCase, Start, response, retryable_error, tool_response


class ReviewRegressionTests(FixTestCase):
    def model_run(self, tool, payloads, **options):
        requests = []

        async def handler(request):
            requests.append(json.loads(request.content))
            return httpx.Response(200, json=payloads[len(requests) - 1])

        name = tool.tool_name if isinstance(tool, Tool) else tool.__name__
        graph = self.api_graph(allowed_tools={name}, **options)
        graph.llm = Node(tools=[tool])
        with self.transport(handler):
            outputs = graph.run([Info("audit")])
        return graph, requests, outputs

    def test_due_retry_runs_while_sibling_is_waiting(self):
        unlocked = asyncio.Event()
        attempts = []

        class Flaky(Node):
            async def forward(self, info):
                attempts.append(1)
                if len(attempts) == 1:
                    raise retryable_error()
                unlocked.set()
                return Info("retried")

        class Waiting(Node):
            async def forward(self, info):
                await unlocked.wait()
                return Info("unlocked")

        graph = self.graph(execution="async", max_concurrency=2, max_attempts=2)
        graph.begin, graph.a, graph.b = Start(), Flaky(), Waiting(timeout=1)
        graph.begin.send_to(graph.a, graph.b)
        graph.start, graph.end = [graph.begin], [graph.a, graph.b]
        with patch.object(type(graph), "_backoff_delay", staticmethod(lambda attempt: 0.01)):
            outputs = graph.run([Info("audit")])
        self.assertEqual([item.text for item in outputs], ["retried", "unlocked"])
        self.assertEqual(len(attempts), 2)

    def test_retry_timer_wait_is_cancelled_cleanly(self):
        async def scenario():
            started = asyncio.Event()

            class Flaky(Node):
                async def forward(self, info):
                    raise retryable_error()

            class Waiting(Node):
                async def forward(self, info):
                    started.set()
                    await asyncio.Event().wait()

            graph = self.graph(execution="async", max_concurrency=2, max_attempts=2)
            graph.begin, graph.a, graph.b = Start(), Flaky(), Waiting()
            graph.begin.send_to(graph.a, graph.b)
            graph.start, graph.end = [graph.begin], [graph.a, graph.b]
            with patch.object(type(graph), "_backoff_delay", staticmethod(lambda attempt: 10)):
                task = asyncio.create_task(graph.arun([Info("audit")]))
                await started.wait()
                await asyncio.sleep(0.01)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            await asyncio.sleep(0)
            self.assertEqual(graph._active_runs, 0)
            self.assertFalse(graph._cleanups)
            self.assertEqual([t for t in asyncio.all_tasks() if t is not asyncio.current_task()], [])

        asyncio.run(scenario())

    def test_cumulative_budget_rejects_overspend_before_tools_or_publish(self):
        for continuation in (False, True):
            with self.subTest(continuation=continuation):
                called, requests = [], []

                def add(a: int, b: int) -> int:
                    called.append((a, b))
                    return a + b

                first = tool_response()
                first["usage"]["completion_tokens"] = 7
                second = tool_response(call_id="second") if continuation else response("done")
                second["usage"]["completion_tokens"] = 6
                payloads = [first, second]

                async def handler(request):
                    requests.append(json.loads(request.content))
                    return httpx.Response(200, json=payloads[len(requests) - 1])

                graph = self.api_graph(allowed_tools={"add"}, max_output_tokens=10, max_attempts=1)
                graph.llm = Node(tools=[add])
                with self.transport(handler), self.assertRaises(NodeExecutionError):
                    graph.run([Info("audit")])
                self.assertEqual([r["max_tokens"] for r in requests], [10, 3])
                self.assertEqual(called, [(1, 2)])
                self.assertFalse(any(e["event_type"] == "GraphRunSucceeded" for e in self.events(graph)))

    def test_exact_budget_final_is_allowed_but_no_budget_continuation_is_not(self):
        for continuation in (False, True):
            with self.subTest(continuation=continuation):
                called = []

                def add(a: int, b: int) -> int:
                    called.append((a, b))
                    return a + b

                first = tool_response()
                first["usage"]["completion_tokens"] = 7
                second = tool_response() if continuation else response("done")
                second["usage"]["completion_tokens"] = 3
                if continuation:
                    with self.assertRaises(NodeExecutionError):
                        self.model_run(add, [first, second], max_output_tokens=10, max_attempts=1)
                else:
                    _, requests, outputs = self.model_run(add, [first, second], max_output_tokens=10)
                    self.assertEqual(outputs[0].text, "done")
                    self.assertEqual(len(requests), 2)
                self.assertEqual(called, [(1, 2)])

    def test_aborted_or_inconsistent_tool_responses_never_execute(self):
        for finish in ("length", "content_filter", "stop", None, "unknown"):
            with self.subTest(finish=finish):
                called = []

                def add(a: int, b: int) -> int:
                    called.append((a, b))
                    return a + b

                payload = tool_response()
                payload["choices"][0]["finish_reason"] = finish
                with self.assertRaises(NodeExecutionError):
                    self.model_run(add, [payload, response("done")], max_attempts=1)
                self.assertEqual(called, [])

    def test_tool_arguments_reject_duplicate_keys_and_nonfinite_nested_values(self):
        for raw in ('{"value":1,"value":2}', '{"value":NaN}', '{"value":Infinity}',
                    '{"value":1e9999}', '{"value":{"x":1,"x":2}}',
                    '{"value":[-Infinity]}', '{"value":{"x":NaN}}',
                    '{"value":{"x":[1e9999]}}'):
            with self.subTest(arguments=raw):
                called = []

                def inspect_value(value: dict | None = None) -> str:
                    called.append(value)
                    return "bad"

                _, requests, outputs = self.model_run(inspect_value, [
                    tool_response(name="inspect_value", arguments=raw), response("done"),
                ])
                self.assertEqual(called, [])
                self.assertEqual(json.loads(requests[1]["messages"][-1]["content"]),
                                 {"error": "invalid_arguments"})
                self.assertEqual(outputs[0].text, "done")

    def test_nonfinite_float_never_reaches_a_number_tool(self):
        called = []

        def inspect_number(value: float) -> str:
            called.append(value)
            return "bad"

        for literal in ("NaN", "Infinity", "-Infinity", "1e9999"):
            with self.subTest(literal=literal):
                _, requests, _ = self.model_run(inspect_number, [
                    tool_response(name="inspect_number", arguments='{"value":' + literal + '}'),
                    response("done"),
                ])
                self.assertEqual(called, [])
                self.assertEqual(json.loads(requests[1]["messages"][-1]["content"]),
                                 {"error": "invalid_arguments"})

    def test_number_and_nullable_contract_matches_declaration(self):
        seen = []

        def number(value: float) -> str:
            seen.append(value)
            return "ok"

        def optional(value: int | None = None) -> str:
            seen.append(value)
            return "ok"

        for tool, raw, expected, value in (
            (number, '{"value":1}', {"type": "number"}, 1),
            (number, '{"value":1.5}', {"type": "number"}, 1.5),
            (optional, '{"value":null}', {"type": ["integer", "null"]}, None),
            (optional, '{"value":2}', {"type": ["integer", "null"]}, 2),
        ):
            with self.subTest(tool=tool.__name__, raw=raw):
                _, requests, _ = self.model_run(tool, [
                    tool_response(name=tool.__name__, arguments=raw), response("done"),
                ])
                schema = requests[0]["tools"][0]["function"]["parameters"]
                self.assertEqual(schema["properties"]["value"], expected)
                self.assertEqual(seen[-1], value)

    def test_bool_does_not_satisfy_number_or_optional_integer(self):
        def number(value: float) -> str:
            self.fail("bool must not reach number")

        def optional(value: int | None = None) -> str:
            self.fail("bool must not reach optional integer")

        for tool in (number, optional):
            with self.subTest(tool=tool.__name__):
                _, requests, _ = self.model_run(tool, [
                    tool_response(name=tool.__name__, arguments='{"value":true}'), response("done"),
                ])
                self.assertEqual(json.loads(requests[1]["messages"][-1]["content"]),
                                 {"error": "invalid_arguments"})

    def test_positional_only_tool_is_rejected_before_http(self):
        def positional(value: int, /) -> int:
            return value

        graph = self.api_graph(allowed_tools={"positional"})
        graph.llm = Node(tools=[positional])
        with self.assertRaises(UnsupportedFeatureError):
            graph.validate()

    def test_runtime_uses_compiled_annotations(self):
        seen = []

        def echo(value: int) -> str:
            seen.append(value)
            return "ok"

        payloads = [tool_response(name="echo", arguments='{"value":true}'), response("done")]
        requests = []

        async def handler(request):
            requests.append(json.loads(request.content))
            if len(requests) == 1:
                echo.__annotations__["value"] = "unresolvable_runtime_annotation"
            return httpx.Response(200, json=payloads[len(requests) - 1])

        graph = self.api_graph(allowed_tools={"echo"})
        graph.llm = Node(tools=[echo])
        with self.transport(handler):
            graph.run([Info("audit")])
        self.assertEqual(seen, [])
        self.assertEqual(json.loads(requests[1]["messages"][-1]["content"]),
                         {"error": "invalid_arguments"})

    def test_internal_tool_rejects_incompatible_input_schema_before_http(self):
        class Restricted(Tool):
            async def forward(self, info):
                self.fail("invalid schema must not execute")

        graph = self.api_graph(allowed_tools={"Restricted"})
        graph.llm = Node(tools=[Restricted(input_schema=str)])
        with self.assertRaises(UnsupportedFeatureError):
            graph.validate()

    def test_internal_tool_validates_output_schema_and_info_return_type(self):
        for invalid in (Info("wrong-type"), 3):
            with self.subTest(invalid=type(invalid).__name__):
                class Restricted(Tool):
                    async def forward(self, info):
                        return invalid

                _, requests, _ = self.model_run(
                    Restricted(input_schema=dict, output_schema=int),
                    [tool_response(name="Restricted", arguments='{"value":1}'), response("done")],
                )
                self.assertEqual(json.loads(requests[1]["messages"][-1]["content"]),
                                 {"error": "tool_error"})

    def test_internal_tool_valid_schema_keeps_info_boundary(self):
        class Count(Tool):
            def forward(self, info):
                return Info(len(info.data))

        _, requests, _ = self.model_run(Count(input_schema=dict, output_schema=int), [
            tool_response(name="Count", arguments='{"a":1}'), response("done"),
        ])
        self.assertEqual(requests[1]["messages"][-1]["content"], "1")

    def test_internal_tool_model_schema_is_declared_and_validated_before_business(self):
        entered = []

        class ObjectSchema:
            @classmethod
            def model_json_schema(cls):
                return {"type": "object", "properties": {"value": {"type": "integer"}},
                        "required": ["value"], "additionalProperties": False}

            @classmethod
            def model_validate(cls, data, *, strict):
                if type(data.get("value")) is not int:
                    raise ValueError()
                instance = cls()
                instance.data = data
                return instance

            def model_dump(self, *, mode):
                return self.data

        class Restricted(Tool):
            async def forward(self, info):
                entered.append(info.data["value"])
                return Info(info.data["value"])

        for value, expected in ((1, "1"), ("invalid", '{"error":"invalid_arguments"}')):
            with self.subTest(value=value):
                _, requests, _ = self.model_run(
                    Restricted(input_schema=ObjectSchema, output_schema=int),
                    [tool_response(name="Restricted", arguments=json.dumps({"value": value})),
                     response("done")],
                )
                self.assertEqual(requests[0]["tools"][0]["function"]["parameters"],
                                 ObjectSchema.model_json_schema())
                self.assertEqual(requests[1]["messages"][-1]["content"], expected)
        self.assertEqual(entered, [1])

    def test_internal_blocking_timeout_reports_actual_thread_and_keeps_lease(self):
        entered, release = threading.Event(), threading.Event()

        def slow(value: str) -> str:
            entered.set()
            release.wait(3)
            return value

        graph = self.api_graph(allowed_tools={"slow"}, timeout=0.2)
        graph.llm = Node(tools=[slow])

        async def handler(request):
            return httpx.Response(200, json=tool_response(name="slow", arguments='{"value":"x"}'))

        try:
            with self.transport(handler), self.assertRaises(NodeTimeoutError):
                graph.run([Info("audit")])
            self.assertTrue(entered.is_set())
            self.assertTrue(graph._cleanups)
            timed = [e for e in self.events(graph) if e["event_type"] == "NodeTimedOut"]
            self.assertFalse(timed[0]["data"]["deadline_enforced"])
            self.assertTrue(timed[0]["data"]["blocking_thread_still_running"])
        finally:
            release.set()
            deadline = time.perf_counter() + 2
            while graph._cleanups and time.perf_counter() < deadline:
                time.sleep(0.005)
        self.assertFalse(graph._cleanups)

    def test_runtime_dataclass_annotations_resolve(self):
        for name in ("_WorkItem", "_Retry", "_Outcome", "_RunState"):
            with self.subTest(name=name):
                cls = getattr(_runtime, name)
                self.assertEqual(set(get_type_hints(cls)), {f.name for f in fields(cls)})
