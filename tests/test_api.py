from __future__ import annotations

import asyncio
from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from easy_agent import (
    ConfigurationError, Graph, Info, LoggingError, Node, NodeExecutionError,
    NodeTimeoutError, Tool, UnsupportedFeatureError,
)
from easy_agent._api import endpoint_url


SECRET = "test-api-secret-never-log"


def response(content="OK", **changes):
    payload = {
        "model": "reported-model",
        "choices": [{"message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
    }
    payload.update(changes)
    return payload


class ApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.env = self.directory / "api.env"
        self.env.write_text(f"TEST_MODEL_KEY={SECRET}\n", encoding="utf-8")
        self.env_patch = patch.dict(os.environ, {"TEST_MODEL_KEY": SECRET})
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def graph(self, **kwargs):
        defaults = dict(
            env_file=self.env, model="requested-model", provider="deepseek",
            api_key_env="TEST_MODEL_KEY", max_output_tokens=32, timeout=1,
            execution="async", log_dir=self.directory / "logs",
        )
        defaults.update(kwargs)
        return Graph(**defaults)

    @contextmanager
    def transport(self, handler):
        clients = []

        def factory():
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False)
            clients.append(client)
            return client

        with patch("easy_agent._api._client", factory):
            yield clients

    def events(self, graph):
        return graph._event_store.query(run_id=graph._last_run_id, limit=1000)

    async def test_request_mapping_identity_provenance_and_secret_protection(self):
        requests = []

        async def handler(request):
            requests.append(request)
            return httpx.Response(200, json=response())

        graph = self.graph(base_url="https://api.deepseek.com/v1/")
        graph.llm = Node(role="Business role", prompt="Instruction", output_schema=str)
        initial = Info("Input")
        with self.transport(handler) as clients:
            result = (await graph.arun([initial]))[0]
        self.assertEqual(str(requests[0].url), "https://api.deepseek.com/v1/chat/completions")
        self.assertEqual(requests[0].headers["authorization"], "Bearer " + SECRET)
        body = json.loads(requests[0].content)
        self.assertEqual(body, {
            "model": "requested-model",
            "messages": [
                {"role": "system", "content": "Business role\n\nInstruction"},
                {"role": "user", "content": "Input"},
            ],
            "temperature": 0, "max_tokens": 32,
            "stream": False, "thinking": {"type": "disabled"},
        })
        self.assertEqual(result.text, "OK")
        self.assertEqual(result.origin_kind, "graph_output")
        self.assertTrue(all(client.is_closed for client in clients))
        events = self.events(graph)
        metadata = next(e["data"] for e in events if e["event_type"] == "ModelRequestFinished")
        self.assertEqual(metadata["requested_model"], "requested-model")
        self.assertEqual(metadata["reported_model"], "reported-model")
        self.assertEqual(metadata["usage"]["completion_tokens"], 3)
        self.assertNotIn(SECRET, json.dumps({"events": events, "export": graph.to_dict()}))
        self.assertEqual(graph._active_runs, 0)

    async def test_json_input_is_deterministic_user_text(self):
        async def handler(request):
            body = json.loads(request.content)
            self.assertEqual(body["messages"], [{"role": "user", "content": '{"a":3,"b":4}'}])
            return httpx.Response(200, json=response("7"))

        graph = self.graph()
        graph.llm = Node()
        with self.transport(handler):
            result = (await graph.arun([Info({"b": 4, "a": 3})]))[0]
        self.assertEqual(result.text, "7")

    async def test_missing_key_fails_before_client_creation_but_validation_is_available(self):
        graph = self.graph(api_key_env="TEST_EMPTY_MODEL_KEY")
        graph.llm = Node()
        with patch.dict(os.environ, {"TEST_EMPTY_MODEL_KEY": ""}):
            graph.validate()
            with patch("easy_agent._api._client") as factory:
                with self.assertRaises(ConfigurationError):
                    await graph.arun([Info("x")])
                factory.assert_not_called()
        self.assertEqual(graph._active_runs, 0)

    async def test_unsupported_builtin_capabilities_fail_before_network(self):
        # Internal tools are supported since the 2026-10-17 execution fixes;
        # structured output is still an explicit limitation.
        graph = self.graph()
        graph.llm = Node(output_schema=dict)
        with patch("easy_agent._api._client") as factory:
            with self.assertRaises(UnsupportedFeatureError):
                await graph.arun([Info("x")])
            factory.assert_not_called()

    async def test_provider_and_invalid_config_are_rejected(self):
        for options in ({"provider": "other"}, {"base_url": "http://remote.example"},
                        {"temperature": 3}):
            with self.subTest(options=list(options)):
                graph = self.graph(**options)
                graph.llm = Node()
                with patch("easy_agent._api._client") as factory:
                    with self.assertRaises((ConfigurationError, UnsupportedFeatureError)):
                        await graph.arun([Info("x")])
                    factory.assert_not_called()

    async def test_graph_and_adapter_share_the_attempt_deadline(self):
        for duration in (0.001, 0.02):
            with self.subTest(duration=duration):
                clock = [100.0]

                def handler(request):
                    clock[0] += duration
                    return httpx.Response(200, json=response())

                graph = self.graph(timeout=0.01)
                graph.llm = Node()
                with self.transport(handler), \
                     patch('easy_agent.graph.perf_counter', side_effect=lambda: clock[0]), \
                     patch('easy_agent._api.perf_counter', side_effect=lambda: clock[0]):
                    if duration < 0.01:
                        self.assertEqual(((await graph.arun([Info('input')]))[0]).text, 'OK')
                    else:
                        with self.assertRaises(NodeTimeoutError):
                            await graph.arun([Info('input')])
                        self.assertFalse(any(event['event_type'] == 'InfoPublished'
                                             for event in self.events(graph)))
                self.assertEqual(graph._active_runs, 0)

    async def test_invalid_prompt_and_role_fail_at_declaration(self):
        for attributes in ({"prompt": 123}, {"role": {"label": "invalid"}}):
            with patch("easy_agent._api._client") as factory:
                with self.assertRaises(TypeError):
                    Node(**attributes)
                factory.assert_not_called()

    async def test_http_failures_and_redirects_are_sanitized_and_not_retried(self):
        for status in (401, 429, 500, 302):
            calls = []

            async def handler(request):
                calls.append(request)
                return httpx.Response(
                    status, text=SECRET,
                    headers={"Location": "https://different.example/chat/completions"},
                )

            with self.subTest(status=status):
                # max_attempts=1 isolates sanitisation from the retry policy;
                # retryable statuses are covered in test_execution_fixes.py.
                graph = self.graph(max_attempts=1)
                graph.llm = Node()
                with self.transport(handler) as clients:
                    with self.assertRaises(NodeExecutionError) as raised:
                        await graph.arun([Info("x")])
                self.assertIn(str(status), str(raised.exception))
                self.assertNotIn(SECRET, str(raised.exception))
                self.assertEqual(len(calls), 1)
                self.assertTrue(all(c.is_closed for c in clients))
                self.assertFalse(any(e["event_type"] == "NodeSucceeded" for e in self.events(graph)))

    async def test_transport_error_and_stage_timeout_keep_safe_error_identity(self):
        for kind in (httpx.ConnectError, httpx.ReadTimeout):
            async def handler(request):
                raise kind(SECRET, request=request)

            with self.subTest(kind=kind.__name__):
                graph = self.graph(max_attempts=1)
                graph.llm = Node()
                with self.transport(handler):
                    with self.assertRaises(NodeExecutionError) as raised:
                        await graph.arun([Info("x")])
                self.assertNotIsInstance(raised.exception, NodeTimeoutError)
                self.assertNotIn(SECRET, str(raised.exception))
                self.assertTrue(raised.exception.__suppress_context__)

    async def test_malformed_response_and_incomplete_content_never_publish(self):
        payloads = [
            [], {"choices": []}, {"choices": [{"message": None}]},
            response(choices=[{"message": {"content": None}, "finish_reason": "stop"}]),
            response(choices=[{"message": {"content": "partial"}, "finish_reason": "length"}]),
            response(choices=[{"message": {"content": "partial"}, "finish_reason": "aborted"}]),
            response(choices=[{"message": {"content": None, "tool_calls": [{"id": "call"}]},
                               "finish_reason": "tool_calls"}]),
            response(usage={"completion_tokens": 33}),
        ]
        for payload in payloads:
            async def handler(request):
                return httpx.Response(200, json=payload)

            with self.subTest(payload_type=type(payload).__name__):
                graph = self.graph()
                graph.llm = Node()
                with self.transport(handler):
                    with self.assertRaises((NodeExecutionError, UnsupportedFeatureError)):
                        await graph.arun([Info("x")])
                self.assertFalse(any(e["event_type"] == "NodeSucceeded" for e in self.events(graph)))

    async def test_invalid_json_is_sanitized(self):
        async def handler(request):
            return httpx.Response(200, text=SECRET)

        graph = self.graph()
        graph.llm = Node()
        with self.transport(handler):
            with self.assertRaises(NodeExecutionError) as raised:
                await graph.arun([Info("x")])
        self.assertNotIn(SECRET, str(raised.exception))

    async def test_missing_usage_and_reported_model_are_unknown(self):
        async def handler(request):
            return httpx.Response(200, json=response(usage=None, model=None))

        graph = self.graph()
        graph.llm = Node()
        with self.transport(handler):
            await graph.arun([Info("x")])
        metadata = next(e["data"] for e in self.events(graph) if e["event_type"] == "ModelRequestFinished")
        self.assertIsNone(metadata["usage"])
        self.assertIsNone(metadata["reported_model"])

    async def test_invalid_usage_fields_are_not_reported_as_zero(self):
        async def handler(request):
            return httpx.Response(200, json=response(
                usage={"prompt_tokens": True, "completion_tokens": -1, "total_tokens": "5",
                       "completion_tokens_details": {"reasoning_tokens": 1}},
            ))

        graph = self.graph()
        graph.llm = Node()
        with self.transport(handler):
            await graph.arun([Info("x")])
        usage = next(e["data"]["usage"] for e in self.events(graph) if e["event_type"] == "ModelRequestFinished")
        self.assertEqual(usage, {
            "prompt_tokens": None, "completion_tokens": None,
            "total_tokens": None, "reasoning_tokens": 1,
        })

    async def test_absolute_deadline_cancels_http_and_closes_client(self):
        entered, exited = asyncio.Event(), asyncio.Event()

        async def handler(request):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                exited.set()

        graph = self.graph(timeout=0.03)
        graph.llm = Node()
        with self.transport(handler) as clients:
            with self.assertRaises(NodeTimeoutError):
                await graph.arun([Info("x")])
        self.assertTrue(entered.is_set())
        self.assertTrue(exited.is_set())
        self.assertTrue(all(c.is_closed for c in clients))
        self.assertEqual(graph._active_runs, 0)
        self.assertFalse(graph._cleanups)
        self.assertFalse(any(e["event_type"] == "NodeSucceeded" for e in self.events(graph)))

    async def test_external_cancellation_closes_http_client(self):
        entered, exited = asyncio.Event(), asyncio.Event()

        async def handler(request):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                exited.set()

        graph = self.graph()
        graph.llm = Node()
        with self.transport(handler) as clients:
            task = asyncio.create_task(graph.arun([Info("x")]))
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(exited.is_set())
        self.assertTrue(all(c.is_closed for c in clients))
        self.assertEqual(graph._active_runs, 0)
        self.assertFalse(graph._cleanups)

    async def test_concurrent_graphs_keep_task_credentials_and_model_identity(self):
        async def handler(request):
            body = json.loads(request.content)
            await asyncio.sleep(0)
            expected = SECRET if body["model"] == "first-model" else "second-test-key"
            self.assertEqual(request.headers["authorization"], "Bearer " + expected)
            return httpx.Response(200, json=response(body["messages"][-1]["content"]))

        first = self.graph(model="first-model")
        second = self.graph(model="second-model", api_key_env="SECOND_TEST_KEY")
        first.llm, second.llm = Node(), Node()
        with patch.dict(os.environ, {"SECOND_TEST_KEY": "second-test-key"}):
            with self.transport(handler):
                results = await asyncio.gather(first.arun([Info("first")]), second.arun([Info("second")]))
        self.assertEqual([r[0].text for r in results], ["first", "second"])
        with self.assertRaises(UnsupportedFeatureError):
            await first.llm.forward(Info("outside"))

    async def test_child_task_cannot_reuse_parent_attempt(self):
        class Child(Node):
            async def forward(self, info):
                return await asyncio.create_task(super().forward(info))

        graph = self.graph()
        graph.llm = Child()
        with patch("easy_agent._api._client") as factory:
            with self.assertRaises(UnsupportedFeatureError):
                await graph.arun([Info("x")])
            factory.assert_not_called()

    def test_endpoint_validation_and_full_path(self):
        self.assertEqual(endpoint_url("https://api.deepseek.com/chat/completions/"),
                         "https://api.deepseek.com/chat/completions")
        self.assertEqual(endpoint_url("http://127.0.0.1:8000/v1"),
                         "http://127.0.0.1:8000/v1/chat/completions")
        for url in ("https://user:password@example.com", "https://example.com?key=value",
                    "https://example.com#fragment", "http://example.com", "bad-url"):
            with self.subTest(url=url):
                with self.assertRaises(ConfigurationError):
                    endpoint_url(url)

    async def test_timeout_log_failure_cannot_allow_sibling_success(self):
        from runtime_faults import timeout_log_failure

        control = await timeout_log_failure(self.directory, False)
        failure = await timeout_log_failure(self.directory, True)
        self.assertEqual(control["sibling_success_after_timeout_observed"], 0)
        self.assertEqual(failure["sibling_success_after_timeout_observed"], 0)
        self.assertIn(failure["caller_error"], {"LoggingError", "NodeTimeoutError"})

    async def test_simultaneous_done_failures_are_all_observed(self):
        from runtime_faults import unobserved_errors

        observation = await unobserved_errors(self.directory)
        self.assertEqual(observation["pending_tasks"], 0)
        self.assertEqual(observation["unretrieved_done_failures"], 0)


if __name__ == "__main__":
    unittest.main()
