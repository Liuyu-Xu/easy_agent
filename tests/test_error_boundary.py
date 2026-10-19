"""Default exception presentation and no-argument examples; synthetic faults only."""

import asyncio
from contextlib import redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import traceback
import unittest
from unittest.mock import patch

import httpx

from easy_agent import (
    ConfigurationError, EasyAgentError, Graph, Info, InfoError, LoggingError,
    Node, NodeExecutionError, Tool,
)
from easy_agent.config import normalize_env_file, read_selected_env
from easy_agent.errors import _safe_error
from easy_agent.events import EventStore

MARKER = "synthetic-sensitive-boundary"
ROOT = Path(__file__).resolve().parents[1]


class Echo(Node):
    def forward(self, info):
        return info


class ErrorBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.child_environment = {name: os.environ[name] for name in ("SYSTEMROOT", "WINDIR", "TEMP", "TMP")
                                  if name in os.environ}
        self.child_environment["PYTHONPATH"] = str(ROOT)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def graph(self, node):
        graph = Graph(log_dir=self.root / "logs")
        graph.node = node
        return graph

    def safe_failure(self, call, error_type):
        try:
            call()
        except error_type as error:
            rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
            for visible in (str(error), repr(error), rendered):
                self.assertNotIn(MARKER, visible)
            if error.run_id is not None:
                self.assertIn("run_id=" + error.run_id, str(error))
                self.assertEqual(str(error).count("run_id="), 1)
            return error
        self.fail("Expected a framework failure")

    def test_sync_external_errors_and_exact_framework_classes_are_normalized(self):
        for kind in (ValueError, EasyAgentError, NodeExecutionError, LoggingError):
            class Fails(Node):
                def forward(self, info):
                    raise kind(MARKER)

            with self.subTest(kind=kind):
                graph = self.graph(Fails())
                error = self.safe_failure(lambda: graph.run([Info("start")])[0],
                                          kind if issubclass(kind, EasyAgentError) else NodeExecutionError)
                self.assertIsNotNone(error.run_id)
                events = graph.logs(run_id=error.run_id, limit=1000)
                self.assertNotIn(MARKER, json.dumps(events))

    def test_async_external_cause_and_foreign_run_id_are_removed(self):
        class Fails(Node):
            async def forward(self, info):
                try:
                    raise ValueError(MARKER)
                except ValueError as cause:
                    raise NodeExecutionError(MARKER, run_id=MARKER) from cause

        graph = self.graph(Fails())
        error = self.safe_failure(lambda: graph.run([Info("start")])[0], NodeExecutionError)
        self.assertEqual(error.run_id, graph._last_run_id)
        self.assertNotEqual(error.run_id, MARKER)
        self.assertEqual(graph._active_runs, 0)
        self.assertFalse(graph._cleanups)

    def test_untrusted_subclass_does_not_execute_its_string_formatter(self):
        class Untrusted(NodeExecutionError):
            def __str__(self):
                raise AssertionError("External formatter must not execute")

        class Fails(Node):
            def forward(self, info):
                raise Untrusted(MARKER)

        error = self.safe_failure(lambda: self.graph(Fails()).run([Info("start")])[0], NodeExecutionError)
        self.assertIs(type(error), NodeExecutionError)

    def test_tampered_framework_message_cannot_bypass_boundary(self):
        class Fails(Node):
            def forward(self, info):
                error = _safe_error(NodeExecutionError, "safe internal description")
                error.args = (MARKER,)
                raise error

        self.safe_failure(lambda: self.graph(Fails()).run([Info("start")])[0], NodeExecutionError)

    def test_trusted_message_survives_but_attached_external_chain_does_not(self):
        class Fails(Node):
            def forward(self, info):
                raise _safe_error(NodeExecutionError, "safe internal description") from ValueError(MARKER)

        error = self.safe_failure(lambda: self.graph(Fails()).run([Info("start")])[0], NodeExecutionError)
        self.assertIn("safe internal description", str(error))

    def test_input_and_output_validators_do_not_expose_error_text_or_input(self):
        class Rejects:
            @classmethod
            def model_validate(cls, data, strict=True):
                raise EasyAgentError(MARKER)

            @classmethod
            def model_json_schema(cls):
                return {}

        for direction in ("input_schema", "output_schema"):
            with self.subTest(direction=direction):
                graph = self.graph(Echo(**{direction: Rejects}))
                self.safe_failure(lambda: graph.run([Info({"value": MARKER})])[0], InfoError)

    def test_invalid_info_does_not_echo_business_field_names(self):
        self.safe_failure(lambda: Info({MARKER: object()}), InfoError)

    def test_config_read_and_path_errors_hide_underlying_exception_chains(self):
        path = self.root / "config.env"
        path.write_text("LLM_MODEL_ID=synthetic\n", encoding="utf-8")
        with patch.object(Path, "read_text", side_effect=OSError(MARKER)):
            self.safe_failure(lambda: read_selected_env(path, {"LLM_MODEL_ID"}), ConfigurationError)
        with patch.object(Path, "is_file", side_effect=OSError(MARKER)):
            self.safe_failure(lambda: read_selected_env(path, set()), ConfigurationError)
            self.safe_failure(lambda: normalize_env_file(path), ConfigurationError)

    def test_log_create_write_and_read_errors_share_safe_presentation(self):
        store = EventStore(self.root / "events")
        with patch.object(Path, "touch", side_effect=OSError(MARKER)):
            self.safe_failure(lambda: store.start_run("create"), LoggingError)
        store.start_run("write")
        with patch.object(Path, "open", side_effect=OSError(MARKER)):
            self.safe_failure(lambda: store.append("write", "Test"), LoggingError)
            self.safe_failure(lambda: store.query(run_id="write"), LoggingError)
        with patch.object(Path, "exists", side_effect=OSError(MARKER)):
            self.safe_failure(lambda: store.query(run_id="write"), LoggingError)

    def test_start_run_failure_is_safe_and_keeps_its_run_id(self):
        graph = self.graph(Echo())
        with patch.object(graph._event_store, "start_run", side_effect=LoggingError(MARKER)):
            error = self.safe_failure(lambda: graph.run([Info("start")])[0], LoggingError)
        self.assertIsNotNone(error.run_id)
        self.assertIsNone(graph._last_run_id)
        self.assertEqual(graph._active_runs, 0)

    def test_model_status_is_preserved_by_all_error_boundaries(self):
        path = self.root / "account.env"
        path.write_text("LLM_MODEL_ID=synthetic\nLLM_BASE_URL=https://model.example/v1\n"
                        "LLM_API_KEY=synthetic-key\n", encoding="utf-8")
        graph = self.graph(Node(env_file=path))
        with patch("easy_agent._api._client", lambda: httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: httpx.Response(401, text=MARKER)))):
            error = self.safe_failure(lambda: graph.run([Info("start")])[0], NodeExecutionError)
        self.assertIn("HTTP 401", str(error))

    def test_import_does_not_construct_run_or_read_credentials(self):
        with patch("easy_agent.Graph", side_effect=AssertionError("Unexpected graph")), \
             patch("easy_agent.Node", side_effect=AssertionError("Unexpected node")), \
             patch("easy_agent.config.read_selected_env", side_effect=AssertionError("Unexpected file read")):
            console = StringIO()
            with redirect_stdout(console):
                for filename in ("test1_2.py", "test1_3.py", "test1_4.py", "test1_5.py"):
                    runpy.run_path(str(ROOT / "test" / filename))
            self.assertEqual(console.getvalue(), "")

    def test_all_public_examples_run_without_argument_or_error_wrappers(self):
        path = self.root / "account.env"
        path.write_text("LLM_MODEL_ID=synthetic\nLLM_BASE_URL=https://model.example/v1\n"
                        "LLM_API_KEY=synthetic-key\n", encoding="utf-8")
        for filename, outputs in (("test_01_api.py", ["API_TEST_OK"]),
                                  ("test_02_chain.py", ["CHAIN_A", "CHAIN_B"]),
                                  ("test1_1.py", ["CHAIN_A", "TEST1_1_OK"]),
                                  ("test1_2.py", ["CHAIN_A", "TEST1_2_OK"]),
                                  ("test1_3.py", ["CHAIN_A", "CHAIN_B", "TEST1_3_OK"])):
            with self.subTest(filename=filename):
                requested = []

                def handler(request):
                    text = outputs[len(requested)]
                    requested.append(request)
                    return httpx.Response(200, json={"choices": [
                        {"message": {"content": text}, "finish_reason": "stop"}]})

                console = StringIO()
                with patch("easy_agent.Graph", lambda **options: Graph(log_dir=self.root / "logs", **options)), \
                     patch("easy_agent.graph.normalize_env_file", return_value=path), \
                     patch("easy_agent.node.normalize_env_file", return_value=path), \
                     patch("easy_agent._api._client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))), \
                     redirect_stdout(console):
                    runpy.run_path(str(ROOT / "test" / filename), run_name="__main__")
                self.assertEqual(console.getvalue(), outputs[-1] + "\n")
                self.assertEqual(len(requested), len(outputs))

    def test_test1_4_main_runs_all_shapes_without_arguments(self):
        account = self.root / 'test1-4.env'
        account.write_text('LLM_MODEL_ID=synthetic\nLLM_BASE_URL=https://model.example/v1\n'
                           'LLM_API_KEY=synthetic-key\n', encoding='utf-8')
        responses = ['A_OK', 'TEST1_4_1_1_OK', 'A_OK', 'B_OK', 'TEST1_4_2_1_OK',
                     'A_OK', 'TEST1_4_1_2_LEFT_OK', 'TEST1_4_1_2_RIGHT_OK']
        requests = []

        def handler(request):
            text = responses[len(requests)]
            requests.append(request)
            return httpx.Response(200, json={'choices': [
                {'message': {'content': text}, 'finish_reason': 'stop'}]})

        console = StringIO()
        with patch('easy_agent.node.normalize_env_file', return_value=account), \
             patch('easy_agent._api._client', lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))), \
             redirect_stdout(console):
            runpy.run_path(str(ROOT / 'test/test1_4.py'), run_name='__main__')
        self.assertEqual(console.getvalue().splitlines(), [
            "one_to_one ['TEST1_4_1_1_OK']", "many_to_one ['TEST1_4_2_1_OK']",
            "one_to_many ['TEST1_4_1_2_RIGHT_OK', 'TEST1_4_1_2_LEFT_OK']"])
        self.assertEqual(len(requests), 8)

    def test_test1_5_main_runs_all_shapes_and_hub_without_arguments(self):
        account = self.root / 'test1-5.env'
        account.write_text('LLM_MODEL_ID=synthetic\nLLM_BASE_URL=https://model.example/v1\n'
                           'LLM_API_KEY=synthetic-key\n', encoding='utf-8')
        responses = ['2', '3', '2', '3', '4', '2', '4', '6',
                     '2', '4', '6', '8', '2', '4', '{"out_1":"8","out_0":"6"}', '7', '9']
        requests = []

        def handler(request):
            text = responses[len(requests)]
            requests.append(request)
            return httpx.Response(200, json={'choices': [
                {'message': {'content': text}, 'finish_reason': 'stop'}]})

        console = StringIO()
        with patch('easy_agent.node.normalize_env_file', return_value=account), \
             patch('easy_agent._api._client', lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler))), \
             redirect_stdout(console):
            runpy.run_path(str(ROOT / 'test/test1_5.py'), run_name='__main__')
        self.assertEqual(console.getvalue().splitlines(), [
            "one_to_one ['3']",
            "one_to_many ['4', '3']",
            "many_to_one ['6']",
            "many_to_many ['8', '6']",
            "hub_many_to_many ['9', '7']"])
        self.assertEqual(len(requests), 17)

    def test_example_leaves_valid_business_output_assertions_to_regression(self):
        path = self.root / "account.env"
        path.write_text("LLM_MODEL_ID=synthetic\nLLM_BASE_URL=https://model.example/v1\n"
                        "LLM_API_KEY=synthetic-key\n", encoding="utf-8")
        namespace = runpy.run_path(str(ROOT / "test/test1_2.py"))
        payload = {"choices": [{"message": {"content": "OTHER_VALID_TEXT"}, "finish_reason": "stop"}]}
        with patch("easy_agent._api._client", lambda: httpx.AsyncClient(
                transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload)))):
            output = namespace["run"](path)[0]
        self.assertIsInstance(output, Info)
        self.assertEqual(output.text, "OTHER_VALID_TEXT")

    def test_no_argument_entry_fails_with_nonzero_exit_and_safe_default_traceback(self):
        code = """
from pathlib import Path
import runpy
from unittest.mock import patch
from easy_agent import Graph, Node

class Fails(Node):
    def forward(self, info):
        raise ValueError("synthetic-sensitive-boundary")

def graph_factory(**kwargs):
    return Graph(log_dir=Path.cwd() / "events", **kwargs)

with patch("easy_agent.Graph", graph_factory), patch("easy_agent.Node", Fails), \\
     patch("easy_agent.node.normalize_env_file", return_value=None):
    runpy.run_path(SCRIPT, run_name="__main__")
""".replace("SCRIPT", repr(str(ROOT / "test/test1_3.py")))
        completed = subprocess.run([sys.executable, "-X", "utf8", "-B", "-c", code], cwd=self.root,
                                   env=self.child_environment, capture_output=True,
                                   encoding="utf-8", timeout=15)
        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(completed.stdout, "")
        self.assertIn("NodeExecutionError", completed.stderr)
        self.assertIn("run_id=", completed.stderr)
        self.assertNotIn(MARKER, completed.stderr)

    def test_unique_execution_entry_and_shared_declaration_implementation(self):
        self.assertIs(Graph.__call__, Graph.run)
        self.assertNotIn("invoke", Tool.__dict__)
        self.assertNotIn("_set_route", Node.__dict__)
        self.assertNotIn("_set_receive", Node.__dict__)
        self.assertNotIn("_validate_positive_int", Node.__dict__)
        self.assertNotIn("_validate_non_negative_int", Node.__dict__)

    def test_shared_attempt_validation_keeps_defaults_and_rejects_invalid_values(self):
        self.assertEqual(Graph()._graph_defaults["max_attempts"], 3)
        self.assertIsNone(Node().max_attempts)
        for constructor in (Graph, Node):
            for value in (0, -1, True, 1.5):
                error_type = TypeError if isinstance(value, (bool, float)) else ValueError
                with self.subTest(constructor=constructor, value=value), self.assertRaises(error_type):
                    constructor(max_attempts=value)
            with self.assertRaises(ValueError):
                constructor(max_attempts=1, retry=0)
            with self.assertWarns(DeprecationWarning):
                instance = constructor(retry=0)
            attempts = instance._graph_defaults["max_attempts"] if constructor is Graph else instance.max_attempts
            self.assertEqual(attempts, 1)
        node = Node(max_attempts=2)
        with self.assertRaises(ValueError):
            node.retry = -1
        self.assertEqual(node.max_attempts, 2)


class ConcurrentErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_failures_keep_messages_and_run_ids_isolated(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            class Fails(Node):
                async def forward(self, info):
                    await asyncio.sleep(0)
                    raise NodeExecutionError(MARKER, run_id=MARKER)

            graphs = [Graph(log_dir=Path(directory) / str(index)) for index in range(2)]
            for graph in graphs:
                graph.node = Fails()
            errors = await asyncio.gather(*(graph.arun([Info("start")]) for graph in graphs),
                                          return_exceptions=True)
            self.assertEqual(len({error.run_id for error in errors}), 2)
            for graph, error in zip(graphs, errors):
                self.assertIs(type(error), NodeExecutionError)
                self.assertEqual(error.run_id, graph._last_run_id)
                self.assertNotIn(MARKER, str(error))
                self.assertEqual(graph._active_runs, 0)
                self.assertFalse(graph._cleanups)

