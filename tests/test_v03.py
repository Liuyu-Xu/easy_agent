from __future__ import annotations

import asyncio
import tempfile
import unittest
import warnings
from pathlib import Path

from easy_agent import (
    ConfigurationError,
    Graph,
    GraphValidationError,
    Info,
    InfoError,
    Node,
    NodeExecutionError,
    Tool,
)


class Identity(Node):
    def forward(self, info: Info) -> Info:
        return info


class Add(Node):
    def __init__(self, suffix: str) -> None:
        super().__init__()
        self.suffix = suffix

    def forward(self, info: Info) -> Info:
        return Info(info.text + self.suffix)


class ForwardOnlyTool(Tool):
    def forward(self, info: Info) -> Info:
        return Info(info.text.upper())


class SyncBusinessTimeout(Node):
    def forward(self, info: Info) -> Info:
        raise TimeoutError("business timeout")


class Cancellable(Node):
    def __init__(self, started: asyncio.Event, cleaned: asyncio.Event) -> None:
        super().__init__()
        self.started = started
        self.cleaned = cleaned

    async def forward(self, info: Info) -> Info:
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cleaned.set()


class ConcurrencyProbe(Node):
    def __init__(self, state: dict[str, int]) -> None:
        super().__init__()
        self.state = state

    async def forward(self, info: Info) -> Info:
        self.state["active"] += 1
        self.state["peak"] = max(self.state["peak"], self.state["active"])
        try:
            await asyncio.sleep(0.02)
            return info
        finally:
            self.state["active"] -= 1


class V03Tests(unittest.TestCase):
    def test_max_attempts_counts_the_first_attempt_and_retry_is_compatible(self) -> None:
        graph = Graph(max_attempts=3, input_token_budget=2048)
        graph.node = Identity()
        result = graph([Info("ok")])[0]
        started = graph.logs(run_id=result.run_id, event_type="GraphRunStarted")[0]
        self.assertEqual(
            started["data"]["config"]["public_values"]["nodes.node.max_attempts"],
            3,
        )
        self.assertEqual(
            started["data"]["config"]["public_values"]["nodes.node.base_url"],
            "https://api.deepseek.com",
        )
        self.assertEqual(
            started["data"]["config"]["public_values"][
                "nodes.node.input_token_budget_status"
            ],
            "accepted_not_applied",
        )
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            compatible = Graph(retry=2)
        self.assertEqual(compatible._graph_defaults["max_attempts"], 3)
        self.assertTrue(any(item.category is DeprecationWarning for item in recorded))
        with self.assertRaises(ValueError):
            Graph(max_attempts=3, retry=2)
        for invalid in (True, 0, -1, 1.5):
            with self.assertRaises(TypeError if isinstance(invalid, (bool, float)) else ValueError):
                Graph(max_attempts=invalid)  # type: ignore[arg-type]

    def test_business_timeout_is_not_a_framework_deadline(self) -> None:
        graph = Graph(max_attempts=3)
        graph.node = SyncBusinessTimeout()

        with self.assertRaises(NodeExecutionError) as raised:
            graph([Info("x")])

        self.assertNotIn("超过", str(raised.exception))
        self.assertEqual(
            len(graph.node.logs(run_id=raised.exception.run_id, event_type="NodeTimedOut")),
            0,
        )

    def test_mutable_definition_values_are_normalized_on_every_assignment(self) -> None:
        graph = Graph()
        graph.node = Identity()
        values = [ForwardOnlyTool()]
        graph.node.tools = values
        graph.node.required_permissions = {"read"}
        values.append(ForwardOnlyTool(tool_name="second"))

        self.assertIsInstance(graph.node.tools, tuple)
        self.assertEqual(len(graph.node.tools), 1)
        self.assertIsInstance(graph.node.required_permissions, frozenset)
        with self.assertRaises(TypeError):
            graph.node.max_parallelism = True

    def test_forward_only_tool_is_instantiable_and_runs(self) -> None:
        graph = Graph(allowed_tools={"ForwardOnlyTool"})
        graph.upper = ForwardOnlyTool()

        self.assertEqual(graph([Info("hello")])[0].text, "HELLO")

    def test_downstream_editing_and_atomic_replacement(self) -> None:
        graph = Graph()
        graph.source = Identity()
        graph.target = Add("-old")
        graph.source.send_to(graph.target)

        snapshot = graph.source.downstreams()
        self.assertEqual(snapshot[0]["target_name"], "target")
        with self.assertRaises(TypeError):
            snapshot[0]["kind"] = "changed"  # type: ignore[index]
        with self.assertRaises(GraphValidationError):
            graph.target = Add("-implicit")

        replacement = Add("-new")
        graph.replace_node("target", replacement, migrate_edges=True)
        self.assertIs(graph.target, replacement)
        self.assertEqual(graph([Info("x")])[0].text, "x-new")

    def test_replacement_migrates_entry_and_self_route(self) -> None:
        graph = Graph()
        graph.loop = Identity()
        graph.start = [graph.loop]
        graph.max_steps = 3
        graph.loop.route(field="done", cases={True: graph.END}, default=graph.loop)

        replacement = Identity()
        graph.replace_node("loop", replacement, migrate_edges=True)

        self.assertIs(graph.start[0], replacement)
        relations = replacement.downstreams()
        self.assertEqual(relations[0]["target_name"], "__END__")
        self.assertEqual(relations[1]["target_name"], "loop")
        graph.validate()

    def test_failed_graph_edit_restores_relations_and_node_config(self) -> None:
        graph = Graph()
        graph.source = Identity()
        graph.target = Identity()
        graph.source.send_to(graph.target)
        graph.source.prompt = "before"

        with self.assertRaises(GraphValidationError):
            with graph.edit():
                graph.source.prompt = "after"
                graph.source.set_downstreams([])

        self.assertEqual(graph.source.prompt, "before")
        self.assertEqual(graph.source.downstreams()[0]["target_name"], "target")
        graph.validate()

        uncommitted = Identity()
        with self.assertRaises(GraphValidationError):
            with graph.edit():
                graph.extra = uncommitted
        self.assertIsNone(uncommitted.owner)

    def test_receive_dependency_cannot_be_removed_outside_transaction(self) -> None:
        graph = Graph()
        graph.source = Identity()
        graph.receiver = Identity()
        graph.source.send_to(graph.receiver)
        graph.receiver.receive(mode="all", sources=[graph.source])

        with self.assertRaises(GraphValidationError):
            graph.source.set_downstreams([])

        self.assertEqual(graph.source.downstreams()[0]["target_name"], "receiver")

    def test_outer_cancellation_cleans_owned_tasks_before_unfreezing(self) -> None:
        async def scenario() -> None:
            started = asyncio.Event()
            cleaned = asyncio.Event()
            graph = Graph(execution="async")
            graph.node = Cancellable(started, cleaned)
            task = asyncio.create_task(graph.arun([Info("x")]))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(cleaned.is_set())
            graph.node.prompt = "editable-after-cleanup"

        asyncio.run(scenario())

    def test_dynamic_scheduler_respects_per_run_concurrency(self) -> None:
        state = {"active": 0, "peak": 0}
        graph = Graph(execution="async", max_concurrency=2)
        graph.source = Identity()
        graph.left = ConcurrencyProbe(state)
        graph.middle = ConcurrencyProbe(state)
        graph.right = ConcurrencyProbe(state)
        graph.join = Identity()
        graph.source.send_to(graph.left, graph.middle, graph.right)
        graph.left.send_to(graph.join)
        graph.middle.send_to(graph.join)
        graph.right.send_to(graph.join)
        graph.join.receive(
            mode="all", sources=[graph.left, graph.middle, graph.right]
        )

        graph([Info("x")])

        self.assertEqual(state["peak"], 2)

    def test_dotenv_is_literal_and_rejects_ambiguous_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            literal = root / "literal.env"
            literal.write_text(
                "ROOT=secret\nEASY_AGENT_API_KEY=${ROOT}\n", encoding="utf-8"
            )
            graph = Graph(env_file=literal)
            graph.node = Identity(api_key_env="EASY_AGENT_API_KEY")
            result = graph([Info("ok")])[0]
            audit = graph.audit(run_id=result.run_id)
            self.assertTrue(any(item["data"]["resolved"] for item in audit))

            duplicate = root / "duplicate.env"
            duplicate.write_text("EASY_AGENT_MODEL=a\nEASY_AGENT_MODEL=b\n", encoding="utf-8")
            graph = Graph(env_file=duplicate)
            graph.node = Identity()
            with self.assertRaises(ConfigurationError):
                graph.validate()

            missing = root / "missing.env"
            missing.write_text("EASY_AGENT_API_KEY\n", encoding="utf-8")
            graph = Graph(env_file=missing)
            graph.node = Identity()
            graph.validate()

    def test_strict_builtin_schema_rejects_bool_as_int(self) -> None:
        graph = Graph()
        graph.node = Identity(input_schema=int)
        with self.assertRaises(InfoError):
            graph([Info(True)])

    def test_optional_pydantic_v2_schema_is_strict(self) -> None:
        try:
            from pydantic import BaseModel
        except ImportError:
            self.skipTest("Pydantic is optional")

        class Payload(BaseModel):
            count: int

        graph = Graph()
        graph.node = Identity(input_schema=Payload)
        with self.assertRaises(InfoError):
            graph([Info({"count": "1"})])


if __name__ == "__main__":
    unittest.main()
