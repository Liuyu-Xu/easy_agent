from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from easy_agent import (
    ConfigurationError,
    Graph,
    GraphValidationError,
    Info,
    InfoError,
    Node,
    OwnershipError,
    ResultError,
    RoutingError,
    UnsupportedFeatureError,
)


class Strip(Node):
    def forward(self, info: Info) -> Info:
        return Info(info.text.strip())


class AddBang(Node):
    def forward(self, info: Info) -> Info:
        return Info(info.text + "!")


class Identity(Node):
    def forward(self, info: Info) -> Info:
        return info


class Left(Node):
    async def forward(self, info: Info) -> Info:
        await asyncio.sleep(0)
        return Info(info.text + "-left")


class Right(Node):
    async def forward(self, info: Info) -> Info:
        await asyncio.sleep(0)
        return Info(info.text + "-right")


class Summary(Node):
    def forward(self, info: Info) -> Info:
        return Info(f"{info.get('left')}|{info.get('right')}")


class Writer(Node):
    def forward(self, info: Info) -> Info:
        if isinstance(info.data, str):
            return Info({"text": info.text, "round": 0})
        return Info({"text": info.get("text"), "round": info.get("round")})


class Reviewer(Node):
    def forward(self, info: Info) -> Info:
        round_number = info.get("round") + 1
        return Info(
            {
                "text": info.get("text"),
                "round": round_number,
                "approved": round_number >= 2,
            }
        )


class V01Tests(unittest.TestCase):
    def test_info_is_deeply_immutable_and_convenient(self) -> None:
        info = Info({"name": "demo", "items": [1, 2]})
        self.assertEqual(info.get("name"), "demo")
        self.assertEqual(info.data["items"], (1, 2))
        with self.assertRaises(TypeError):
            info.data["name"] = "changed"  # type: ignore[index]
        with self.assertRaises(InfoError):
            _ = info.text
        self.assertEqual(Info("hello").text, "hello")

    def test_auto_registration_relations_and_ownership(self) -> None:
        graph = Graph()
        graph.a = Strip()
        graph.b = AddBang()
        graph.a.send_to(graph.b)

        self.assertIn("b", graph.relations["a"])
        with self.assertRaises(TypeError):
            graph.relations["a"]["b"] = ()  # type: ignore[index]

        other = Graph()
        with self.assertRaises(OwnershipError):
            other.a = graph.a

    def test_failed_replacement_keeps_original_registration(self) -> None:
        graph = Graph()
        graph.a = Strip()
        original = graph.a
        other = Graph()
        other.foreign = AddBang()

        with self.assertRaises(OwnershipError):
            graph.a = other.foreign

        self.assertIs(graph.a, original)
        graph.validate()

    def test_removed_node_cannot_masquerade_as_replacement(self) -> None:
        graph = Graph()
        graph.source = Strip()
        old_target = AddBang()
        graph.target = old_target
        graph.target = Identity()

        with self.assertRaises(OwnershipError):
            graph.source.send_to(old_target)
        with self.assertRaises(OwnershipError):
            graph.start = [old_target]

    def test_sequential_chain_result_provenance_and_logs(self) -> None:
        graph = Graph()
        graph.strip = Strip()
        graph.finish = AddBang()
        graph.strip.send_to(graph.finish)

        result = graph([Info(" hello ")])[0]

        self.assertEqual(result.text, "hello!")
        self.assertEqual(result.origin_kind, "graph_output")
        self.assertIsNotNone(result.run_id)
        self.assertEqual(len(result.parent_ids), 1)
        graph_events = graph.logs(run_id=result.run_id, limit=1000)
        node_events = graph.finish.logs(run_id=result.run_id, limit=1000)
        self.assertTrue(any(item["event_type"] == "InfoDeliveryRequested" for item in graph_events))
        self.assertTrue(any(item["event_type"] == "NodeSucceeded" for item in node_events))

    def test_passthrough_keeps_identity_until_graph_publishes_result(self) -> None:
        graph = Graph()
        graph.identity = Identity()
        source = Info("same")

        result = graph([source])[0]

        self.assertNotEqual(result.id, source.id)
        self.assertEqual(result.parent_ids, (source.id,))
        produced = graph.identity.logs(
            run_id=result.run_id, event_type="InfoProduced", limit=100
        )
        self.assertEqual(produced, [])

    def test_async_broadcast_and_structured_join(self) -> None:
        async def scenario() -> Info:
            graph = Graph(execution="async")
            graph.source = Identity()
            graph.left = Left()
            graph.right = Right()
            graph.summary = Summary()
            graph.source.send_to(graph.left, graph.right)
            graph.left.send_to(graph.summary)
            graph.right.send_to(graph.summary)
            graph.summary.receive(mode="all", sources=[graph.left, graph.right])
            return (await graph.arun([Info("x")]))[0]

        result = asyncio.run(scenario())
        self.assertEqual(result.text, "x-left|x-right")

    def test_broadcast_without_join_is_rejected(self) -> None:
        graph = Graph()
        graph.source = Identity()
        graph.left = Left()
        graph.right = Right()
        graph.source.send_to(graph.left, graph.right)
        with self.assertRaises(ResultError):
            graph.validate()

    def test_bounded_feedback_loop(self) -> None:
        graph = Graph()
        graph.writer = Writer()
        graph.reviewer = Reviewer()
        graph.writer.send_to(graph.reviewer)
        graph.reviewer.route(
            field="approved",
            cases={True: graph.END, False: graph.writer},
        )
        graph.start = [graph.writer]
        graph.max_steps = 10

        result = graph([Info("draft")])[0]

        self.assertTrue(result.get("approved"))
        self.assertEqual(result.get("round"), 2)

    def test_route_without_match_fails_with_run_id(self) -> None:
        class Classifier(Node):
            def forward(self, info: Info) -> Info:
                return Info({"kind": "unknown"})

        graph = Graph()
        graph.classifier = Classifier()
        graph.done = Identity()
        graph.classifier.route(field="kind", cases={"known": graph.done})

        with self.assertRaises(RoutingError) as raised:
            graph([Info("input")])
        self.assertIsNotNone(raised.exception.run_id)

    def test_dotenv_is_private_and_secret_is_not_exported_or_logged(self) -> None:
        previous = os.environ.pop("CUSTOM_TEST_KEY", None)
        try:
            with tempfile.TemporaryDirectory() as directory:
                env_path = Path(directory) / ".env"
                env_path.write_text(
                    "EASY_AGENT_MODEL=file-model\nCUSTOM_TEST_KEY=super-secret-value\n",
                    encoding="utf-8",
                )
                graph = Graph(env_file=env_path, api_key_env="CUSTOM_TEST_KEY")
                graph.node = Identity(api_key_env="CUSTOM_TEST_KEY")
                result = graph([Info("ok")])[0]

                serialized = json.dumps(graph.to_dict(), ensure_ascii=False)
                logs = json.dumps(
                    graph.logs(run_id=result.run_id, limit=1000), ensure_ascii=False
                )
                audit = json.dumps(
                    graph.audit(run_id=result.run_id, limit=1000), ensure_ascii=False
                )
                self.assertNotIn("super-secret-value", serialized)
                self.assertNotIn("super-secret-value", logs)
                self.assertNotIn("super-secret-value", audit)
                self.assertIn("SecretReferenceResolved", audit)
                self.assertNotIn("CUSTOM_TEST_KEY", os.environ)
        finally:
            if previous is not None:
                os.environ["CUSTOM_TEST_KEY"] = previous

    def test_unsupported_execution_modes_fail_without_downgrade(self) -> None:
        graph = Graph(execution="multiprocess")
        graph.node = Identity()
        with self.assertRaises(UnsupportedFeatureError):
            graph([Info("x")])

    def test_builtin_llm_requires_a_resolved_api_key(self) -> None:
        graph = Graph(model="placeholder-model")
        graph.llm = Node(prompt="placeholder", api_key_env="EASY_AGENT_TEST_MISSING_KEY")
        with patch.dict(os.environ, {"EASY_AGENT_TEST_MISSING_KEY": ""}):
            with self.assertRaises(ConfigurationError):
                graph([Info("x")])

    def test_sync_call_inside_running_loop_is_rejected(self) -> None:
        async def scenario() -> None:
            graph = Graph()
            graph.node = Identity()
            with self.assertRaisesRegex(RuntimeError, "graph.arun"):
                graph([Info("x")])

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
