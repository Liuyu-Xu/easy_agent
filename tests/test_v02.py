from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from easy_agent import (
    Graph,
    GraphValidationError,
    Info,
    Node,
    NodeExecutionError,
    NodeTimeoutError,
    Tool,
)


class Identity(Node):
    def forward(self, info: Info) -> Info:
        return info


class Append(Node):
    def __init__(self, suffix: str) -> None:
        super().__init__()
        self.suffix = suffix

    def forward(self, info: Info) -> Info:
        return Info(info.text + self.suffix)


class Join(Node):
    def forward(self, info: Info) -> Info:
        return Info(f"{info.get('tail')}|{info.get('right')}")


class UpperTool(Tool):
    def invoke(self, info: Info) -> Info:
        return Info(info.text.upper())


class Slow(Node):
    async def forward(self, info: Info) -> Info:
        await asyncio.sleep(0.1)
        return info


class RaisesPythonTimeout(Node):
    async def forward(self, info: Info) -> Info:
        raise TimeoutError("tool-side timeout")


class Gate(Node):
    def __init__(self, started: asyncio.Event, release: asyncio.Event) -> None:
        super().__init__()
        self.started = started
        self.release = release

    async def forward(self, info: Info) -> Info:
        self.started.set()
        await self.release.wait()
        return info


class V02Tests(unittest.TestCase):
    def test_tool_is_a_node_and_participates_in_info_flow(self) -> None:
        graph = Graph(allowed_tools={"UpperTool"})
        graph.upper = UpperTool()

        result = graph([Info("hello")])[0]

        self.assertIsInstance(graph.upper, Node)
        self.assertEqual(result.text, "HELLO")

    def test_defaults_log_dir_and_python_dotenv(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env_file = root / ".env"
            log_dir = root / "events"
            env_file.write_text("EASY_AGENT_API_KEY='secret'\n", encoding="utf-8")
            graph = Graph(env_file=env_file, log_dir=log_dir)
            graph.node = Identity()

            result = graph([Info("ok")])[0]
            started = graph.logs(
                run_id=result.run_id, event_type="GraphRunStarted", limit=10
            )[0]
            config = started["data"]["config"]

            self.assertEqual(
                config["public_values"]["nodes.node.model"],
                "deepseek-v4-flash",
            )
            self.assertEqual(config["public_values"]["nodes.node.temperature"], 0)
            self.assertEqual(
                config["public_values"]["nodes.node.max_output_tokens"], 10000
            )
            self.assertEqual(config["public_values"]["nodes.node.timeout"], 180.0)
            self.assertEqual(config["max_concurrency"], 1)
            self.assertTrue((log_dir / f"{result.run_id}.jsonl").is_file())

    def test_async_timeout_stops_run_without_retrying(self) -> None:
        async def scenario() -> tuple[Graph, str]:
            graph = Graph(execution="async", timeout=0.01)
            graph.slow = Slow()
            with self.assertRaises(NodeTimeoutError) as raised:
                await graph.arun([Info("x")])
            return graph, raised.exception.run_id  # type: ignore[return-value]

        graph, run_id = asyncio.run(scenario())
        attempts = graph.slow.logs(
            run_id=run_id, event_type="NodeAttemptStarted", limit=10
        )
        self.assertEqual(len(attempts), 1)
        self.assertEqual(
            len(graph.slow.logs(run_id=run_id, event_type="NodeTimedOut")), 1
        )
        self.assertEqual(
            len(graph.logs(run_id=run_id, event_type="GraphRunFailed")), 1
        )

    def test_python_timeout_error_is_not_graph_deadline(self) -> None:
        graph = Graph(execution="async", retry=0, timeout=1)
        graph.node = RaisesPythonTimeout()

        with self.assertRaises(NodeExecutionError) as raised:
            graph([Info("x")])

        self.assertNotIsInstance(raised.exception, NodeTimeoutError)
        run_id = raised.exception.run_id
        self.assertEqual(
            len(graph.node.logs(run_id=run_id, event_type="NodeTimedOut")), 0
        )
        self.assertEqual(
            len(graph.node.logs(run_id=run_id, event_type="NodeFailed")), 1
        )

    def test_node_definition_is_frozen_while_graph_runs(self) -> None:
        async def scenario() -> None:
            started = asyncio.Event()
            release = asyncio.Event()
            graph = Graph(execution="async")
            graph.gate = Gate(started, release)
            task = asyncio.create_task(graph.arun([Info("x")]))
            await started.wait()
            with self.assertRaises(GraphValidationError):
                graph.gate.prompt = "changed"
            with self.assertRaises(GraphValidationError):
                graph.other = Identity()
            release.set()
            await task
            graph.gate.prompt = "allowed-after-run"

        asyncio.run(scenario())

    def test_multi_step_linear_branches_can_join(self) -> None:
        graph = Graph(execution="async", max_concurrency=2)
        graph.source = Identity()
        graph.left = Append("-left")
        graph.tail = Append("-tail")
        graph.right = Append("-right")
        graph.join = Join()
        graph.source.send_to(graph.left, graph.right)
        graph.left.send_to(graph.tail)
        graph.tail.send_to(graph.join)
        graph.right.send_to(graph.join)
        graph.join.receive(mode="all", sources=[graph.tail, graph.right])

        result = graph([Info("x")])[0]

        self.assertEqual(result.text, "x-left-tail|x-right")


if __name__ == "__main__":
    unittest.main()
