"""Deterministic regressions for the first-stage correctness audit."""

import asyncio
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from easy_agent import (
    Graph, Node, Tool, Info, GraphValidationError, OwnershipError,
    PermissionDeniedError, UnsupportedFeatureError, NodeTimeoutError,
    NodeExecutionError, InfoError, ConfigurationError,
)
from easy_agent.config import read_selected_env


class Identity(Node):
    def forward(self, info):
        return info


class ReadTool(Tool):
    def forward(self, info):
        return info


class WriteTool(ReadTool):
    side_effect = True


class Step1DefinitionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def graph(self, **kwargs):
        return Graph(log_dir=self.directory.name, **kwargs)

    def test_class_effect_is_inherited_without_constructor_override(self):
        tool = WriteTool()
        self.assertTrue(tool.side_effect)
        for internal in (False, True):
            graph = self.graph(allowed_tools={tool.tool_name})
            if internal:
                graph.node = Identity(tools=[WriteTool()])
            else:
                graph.node = WriteTool()
            with self.assertRaises(UnsupportedFeatureError):
                graph.validate()

    def test_allowlist_is_identical_for_both_placements(self):
        for allowed in (None, set(), {"read"}):
            for internal in (False, True):
                graph = self.graph(allowed_tools=allowed)
                tool = ReadTool(tool_name="read")
                graph.node = Identity(tools=[tool]) if internal else tool
                if allowed:
                    self.assertEqual(graph([Info("ok")])[0].text, "ok")
                else:
                    with self.assertRaises(PermissionDeniedError):
                        graph([Info("ok")])

    def test_send_to_validates_all_targets_before_committing(self):
        graph = self.graph()
        graph.source = Identity()
        graph.target = Identity()
        other = self.graph()
        other.foreign = Identity()
        before = graph.source.downstreams()
        for targets in ((other.foreign, graph.target),
                        (graph.target, other.foreign, graph.target),
                        (graph.target, other.foreign)):
            with self.assertRaises(OwnershipError):
                graph.source.send_to(*targets)
            self.assertEqual(graph.source.downstreams(), before)

    def test_effect_values_and_tool_permissions_are_checked_before_calls(self):
        class Inherited(WriteTool):
            pass

        self.assertTrue(Inherited().side_effect)
        self.assertFalse(ReadTool().side_effect)
        self.assertFalse(Inherited(side_effect=False).side_effect)
        for invalid in (None, 1, "true"):
            with self.assertRaises(TypeError):
                ReadTool(side_effect=invalid)
        calls = []

        class CountTool(ReadTool):
            def forward(self, info):
                calls.append(info)
                return info

        for internal in (False, True):
            tool = CountTool(tool_name="stable", required_permissions={"read"})
            graph = self.graph(allowed_tools={"stable"})
            graph.different_property = Identity(tools=[tool]) if internal else tool
            with self.assertRaises(PermissionDeniedError):
                graph([Info("start")])
        self.assertEqual(calls, [])

    def test_invalid_local_changes_preserve_existing_declarations(self):
        graph = self.graph()
        graph.source, graph.target = Identity(), Identity()
        graph.source.route(field="flag", cases={True: graph.target}, default=graph.END)
        before = graph.to_dict()
        with self.assertRaises(GraphValidationError):
            graph.source.set_route(field="flag", cases={True: graph.target, "bad": graph.END})
        self.assertEqual(graph.to_dict(), before)
        graph.source.clear_route()
        graph.source.send_to(graph.target)
        graph.target.receive(mode="all", sources=[graph.source])
        before = graph.to_dict()
        with self.assertRaises(UnsupportedFeatureError):
            graph.target.set_receive(mode="sequence", sources=[graph.source])
        with self.assertRaises(GraphValidationError):
            graph.source.remove_downstream(graph.target)
        self.assertEqual(graph.to_dict(), before)

    def test_rollback_restores_shared_declarations_but_preserves_business_resources(self):
        tool = ReadTool(tool_name="read")
        graph = self.graph(allowed_tools={"read"})
        graph.node = Identity(tools=[tool])
        graph.node.resource = []
        with self.assertRaises(RuntimeError):
            with graph.edit():
                tool.tool_name = "changed"
                graph.node.resource.append("external state")
                raise RuntimeError("abort")
        self.assertEqual(tool.tool_name, "read")
        self.assertEqual(graph.node.resource, ["external state"])
        graph.validate()

    def test_candidate_with_conflicting_topology_is_rejected(self):
        graph = self.graph()
        graph.node = Identity()
        candidate = Identity()
        candidate._outgoing = ["unbound-target"]
        with self.assertRaises(GraphValidationError):
            graph.replace_node("node", candidate, migrate_edges=True)
        self.assertIsNone(candidate.owner)
        self.assertEqual(candidate._outgoing, ["unbound-target"])
        graph.validate()

    def test_failed_registration_and_delete_preserve_export_and_candidate(self):
        graph = self.graph()
        graph.source, graph.middle, graph.target = Identity(), Identity(), Identity()
        graph.source.send_to(graph.middle)
        graph.middle.send_to(graph.target)
        before = graph.to_dict()
        candidate = Identity()
        with self.assertRaises(AttributeError):
            graph.id = candidate
        self.assertIsNone(candidate.owner)
        with self.assertRaises(GraphValidationError):
            del graph.middle
        self.assertEqual(graph.to_dict(), before)

    def test_removed_candidate_is_restored_after_edit_failure(self):
        graph = self.graph()
        graph.node = Identity()
        candidate = Identity()
        with self.assertRaises(RuntimeError):
            with graph.edit():
                graph.extra = candidate
                del graph.extra
                raise RuntimeError("abort")
        self.assertIsNone(candidate.owner)
        self.assertIsNone(candidate.name)
        other = self.graph()
        other.reused = candidate

    def test_failed_migration_restores_candidate_topology(self):
        graph = self.graph()
        graph.source = Identity()
        graph.target = Identity()
        graph.source.send_to(graph.target)
        candidate = Identity(required_permissions={"denied"})
        candidate_id = candidate.id
        with self.assertRaises(PermissionDeniedError):
            graph.replace_node("source", candidate, migrate_edges=True)
        self.assertIsNone(candidate.owner)
        self.assertEqual(candidate._outgoing, [])
        self.assertEqual(candidate.id, candidate_id)
        graph.validate()

    def test_replacement_participates_in_existing_edit(self):
        graph = self.graph()
        graph.source = Identity()
        graph.target = Identity()
        graph.source.send_to(graph.target)
        candidate = Identity()
        with graph.edit():
            graph.replace_node("target", candidate, migrate_edges=True)
        self.assertIs(graph.target, candidate)
        graph.validate()

    def test_dotenv_missing_value_does_not_override_explicit_value(self):
        path = Path(self.directory.name) / "config.env"
        path.write_text("EASY_AGENT_MODEL\nEASY_AGENT_API_KEY\n", encoding="utf-8")
        self.assertEqual(read_selected_env(path, {"EASY_AGENT_MODEL"}), {})
        graph = self.graph(env_file=path, model="explicit")
        graph.node = Identity()
        self.assertEqual(graph([Info("ok")])[0].text, "ok")
        path.write_text("EASY_AGENT_MODEL=\nKEY=${LITERAL}\n", encoding="utf-8")
        self.assertEqual(
            read_selected_env(path, {"EASY_AGENT_MODEL", "KEY"}),
            {"EASY_AGENT_MODEL": "", "KEY": "${LITERAL}"},
        )

    def test_dotenv_is_one_snapshot_and_empty_high_priority_value_is_not_replaced(self):
        path = Path(self.directory.name) / "snapshot.env"
        path.write_text("EASY_AGENT_MODEL=from_file\nKEY=${LITERAL}\n", encoding="utf-8")
        original = Path.read_text
        reads = []

        def read_once(target, *args, **kwargs):
            reads.append(target)
            result = original(target, *args, **kwargs)
            target.write_text("EASY_AGENT_MODEL=changed\n", encoding="utf-8")
            return result

        with patch.object(Path, "read_text", read_once):
            values = read_selected_env(path, {"EASY_AGENT_MODEL", "KEY"})
        self.assertEqual(len(reads), 1)
        self.assertEqual(values, {"EASY_AGENT_MODEL": "from_file", "KEY": "${LITERAL}"})
        graph = self.graph(env_file=path)
        graph.node = Node()
        with patch.dict("os.environ", {"EASY_AGENT_MODEL": ""}):
            with self.assertRaises(ConfigurationError):
                graph.validate()

    def test_output_schema_normalization_is_published_once(self):
        try:
            from pydantic import BaseModel, field_validator
        except ImportError:
            self.skipTest("Pydantic v2 is optional")
        validations = []

        class Payload(BaseModel):
            text: str
            count: int = 7

            @field_validator("text")
            @classmethod
            def strip_text(cls, value):
                validations.append(value)
                return value.strip()

        class Emit(Node):
            def forward(self, info):
                return Info({"text": "  hello  "})

        graph = self.graph()
        graph.node = Emit(output_schema=Payload)
        result = graph([Info("start")])[0]
        self.assertEqual(result.data["text"], "hello")
        self.assertEqual(result.data["count"], 7)
        self.assertEqual(validations, ["  hello  "])

    def test_changed_input_is_explicitly_blocked_before_forward(self):
        try:
            from pydantic import BaseModel
        except ImportError:
            self.skipTest("Pydantic v2 is optional")

        class Payload(BaseModel):
            count: int = 7

        calls = []

        class Observe(Node):
            def forward(self, info):
                calls.append(info)
                return info

        graph = self.graph()
        graph.node = Observe(input_schema=Payload)
        with self.assertRaises(UnsupportedFeatureError):
            graph([Info({})])
        self.assertEqual(calls, [])

    def test_json_type_change_is_not_mistaken_for_equal_passthrough(self):
        try:
            from pydantic import BaseModel, field_validator
        except ImportError:
            self.skipTest("Pydantic v2 is optional")
        from typing import Any

        class Payload(BaseModel):
            value: Any

            @field_validator("value")
            @classmethod
            def as_bool(cls, value):
                return True

        graph = self.graph()
        graph.node = Identity(output_schema=Payload)
        initial = Info({"value": 1})
        result = graph([initial])[0]
        self.assertIs(result.data["value"], True)
        self.assertNotEqual(result.content_hash, initial.content_hash)
        produced = graph.node.logs(run_id=result.run_id, event_type="InfoProduced")
        self.assertEqual(len(produced), 1)
        graph.node.input_schema = Payload
        with self.assertRaises(UnsupportedFeatureError):
            graph([Info({"value": 1})])

    def test_nested_output_normalization_reaches_route_and_preserves_provenance(self):
        try:
            from pydantic import BaseModel, ConfigDict, field_validator
        except ImportError:
            self.skipTest("Pydantic v2 is optional")

        class Payload(BaseModel):
            model_config = ConfigDict(extra="ignore")
            flag: str
            nested: dict[str, int]

            @field_validator("flag")
            @classmethod
            def strip(cls, value):
                return value.strip()

        observed = []

        class Emit(Node):
            def forward(self, info):
                return Info({"flag": " yes ", "nested": {"count": 7}, "ignored": 1})

        class Observe(Node):
            def forward(self, info):
                observed.append(info)
                return info

        graph = self.graph()
        graph.source = Emit(output_schema=Payload)
        graph.target = Observe()
        graph.source.route(field="flag", cases={"yes": graph.target})
        initial = Info("start")
        result = graph([initial])[0]
        produced = observed[0]
        self.assertEqual(produced.data["nested"]["count"], 7)
        self.assertNotIn("ignored", produced.data)
        self.assertNotEqual(produced.id, initial.id)
        self.assertEqual(produced.origin_kind, "node_output")
        self.assertEqual(produced.parent_ids, (initial.id,))
        self.assertEqual(produced.content_hash, Info(dict(result.data)).content_hash)

    def test_schema_serialization_failure_and_deterministic_failure_do_not_retry(self):
        try:
            from pydantic import BaseModel, field_serializer
        except ImportError:
            self.skipTest("Pydantic v2 is optional")

        class Payload(BaseModel):
            text: str

            @field_serializer("text")
            def fail(self, value):
                raise ValueError("cannot serialize")

        calls = []

        class Emit(Node):
            def forward(self, info):
                calls.append(info)
                return Info({"text": "ok"})

        graph = self.graph(max_attempts=3)
        graph.node = Emit(output_schema=Payload)
        with self.assertRaises(InfoError) as raised:
            graph([Info("start")])
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(graph.node.logs(run_id=raised.exception.run_id, event_type="NodeSucceeded")), 0)

    def test_unclassified_adapter_like_failure_still_has_one_attempt(self):
        calls = []

        class TransientCandidate(Node):
            def forward(self, info):
                calls.append(info)
                raise ConnectionError("controlled adapter-like failure")

        graph = self.graph(max_attempts=3)
        graph.node = TransientCandidate()
        with self.assertRaises(NodeExecutionError) as raised:
            graph([Info("start")])
        self.assertEqual(len(calls), 1)  # A03-011 remains a documented dependency.
        self.assertEqual(len(graph.node.logs(run_id=raised.exception.run_id, event_type="NodeAttemptStarted")), 1)

    def test_synchronous_entry_waits_for_actual_async_cleanup(self):
        swallowed, release = threading.Event(), threading.Event()
        errors = []

        class Stubborn(Node):
            async def forward(self, info):
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    swallowed.set()
                    await asyncio.to_thread(release.wait)
                return Info("late")

        # 0.05s stays above this host's 15.625ms event-loop clock resolution so
        # the deadline really elapses after the node starts.
        graph = self.graph(timeout=0.05)
        graph.node = Stubborn()

        def run():
            try:
                graph([Info("start")])
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=run)
        thread.start()
        try:
            self.assertTrue(swallowed.wait(2))
            self.assertTrue(thread.is_alive())  # No unapproved finite cleanup default.
            events = graph.logs(run_id=graph._last_run_id, limit=1000)
            self.assertEqual(sum(e["event_type"] == "GraphRunFailed" for e in events), 1)
            with self.assertRaises(GraphValidationError):
                graph.node.prompt = "early"
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], NodeTimeoutError)
        graph.node.prompt = "clean"


class Step1RuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)

    def graph(self, **kwargs):
        return Graph(execution="async", log_dir=self.directory.name, **kwargs)

    async def test_timeout_discards_a_result_from_suppressed_cancellation(self):
        calls = []

        class Late(Node):
            async def forward(self, info):
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    return Info("late")

        class Downstream(Node):
            def forward(self, info):
                calls.append(info)
                return info

        graph = self.graph(timeout=0.01)
        graph.source = Late()
        graph.target = Downstream()
        graph.source.send_to(graph.target)
        with self.assertRaises(NodeTimeoutError) as raised:
            await graph.arun([Info("start")])
        events = graph._event_store.query(run_id=raised.exception.run_id, limit=1000)
        self.assertFalse(calls)
        self.assertFalse(any(e["event_type"] == "NodeSucceeded" for e in events))
        self.assertEqual(sum(e["event_type"] == "GraphRunFailed" for e in events), 1)

    async def test_output_validation_shares_attempt_deadline(self):
        try:
            from pydantic import BaseModel, field_validator
        except ImportError:
            self.skipTest("Pydantic v2 is optional")
        import time

        class Payload(BaseModel):
            text: str

            @field_validator("text")
            @classmethod
            def slow(cls, value):
                time.sleep(0.03)
                return value

        class Emit(Node):
            async def forward(self, info):
                return Info({"text": "late"})

        graph = self.graph(timeout=0.01)
        graph.node = Emit(output_schema=Payload)
        with self.assertRaises(NodeTimeoutError):
            await graph.arun([Info("start")])

    async def test_cancellation_is_visible_before_resources_exit(self):
        entered, release, swallowed = asyncio.Event(), asyncio.Event(), asyncio.Event()

        class Stubborn(Node):
            async def forward(self, info):
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    swallowed.set()
                    await release.wait()
                return info

        graph = self.graph()
        graph.node = Stubborn()
        task = asyncio.create_task(graph.arun([Info("start")]))
        second = None
        try:
            await entered.wait()
            task.cancel()
            await swallowed.wait()
            await asyncio.sleep(0)
            events = graph.logs(run_id=graph._last_run_id, limit=1000)
            self.assertEqual(sum(e["event_type"] == "GraphRunCancelled" for e in events), 1)
            second = asyncio.create_task(graph.arun([Info("second")]))
            await asyncio.sleep(0)
            self.assertTrue(second.done(), "cleanup must reject a new run immediately")
            with self.assertRaises(GraphValidationError):
                second.result()
            with self.assertRaises(GraphValidationError):
                graph.node.prompt = "changed"
        finally:
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
            if second is not None:
                await asyncio.gather(second, return_exceptions=True)
        graph.node.prompt = "cleaned"

    async def test_internal_tool_declarations_are_frozen(self):
        entered, release = asyncio.Event(), asyncio.Event()

        class Waiting(Node):
            async def forward(self, info):
                entered.set()
                await release.wait()
                return info

        tool = ReadTool(tool_name="read")
        graph = self.graph(allowed_tools={"read"})
        graph.node = Waiting(tools=[tool])
        task = asyncio.create_task(graph.arun([Info("start")]))
        try:
            await entered.wait()
            with self.assertRaises(GraphValidationError):
                tool.tool_name = "changed"
            with self.assertRaises(GraphValidationError):
                tool.side_effect = True
            with self.assertRaises(GraphValidationError):
                tool.required_permissions = {"write"}
            with self.assertRaises(GraphValidationError):
                tool.input_schema = str
            with self.assertRaises(GraphValidationError):
                del tool.description
        finally:
            release.set()
            await task
        tool.tool_name = "editable"

    async def test_shared_tool_stays_frozen_until_every_consumer_exits(self):
        entries = [asyncio.Event(), asyncio.Event()]
        releases = [asyncio.Event(), asyncio.Event()]

        class Waiting(Node):
            def __init__(self, index, tool):
                super().__init__(tools=[tool])
                self.index = index

            async def forward(self, info):
                entries[self.index].set()
                await releases[self.index].wait()
                return info

        tool = ReadTool(tool_name="read")
        graphs = [self.graph(allowed_tools={"read"}) for _ in range(2)]
        for index, graph in enumerate(graphs):
            graph.node = Waiting(index, tool)
        tasks = [asyncio.create_task(graph.arun([Info("start")])) for graph in graphs]
        try:
            await asyncio.gather(*(event.wait() for event in entries))
            releases[0].set()
            await tasks[0]
            with self.assertRaises(GraphValidationError):
                tool.tool_name = "changed"
            releases[1].set()
            await tasks[1]
        finally:
            for event in releases:
                event.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.assertIsNone(tool.owner)
        tool.tool_name = "editable"

    async def test_repeat_cancellation_retains_cleanup_lease(self):
        entered, swallowed, release, exited = (asyncio.Event() for _ in range(4))

        class Stubborn(Node):
            async def forward(self, info):
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    swallowed.set()
                    await release.wait()
                finally:
                    exited.set()
                return info

        tool = ReadTool(tool_name="read")
        graph = self.graph(allowed_tools={"read"})
        graph.node = Stubborn(tools=[tool])
        task = asyncio.create_task(graph.arun([Info("start")]))
        try:
            await entered.wait()
            task.cancel()
            await swallowed.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(exited.is_set())
            with self.assertRaises(GraphValidationError):
                graph.node.prompt = "early"
            with self.assertRaises(GraphValidationError):
                tool.tool_name = "early"
            with self.assertRaises(GraphValidationError):
                await graph.arun([Info("again")])
            events = graph.logs(run_id=graph._last_run_id, limit=1000)
            self.assertEqual(sum(e["event_type"] == "GraphRunCancelled" for e in events), 1)
        finally:
            release.set()
            await exited.wait()
            for _ in range(10):
                if not graph._cleanups:
                    break
                await asyncio.sleep(0)
        graph.node.prompt = "clean"
        tool.tool_name = "clean"

    async def test_terminal_log_failure_does_not_release_active_resources(self):
        entered, swallowed, release = (asyncio.Event() for _ in range(3))

        class Stubborn(Node):
            async def forward(self, info):
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    swallowed.set()
                    await release.wait()
                return info

        graph = self.graph()
        graph.node = Stubborn()
        append = graph._event_store.append

        def broken_append(run_id, event_type, **kwargs):
            if event_type == "GraphRunCancelled":
                raise RuntimeError("controlled log failure")
            return append(run_id, event_type, **kwargs)

        with patch.object(graph._event_store, "append", broken_append):
            task = asyncio.create_task(graph.arun([Info("start")]))
            try:
                await entered.wait()
                task.cancel()
                await swallowed.wait()
                with self.assertRaises(GraphValidationError):
                    graph.node.prompt = "early"
                with self.assertRaises(GraphValidationError):
                    await graph.arun([Info("again")])
            finally:
                release.set()
                from easy_agent import EasyAgentError
                with self.assertRaises(EasyAgentError):
                    await task
        graph.node.prompt = "clean"

    async def test_editor_context_is_not_shared_by_async_tasks(self):
        tool = ReadTool(tool_name="read")
        graph = self.graph(allowed_tools={"read"})
        graph.node = Identity(tools=[tool])
        other = self.graph(allowed_tools={"read"})
        other.node = Identity(tools=[tool])

        async def foreign_editor():
            with self.assertRaises(GraphValidationError):
                graph.node.prompt = "foreign"
            with self.assertRaises(GraphValidationError):
                graph.to_dict()
            with self.assertRaises(GraphValidationError):
                await graph.arun([Info("start")])
            with self.assertRaises(GraphValidationError):
                tool.tool_name = "foreign"
            with self.assertRaises(GraphValidationError):
                other.validate()
            with self.assertRaises(GraphValidationError):
                await other.arun([Info("start")])

        with graph.edit():
            graph.node.prompt = "owner"
            await asyncio.create_task(foreign_editor())
        self.assertEqual(graph.node.prompt, "owner")
        self.assertEqual(tool.tool_name, "read")
        other.validate()

    async def test_downstream_wait_does_not_extend_upstream_attempt(self):
        clock = [0.0]

        class Later(Node):
            async def forward(self, info):
                clock[0] += 0.2
                await asyncio.sleep(0)
                return info

        # 0.1s keeps the attempt above this host's 15.625ms event-loop clock
        # resolution; the controlled clock still proves the ordering claim.
        graph = self.graph(timeout=0.1)
        graph.source = Identity()
        graph.target = Later(timeout=1)
        graph.source.send_to(graph.target)
        with patch("easy_agent.graph.perf_counter", side_effect=lambda: clock[0]):
            self.assertEqual(((await graph.arun([Info("ok")]))[0]).text, "ok")
        self.assertGreater(clock[0], graph._graph_defaults["timeout"])

    async def test_candidate_conversion_cannot_commit_during_run(self):
        conversion_started, resume = threading.Event(), threading.Event()
        entered, release = asyncio.Event(), asyncio.Event()
        errors = []

        class PausedList(list):
            def __iter__(self):
                conversion_started.set()
                resume.wait(2)
                return super().__iter__()

        class Waiting(Node):
            async def forward(self, info):
                entered.set()
                await release.wait()
                return info

        graph = self.graph()
        graph.node = Waiting()

        def assign():
            try:
                graph.node.tools = PausedList([ReadTool()])
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=assign)
        thread.start()
        self.assertTrue(await asyncio.to_thread(conversion_started.wait, 2))
        task = asyncio.create_task(graph.arun([Info("start")]))
        try:
            await entered.wait()
            resume.set()
            await asyncio.to_thread(thread.join, 2)
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], GraphValidationError)
            self.assertEqual(graph.node.tools, ())
        finally:
            resume.set()
            release.set()
            await task
            thread.join(2)

    async def test_completion_order_and_delivery_order_agree(self):
        class Emit(Node):
            async def forward(self, info):
                return Info(self.name)

        graph = self.graph(max_concurrency=2)
        graph.source = Identity()
        graph.left, graph.right, graph.join = Emit(), Emit(), Identity()
        graph.source.send_to(graph.left, graph.right)
        graph.left.send_to(graph.join)
        graph.right.send_to(graph.join)
        graph.join.receive(mode="all", sources=[graph.left, graph.right])
        original_wait = asyncio.wait

        class ReverseDoneSet(set):
            def __iter__(self):
                return iter(sorted(set.__iter__(self), key=lambda task: task.result().output.text, reverse=True))

        async def controlled_wait(tasks, **kwargs):
            # A valid unordered set whose iteration deliberately opposes the
            # legacy success event order; a coordinator must not rely on it.
            if len(tasks) > 1:
                done, pending = await original_wait(tasks)
                return ReverseDoneSet(done), pending
            return await original_wait(tasks, **kwargs)

        with patch.object(asyncio, "wait", controlled_wait):
            result = (await graph.arun([Info("start")]))[0]
        events = graph._event_store.query(run_id=result.run_id, limit=1000)
        branches = {graph.left.id, graph.right.id}
        successes = [e["node_id"] for e in events if e["event_type"] == "NodeSucceeded" and e["node_id"] in branches]
        deliveries = [e["node_id"] for e in events if e["event_type"] == "InfoDeliveryRequested" and e["node_id"] in branches]
        self.assertEqual(successes, deliveries)
        sequences = [e["data"]["completion_sequence"] for e in events if e["event_type"] == "NodeSucceeded"]
        self.assertEqual(sequences, sorted(sequences))
        received = [e["data"]["reception_sequence"] for e in events if e["event_type"] == "InfoDeliveryReceived"]
        self.assertEqual(received, list(range(1, len(received) + 1)))

    async def test_reverse_notifications_are_delivered_in_observed_order(self):
        entered = [asyncio.Event(), asyncio.Event()]
        released = [asyncio.Event(), asyncio.Event()]

        class Emit(Node):
            def __init__(self, index):
                super().__init__()
                self.index = index

            async def forward(self, info):
                entered[self.index].set()
                await released[self.index].wait()
                return Info(self.name)

        graph = self.graph(max_concurrency=2)
        graph.source = Identity()
        graph.left, graph.right, graph.join = Emit(0), Emit(1), Identity()
        graph.source.send_to(graph.left, graph.right)
        graph.left.send_to(graph.join)
        graph.right.send_to(graph.join)
        graph.join.receive(mode="all", sources=[graph.left, graph.right])
        task = asyncio.create_task(graph.arun([Info("start")]))
        try:
            await asyncio.gather(*(event.wait() for event in entered))
            released[1].set()
            for _ in range(20):
                deliveries = graph.logs(event_type="InfoDeliveryRequested", limit=1000)
                if any(e["node_id"] == graph.right.id for e in deliveries):
                    break
                await asyncio.sleep(0)
            self.assertTrue(any(e["node_id"] == graph.right.id for e in deliveries))
            released[0].set()
            result = (await task)[0]
        finally:
            for event in released:
                event.set()
            await asyncio.gather(task, return_exceptions=True)
        events = graph.logs(run_id=result.run_id, event_type="InfoDeliveryRequested", limit=1000)
        order = [e["node_id"] for e in events if e["node_id"] in {graph.left.id, graph.right.id}]
        self.assertEqual(order, [graph.right.id, graph.left.id])
        self.assertEqual([item for item in result.data], ["left", "right"])

    async def test_failure_observed_first_blocks_same_batch_success(self):
        entered = [asyncio.Event(), asyncio.Event()]
        release = asyncio.Event()

        class Fail(Node):
            async def forward(self, info):
                entered[0].set()
                await release.wait()
                raise ValueError("first notification fails")

        class Emit(Node):
            async def forward(self, info):
                entered[1].set()
                await release.wait()
                return Info("late")

        graph = self.graph(max_concurrency=2)
        graph.source = Identity()
        graph.left, graph.right, graph.join = Fail(), Emit(), Identity()
        graph.source.send_to(graph.left, graph.right)
        graph.left.send_to(graph.join)
        graph.right.send_to(graph.join)
        graph.join.receive(mode="all", sources=[graph.left, graph.right])
        task = asyncio.create_task(graph.arun([Info("start")]))
        try:
            await asyncio.gather(*(event.wait() for event in entered))
        finally:
            release.set()
        with self.assertRaises(NodeExecutionError) as raised:
            await task
        events = graph._event_store.query(run_id=raised.exception.run_id, limit=1000)
        self.assertFalse(any(e["event_type"] == "NodeSucceeded" and e["node_id"] == graph.right.id for e in events))
        self.assertEqual(sum(e["event_type"] == "GraphRunFailed" for e in events), 1)

    async def test_cancellation_only_cleans_its_run(self):
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        class Waiting(Node):
            async def forward(self, info):
                calls.append(info.text)
                if len(calls) == 2:
                    entered.set()
                await release.wait()
                return info

        graph = self.graph()
        graph.node = Waiting()
        tasks = [asyncio.create_task(graph.arun([Info(text)])) for text in ("cancel", "keep")]
        try:
            await entered.wait()
            tasks[0].cancel()
            with self.assertRaises(asyncio.CancelledError):
                await tasks[0]
            self.assertFalse(tasks[1].done())
            release.set()
            result = (await tasks[1])[0]
            self.assertEqual(result.text, "keep")
            self.assertEqual(len(graph.logs(run_id=result.run_id, event_type="GraphRunCancelled")), 0)
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
