"""Hub contracts, implicit joins, provenance and concurrent failure isolation."""

import asyncio
import json
import unittest
from unittest.mock import patch

import httpx

from easy_agent import (
    GraphValidationError, HubNode, Info, LoggingError, Node,
    NodeExecutionError, NodeTimeoutError, OwnershipError, UnsupportedFeatureError,
)
from test_involve import Echo, Fixture


class HubFixture(Fixture):
    def assembled(self, *, same_source=False, execution="sequential", target_type=Echo):
        left, right = Echo(), Echo()
        hub = HubNode(
            env_file=self.account(), input_count=2, output_count=2,
            input_keys=["left", "right"], rules=["left content", "right content"],
            timeout=2, max_attempts=1, max_output_tokens=128,
        )
        first, second = target_type(), target_type()
        nodes = [second, right, hub, first, left] if not same_source else [second, hub, first, left]
        graph = self.graph(execution=execution, max_concurrency=4)
        graph.involve(
            nodes=nodes,
            relations=[(left if same_source else right, hub.inputs["right"]),
                       (left, hub.inputs["left"]), (hub.outputs[1], second), (hub.outputs[0], first)],
            start=[left] if same_source else [left, right], end=[second, first],
        )
        return graph, left, right, hub, first, second


class HubTests(HubFixture, unittest.TestCase):
    def test_counts_keys_rules_and_managed_parameters(self):
        base = dict(input_count=2, output_count=2, input_keys=["a", "b"], rules=["A", "B"])
        for change, error in [
            ({"input_count": True}, TypeError), ({"output_count": 0}, ValueError),
            ({"input_count": 3}, ValueError), ({"output_count": 1}, ValueError),
            ({"input_keys": ["a", "a"]}, ValueError), ({"input_keys": [" a", "b"]}, ValueError),
            ({"input_keys": ["", "b"]}, ValueError), ({"rules": [" ", "b"]}, ValueError),
            ({"rules": ("a", "b")}, TypeError), ({"rules": ["a", 1]}, TypeError),
            ({"prompt": "override"}, TypeError), ({"output_schema": dict}, TypeError),
        ]:
            with self.subTest(change=change), self.assertRaises(error):
                HubNode(**{**base, **change})

    def test_declarations_are_copied_and_protected(self):
        keys, rules = ["a"], ["A"]
        hub = HubNode(input_count=1, output_count=1, input_keys=keys, rules=rules)
        keys[0], rules[0] = "b", "B"
        self.assertEqual((hub.input_keys, hub.rules), (("a",), ("A",)))
        for name in ("input_count", "output_count", "input_keys", "rules", "inputs", "outputs"):
            with self.subTest(name=name), self.assertRaises(AttributeError):
                delattr(hub, name)
        for name in ("prompt", "role", "output_schema", "input_schema"):
            with self.subTest(name=name), self.assertRaises(AttributeError):
                setattr(hub, name, None)
        with self.assertRaises(AttributeError):
            hub.input_keys = ["b"]
        with self.assertRaises(ValueError):
            hub.rules = ["one", "two"]
        hub.rules = ["new"]
        self.assertEqual(hub.rules, ("new",))
        ordinary = Node()
        ordinary.rules = "business attribute"
        del ordinary.rules

    def test_invalid_port_references(self):
        hub = HubNode(input_count=1, output_count=1, input_keys=["a"], rules=["A"])
        for collection, key in [(hub.inputs, "missing"), (hub.inputs, 0),
                                (hub.outputs, -1), (hub.outputs, 1), (hub.outputs, True)]:
            with self.subTest(key=key), self.assertRaises(GraphValidationError):
                collection[key]

    def test_invalid_assembly_rolls_back_nodes_and_ports(self):
        for mode in ("missing", "duplicate", "direction", "bare", "foreign", "boundary", "same_target"):
            with self.subTest(mode=mode):
                graph, a, b, out = self.graph(), Echo(), Echo(), Echo()
                hub = HubNode(env_file=self.account(), input_count=1, output_count=2,
                              input_keys=["a"], rules=["A", "B"])
                relations = [(a, hub.inputs["a"]), (hub.outputs[0], b), (hub.outputs[1], out)]
                error, start, end = GraphValidationError, [a], [b, out]
                if mode == "missing":
                    relations.pop()
                elif mode == "duplicate":
                    relations[-1] = (hub.outputs[0], out)
                elif mode == "direction":
                    relations[0] = (hub.inputs["a"], a)
                elif mode == "bare":
                    relations[0] = (a, hub)
                elif mode == "foreign":
                    relations[0], error = (Echo(), hub.inputs["a"]), OwnershipError
                elif mode == "boundary":
                    start, error = [hub], UnsupportedFeatureError
                elif mode == "same_target":
                    relations[-1], error = (hub.outputs[1], b), UnsupportedFeatureError
                with self.assertRaises(error):
                    graph.involve(nodes=[a, b, hub, out], relations=relations, start=start, end=end)
                self.assertEqual(graph.to_dict()["port_relations"], [])
                self.assertEqual(graph.to_dict()["nodes"], [])
                self.assertTrue(all(node.owner is None for node in (a, b, hub, out)))

    def test_implicit_many_to_many_shares_one_info_without_overwriting_fields(self):
        seen = []

        class Capture(Node):
            def forward(self, info):
                seen.append(info)
                return info

        graph, a, b, x, y = self.graph(), Echo(), Echo(), Capture(), Capture()
        graph.involve(nodes=[y, b, x, a],
                      relations=[(a, x), (b, x), (b, y), (a, y)], start=[a, b], end=[y, x])
        outputs = graph.run([Info({"text": "A"}), Info({"text": "B"})])
        self.assertIs(seen[0], seen[1])
        self.assertEqual(seen[0].data[a.name]["text"], "A")
        self.assertEqual(seen[0].data[b.name]["text"], "B")
        joins = graph.logs(event_type="InfoJoined")
        self.assertEqual(len({event["info_ids"][0] for event in joins}), 1)
        self.assertEqual([event["data"]["reused"] for event in joins], [False, True])
        self.assertEqual(len(outputs), 2)

    def test_different_source_sets_stay_separate(self):
        graph = self.graph()
        a, b, c, x, y = [Echo() for _ in range(5)]
        graph.involve(nodes=[a, b, c, x, y],
                      relations=[(a, x), (b, x), (b, y), (c, y)],
                      start=[a, b, c], end=[x, y])
        outputs = graph.run([Info("A"), Info("B"), Info("C")])
        self.assertEqual(dict(outputs[0].data), {a.name: "A", b.name: "B"})
        self.assertEqual(dict(outputs[1].data), {b.name: "B", c.name: "C"})

    def test_named_inputs_prompt_reversed_output_keys_and_provenance(self):
        received = []

        class Capture(Echo):
            def forward(self, info):
                received.append(info)
                return info

        graph, _, _, hub, _, y = self.assembled(target_type=Capture)
        requests = []

        def handler(request):
            requests.append(json.loads(request.content))
            return self.response('{"out_1":"RIGHT","out_0":"LEFT"}')

        with self.transport(handler):
            outputs = graph.run([Info("A"), Info("B")])
        self.assertEqual([output.text for output in outputs], ["RIGHT", "LEFT"])
        self.assertEqual(len(requests), 1)
        messages = requests[0]["messages"]
        contract = json.loads(messages[0]["content"].splitlines()[-1])
        self.assertEqual(contract["input_keys"], ["left", "right"])
        self.assertEqual(contract["outputs"], {"out_0": "left content", "out_1": "right content"})
        self.assertEqual(json.loads(messages[1]["content"]), {"left": "A", "right": "B"})
        prepared = graph.logs(event_type="HubOutputsPrepared")[0]
        deliveries = [event for event in graph.logs(event_type="InfoDeliveryReceived")
                      if event["data"]["source"] == hub.name]
        self.assertEqual([event["data"]["output_port"] for event in deliveries], [1, 0])
        self.assertEqual(outputs[0].parent_ids, (prepared["info_ids"][1],))
        self.assertEqual(outputs[1].parent_ids, (prepared["info_ids"][0],))
        self.assertTrue(all(info.origin_kind == "hub_output"
                            and info.parent_ids == (prepared["data"]["parent_info_id"],)
                            and info.producer_node_id == hub.id
                            and info.producer_execution_id == prepared["node_execution_id"]
                            for info in received))
        self.assertEqual(graph.relations[hub.name][y.name][0]["output_port"], 1)
        self.assertEqual(hub.downstreams()[0]["output_port"], 1)
        self.assertEqual(graph.to_dict()["nodes"][2]["hub"]["input_count"], 2)
        self.assertFalse(graph._active_runs or graph._cleanups)

    def test_one_source_can_fill_two_ports_including_none_data(self):
        graph, _, _, _, _, _ = self.assembled(same_source=True)
        requests = []

        def handler(request):
            requests.append(json.loads(request.content))
            return self.response('{"out_0":"A","out_1":"B"}')

        with self.transport(handler):
            graph.run([Info(None)])
        self.assertEqual(json.loads(requests[0]["messages"][-1]["content"]), {"left": None, "right": None})
        joined = graph.logs(event_type="InfoJoined")[0]
        self.assertEqual(joined["info_ids"][1], joined["info_ids"][2])

    def test_invalid_json_publishes_no_ports_or_downstream_tasks(self):
        seen = []

        class Capture(Echo):
            def forward(self, info):
                seen.append(info)
                return info

        invalid = ['not-json', '[]', '{"out_0":"A"}', '{"out_0":"A","out_1":"B","x":"C"}',
                   '{"out_0":"A","out_1":null}', '{"out_0":"A","out_1":" "}',
                   '{"out_0":"A","out_0":"B","out_1":"C"}',
                   'prefix {"out_0":"A","out_1":"B"}',
                   chr(96) * 3 + 'json\n{"out_0":"A","out_1":"B"}\n' + chr(96) * 3]
        for text in invalid:
            with self.subTest(text=text):
                graph, _, _, _, _, _ = self.assembled(target_type=Capture)
                with self.transport(lambda request: self.response(text)):
                    with self.assertRaises(NodeExecutionError) as caught:
                        graph.run([Info("A"), Info("B")])
                self.assertNotIn(text, str(caught.exception))
                self.assertEqual(graph.logs(event_type="HubOutputsPrepared"), [])
                self.assertEqual(graph.logs(event_type="InfoPublished"), [])
                self.assertFalse(graph._active_runs or graph._cleanups)
        self.assertEqual(seen, [])

    def test_http_errors_hide_response_and_never_retry(self):
        for status in (401, 429, 503):
            graph, _, _, _, _, _ = self.assembled()
            requests = []

            def handler(request):
                requests.append(request)
                return httpx.Response(status, text="synthetic-private-response")

            with self.subTest(status=status), self.transport(handler):
                with self.assertRaises(NodeExecutionError) as caught:
                    graph.run([Info("A"), Info("B")])
            self.assertEqual(len(requests), 1)
            self.assertIn(f"HTTP {status}", str(caught.exception))
            self.assertNotIn("synthetic-private-response", str(caught.exception))

    def test_rule_edits_and_port_linked_replacement_are_atomic(self):
        graph, a, _, hub, _, _ = self.assembled()
        before = graph.to_dict()
        with self.assertRaises(RuntimeError):
            with graph.edit():
                hub.rules = ["changed left", "changed right"]
                raise RuntimeError("abort")
        self.assertEqual(graph.to_dict(), before)
        with self.assertRaises(UnsupportedFeatureError):
            graph.replace_node(a.name, Echo(), migrate_edges=True)
        self.assertEqual(graph.to_dict(), before)
        with self.assertRaises(GraphValidationError):
            delattr(graph, hub.name)

    def test_nested_merges_are_rejected_before_running(self):
        graph = self.graph()
        a, b, c, x, y = [Echo() for _ in range(5)]
        with self.assertRaises(UnsupportedFeatureError):
            graph.involve(nodes=[a, b, c, x, y],
                          relations=[(a, x), (b, x), (x, y), (c, y)],
                          start=[a, b, c], end=[y])
        self.assertEqual(graph.to_dict()["nodes"], [])

    def test_hub_requires_valid_model_configuration(self):
        a, out = Echo(), Echo()
        hub = HubNode(env_file=self.account(), provider="unsupported",
                      input_count=1, output_count=1, input_keys=["a"], rules=["A"])
        with self.assertRaises(UnsupportedFeatureError), self.transport(
                lambda request: self.fail("No request allowed")):
            self.graph().involve(nodes=[a, hub, out],
                                 relations=[(a, hub.inputs["a"]), (hub.outputs[0], out)],
                                 start=[a], end=[out])

    def test_log_failure_after_preparation_cannot_deliver_any_port(self):
        graph, _, _, hub, _, _ = self.assembled()
        original = graph._event_store.append

        def append(run_id, event_type, **values):
            if event_type == "HubOutputsPrepared":
                raise LoggingError("injected log failure")
            return original(run_id, event_type, **values)

        with self.transport(lambda request: self.response('{"out_0":"A","out_1":"B"}')), \
             patch.object(graph._event_store, "append", append):
            with self.assertRaises(LoggingError):
                graph.run([Info("A"), Info("B")])
        self.assertFalse(any(event["data"]["source"] == hub.name
                             for event in graph.logs(event_type="InfoDeliveryReceived")))
        self.assertEqual(graph.logs(event_type="InfoPublished"), [])
        self.assertFalse(graph._active_runs or graph._cleanups)

    def test_truncated_hub_response_has_no_outputs(self):
        graph, _, _, _, _, _ = self.assembled()
        response = httpx.Response(200, json={"choices": [{
            "message": {"content": '{"out_0":"A","out_1":"B"}'}, "finish_reason": "length",
        }]})
        with self.transport(lambda request: response), self.assertRaises(NodeExecutionError):
            graph.run([Info("A"), Info("B")])
        self.assertEqual(graph.logs(event_type="HubOutputsPrepared"), [])


class AsyncHubTests(HubFixture, unittest.IsolatedAsyncioTestCase):
    async def test_hub_timeout_cleans_up_without_output_delivery(self):
        graph, _, _, hub, _, _ = self.assembled(execution="async")
        hub.timeout = 0.02

        async def handler(request):
            await asyncio.sleep(0.2)
            return self.response('{"out_0":"A","out_1":"B"}')

        with self.transport(handler), self.assertRaises(NodeTimeoutError):
            await graph.arun([Info("A"), Info("B")])
        self.assertEqual(graph.logs(event_type="HubOutputsPrepared"), [])
        self.assertFalse(graph._active_runs or graph._cleanups)

    async def test_downstream_failure_cancels_sibling_and_returns_no_partial_result(self):
        started, cancelled = asyncio.Event(), asyncio.Event()

        class Target(Node):
            async def forward(self, info):
                if info.text == "LEFT":
                    await asyncio.wait_for(started.wait(), 2)
                    raise ValueError("injected failure")
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        graph, _, _, _, _, _ = self.assembled(execution="async", target_type=Target)
        with self.transport(lambda request: self.response('{"out_0":"LEFT","out_1":"RIGHT"}')):
            with self.assertRaises(NodeExecutionError):
                await graph.arun([Info("A"), Info("B")])
        self.assertTrue(cancelled.is_set())
        self.assertEqual(graph.logs(event_type="InfoPublished"), [])
        self.assertFalse(graph._active_runs or graph._cleanups)

    async def test_concurrent_runs_keep_inputs_outputs_and_run_ids_separate(self):
        graph, _, _, _, _, _ = self.assembled(execution="async")
        release = asyncio.Event()

        async def handler(request):
            data = json.loads(json.loads(request.content)["messages"][-1]["content"])
            if data["left"] == "A1":
                await asyncio.wait_for(release.wait(), 2)
            else:
                release.set()
            return self.response(json.dumps({"out_0": data["left"], "out_1": data["right"]}))

        with self.transport(handler):
            results = await asyncio.gather(graph.arun([Info("A1"), Info("B1")]),
                                           graph.arun([Info("A2"), Info("B2")]))
        self.assertEqual([[item.text for item in result] for result in results], [["B1", "A1"], ["B2", "A2"]])
        self.assertNotEqual(results[0][0].run_id, results[1][0].run_id)
        self.assertFalse(graph._active_runs or graph._cleanups)

    async def test_cancellation_freezes_rules_and_prevents_output_delivery(self):
        graph, _, _, hub, _, _ = self.assembled(execution="async")
        started = asyncio.Event()

        async def handler(request):
            started.set()
            await asyncio.Event().wait()

        with self.transport(handler):
            task = asyncio.create_task(graph.arun([Info("A"), Info("B")]))
            await asyncio.wait_for(started.wait(), 2)
            with self.assertRaises(GraphValidationError):
                hub.rules = ["changed left", "changed right"]
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(graph.logs(event_type="HubOutputsPrepared"), [])
        self.assertFalse(graph._active_runs or graph._cleanups)
        hub.rules = ["after left", "after right"]

    async def test_upstream_failure_does_not_invoke_hub(self):
        class Fail(Echo):
            def forward(self, info):
                raise RuntimeError("failure")

        graph, a, _, _, _, _ = self.assembled(execution="async")
        with patch.object(a, "forward", Fail().forward), self.transport(
                lambda request: self.fail("Hub must not start")):
            with self.assertRaises(NodeExecutionError):
                await graph.arun([Info("A"), Info("B")])
        self.assertEqual(graph.logs(event_type="HubOutputsPrepared"), [])
        self.assertFalse(graph._active_runs or graph._cleanups)


if __name__ == "__main__":
    unittest.main()
