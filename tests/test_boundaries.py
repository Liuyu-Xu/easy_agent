"""Ordered workflow boundaries, explicit joins and failure semantics; no live calls."""

import asyncio
import inspect
import unittest
from unittest.mock import patch

from easy_agent import Graph, GraphValidationError, Info, Node, NodeExecutionError, OwnershipError, UnsupportedFeatureError
from test_involve import Echo, Fixture


class BoundaryTests(Fixture, unittest.TestCase):
    def test_many_inputs_join_once_with_both_original_values(self):
        seen = []

        class Collect(Node):
            def forward(self, info):
                seen.append(info)
                return info

        a, b, target = Echo(), Echo(), Collect()
        graph = self.graph()
        graph.involve(nodes=[target, b, a], relations=[(a, target), (b, target)], start=[a, b], end=[target])
        target.receive(mode='all', sources=[b, a])
        initial = [Info('LEFT'), Info('RIGHT')]
        outputs = graph.run(initial)
        self.assertEqual(len(seen), 1)
        self.assertEqual(dict(seen[0].data), {b.name: 'RIGHT', a.name: 'LEFT'})
        self.assertEqual(seen[0].parent_ids, (initial[1].id, initial[0].id))
        self.assertEqual(outputs[0].parent_ids, (seen[0].id,))

    def test_output_order_and_passthrough_identity_are_independent_of_node_list(self):
        a, b = Echo(), Echo()
        graph = self.graph()
        graph.involve(nodes=[b, a], relations=[], start=[a, b], end=[b, a])
        initial = [Info('A'), Info('B')]
        outputs = graph.run(initial)
        self.assertEqual([output.text for output in outputs], ['B', 'A'])
        self.assertEqual([output.parent_ids for output in outputs], [(initial[1].id,), (initial[0].id,)])
        self.assertNotIn('entry', graph.to_dict()['graph'])
        self.assertEqual(graph.to_dict()['graph']['start'], [a.name, b.name])
        self.assertEqual(graph.to_dict()['graph']['end'], [b.name, a.name])

    def test_invalid_boundaries_roll_back_every_declaration(self):
        for case in ('empty_start', 'empty_end', 'duplicate', 'foreign', 'bad_type', 'missing_root', 'missing_end'):
            with self.subTest(case=case):
                graph, a, b, foreign = self.graph(), Echo(), Echo(), Echo()
                start, end = [a, b], [a, b]
                if case == 'empty_start': start = []
                if case == 'empty_end': end = []
                if case == 'duplicate': start = [a, a]
                if case == 'foreign': end = [foreign]
                if case == 'bad_type': start = (a, b)
                if case == 'missing_root': start = [a]
                if case == 'missing_end': end = [a]
                with self.assertRaises((GraphValidationError, OwnershipError, TypeError)):
                    graph.involve(nodes=[a, b], relations=[], start=start, end=end)
                self.assertEqual(graph.to_dict()['nodes'], [])
                self.assertIsNone(a.owner)
                self.assertIsNone(b.owner)
                self.assertIsNone(graph.start)
                self.assertIsNone(graph.end)

    def test_input_validation_precedes_any_model_request(self):
        graph = self.graph()
        node = Node(env_file=self.account())
        graph.involve(nodes=[node], relations=[], start=[node], end=[node])
        for value, error in ((Info('old form'), TypeError), (['raw'], TypeError),
                             ([], GraphValidationError), ([Info('a'), Info('b')], GraphValidationError)):
            with self.subTest(value_type=type(value).__name__), patch('easy_agent._api._client') as factory:
                with self.assertRaises(error):
                    graph.run(value)
                factory.assert_not_called()

    def test_multiple_incoming_edges_join_automatically_once(self):
        graph, a, b, target = self.graph(), Echo(), Echo(), Echo()
        graph.involve(nodes=[a, b, target], relations=[(a, target), (b, target)], start=[a, b], end=[target])
        outputs = graph.run([Info('A'), Info('B')])
        self.assertEqual(dict(outputs[0].data), {a.name: 'A', b.name: 'B'})
        self.assertEqual(len(graph.logs(event_type='InfoJoined')), 1)

    def test_explicit_boundaries_reject_cycles_and_conditional_routes(self):
        graph, a, b = self.graph(), Echo(), Echo()
        with self.assertRaises(UnsupportedFeatureError):
            graph.involve(nodes=[a, b], relations=[(a, b), (b, a)], start=[a], end=[b])
        graph.involve(nodes=[a, b], relations=[(a, b)], start=[a], end=[b])
        a.set_downstreams([])
        a.route(field='done', cases={True: b}, default=graph.END)
        with self.assertRaises(UnsupportedFeatureError):
            graph.run([Info({'done': True})])

    def test_boundary_lists_are_copied_and_replacement_migrates_both(self):
        graph, a = self.graph(), Echo()
        start, end = [a], [a]
        graph.involve(nodes=[a], relations=[], start=start, end=end)
        start.clear()
        end.clear()
        graph.start.clear()
        graph.end.clear()
        replacement = Echo()
        graph.replace_node(a.name, replacement, migrate_edges=True)
        self.assertEqual(graph.start, [replacement])
        self.assertEqual(graph.end, [replacement])
        self.assertEqual(graph.run([Info('ok')])[0].text, 'ok')
        self.assertNotIn('entry', inspect.signature(Graph.involve).parameters)
        with self.assertRaises(AttributeError):
            graph.entry = replacement

    def test_start_cannot_also_receive_an_edge_in_a_dag(self):
        graph = self.graph()
        graph.a, graph.b = Echo(), Echo()
        graph.a.send_to(graph.b)
        graph.start = [graph.a, graph.b]
        with self.assertRaises(GraphValidationError):
            graph.run([Info('a'), Info('b')])
        self.assertEqual(graph.logs(event_type='GraphRunStarted'), [])


class AsyncBoundaryTests(Fixture, unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_multi_input_runs_do_not_mix_join_groups(self):
        class Yielding(Node):
            async def forward(self, info):
                await asyncio.sleep(0)
                return info

        graph, a, b, target = self.graph(execution='async', max_concurrency=2), Yielding(), Yielding(), Echo()
        graph.involve(nodes=[target, b, a], relations=[(a, target), (b, target)], start=[a, b], end=[target])
        target.receive(mode='all', sources=[a, b])
        results = await asyncio.gather(graph.arun([Info('A1'), Info('B1')]),
                                       graph.arun([Info('A2'), Info('B2')]))
        self.assertEqual([dict(outputs[0].data) for outputs in results],
                         [{a.name: 'A1', b.name: 'B1'}, {a.name: 'A2', b.name: 'B2'}])
        self.assertNotEqual(results[0][0].run_id, results[1][0].run_id)

    async def test_reversed_completion_still_returns_declared_end_order(self):
        left_ready, release, committed = asyncio.Event(), asyncio.Event(), asyncio.Event()

        class Left(Node):
            async def forward(self, info):
                left_ready.set()
                await release.wait()
                return Info('LEFT')

        class Right(Node):
            async def forward(self, info):
                await left_ready.wait()
                return Info('RIGHT')

        graph, source, left, right = self.graph(execution='async', max_concurrency=2), Echo(), Left(), Right()
        graph.involve(nodes=[left, right, source], relations=[(source, left), (source, right)],
                      start=[source], end=[left, right])
        append = graph._event_store.append

        def observe(run_id, event_type, **options):
            event = append(run_id, event_type, **options)
            if event_type == 'NodeSucceeded' and options.get('node_id') == right.id:
                committed.set()
            return event

        with patch.object(graph._event_store, 'append', observe):
            task = asyncio.create_task(graph.arun([Info('start')]))
            try:
                await asyncio.wait_for(committed.wait(), 2)
            finally:
                release.set()
            outputs = await task
        self.assertEqual([output.text for output in outputs], ['LEFT', 'RIGHT'])
        finished = [event['node_id'] for event in graph._event_store.query(
                    run_id=outputs[0].run_id, event_type='NodeSucceeded')
                    if event['node_id'] in {left.id, right.id}]
        self.assertEqual(finished, [right.id, left.id])

    async def test_input_list_mutation_does_not_change_an_active_run(self):
        entered, release = asyncio.Event(), asyncio.Event()

        class Waiting(Node):
            async def forward(self, info):
                entered.set()
                await release.wait()
                return info

        graph, node = self.graph(), Waiting()
        graph.involve(nodes=[node], relations=[], start=[node], end=[node])
        inputs = [Info('original')]
        task = asyncio.create_task(graph.arun(inputs))
        await entered.wait()
        inputs[:] = [Info('changed')]
        release.set()
        self.assertEqual((await task)[0].text, 'original')

    async def test_branch_failure_publishes_no_partial_results(self):
        finished = asyncio.Event()

        class Success(Node):
            async def forward(self, info):
                finished.set()
                return Info('ok')

        class Failure(Node):
            async def forward(self, info):
                await finished.wait()
                raise ValueError('synthetic branch failure')

        graph, a, b = self.graph(execution='async', max_concurrency=2), Success(), Failure()
        graph.involve(nodes=[a, b], relations=[], start=[a, b], end=[a, b])
        with self.assertRaises(NodeExecutionError):
            await graph.arun([Info('A'), Info('B')])
        self.assertEqual(graph.logs(event_type='InfoPublished'), [])
        self.assertFalse(graph._active_runs or graph._cleanups)
