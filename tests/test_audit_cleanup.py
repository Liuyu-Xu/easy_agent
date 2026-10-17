"""Regression for declaration isolation and bounded runtime bookkeeping."""

import asyncio
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from easy_agent import Graph, GraphValidationError, Info, Node, NodeExecutionError, OwnershipError, Tool
from easy_agent.config import read_selected_env


class Echo(Node):
    def forward(self, info):
        return info


class DefinitionCleanupTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def graph(self, **options):
        graph = Graph(log_dir=self.root, **options)
        graph.node = Echo()
        return graph

    def test_rejected_foreign_registration_and_replacement_cannot_rollback_owner(self):
        for operation in ('register', 'replace'):
            with self.subTest(operation=operation):
                graph, other = self.graph(), self.graph()
                other.node.prompt = 'before'
                with self.assertRaisesRegex(RuntimeError, 'rollback'):
                    with graph.edit():
                        with self.assertRaises(OwnershipError):
                            if operation == 'register':
                                graph.foreign = other.node
                            else:
                                graph.replace_node('node', other.node)
                        self.assertIsNone(other.node._definition_editor)
                        other.node.prompt = 'legitimate update'
                        raise RuntimeError('rollback')
                self.assertEqual(other.node.prompt, 'legitimate update')
                self.assertIs(other.node.owner, other)
                graph.validate()

    def test_independent_graph_can_query_and_run_inside_another_graph_edit(self):
        graph, other = self.graph(), self.graph()
        done = threading.Event()
        results = []

        def work():
            try:
                other.to_dict()
                results.append(other.run([Info('independent')])[0].text)
            except Exception as error:
                results.append(type(error).__name__)
            finally:
                done.set()

        thread = threading.Thread(target=work)
        try:
            with graph.edit():
                thread.start()
                self.assertTrue(done.wait(2), 'edit must not retain the global lock across user code')
        finally:
            thread.join(2)
        self.assertEqual(results, ['independent'])

    def test_node_business_cache_named_nodes_does_not_change_framework_role(self):
        graph = self.graph()
        graph.node._nodes = {'cached': True}
        graph.target = Echo()
        graph.node.send_to(graph.target)
        self.assertEqual(graph.node.downstreams()[0]['target_id'], graph.target.id)
        with graph.edit():
            graph.node.prompt = 'updated'
        self.assertEqual(graph.run([Info('ok')])[0].text, 'ok')

    def test_invalid_receive_source_has_clear_error_and_preserves_previous_rule(self):
        graph = self.graph()
        graph.source = Echo()
        graph.node.receive(mode='all', sources=[graph.source])
        before = graph.node._receive_rule
        with self.assertRaises(TypeError):
            graph.node.set_receive(mode='all', sources=[object()])
        self.assertIs(graph.node._receive_rule, before)

    def test_success_uses_runtime_steps_without_reading_log_file(self):
        graph = self.graph()
        graph.target = Echo()
        graph.node.send_to(graph.target)
        with patch.object(graph._event_store, 'query', side_effect=AssertionError('unexpected log read')):
            output = graph.run([Info('ok')])[0]
        success = graph.logs(run_id=output.run_id, event_type='GraphRunSucceeded')
        self.assertEqual(success[0]['data']['steps'], 2)

    def test_env_parser_keeps_literal_quoted_and_empty_values(self):
        path = self.root / 'literal.env'
        path.write_text('export FIRST="line\\nnext"\nSECOND=\'${FIRST}\'\nEMPTY=\nBARE\n'
                        'IGNORED=unused\n', encoding='utf-8')
        self.assertEqual(read_selected_env(path, {'FIRST', 'SECOND', 'EMPTY', 'BARE'}),
                         {'FIRST': 'line\nnext', 'SECOND': '${FIRST}', 'EMPTY': ''})


class RuntimeCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    async def test_shared_tool_edit_conflict_does_not_break_success_failure_or_cancellation(self):
        for outcome in ('success', 'failure', 'cancel'):
            with self.subTest(outcome=outcome):
                entered, release = asyncio.Event(), asyncio.Event()

                class Waiting(Node):
                    async def forward(self, info):
                        entered.set()
                        await release.wait()
                        if outcome == 'failure':
                            raise ValueError('synthetic failure')
                        return info

                tool = Tool(tool_name='shared')
                active = Graph(log_dir=self.root, allowed_tools={'shared'})
                active.node = Waiting(tools=[tool])
                editing = Graph(log_dir=self.root, allowed_tools={'shared'})
                editing.node = Echo(tools=[tool])
                task = asyncio.create_task(active.arun([Info('ok')]))
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    with self.assertRaises(GraphValidationError):
                        with editing.edit():
                            self.fail('leased declarations cannot enter edit')
                    self.assertIsNone(tool._definition_editor)
                    self.assertIsNone(editing._edit_journal)
                    self.assertIsNone(editing.node._definition_editor)
                    if outcome == 'cancel':
                        task.cancel()
                finally:
                    release.set()
                    result = (await asyncio.gather(task, return_exceptions=True))[0]
                if outcome == 'success':
                    self.assertEqual(result[0].text, 'ok')
                else:
                    self.assertIsInstance(result, asyncio.CancelledError if outcome == 'cancel' else NodeExecutionError)
                self.assertEqual(active._active_runs, 0)
                self.assertFalse(active._cleanups)
                self.assertEqual(tool._definition_leases, 0)
                with editing.edit():
                    editing.node.prompt = 'editable after exit'

    async def test_failed_multi_tool_assignment_acquires_no_partial_edit_ownership(self):
        entered, release = asyncio.Event(), asyncio.Event()

        class Waiting(Node):
            async def forward(self, info):
                entered.set()
                await release.wait()
                return info

        leased, available = Tool(tool_name='leased'), Tool(tool_name='available')
        active = Graph(log_dir=self.root, allowed_tools={'leased'})
        active.node = Waiting(tools=[leased])
        editing = Graph(log_dir=self.root)
        editing.node = Echo()
        task = asyncio.create_task(active.arun([Info('ok')]))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            with editing.edit():
                with self.assertRaises(GraphValidationError):
                    editing.node.tools = [leased, available]
                self.assertEqual(editing.node.tools, ())
                self.assertIsNone(available._definition_editor)
                self.assertIsNone(leased._definition_editor)
        finally:
            release.set()
            await task

    async def test_completed_tasks_are_released_during_long_feedback_run(self):
        counts = []

        class CountingGraph(Graph):
            def _commit_outcome(self, state, outcome, sequence):
                counts.append(len(state.owned_tasks))
                return super()._commit_outcome(state, outcome, sequence)

        class Decrement(Node):
            async def forward(self, info):
                remaining = info.get('remaining') - 1
                return Info({'remaining': remaining, 'again': remaining > 0})

        graph = CountingGraph(log_dir=self.root)
        graph.node = Decrement()
        graph.node.route(field='again', cases={True: graph.node, False: graph.END})
        graph.start = [graph.node]
        graph.max_steps = 40
        result = (await graph.arun([Info({'remaining': 40})]))[0]
        self.assertEqual(result.get('remaining'), 0)
        self.assertEqual(len(counts), 40)
        self.assertLessEqual(max(counts), 1)
        self.assertFalse(graph._cleanups)


if __name__ == '__main__':
    unittest.main()
