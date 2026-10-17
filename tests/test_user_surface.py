"""Public workflow, configuration and invocation-ownership regression (no live calls)."""

import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx

from easy_agent import (
    ConfigurationError, Graph, GraphValidationError, Info, LoggingError, Node,
    NodeExecutionError, NodeTimeoutError, OwnershipError, ResultError, Tool,
)
from easy_agent.config import read_selected_env


class Echo(Node):
    def forward(self, info):
        return info


class SurfaceFixture:
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.environment = patch.dict(os.environ, {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def graph(self, **options):
        return Graph(log_dir=self.root / 'logs', **options)


class PublicWorkflowTests(SurfaceFixture, unittest.TestCase):
    def test_two_node_workflow_returns_info_and_keeps_provenance(self):
        inputs = []

        class First(Node):
            def forward(self, info):
                inputs.append(('first', info.text))
                return Info(info.text + '-A')

        class Second(Node):
            def forward(self, info):
                inputs.append(('second', info.text))
                return Info(info.text + '-B')

        graph = self.graph()
        graph.first, graph.second = First(), Second()
        graph.first.send_to(graph.second)
        output = graph.run([Info('input')])[0]
        self.assertIsInstance(output, Info)
        self.assertEqual(output.text, 'input-A-B')
        self.assertEqual(inputs, [('first', 'input'), ('second', 'input-A')])
        self.assertEqual(output.origin_kind, 'graph_output')
        self.assertEqual(len(output.parent_ids), 1)
        self.assertEqual(len(graph.logs(run_id=output.run_id, event_type='GraphRunSucceeded')), 1)

    def test_result_reader_and_its_cache_are_removed(self):
        graph = self.graph()
        graph.echo = Echo()
        output = graph.run([Info('value')])[0]
        self.assertEqual(output.text, 'value')
        self.assertFalse(hasattr(Graph, 'result'))
        self.assertFalse(hasattr(graph, 'result'))
        self.assertNotIn('_latest_result', vars(graph))

    def test_repeat_and_legacy_call_return_distinct_owned_results(self):
        graph = self.graph()
        graph.echo = Echo()
        first = graph.run([Info('first')])[0]
        second = graph([Info('second')])[0]
        self.assertEqual((first.text, second.text), ('first', 'second'))
        self.assertNotEqual(first.run_id, second.run_id)

    def test_invalid_input_raises_and_does_not_change_owned_output(self):
        graph = self.graph()
        graph.echo = Echo()
        old = graph.run([Info('old')])[0]
        with self.assertRaises(TypeError):
            graph.run('invalid')
        self.assertEqual(old.text, 'old')
        self.assertEqual(graph._active_runs, 0)

    def test_business_failure_has_its_own_run_id(self):
        class SometimesFails(Node):
            def forward(self, info):
                if info.text == 'fail':
                    raise ValueError('synthetic failure')
                return info

        graph = self.graph()
        graph.node = SometimesFails()
        old = graph.run([Info('success')])[0]
        with self.assertRaises(NodeExecutionError) as failed:
            graph.run([Info('fail')])
        self.assertEqual(old.text, 'success')
        self.assertNotEqual(old.run_id, failed.exception.run_id)
        self.assertEqual(len(graph.logs(run_id=failed.exception.run_id, event_type='GraphRunFailed')), 1)

    def test_success_log_failure_raises_without_returning_old_output(self):
        graph = self.graph()
        graph.echo = Echo()
        old = graph.run([Info('old')])[0]
        append = graph._event_store.append

        def fail_success(run_id, event_type, **options):
            if event_type == 'GraphRunSucceeded':
                raise LoggingError('synthetic log failure')
            return append(run_id, event_type, **options)

        with patch.object(graph._event_store, 'append', fail_success):
            with self.assertRaises(LoggingError) as failed:
                graph.run([Info('new')])
        self.assertNotEqual(failed.exception.run_id, old.run_id)
        self.assertEqual(old.text, 'old')
        self.assertEqual(graph._active_runs, 0)
        self.assertFalse(graph._cleanups)

    def test_reserved_methods_cannot_be_shadowed_or_bind_candidate(self):
        graph = self.graph()
        for name in ('run', 'arun', 'involve'):
            candidate = Echo()
            with self.subTest(name=name):
                with self.assertRaises(OwnershipError):
                    setattr(graph, name, candidate)
                self.assertIsNone(candidate.owner)
                with self.assertRaises(OwnershipError):
                    setattr(graph, name, 'shadow')
                self.assertTrue(callable(getattr(graph, name)))

    def test_multiple_unjoined_outputs_raise(self):
        graph = self.graph()
        graph.source, graph.left, graph.right = Echo(), Echo(), Echo()
        graph.source.send_to(graph.left, graph.right)
        with self.assertRaises((GraphValidationError, ResultError)):
            graph.run([Info('ambiguous')])


class AsyncWorkflowTests(SurfaceFixture, unittest.IsolatedAsyncioTestCase):
    async def test_pending_call_returns_only_after_completion(self):
        entered, release = asyncio.Event(), asyncio.Event()

        class Waiting(Node):
            async def forward(self, info):
                entered.set()
                await release.wait()
                return info

        graph = self.graph()
        graph.waiting = Waiting()
        task = asyncio.create_task(graph.arun([Info('value')]))
        await entered.wait()
        try:
            self.assertFalse(task.done())
        finally:
            release.set()
        self.assertEqual((await task)[0].text, 'value')
        self.assertEqual(graph._active_runs, 0)

    async def test_reversed_completions_return_each_calls_output(self):
        entered, release = asyncio.Event(), asyncio.Event()

        class Ordered(Node):
            async def forward(self, info):
                if info.text == 'old':
                    entered.set()
                    await release.wait()
                return info

        graph = self.graph()
        graph.node = Ordered()
        old_task = asyncio.create_task(graph.arun([Info('old')]))
        await entered.wait()
        try:
            new = (await graph.arun([Info('new')]))[0]
        finally:
            release.set()
        old = (await old_task)[0]
        self.assertEqual((old.text, new.text), ('old', 'new'))
        self.assertNotEqual(old.run_id, new.run_id)

    async def test_concurrent_failure_keeps_each_calls_outcome_and_run_id(self):
        for old_fails in (False, True):
            with self.subTest(old_fails=old_fails):
                entered, release = asyncio.Event(), asyncio.Event()

                class Fails(Node):
                    async def forward(self, info):
                        if info.text == 'old':
                            entered.set()
                            await release.wait()
                            if not old_fails:
                                return info
                        raise ValueError('synthetic failure')

                graph = self.graph()
                graph.node = Fails()
                initial = [Info('old'), Info('new')]
                old_task = asyncio.create_task(graph.arun([initial[0]]))
                await entered.wait()
                try:
                    with self.assertRaises(NodeExecutionError) as new_failure:
                        await graph.arun([initial[1]])
                finally:
                    release.set()
                if old_fails:
                    with self.assertRaises(NodeExecutionError) as old_failure:
                        await old_task
                    old_id = old_failure.exception.run_id
                else:
                    output = (await old_task)[0]
                    self.assertEqual(output.text, 'old')
                    old_id = output.run_id
                ids = [old_id, new_failure.exception.run_id]
                self.assertNotEqual(*ids)
                for index, (run_id, info) in enumerate(zip(ids, initial)):
                    consumed = graph.logs(run_id=run_id, event_type='InfoConsumed')
                    self.assertEqual(consumed[0]['info_ids'], [info.id])
                    terminal = 'GraphRunFailed' if index or old_fails else 'GraphRunSucceeded'
                    self.assertEqual(len(graph.logs(run_id=run_id, event_type=terminal)), 1)
                self.assertEqual(graph._active_runs, 0)

    async def test_cancellation_preserves_owned_output_and_cleans_up(self):
        entered = asyncio.Event()

        class Waiting(Node):
            async def forward(self, info):
                if info.text == 'wait':
                    entered.set()
                    await asyncio.Event().wait()
                return info

        graph = self.graph()
        graph.node = Waiting()
        old = (await graph.arun([Info('old')]))[0]
        task = asyncio.create_task(graph.arun([Info('wait')]))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(old.text, 'old')
        self.assertEqual(graph._active_runs, 0)
        self.assertFalse(graph._cleanups)

    async def test_deadline_raises_and_cleans_up(self):
        class Waiting(Node):
            async def forward(self, info):
                await asyncio.Event().wait()
                return info

        graph = self.graph(timeout=0.02)
        graph.node = Waiting()
        with self.assertRaises(NodeTimeoutError) as failure:
            await graph.arun([Info('timeout')])
        self.assertIsNotNone(failure.exception.run_id)
        self.assertEqual(graph._active_runs, 0)
        self.assertFalse(graph._cleanups)

    async def test_sync_entry_inside_event_loop_raises(self):
        graph = self.graph()
        graph.node = Echo()
        old = (await graph.arun([Info('old')]))[0]
        with self.assertRaisesRegex(RuntimeError, 'arun'):
            graph.run([Info('new')])
        self.assertEqual(old.text, 'old')
        self.assertEqual(graph._active_runs, 0)

    async def test_edit_admission_failure_raises(self):
        graph = self.graph()
        graph.node = Echo()
        old = (await graph.arun([Info('old')]))[0]
        with graph.edit():
            with self.assertRaises(GraphValidationError):
                await graph.arun([Info('new')])
        self.assertEqual(old.text, 'old')
        self.assertEqual(graph._active_runs, 0)


class DeclarationContractTests(SurfaceFixture, unittest.TestCase):
    def test_framework_fields_cannot_be_deleted_but_business_fields_can(self):
        for node in (Echo(), Tool()):
            before = dict(vars(node))
            for name in before:
                with self.subTest(node=type(node).__name__, field=name):
                    with self.assertRaises(AttributeError):
                        delattr(node, name)
            self.assertEqual(vars(node), before)
            node.cache = 'temporary'
            del node.cache
            self.assertFalse(hasattr(node, 'cache'))
            node.tools = ()
            node.model = None
            self.assertEqual(node.tools, ())

    def test_rejected_deletion_preserves_binding_edit_and_execution(self):
        graph = self.graph()
        graph.node = Echo()
        with graph.edit():
            with self.assertRaises(AttributeError):
                del graph.node.tools
            graph.node.prompt = 'updated'
        self.assertIs(graph.node.owner, graph)
        self.assertEqual(graph.run([Info('ok')])[0].text, 'ok')

    def test_graph_and_nodes_share_finite_number_and_string_checks(self):
        for constructor in (lambda **options: self.graph(**options), Node, Echo):
            for field in ('timeout', 'temperature'):
                for value in (float('nan'), float('inf'), -float('inf'), 10 ** 400):
                    with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                        constructor(**{field: value})
                for value in (True, '1', object()):
                    with self.subTest(field=field), self.assertRaises(TypeError):
                        constructor(**{field: value})
            for field in ('model', 'provider', 'base_url', 'api_key_env'):
                with self.subTest(field=field), self.assertRaises(TypeError):
                    constructor(**{field: 123})
            for value in (0, -1):
                with self.subTest(timeout=value), self.assertRaises(ValueError):
                    constructor(timeout=value)

    def test_invalid_assignment_keeps_previous_values(self):
        node = Echo(timeout=2, temperature=0.5, model='valid')
        for field, value, error in (
            ('timeout', float('nan'), ValueError), ('timeout', 0, ValueError),
            ('timeout', True, TypeError), ('temperature', float('inf'), ValueError),
            ('model', [], TypeError), ('prompt', {}, TypeError),
            ('max_output_tokens', 0, ValueError), ('max_parallelism', True, TypeError),
        ):
            previous = getattr(node, field)
            with self.subTest(field=field), self.assertRaises(error):
                setattr(node, field, value)
            self.assertEqual(getattr(node, field), previous)
        node.timeout = None
        self.assertIsNone(node.timeout)

    def test_provider_ranges_are_not_applied_to_custom_forward(self):
        graph = self.graph(temperature=3)
        graph.node = Echo(provider='custom', temperature=-1)
        self.assertEqual(graph.run([Info('ok')])[0].text, 'ok')


class ConfigurationSurfaceTests(SurfaceFixture, unittest.TestCase):
    def env_file(self, text):
        path = self.root / 'settings.env'
        path.write_text(text, encoding='utf-8')
        return path

    def call(self, graph):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(200, json={
                'model': 'reported',
                'choices': [{'message': {'content': 'OK'}, 'finish_reason': 'stop'}],
            })

        def client():
            return httpx.AsyncClient(transport=httpx.MockTransport(handler))

        graph.node = Node(prompt='test')
        with patch('easy_agent._api._client', client):
            output = graph.run([Info('input')])[0]
        self.assertEqual(output.text, 'OK')
        return requests

    def test_standard_and_llm_profiles_are_loaded_in_library(self):
        for model_key, url_key, secret_key in (
            ('EASY_AGENT_MODEL', 'EASY_AGENT_BASE_URL', 'EASY_AGENT_API_KEY'),
            ('LLM_MODEL_ID', 'LLM_BASE_URL', 'LLM_API_KEY'),
        ):
            with self.subTest(model_key=model_key):
                env = self.env_file(
                    f'{model_key}=configured\n{url_key}=https://model.example/v1\n'
                    f'{secret_key}=synthetic-secret\nEASY_AGENT_PROVIDER=deepseek\n'
                )
                before = dict(os.environ)
                with patch('easy_agent.config.read_selected_env', wraps=read_selected_env) as reader:
                    requests = self.call(self.graph(env_file=env))
                self.assertEqual(reader.call_count, 1)
                self.assertEqual(os.environ, before)
                self.assertEqual(json.loads(requests[0].content)['model'], 'configured')
                self.assertEqual(str(requests[0].url), 'https://model.example/v1/chat/completions')
                self.assertEqual(requests[0].headers['authorization'], 'Bearer synthetic-secret')

    def test_mixed_accounts_fail_before_network_including_empty_values(self):
        for standard_value in ('standard', ''):
            with self.subTest(empty=not standard_value):
                env = self.env_file(f'EASY_AGENT_MODEL={standard_value}\nLLM_API_KEY=synthetic-secret\n')
                graph = self.graph(env_file=env)
                graph.node = Node()
                with patch('easy_agent._api._client') as factory:
                    with self.assertRaises(ConfigurationError):
                        graph.run([Info('input')])
                    factory.assert_not_called()

    def test_mixed_file_and_system_accounts_are_rejected(self):
        env = self.env_file('LLM_API_KEY=synthetic-secret\n')
        with patch.dict(os.environ, {'EASY_AGENT_BASE_URL': 'https://other.example'}):
            graph = self.graph(env_file=env)
            graph.node = Node()
            with patch('easy_agent._api._client') as factory:
                with self.assertRaises(ConfigurationError):
                    graph.run([Info('input')])
                factory.assert_not_called()

    def test_selected_profile_keeps_system_priority(self):
        env = self.env_file('LLM_MODEL_ID=file-model\nLLM_API_KEY=file-synthetic-secret\n')
        with patch.dict(os.environ, {'LLM_MODEL_ID': 'system-model', 'LLM_API_KEY': 'system-secret'}):
            requests = self.call(self.graph(env_file=env))
        self.assertEqual(json.loads(requests[0].content)['model'], 'system-model')
        self.assertEqual(requests[0].headers['authorization'], 'Bearer system-secret')

    def test_explicit_model_and_custom_secret_reference_are_preserved(self):
        env = self.env_file('LLM_MODEL_ID=file-model\nLLM_API_KEY=unused-secret\nCUSTOM_KEY=chosen-secret\n')
        requests = self.call(self.graph(env_file=env, model='explicit-model', api_key_env='CUSTOM_KEY'))
        self.assertEqual(json.loads(requests[0].content)['model'], 'explicit-model')
        self.assertEqual(requests[0].headers['authorization'], 'Bearer chosen-secret')

    def test_explicit_missing_standard_secret_is_not_replaced_by_llm_secret(self):
        env = self.env_file('LLM_API_KEY=synthetic-secret\n')
        graph = self.graph(env_file=env, api_key_env='EASY_AGENT_API_KEY')
        graph.node = Node()
        with patch('easy_agent._api._client') as factory:
            with self.assertRaises(ConfigurationError):
                graph.run([Info('input')])
            factory.assert_not_called()


if __name__ == '__main__':
    unittest.main()
