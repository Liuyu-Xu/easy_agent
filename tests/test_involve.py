"""Node accounts and atomic graph assembly; synthetic credentials only."""

import asyncio
from contextlib import redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import patch

import httpx

from easy_agent import ConfigurationError, Graph, GraphValidationError, Info, Node, OwnershipError, UnsupportedFeatureError
from easy_agent.config import read_selected_env


class Echo(Node):
    def forward(self, info):
        return info


class Fixture:
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def graph(self, **kwargs):
        return Graph(log_dir=self.root / 'logs', **kwargs)

    def account(self, name='one', *, standard=False):
        path = self.root / (name + '.env')
        model, base, key = ('EASY_AGENT_MODEL', 'EASY_AGENT_BASE_URL', 'EASY_AGENT_API_KEY') if standard else (
            'LLM_MODEL_ID', 'LLM_BASE_URL', 'LLM_API_KEY')
        path.write_text(f'{model}={name}-model\n{base}=https://{name}.example/v1\n'
                        f'{key}={name}-synthetic-key\n', encoding='utf-8')
        return path

    def transport(self, handler):
        return patch('easy_agent._api._client',
                     lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)))

    @staticmethod
    def response(text='ok', model='test-model'):
        return httpx.Response(200, json={'model': model, 'choices': [
            {'message': {'content': text}, 'finish_reason': 'stop'}]})


class AssemblyTests(Fixture, unittest.TestCase):
    def test_relations_and_entry_control_order_not_list_position(self):
        seen = []

        class Add(Node):
            def forward(self, info):
                seen.append((self.role, info.text))
                return Info(info.text + self.role)

        source, target = Add(role='A'), Add(role='B')
        graph = self.graph()
        self.assertIsNone(graph.involve(nodes=[target, source], relations=[(source, target)], start=[source], end=[target]))
        output = graph.run([Info('start')])[0]
        self.assertEqual(output.text, 'startAB')
        self.assertEqual(seen, [('A', 'start'), ('B', 'startA')])
        self.assertEqual(output.origin_kind, 'graph_output')
        self.assertIs(graph.start[0], source)
        self.assertEqual(source.name, 'node_' + source.id)
        self.assertIn(target.name, graph.relations[source.name])

    def test_unreachable_graph_rolls_back_ownership_names_edges_and_entry(self):
        for edges in ([], [(0, 1), (2, 1)]):
            with self.subTest(edges=edges):
                graph = self.graph()
                nodes = [Echo(), Echo(), Echo()]
                with self.assertRaises(GraphValidationError):
                    graph.involve(nodes=nodes, relations=[(nodes[a], nodes[b]) for a, b in edges], start=[nodes[0]], end=[(nodes[-1] if nodes else nodes[0])])
                self.assertEqual(graph.to_dict()['nodes'], [])
                self.assertIsNone(graph.start)
                for node in nodes:
                    self.assertIsNone(node.owner)
                    self.assertIsNone(node.name)
                    self.assertEqual(node._outgoing, [])
                graph.involve(nodes=nodes, relations=[(nodes[0], nodes[1]), (nodes[1], nodes[2])], start=[nodes[0]], end=[(nodes[-1] if nodes else nodes[0])])
                self.assertEqual(graph.run([Info('ok')])[0].text, 'ok')

    def test_invalid_members_relations_and_entry_fail_without_binding(self):
        cases = ('empty', 'duplicate_node', 'duplicate_edge', 'foreign_entry', 'foreign_edge', 'bad_edge', 'bad_list')
        for case in cases:
            with self.subTest(case=case):
                graph = self.graph()
                a, b, outsider = Echo(), Echo(), Echo()
                nodes, relations, entry = [a, b], [(a, b)], a
                if case == 'empty': nodes = []
                if case == 'duplicate_node': nodes = [a, a]
                if case == 'duplicate_edge': relations = [(a, b), (a, b)]
                if case == 'foreign_entry': entry = outsider
                if case == 'foreign_edge': relations = [(a, outsider)]
                if case == 'bad_edge': relations = [[a, b]]
                if case == 'bad_list': nodes = (a, b)
                with self.assertRaises((TypeError, GraphValidationError, OwnershipError)):
                    graph.involve(nodes=nodes, relations=relations, start=[entry], end=[(nodes[-1] if nodes else entry)])
                self.assertIsNone(a.owner)
                self.assertEqual(graph.to_dict()['nodes'], [])

    def test_nonempty_graph_and_foreign_owned_nodes_are_rejected(self):
        graph, other = self.graph(), self.graph()
        graph.old = Echo()
        other.foreign = Echo()
        candidate = Echo()
        with self.assertRaises(GraphValidationError):
            graph.involve(nodes=[candidate], relations=[], start=[candidate], end=[candidate])
        with self.assertRaises(OwnershipError):
            self.graph().involve(nodes=[other.foreign], relations=[], start=[other.foreign], end=[other.foreign])
        self.assertIs(other.foreign.owner, other)
        self.assertIsNone(candidate.owner)

    def test_successful_involve_is_not_repeated_even_after_nodes_removed(self):
        graph, node = self.graph(), Echo()
        graph.involve(nodes=[node], relations=[], start=[node], end=[node])
        self.assertEqual(graph.run([Info('single')])[0].text, 'single')
        graph.start = None
        graph.end = None
        graph._remove_registration(node.name)
        replacement = Echo()
        with self.assertRaises(GraphValidationError):
            graph.involve(nodes=[replacement], relations=[], start=[replacement], end=[replacement])

    def test_binding_failure_restores_all_candidate_nodes(self):
        class Broken(Echo):
            def _bind(self, graph, name):
                super()._bind(graph, name)
                raise RuntimeError('controlled bind failure')

        graph, a, b = self.graph(), Echo(), Broken()
        with self.assertRaises(RuntimeError):
            graph.involve(nodes=[a, b], relations=[(a, b)], start=[a], end=[b])
        for node in (a, b):
            self.assertIsNone(node.owner)
            self.assertIsNone(node.name)
            self.assertIsNone(node._definition_editor)
        self.assertEqual(graph.to_dict()['nodes'], [])

    def test_edit_rollback_does_not_create_attributes_for_list_registered_nodes(self):
        graph, a, b = self.graph(), Echo(), Echo()
        graph.involve(nodes=[a, b], relations=[(a, b)], start=[a], end=[b])
        before = graph.to_dict()
        with self.assertRaises(GraphValidationError):
            with graph.edit():
                a.set_downstreams([])
        self.assertEqual(graph.to_dict(), before)
        self.assertNotIn(a.name, graph.__dict__)
        self.assertEqual(graph.run([Info('ok')])[0].text, 'ok')

    def test_involve_method_cannot_be_shadowed(self):
        graph, node = self.graph(), Echo()
        for value in (node, 1):
            with self.assertRaises(OwnershipError):
                graph.involve = value
        self.assertIsNone(node.owner)


class AccountTests(Fixture, unittest.TestCase):
    def test_distinct_files_same_secret_name_and_independent_request_parameters(self):
        requests = []
        a = Node(env_file=self.account('one'), max_output_tokens=12, temperature=0.2)
        b = Node(env_file=self.account('two'), max_output_tokens=24, temperature=0.7)
        graph = self.graph()
        graph.involve(nodes=[a, b], relations=[(a, b)], start=[a], end=[b])

        def handler(request):
            requests.append(request)
            return self.response('FIRST' if len(requests) == 1 else 'SECOND')

        with self.transport(handler):
            output = graph.run([Info('start')])[0]
            self.assertEqual(output.text, 'SECOND')
        for request, name, maximum, temperature in zip(requests, ('one', 'two'), (12, 24), (0.2, 0.7)):
            self.assertEqual(str(request.url), f'https://{name}.example/v1/chat/completions')
            self.assertEqual(request.headers['authorization'], f'Bearer {name}-synthetic-key')
            body = json.loads(request.content)
            self.assertEqual((body['model'], body['max_tokens'], body['temperature']), (name + '-model', maximum, temperature))
        self.assertEqual(json.loads(requests[1].content)['messages'][-1]['content'], 'FIRST')
        logs = json.dumps(graph._event_store.query(run_id=output.run_id, limit=1000))
        self.assertNotIn('one-synthetic-key', logs)
        self.assertNotIn('two-synthetic-key', logs)

    def test_per_node_field_groups_ignore_system_and_unused_graph_account(self):
        a = Node(env_file=self.account('one'))
        b = Node(env_file=self.account('two', standard=True))
        graph_env = self.root / 'unused.env'
        graph_env.write_text('LLM_API_KEY=wrong\nEASY_AGENT_API_KEY=wrong\n', encoding='utf-8')
        graph = self.graph(env_file=graph_env, model='wrong', provider='wrong')
        with patch.dict(os.environ, {'LLM_API_KEY': 'wrong', 'EASY_AGENT_API_KEY': 'wrong'}):
            graph.involve(nodes=[a, b], relations=[(a, b)], start=[a], end=[b])
            requests = []
            with self.transport(lambda request: requests.append(request) or self.response()):
                graph.run([Info('ok')])
        self.assertEqual([request.headers['authorization'] for request in requests],
                         ['Bearer one-synthetic-key', 'Bearer two-synthetic-key'])

    def test_scoped_missing_fields_and_custom_key_never_fall_back(self):
        for missing in ('LLM_MODEL_ID', 'LLM_BASE_URL', 'LLM_API_KEY', 'CUSTOM_KEY'):
            with self.subTest(missing=missing):
                path = self.account()
                if missing != 'CUSTOM_KEY':
                    path.write_text('\n'.join(line for line in path.read_text().splitlines() if not line.startswith(missing + '=')), encoding='utf-8')
                node = Node(env_file=path, api_key_env='CUSTOM_KEY' if missing == 'CUSTOM_KEY' else None)
                graph = self.graph(model='fallback', base_url='https://fallback.example')
                with patch.dict(os.environ, {missing: 'forbidden-fallback'}):
                    with patch('easy_agent._api._client') as client:
                        with self.assertRaises(ConfigurationError):
                            graph.involve(nodes=[node], relations=[], start=[node], end=[node])
                        client.assert_not_called()
                self.assertIsNone(node.owner)

    def test_explicit_node_account_values_and_custom_file_reference_override_file(self):
        path = self.account()
        path.write_text(path.read_text() + 'CUSTOM_KEY=custom-synthetic-key\n', encoding='utf-8')
        node = Node(env_file=path, model='chosen-model', base_url='https://chosen.example', api_key_env='CUSTOM_KEY')
        graph = self.graph(max_output_tokens=18)
        graph.involve(nodes=[node], relations=[], start=[node], end=[node])
        requests = []
        with self.transport(lambda request: requests.append(request) or self.response()):
            graph.run([Info('ok')])
        self.assertEqual(requests[0].headers['authorization'], 'Bearer custom-synthetic-key')
        self.assertEqual(json.loads(requests[0].content)['model'], 'chosen-model')
        self.assertEqual(json.loads(requests[0].content)['max_tokens'], 18)
        exported = graph.to_dict()['nodes'][0]['config']
        self.assertEqual(exported['env_file'], str(path.resolve()))
        self.assertEqual(exported['api_key_env'], 'CUSTOM_KEY')
        self.assertNotIn('custom-synthetic-key', json.dumps(graph.to_dict()))

    def test_shared_file_is_read_once_per_assembly_and_once_per_run(self):
        path = self.account()
        nodes = [Node(env_file=path), Node(env_file=path)]
        graph = self.graph()
        with patch('easy_agent.config.read_selected_env', wraps=read_selected_env) as reader:
            graph.involve(nodes=nodes, relations=[tuple(nodes)], start=[nodes[0]], end=[(nodes[-1] if nodes else nodes[0])])
            self.assertEqual(reader.call_count, 1)
            reader.reset_mock()
            with self.transport(lambda request: self.response()):
                graph.run([Info('ok')])
            self.assertEqual(reader.call_count, 1)

    def test_invalid_account_or_config_rolls_back_without_http(self):
        for tail in ('EASY_AGENT_API_KEY=other\n', 'LLM_API_KEY=duplicate\n', 'EASY_AGENT_PROVIDER=other\n'):
            with self.subTest(tail=tail):
                path = self.account()
                path.write_text(path.read_text() + tail, encoding='utf-8')
                a, b = Echo(), Node(env_file=path)
                graph = self.graph()
                with patch('easy_agent._api._client') as client:
                    with self.assertRaises((ConfigurationError, UnsupportedFeatureError)):
                        graph.involve(nodes=[a, b], relations=[(a, b)], start=[a], end=[b])
                    client.assert_not_called()
                self.assertIsNone(a.owner)
                self.assertEqual(a._outgoing, [])

    def test_path_assignment_checks_existence_and_keeps_previous_path_on_failure(self):
        path = self.account()
        node = Node(env_file=path)
        self.assertEqual(node.env_file, path.resolve())
        with self.assertRaises(ConfigurationError):
            node.env_file = self.root / 'missing.env'
        self.assertEqual(node.env_file, path.resolve())

    def test_failed_config_edit_restores_node_file_and_retains_input_budget_limitation(self):
        original, invalid = self.account(), self.root / 'invalid.env'
        invalid.write_text('LLM_MODEL_ID=incomplete\n', encoding='utf-8')
        node, graph = Echo(env_file=original, input_token_budget=10), self.graph()
        graph.involve(nodes=[node], relations=[], start=[node], end=[node])
        with self.assertRaises(ConfigurationError):
            with graph.edit():
                node.env_file = invalid
        self.assertEqual(node.env_file, original.resolve())
        graph.run([Info('ok')])
        started = graph.logs(event_type='GraphRunStarted')[0]
        self.assertEqual(started['data']['config']['public_values'][
            f'nodes.{node.name}.input_token_budget_status'], 'accepted_not_applied')
    def test_snapshot_does_not_change_mid_run_but_next_run_rereads(self):
        path = self.account()
        a, b = Node(env_file=path), Node(env_file=path)
        graph = self.graph()
        graph.involve(nodes=[a, b], relations=[(a, b)], start=[a], end=[b])
        requests = []

        def handler(request):
            requests.append(request)
            if len(requests) == 1:
                path.write_text(path.read_text().replace('one-synthetic-key', 'new-synthetic-key'), encoding='utf-8')
            return self.response()

        with self.transport(handler):
            graph.run([Info('first')])
            graph.run([Info('second')])
        self.assertEqual([request.headers['authorization'] for request in requests],
                         ['Bearer one-synthetic-key'] * 2 + ['Bearer new-synthetic-key'] * 2)

    def test_test1_2_module_entrypoint_prints_only_fixed_success(self):
        path = self.account()
        requests = []

        def handler(request):
            requests.append(request)
            return self.response('CHAIN_A' if len(requests) == 1 else 'TEST1_2_OK')

        console = StringIO()
        with self.transport(handler), patch('easy_agent.Graph', self.graph):
            with patch('easy_agent.node.normalize_env_file', return_value=path.resolve()), redirect_stdout(console):
                runpy.run_path(str(Path(__file__).resolve().parents[1] / 'test/test1_2.py'), run_name='__main__')
        self.assertEqual(console.getvalue(), 'TEST1_2_OK\n')
        self.assertEqual(len(requests), 2)


class ConcurrentTests(Fixture, unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_graph_accounts_are_isolated(self):
        entered, release = asyncio.Event(), asyncio.Event()
        graphs = []
        for name in ('one', 'two'):
            node = Node(env_file=self.account(name))
            graph = self.graph(execution='async')
            graph.involve(nodes=[node], relations=[], start=[node], end=[node])
            graphs.append(graph)
        requests = []

        async def handler(request):
            requests.append(request)
            if len(requests) == 1:
                entered.set()
                await release.wait()
            else:
                release.set()
            return self.response(json.loads(request.content)['model'])

        with self.transport(handler):
            first = asyncio.create_task(graphs[0].arun([Info('one')]))
            await asyncio.wait_for(entered.wait(), 2)
            outputs = await asyncio.gather(first, graphs[1].arun([Info('two')]))
        self.assertEqual([item[0].text for item in outputs], ['one-model', 'two-model'])
        self.assertEqual([item.headers['authorization'] for item in requests],
                         ['Bearer one-synthetic-key', 'Bearer two-synthetic-key'])

    async def test_running_definition_rejects_env_change_and_involve(self):
        entered, release = asyncio.Event(), asyncio.Event()

        class Waiting(Echo):
            async def forward(self, info):
                entered.set()
                await release.wait()
                return info

        node, graph = Waiting(env_file=self.account()), self.graph(execution='async')
        graph.involve(nodes=[node], relations=[], start=[node], end=[node])
        task = asyncio.create_task(graph.arun([Info('start')]))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            with self.assertRaises(GraphValidationError):
                node.env_file = None
            with self.assertRaises(GraphValidationError):
                graph.involve(nodes=[Echo()], relations=[], start=[node], end=[node])
        finally:
            release.set()
            await task


if __name__ == '__main__':
    unittest.main()
