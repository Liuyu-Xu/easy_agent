"""Run the real-test harness against a controlled HTTP transport, never live."""

from contextlib import redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import httpx

from easy_agent import Graph
import live_api
import run_api_round


class LiveEvidenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.env = self.root / 'live.env'
        self.secret = 'synthetic-live-secret'
        self.env.write_text(
            'LLM_MODEL_ID=test-model\nLLM_BASE_URL=https://model.example/v1\n'
            f'LLM_API_KEY={self.secret}\n', encoding='utf-8',
        )
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def execute(self, fail_index=None, hub_output=None, single_output=None):
        requests = []
        expected = ['API_TEST_OK', '2', '3', '2', '4', '6', '2', '3', '4',
                    '2', '4', '6', '8', '2', '4', '{"out_1":"8","out_0":"6"}',
                    '7', '9', '7']

        def handler(request):
            index = len(requests)
            requests.append(request)
            if index == fail_index:
                return httpx.Response(503, text='do not expose ' + self.secret)
            text = expected[index]
            if index == 15 and hub_output is not None:
                text = hub_output
            if index == 2 and single_output is not None:
                text = single_output
            return httpx.Response(200, json={
                'model': 'test-model',
                'choices': [{'message': {'content': text}, 'finish_reason': 'stop'}],
            })

        def client():
            return httpx.AsyncClient(transport=httpx.MockTransport(handler))

        def graph(**options):
            return Graph(log_dir=self.root / 'logs', **options)

        with patch('easy_agent._api._client', client), patch('live_api.Graph', graph):
            report = live_api.run_live(self.env, 'controlled-evidence')
        self.assertNotIn(self.secret, json.dumps(report))
        return report, requests

    def test_all_shapes_hub_and_async_case_run_exactly_nineteen_requests(self):
        report, requests = self.execute()
        self.assertEqual(report['status'], 'passed')
        self.assertEqual(report['request_count'], 19)
        self.assertEqual([case['request_count'] for case in report['cases']], [1, 2, 3, 3, 4, 5, 1])
        for case, shape in zip(report['cases'][1:4], ((1, 1), (2, 1), (1, 2))):
            self.assertEqual((case['start_count'], case['end_count']), shape)
            self.assertTrue(case['message_delivery_verified'])
            self.assertTrue(case['start_input_mapping_verified'])
            self.assertTrue(case['end_order_verified'])
            self.assertTrue(case['node_configuration_verified'])
        self.assertEqual(json.loads(requests[2].content)['messages'][-1]['content'], '2')
        joined = json.loads(json.loads(requests[5].content)['messages'][-1]['content'])
        self.assertEqual(set(joined.values()), {'2', '4'})
        self.assertEqual(report['cases'][2]['join_count'], 1)
        self.assertIn('test/test_01_api.py', report['source_sha256'])
        self.assertEqual(report['cases'][1]['case'], 'one_to_one')
        self.assertEqual(report['cases'][1]['credential_reference_count'], 2)
        self.assertEqual([item['max_output_tokens'] for item in report['cases'][1]['models']], [16, 16])
        self.assertEqual(report['cases'][3]['output'], ['4', '3'])
        self.assertTrue(report['cases'][4]['shared_join_info_verified'])
        self.assertEqual(report['cases'][4]['unique_join_info_count'], 1)
        self.assertTrue(report['cases'][5]['hub_port_mapping_verified'])
        self.assertIn('test/test1_5.py', report['source_sha256'])
        self.assertIn('tests/api_support.py', report['source_sha256'])

    def test_chain_upstream_failure_keeps_previous_case_and_stops_downstream(self):
        report, requests = self.execute(fail_index=1)
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['http_status'], 503)
        self.assertEqual(report['request_count'], 2)
        self.assertEqual(len(requests), 2)
        self.assertEqual([case['status'] for case in report['cases']], ['passed', 'failed'])
        self.assertEqual(report['cases'][0]['output'], ['API_TEST_OK'])
        self.assertEqual(report['cases'][1]['request_count'], 1)

    def test_chain_downstream_failure_counts_attempts_without_retry(self):
        for fail_index in (2, 5, 8):
            with self.subTest(fail_index=fail_index):
                report, requests = self.execute(fail_index=fail_index)
                self.assertEqual(report['status'], 'failed')
                self.assertEqual(report['request_count'], fail_index + 1)
                self.assertEqual(len(requests), fail_index + 1)
                self.assertEqual(report['cases'][0]['status'], 'passed')
                self.assertEqual(report['cases'][-1]['status'], 'failed')
                self.assertNotIn('async_json_input', [case['case'] for case in report['cases']])

    def test_parallel_branch_failure_counts_started_sibling_without_retry(self):
        report, requests = self.execute(fail_index=7)
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['request_count'], len(requests))
        self.assertIn(len(requests), (8, 9))
        self.assertEqual(report['cases'][-1]['case'], 'one_to_many')

    def test_hub_http_failure_stops_before_downstream_and_async_case(self):
        report, requests = self.execute(fail_index=15)
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(len(requests), 16)
        self.assertEqual(report['request_count'], 16)
        self.assertEqual(report['cases'][-1]['case'], 'hub_many_to_many')
        self.assertEqual(report['cases'][-1]['request_count'], 3)

    def test_invalid_hub_output_is_a_failed_check_without_repair_requests(self):
        report, requests = self.execute(hub_output='{"out_0":"A"}')
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(len(requests), 16)
        self.assertEqual(report['error_type'], 'NodeExecutionError')
        self.assertEqual(report['cases'][-1]['case'], 'hub_many_to_many')

    def test_unexpected_output_is_recorded_after_credential_check(self):
        report, requests = self.execute(single_output='OTHER_FIXED_TEXT')
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['observed_output'], ['OTHER_FIXED_TEXT'])
        self.assertEqual(len(requests), 3)
        self.assertEqual(len(report['cases'][-1]['models']), 2)
        protected, _ = self.execute(single_output=self.secret)
        self.assertEqual(protected['failed_check'], 'credential_protection')
        self.assertNotIn('observed_output', protected)

    def test_existing_evidence_is_rejected_before_any_live_requests(self):
        evidence = self.root / 'existing.json'
        evidence.write_text('preserved', encoding='utf-8')
        arguments = ['live_api.py', '--env-file', str(self.env), '--round', 'duplicate',
                     '--evidence', str(evidence)]
        with patch('sys.argv', arguments), patch('live_api.run_live') as run:
            with redirect_stdout(StringIO()):
                self.assertEqual(live_api.main(), 1)
            run.assert_not_called()
        self.assertEqual(evidence.read_text(encoding='utf-8'), 'preserved')

    def test_environment_endpoint_override_fails_without_sending_file_credential(self):
        with patch.dict(os.environ, {'LLM_BASE_URL': 'https://other.example'}):
            report, requests = self.execute()
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['error_type'], 'ConfigurationError')
        self.assertEqual(report['request_count'], 0)
        self.assertEqual(requests, [])

    def test_round_resolves_relative_paths_before_switching_child_working_directory(self):
        original = Path.cwd()
        try:
            os.chdir(self.root)
            arguments = ['run_api_round.py', '--env-file', 'live.env', '--round', 'paths',
                         '--evidence', 'evidence/result.json']
            with patch('sys.argv', arguments), patch('run_api_round.subprocess.run',
                                                     return_value=SimpleNamespace(returncode=0)) as run:
                self.assertEqual(run_api_round.main(), 0)
            command = run.call_args_list[-1].args[0]
            self.assertEqual(command[command.index('--env-file') + 1], str(self.env.resolve()))
            self.assertEqual(command[command.index('--evidence') + 1],
                             str((self.root / 'evidence/result.json').resolve()))
        finally:
            os.chdir(original)


if __name__ == '__main__':
    unittest.main()
