"""One bounded live regression: test1-5 shapes, explicit hub and API baselines."""

import argparse
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import runpy
import sys
from time import monotonic
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from easy_agent import Graph, Info, Node
from api_support import (
    events_for, inspect_workflow, inspect_success, model_records, options, safe_error, source_hashes,
)


def run_live(env_file, round_name):
    report = {
        'status': 'failed', 'round': round_name,
        'python': platform.python_version(), 'python_executable': sys.executable,
        'python_environment': sys.prefix, 'env_file': str(Path(env_file).resolve()),
        'planned_request_count': 19, 'request_count': 0,
        'request_count_basis': 'ModelRequestStarted events',
        'max_output_tokens_per_request': 128, 'max_attempts': 1,
        'cases': [], 'source_sha256': source_hashes(),
    }
    graphs = []
    current = None
    try:
        values = options(env_file)

        def capture_graph(**kwargs):
            graph = Graph(**kwargs)
            graphs.append(graph)
            return graph

        for filename, function, expected, requests, shape in (
            ('test_01_api.py', 'run', ('API_TEST_OK',), 1, None),
            ('test1_5.py', 'one_to_one', ('3',), 2, (1, 1)),
            ('test1_5.py', 'many_to_one', ('6',), 3, (2, 1)),
            ('test1_5.py', 'one_to_many', ('4', '3'), 3, (1, 2)),
            ('test1_5.py', 'many_to_many', ('8', '6'), 4, (2, 2)),
            ('test1_5.py', 'hub_many_to_many', ('9', '7'), 5, (2, 2)),
        ):
            current = {'case': filename[:-3] if shape is None else function,
                       'status': 'failed', 'planned_requests': requests}
            report['cases'].append(current)
            started = monotonic()
            namespace = runpy.run_path(str(ROOT / 'test' / filename))
            with patch.dict(namespace[function].__globals__, {'Graph': capture_graph}):
                outputs = namespace[function](env_file)
            graph = graphs[-1]
            current.update(inspect_success(graph, outputs, expected, values['LLM_API_KEY'],
                                           requests, values['LLM_MODEL_ID'],
                                           expected_references=requests))
            if shape is not None:
                current.update(inspect_workflow(graph, outputs, env_file, shape))
            current.update(status='passed', duration_seconds=round(monotonic() - started, 3))

        current = {'case': 'async_json_input', 'status': 'failed', 'planned_requests': 1}
        report['cases'].append(current)

        async def async_case():
            graph = capture_graph(env_file=env_file, execution='async', timeout=30,
                                  max_output_tokens=32, max_attempts=1)
            graph.llm = Node(prompt='Reply with only the sum of a and b as one digit. No explanations.')
            outputs = await graph.arun([Info({'a': 3, 'b': 4})])
            return inspect_success(graph, outputs, ('7',), values['LLM_API_KEY'], 1, values['LLM_MODEL_ID'])

        started = monotonic()
        current.update(asyncio.run(async_case()))
        current.update(status='passed', duration_seconds=round(monotonic() - started, 3))
        report['status'] = 'passed'
    except Exception as error:
        failure = safe_error(error)
        report.update(failure)
        if current is not None:
            current.update(failure)
            current['duration_seconds'] = round(monotonic() - started, 3)
    finally:
        # Event counts describe adapter attempts, not guaranteed wire delivery.
        for graph, case in zip(graphs, report['cases']):
            try:
                events = events_for(graph)
                case.setdefault('models', model_records(graph, events))
                case['request_count'] = sum(event['event_type'] == 'ModelRequestStarted' for event in events)
                if not case.get('run_id'):
                    case['run_id'] = graph._last_run_id
                serialized = json.dumps({'events': events, 'export': graph.to_dict()})
                protected = values['LLM_API_KEY'] not in serialized
                case['secret_absent_from_logs_and_export'] = protected
                case['resources_exited'] = graph._active_runs == 0 and not graph._cleanups
                if not protected:
                    case.update(status='failed', failed_check='credential_protection')
                    report.update(status='failed', failed_check='credential_protection')
            except Exception:
                case['request_count'] = None
                case['request_count_unknown'] = True
        counts = [case.get('request_count', 0) for case in report['cases']]
        report['request_count'] = None if None in counts else sum(counts)
        if report['request_count'] is None:
            report.update(status='failed', evidence_error='RequestCountUnknown')
        report['timestamp_utc'] = datetime.now(timezone.utc).isoformat()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--round', required=True)
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    evidence = Path(args.evidence)
    if evidence.exists():
        print(json.dumps({'status': 'failed', 'error_type': 'EvidenceAlreadyExists'}))
        return 1
    report = run_live(args.env_file, args.round)
    evidence.parent.mkdir(parents=True, exist_ok=True)
    with evidence.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
