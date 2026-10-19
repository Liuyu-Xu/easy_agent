"""Internal live-test checks and evidence; never imported by user examples."""

import hashlib
import json
import os
from pathlib import Path
import re

from easy_agent import ConfigurationError, HubNode, Info
from easy_agent.config import read_selected_env


ROOT = Path(__file__).resolve().parents[1]


class LiveCheckError(RuntimeError):
    def __init__(self, check, run_id=None, observed_output=None):
        super().__init__(check)
        self.check = check
        self.run_id = run_id
        self.observed_output = observed_output


def source_hashes():
    files = [path for folder in ('easy_agent', 'tests', 'test')
             for path in sorted((ROOT / folder).glob('*.py'))]
    files.append(ROOT / 'pyproject.toml')
    return {path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in files}


def options(env_file):
    values = read_selected_env(Path(env_file), {'LLM_MODEL_ID', 'LLM_BASE_URL', 'LLM_API_KEY'})
    if not all(values.get(key) for key in ('LLM_MODEL_ID', 'LLM_BASE_URL', 'LLM_API_KEY')):
        raise ConfigurationError('Live tests require the three LLM_* settings')
    if any(name in os.environ and os.environ[name] != values[name]
           for name in ('LLM_MODEL_ID', 'LLM_BASE_URL', 'LLM_API_KEY')):
        raise ConfigurationError('An environment variable overrides the supplied test configuration')
    return values


def safe_error(error):
    data = {'error_type': type(error).__name__, 'run_id': getattr(error, 'run_id', None)}
    status = re.search(r'\bHTTP (\d{3})\b', str(error))
    if status:
        data['http_status'] = int(status.group(1))
    if isinstance(error, LiveCheckError):
        data['failed_check'] = error.check
        if error.observed_output is not None:
            data['observed_output'] = error.observed_output
    return data


def events_for(graph):
    # White-box access belongs in internal regression, not usage examples.
    run_id = graph._last_run_id
    return [] if run_id is None else graph._event_store.query(run_id=run_id, limit=1000)


def model_records(graph, events):
    names = {node.id: node.name for node in graph._nodes.values()}
    def key(event):
        return (event['node_execution_id'], event['attempt'], event['data'].get('round'))

    finished = {key(event): event['data'] for event in events
                if event['event_type'] == 'ModelRequestFinished'}
    return [{'node_id': event['node_id'], 'node_name': names[event['node_id']],
             'attempt': event['attempt'], **event['data'],
             **finished.get(key(event), {})}
            for event in events if event['event_type'] == 'ModelRequestStarted']


def inspect_success(graph, outputs, expected, secret, expected_requests, expected_model,
                    *, expected_references=1):
    run_id = outputs[0].run_id if outputs else None
    events = events_for(graph)

    def check(condition, name):
        if not condition:
            raise LiveCheckError(name, run_id)

    check(isinstance(outputs, list) and len(outputs) == len(expected)
          and all(isinstance(output, Info) and output.origin_kind == 'graph_output'
                  and output.run_id == run_id for output in outputs), 'final_info')
    serialized = json.dumps({'events': events, 'export': graph.to_dict(),
                             'output': [output.text for output in outputs]})
    check(secret not in serialized, 'credential_protection')
    actual = [output.text.strip() for output in outputs]
    if tuple(actual) != expected:
        raise LiveCheckError('expected_output', run_id, [text[:512] for text in actual])
    starts = [event for event in events if event['event_type'] == 'ModelRequestStarted']
    models = [event for event in events if event['event_type'] == 'ModelRequestFinished']
    successes = [event for event in events if event['event_type'] == 'GraphRunSucceeded']
    check(len(starts) == len(models) == expected_requests and len(successes) == 1, 'event_counts')
    check(successes[0]['info_ids'] == [output.id for output in outputs], 'published_outputs')
    check(all(event['data']['requested_model'] == expected_model for event in models), 'requested_model')
    check(graph._active_runs == 0 and not graph._cleanups, 'resource_cleanup')
    references = [event['data'] for event in events if event['event_type'] == 'SecretReferenceResolved']
    check(len(references) == expected_references
          and all(item['source'] == 'env_file' and item['resolved'] for item in references),
          'credential_source')
    return {
        'run_id': run_id, 'output': [output.text.strip() for output in outputs],
        'request_count': len(starts),
        'models': model_records(graph, events),
        **(models[0]['data'] if expected_requests == 1 else {}),
        'secret_source': references[0]['source'], 'credential_matches_test_file': True,
        'credential_reference_count': len(references),
        'secret_absent_from_logs_and_export': True, 'resources_exited': True,
    }


def inspect_workflow(graph, outputs, env_file, shape):
    """Verify fixed inputs, every edge, shared joins, hub ports and ordered results."""
    events = events_for(graph)
    run_id = outputs[0].run_id

    def check(condition, name):
        if not condition:
            raise LiveCheckError(name, run_id)

    check((len(graph.start), len(graph.end)) == shape, 'boundary_shape')
    check(len(outputs) == len(graph.end), 'output_count')
    consumed = [event for event in events if event['event_type'] == 'InfoConsumed']
    attempts = [event for event in events if event['event_type'] == 'NodeAttemptStarted']
    produced = [event for event in events if event['event_type'] == 'InfoProduced']
    check(len(consumed) == len(graph.start), 'initial_inputs')
    check(len(attempts) == len(produced) == len(graph._nodes), 'node_execution_count')
    inputs = {event['node_id']: event['info_ids'] for event in attempts}
    output_ids = {event['node_id']: event['info_ids'][0] for event in produced}
    for node, event in zip(graph.start, consumed):
        check(inputs[node.id] == event['info_ids'], 'start_input_mapping')

    hubs = {node.id: node for node in graph._nodes.values() if isinstance(node, HubNode)}
    prepared = [event for event in events if event['event_type'] == 'HubOutputsPrepared']
    check(len(prepared) == len(hubs), 'hub_execution_count')
    port_outputs = {}
    for node_id, node in hubs.items():
        matches = [event for event in prepared if event['node_id'] == node_id]
        check(len(matches) == 1, 'hub_preparation')
        event = matches[0]
        check(event['data']['parent_info_id'] == output_ids[node_id]
              and len(event['info_ids']) == node.output_count
              and event['data']['ports'] == list(node._hub_contract.output_keys), 'hub_output_parents')
        port_outputs[node_id] = event['info_ids']

    edges = [(node.id, target, None, None) for node in graph._nodes.values() for target in node._outgoing]
    edges += [(edge.source, edge.target, edge.output, edge.input) for edge in graph._port_edges]
    delivered = [event for event in events if event['event_type'] == 'InfoDeliveryReceived']
    check(len(delivered) == len(edges), 'edge_count')
    incoming = {}
    for source, target, output_port, input_port in edges:
        matches = [event for event in delivered if event['node_id'] == target
                   and event['data']['source'] == graph._nodes_by_id[source].name
                   and event['data']['input_port'] == input_port
                   and event['data']['output_port'] == output_port]
        expected = output_ids[source] if output_port is None else port_outputs[source][output_port]
        check(len(matches) == 1 and matches[0]['info_ids'] == [expected], 'edge_delivery')
        incoming.setdefault(target, {})[input_port if input_port is not None else source] = expected

    joins = [event for event in events if event['event_type'] == 'InfoJoined']
    expected_join_count = 0
    shared = {}
    shared_verified = False
    for node in graph._nodes.values():
        sources = incoming.get(node.id, {})
        if not sources:
            continue
        receive = node._receive_rule
        if isinstance(node, HubNode):
            order = node.input_keys
        elif receive is not None:
            order = receive.source_ids
        elif len(sources) > 1:
            order = tuple(source.id for source in graph._nodes.values() if source.id in sources)
        else:
            check(inputs[node.id] == [next(iter(sources.values()))], 'downstream_input')
            continue
        expected_join_count += 1
        matches = [event for event in joins if event['node_id'] == node.id]
        parents = [sources[source] for source in order]
        check(len(matches) == 1 and matches[0]['info_ids'][1:] == parents
              and inputs[node.id] == matches[0]['info_ids'][:1], 'join_parents')
        signature = (tuple(matches[0]['data']['sources']), tuple(parents))
        if signature in shared:
            check(shared[signature] == inputs[node.id][0], 'shared_join_info')
            shared_verified = True
        shared[signature] = inputs[node.id][0]
    check(len(joins) == expected_join_count, 'join_count')
    for node, output in zip(graph.end, outputs):
        check(output.parent_ids == (output_ids[node.id],), 'end_order')

    exported = graph.to_dict()['nodes']
    check(graph._env_file is None
          and all(item['config']['env_file'] == str(Path(env_file).resolve())
                  and item['name'] == 'node_' + item['id'] for item in exported), 'node_configuration')
    return {'start_count': shape[0], 'end_count': shape[1], 'join_count': len(joins),
            'unique_join_info_count': len({event['info_ids'][0] for event in joins}),
            'shared_join_info_verified': shared_verified, 'hub_count': len(hubs),
            'hub_port_mapping_verified': bool(hubs),
            'message_delivery_verified': True, 'start_input_mapping_verified': True,
            'end_order_verified': True, 'node_configuration_verified': True}
