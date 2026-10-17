"""Deterministic probes for the 2026-10-19 static analysis findings.

No network, no credentials, no writes outside a temp directory.
Prints one ASCII line per observation; exit code 0 means "probes ran".
"""

from __future__ import annotations

import json
import tempfile
import time
import warnings
from pathlib import Path

import easy_agent
from easy_agent import Graph, Info, Node
from easy_agent.errors import EasyAgentError, NodeTimeoutError

TMP = Path(tempfile.mkdtemp(prefix="ea-probe-"))
OUT: dict[str, object] = {}


def record(name: str, value: object) -> None:
    OUT[name] = value
    print(f"{name} = {value}")


def chain(node_a: Node, node_b: Node, **graph_kwargs) -> Graph:
    graph_kwargs.setdefault("log_dir", TMP / f"g{len(OUT)}")
    g = Graph(**graph_kwargs)
    g.a = node_a
    g.b = node_b
    g.a.send_to(g.b)
    g.start = [node_a]
    g.end = [node_b]
    return g


# ---------------------------------------------------------------- P0-1 max_attempts
calls: list[int] = []


class Boom(Node):
    def forward(self, info: Info) -> Info:
        calls.append(1)
        raise ConnectionError("transient-looking failure")


g = chain(Boom(), Boom(), max_attempts=3)
try:
    g.run([Info("x")])
except EasyAgentError:
    pass
record("max_attempts_configured", 3)
record("max_attempts_forward_calls", len(calls))


# ---------------------------------------------------------------- P0-2 sync deadline
slept: list[float] = []


class Slow(Node):
    def forward(self, info: Info) -> Info:
        started = time.perf_counter()
        time.sleep(0.5)
        slept.append(time.perf_counter() - started)
        return Info("late")


g = chain(Slow(), Slow(), timeout=0.05)
warnings.simplefilter("always")
with warnings.catch_warnings(record=True) as caught:
    g.validate()
    compile_warnings = [str(item.message) for item in caught]
started = time.perf_counter()
error_name = None
try:
    g.run([Info("x")])
except EasyAgentError as exc:
    error_name = type(exc).__name__
elapsed = time.perf_counter() - started
record("sync_timeout_limit_seconds", 0.05)
record("sync_timeout_elapsed_seconds", round(elapsed, 3))
record("sync_timeout_error", error_name)
record("sync_timeout_compile_warnings", compile_warnings)


# ---------------------------------------------------------------- topology_hash
def build_hash() -> str:
    a = Node(prompt="same prompt")
    b = Node(prompt="same prompt")
    g = Graph(log_dir=TMP / "hash")
    g.a = a
    g.b = b
    g.a.send_to(g.b)
    g.start = [a]
    g.end = [b]
    plan = g._compile(max_steps_override=None, max_concurrency_override=None, for_run=False)
    return plan.topology_hash


h1, h2 = build_hash(), build_hash()
record("topology_hash_equal_for_identical_topology", h1 == h2)


# ---------------------------------------------------------------- log_dir / plain_data
record("log_dir_none_directory", Graph(log_dir=None)._event_store.directory.name)
record("log_dir_none_is_default", str(Graph(log_dir=None)._event_store.directory)
       .replace("\\", "/").endswith(".easy_agent/logs"))
record("plain_data_exported", hasattr(easy_agent, "plain_data"))
record("info_to_plain", hasattr(Info("x"), "to_plain"))
record("graph_from_dict", hasattr(Graph, "from_dict"))
node = Node(prompt="p")
record("node_repr_has_address", "0x" in repr(node))


# ---------------------------------------------------------------- pure-python account
mixed_env = TMP / "mixed.env"
mixed_env.write_text("EASY_AGENT_MODEL=a\nLLM_MODEL_ID=b\n", encoding="utf-8")
pure_calls: list[int] = []


class Pure(Node):
    def forward(self, info: Info) -> Info:
        pure_calls.append(1)
        return info


g = Graph(log_dir=TMP / "pure", env_file=mixed_env)
g.a = Pure()
g.start = [g.a]
g.end = [g.a]
try:
    g.run([Info("x")])
    pure_error = None
except EasyAgentError as exc:
    pure_error = type(exc).__name__
record("pure_python_graph_mixed_env_error", pure_error)
record("pure_python_graph_forward_calls", len(pure_calls))


# ---------------------------------------------------------------- input_token_budget
budget_node = Node(prompt="p", input_token_budget=123)
g = chain(budget_node, Node(prompt="p"), log_dir=TMP / "budget")
plan = g._compile(max_steps_override=None, max_concurrency_override=None, for_run=False)
record("input_token_budget_status", [
    value for key, value in plan.config.public_values.items()
    if key.endswith("input_token_budget_status")
])


# ---------------------------------------------------------------- max_steps node event
step_calls: list[int] = []


class Step(Node):
    def forward(self, info: Info) -> Info:
        step_calls.append(1)
        return info


g = chain(Step(), Step(), log_dir=TMP / "steps")
g.max_steps = 1
step_error = None
try:
    g.run([Info("x")])
except EasyAgentError as exc:
    step_error = type(exc).__name__
events = g.logs(limit=1000)
node_event_types = [event["event_type"] for event in events if event["category"] == "node"]
record("max_steps_error", step_error)
record("max_steps_forward_calls", len(step_calls))
record("max_steps_node_event_types", sorted(set(node_event_types)))


# ---------------------------------------------------------------- log view scope
raw = g._event_store.query(run_id=g._last_run_id, category=None, limit=1000)
categories: dict[str, int] = {}
for event in raw:
    categories[event["category"]] = categories.get(event["category"], 0) + 1
record("events_total_by_category", categories)
record("public_logs_default_count", len(g.logs(limit=1000)))
record(
    "public_logs_default_categories",
    sorted({event["category"] for event in g.logs(limit=1000)}),
)
record("public_logs_node_b_count", len(g.logs(node=g.b, limit=1000)))
record(
    "public_logs_node_b_types",
    sorted({event["event_type"] for event in g.logs(node=g.b, limit=1000)}),
)


# ---------------------------------------------------------------- final
print("PROBE_JSON " + json.dumps(OUT, ensure_ascii=False, sort_keys=True))
raise SystemExit(0)
