"""Replay the captured original runtime: one expected A03-012 failure.

This audit harness is excluded from unittest discovery. Production files are
never restored or modified; only the original executor is loaded into a subclass.
"""

from dataclasses import make_dataclass
import importlib
from pathlib import Path
import sys
import unittest

root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / "tests"))
runtime = importlib.import_module("easy_agent.graph")
regressions = importlib.import_module("test_step1")
namespace = dict(vars(runtime))
namespace["_Outcome"] = make_dataclass(
    "OriginalOutcome",
    [(name, object) for name in (
        "item", "node_plan", "output", "node_execution_id", "event_id"
    )], frozen=True,
)
original = Path(__file__).with_name("original-runtime.txt").read_text(encoding="utf-8")
original = original.replace("async def _run_plan(", "async def _original_run_plan(", 1)
exec("class OriginalRuntimeGraph(Graph):\n" + original + '''
    async def _run_plan(self, plan, initial_info, run_id, current_state):
        return await self._original_run_plan(plan, initial_info, run_id)
''', namespace)
regressions.Graph = namespace["OriginalRuntimeGraph"]
suite = unittest.TestSuite([
    regressions.Step1RuntimeTests("test_completion_order_and_delivery_order_agree")
])
result = unittest.TextTestRunner(verbosity=2).run(suite)
if len(result.failures) != 1 or result.errors:
    raise SystemExit("Expected exactly one original-runtime order assertion failure")
print("EXPECTED: original runtime fails A03-012 under controlled set iteration.")
