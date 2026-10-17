"""Live check: does the provider accept our declared tools and drive the loop?

Authorized real API use for this project (see AGENTS.md). Credentials are read
inside this process only; nothing secret is printed or written.
"""

from __future__ import annotations

from pathlib import Path

from easy_agent import Graph, Info, Node

ENV_FILE = "D:/code/Python/agent/hello_agent/test.env"
calls: list[tuple[int, int]] = []


def add(a: int, b: int) -> int:
    """Add two integers and return the sum."""
    calls.append((a, b))
    return a + b


def main() -> int:
    graph = Graph(
        env_file=ENV_FILE, allowed_tools={"add"},
        timeout=60, max_attempts=1, max_output_tokens=64,
        log_dir=Path(".easy_agent/live-tool"),
    )
    graph.llm = Node(
        tools=[add],
        prompt=(
            "Use the add tool to compute the sum of 2 and 3. "
            "Then reply with only the resulting number."
        ),
    )
    try:
        result = graph.run([Info("what is 2 + 3?")])[0]
    except Exception as exc:  # noqa: BLE001 - report type only
        print("live_tool_status = failed")
        print("error_type =", type(exc).__name__)
        print("error_text =", str(exc)[:200])
        return 1
    events = graph._event_store.query(run_id=result.run_id, limit=500)
    starts = sum(1 for e in events if e["event_type"] == "ModelRequestStarted")
    tool_calls = [
        e for e in events
        if e["event_type"] in {"ToolCallStarted", "ToolCallFinished", "ToolCallFailed"}
    ]
    print("live_tool_status = ok")
    print("output =", result.text.strip()[:40])
    print("model_requests =", starts)
    print("tool_invoked =", calls)
    print("tool_events =", [e["event_type"] for e in tool_calls])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
