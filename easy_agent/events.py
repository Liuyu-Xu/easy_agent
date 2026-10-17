"""Local JSONL event storage used by graph and node log views."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

from .errors import _safe_error, LoggingError


class EventStore:
    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory).resolve()
        self._lock = Lock()
        self._sequences: dict[str, int] = {}

    def start_run(self, run_id: str) -> None:
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            path = self._path(run_id)
            with self._lock:
                path.touch(exist_ok=False)
                self._sequences[run_id] = 0
        except Exception:
            raise _safe_error(LoggingError,
                "无法创建运行日志", run_id=run_id
            ) from None

    def append(
        self,
        run_id: str,
        event_type: str,
        *,
        category: str = "graph",
        trace_id: str | None = None,
        span_id: str | None = None,
        parent_span_id: str | None = None,
        node_id: str | None = None,
        node_execution_id: str | None = None,
        attempt: int | None = None,
        info_ids: list[str] | None = None,
        delivery_id: str | None = None,
        cause_event_id: str | None = None,
        fork_group_id: str | None = None,
        session_id: str | None = None,
        round_index: int | None = None,
        tool_call_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            sequence = self._sequences.get(run_id, 0) + 1
            self._sequences[run_id] = sequence
            event = {
                "event_id": uuid4().hex,
                "event_type": event_type,
                "category": category,
                "run_id": run_id,
                "trace_id": trace_id or run_id,
                "span_id": span_id,
                "parent_span_id": parent_span_id,
                "node_id": node_id,
                "node_execution_id": node_execution_id,
                "attempt": attempt,
                "info_ids": list(info_ids or []),
                "delivery_id": delivery_id,
                "cause_event_id": cause_event_id,
                "fork_group_id": fork_group_id,
                "session_id": session_id,
                "round_index": round_index,
                "tool_call_id": tool_call_id,
                "worker_id": "coordinator",
                "local_sequence": sequence,
                "commit_sequence": sequence,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": data or {},
            }
            try:
                with self._path(run_id).open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(event, ensure_ascii=False) + "\n")
            except Exception:
                raise _safe_error(LoggingError,
                    "必要运行日志写入失败", run_id=run_id
                ) from None
            return event

    def query(
        self,
        *,
        run_id: str,
        category: str | None = None,
        node_id: str | None = None,
        node_execution_id: str | None = None,
        event_type: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if offset < 0 or limit <= 0:
            raise ValueError("offset 必须非负且 limit 必须为正数")
        path = self._path(run_id)
        matched: list[dict[str, Any]] = []
        skipped = 0
        try:
            if not path.exists():
                return []
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    event = json.loads(line)
                    if category is not None and event["category"] != category:
                        continue
                    if node_id is not None and event["node_id"] != node_id:
                        continue
                    if (
                        node_execution_id is not None
                        and event["node_execution_id"] != node_execution_id
                    ):
                        continue
                    if event_type is not None and event["event_type"] != event_type:
                        continue
                    if skipped < offset:
                        skipped += 1
                        continue
                    matched.append(event)
                    if len(matched) >= limit:
                        break
        except Exception:
            raise _safe_error(LoggingError, "无法读取运行日志", run_id=run_id) from None
        return matched

    def _path(self, run_id: str) -> Path:
        safe = "".join(char for char in run_id if char.isalnum() or char in "-_")
        if safe != run_id or not safe:
            raise _safe_error(LoggingError, "run_id 包含非法字符", run_id=run_id)
        return self.directory / f"{safe}.jsonl"
