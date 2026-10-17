"""Internal synchronization for graph and shared tool declarations."""

import asyncio
from functools import wraps
from threading import RLock, get_ident

from .errors import _safe_error, GraphValidationError


# Shared tools may have consumers in different graphs. One short-lived lock
# avoids graph/tool lock-order inversions; no business execution uses this lock.
DEFINITION_LOCK = RLock()


def editor_identity():
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return get_ident(), task


def definition_locked(method):
    @wraps(method)
    def locked(self, *args, **kwargs):
        from .graph import Graph

        with DEFINITION_LOCK:
            graph = self if isinstance(self, Graph) else self.owner
            if (graph is not None and getattr(graph, "_edit_depth", 0)
                    and graph._edit_owner != editor_identity()):
                raise _safe_error(GraphValidationError, "Graph 正由另一执行上下文编辑")
            if graph is not None:
                for tool in graph._tool_declarations():
                    editor = tool._definition_editor
                    if editor is not None and editor != (graph.id, editor_identity()):
                        raise _safe_error(GraphValidationError, "共享工具声明正由另一事务编辑")
            return method(self, *args, **kwargs)
    return locked
