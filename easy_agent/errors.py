"""Framework exceptions. Runtime failures carry a run id when one exists."""

from __future__ import annotations


class EasyAgentError(Exception):
    def __init__(self, message: str, *, run_id: str | None = None) -> None:
        super().__init__(message)
        self.run_id = run_id

    def __str__(self) -> str:
        message = super().__str__()
        return message if self.run_id is None else f"{message} [run_id={self.run_id}]"


class GraphValidationError(EasyAgentError):
    pass


class OwnershipError(GraphValidationError):
    pass


class InfoError(EasyAgentError):
    pass


class RoutingError(EasyAgentError):
    pass


class ResultError(EasyAgentError):
    pass


class NodeExecutionError(EasyAgentError):
    pass


class NodeTimeoutError(NodeExecutionError):
    """A node attempt exceeded its configured deadline."""


class ExecutionLimitError(EasyAgentError):
    pass


class ConfigurationError(EasyAgentError):
    pass


class PermissionDeniedError(EasyAgentError):
    pass


class LoggingError(EasyAgentError):
    pass


class UnsupportedFeatureError(EasyAgentError):
    pass


_ERROR_TYPES = frozenset({
    EasyAgentError, GraphValidationError, OwnershipError, InfoError, RoutingError,
    ResultError, NodeExecutionError, NodeTimeoutError, ExecutionLimitError,
    ConfigurationError, PermissionDeniedError, LoggingError, UnsupportedFeatureError,
})
_FRAMEWORK_ORIGIN = object()


def _safe_error(
    error_type: type[EasyAgentError], message: str, *,
    run_id: str | None = None, cause: Exception | None = None,
) -> EasyAgentError:
    """Create or normalize an error without formatting untrusted exceptions.

    Only exact framework classes with an unchanged internal message are trusted.
    Callers raise the returned exception ``from None`` at external boundaries.
    This is a text boundary, not isolation from code executing in this process.
    """
    if cause is not None and type(cause) in _ERROR_TYPES:
        error_type = type(cause)
        state = cause.__dict__
        if (state.get('_framework_origin') is _FRAMEWORK_ORIGIN
                and cause.args == (state.get('_framework_message'),)):
            message = state['_framework_message']
    error = error_type(message, run_id=run_id)
    error._framework_origin = _FRAMEWORK_ORIGIN
    error._framework_message = message
    if cause is not None:
        # Only the adapter's private retry classification crosses this boundary;
        # no third-party attribute or text is trusted or forwarded.
        error._easy_agent_retryable = bool(
            getattr(cause, "_easy_agent_retryable", False)
        )
    return error
