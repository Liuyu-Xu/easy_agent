# Project testing rules

The user has authorized real API testing using the external file
`D:/code/Python/agent/hello_agent/test.env`.

After each round of project changes, run the deterministic tests and the live
API regression through `tests/run_api_round.py`. Follow `v0.3-api-minimal.md`
and register the actual results. A failed live request is a failed check, not
a skip. Do not run recurring or background calls.

Read credentials only inside local test/runtime processes. Never print, copy,
commit, or include secret values in commands, logs, exceptions, or evidence.
Do not change the external env file. Use only the model API credentials for
this task; the unrelated Tavily credential is outside this scope.

Keep unresolved public behavior in an issue document and implement against
the approved scope and written design. Live smoke tests do not replace fault
injection, concurrency, or deterministic regression tests.
