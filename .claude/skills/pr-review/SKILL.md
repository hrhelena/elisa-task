---
name: pr-review
description: Review a pull request for correctness against its ticket spec and Gherkin scenarios, and post the review on GitHub. Use when a PR is ready for review.
---

# Reviewing a PR

## Procedure
1. Gather context: `gh pr view <n> --json title,body,headRefName,files`, the linked issue (`gh issue view <N>`),
   and the relevant ADRs in `Docs/ADR`.
2. Check out the branch in your worktree (`gh pr checkout <n>`). Run `make lint test`, and the integration tests if
   Docker is available. Record the actual results.
3. Read the full diff (`gh pr diff <n>`) and the surrounding code it touches, not just the hunks.
4. Go through the checklist below. For every finding, give a concrete failure scenario: input or state, then the wrong result.
5. Post the review (see Posting).

## Checklist (correctness first)
- **Spec conformance:** every Gherkin scenario in the issue is implemented and covered by a test that would fail without the code.
- **Correctness:** edge cases, off-by-one errors, nil and empty handling, error paths that are swallowed or wrongly wrapped, malformed input
  (bad headers, odd encodings, missing From) handled without aborting ingestion.
- **Concurrency:** data races, goroutine leaks, context cancellation and shutdown, double `/start`, shared state across replicas.
- **Database:** SQL correctness, transactions, idempotency of re-ingestion, indexes for the queries, migrations that can be rolled back.
- **HTTP:** status codes, content types, JSON shape matches the spec, method handling (405), timeouts.
- **Go practice:** errors wrapped with `%w`, no panics in library code, `context.Context` first, small interfaces at the consumer,
  `log/slog` for logging, and tests that are table-driven and use `t.Parallel()` where safe.
- **Ops:** config via env with defaults, health/readiness probes still valid, Dockerfile and k8s manifests updated if needed.
- Do not nitpick style that linters already enforce.

## Posting
The reviewer and the author share one GitHub account, and GitHub forbids `--request-changes` and `--approve` on your own PR.
So always post with `gh pr review <n> --comment --body-file <file>`, and begin the body with a verdict line:

```markdown
**Verdict: CHANGES REQUESTED** (or **Verdict: APPROVED**)

Tests run: `make lint test` ✅ / `make itest` ✅ (or the failures, with output)

### Blocking
1. `path/file.go:123`: <defect>. Failure scenario: <...>. Suggested fix: <...>.

### Non-blocking
- ...
```

For line-specific comments, `gh api repos/hrhelena/elisa-task/pulls/<n>/comments` with `commit_id`, `path`, `line`
and `side=RIGHT` can be used in addition to the summary.

Severity: anything that yields wrong results, data loss, crashes, races, or a missing scenario test is **Blocking**.
If you and the author disagree after two rounds on the same point, stop and escalate to the user via the orchestrator,
setting out both positions.
