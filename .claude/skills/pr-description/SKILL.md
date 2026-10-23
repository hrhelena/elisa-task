---
name: pr-description
description: Open a well-structured pull request for a ticket with gh. Use when a coding agent has finished a ticket and pushed its branch.
---

# Opening a PR

Before opening:
1. Rebase on the latest `origin/main`, and confirm `make lint test` passes locally (plus integration tests if the change touches them).
2. Make the commits follow Conventional Commits (`feat(ingest): ...`). Each commit message ends with
   `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
3. Push the branch, named `<issue#>-<short-slug>`, for example `7-top-senders-endpoint`.

Create the PR with `gh pr create --base main --head <branch> --title "<type>(<scope>): <summary> (#N)" --body-file <file>`,
then move the issue to `In review` (kanban skill).

## Body template

```markdown
Closes #N

## Summary
2–4 sentences: what changed and why.

## Changes
- Grouped bullet list by package or area.

## Acceptance criteria coverage
| Gherkin scenario | Test(s) |
|---|---|
| Scenario: ... | `internal/ingest/ingest_test.go:TestX` |

## Design notes
Non-obvious choices, trade-offs, links to ADRs. Mention any deviation from the ticket spec and why.

## How to test
Commands a reviewer can run (`make test`, `make itest`, curl examples).

## Checklist
- [ ] `make lint test` passes
- [ ] Integration tests pass
- [ ] Docs/README/ADR updated where needed

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

Keep it factual. Don't claim tests pass unless you ran them in this session.
