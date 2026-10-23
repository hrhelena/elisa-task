# Elisa test assignment: Enron email metrics service

Go service that ingests the Enron maildir dataset into Postgres and exposes metrics over HTTP
(`POST /fetch`, `POST /start`, `GET /status`, `GET /top-senders`). Requirements are in `Docs/task-description.txt`,
working rules are in `prompt.md`, and decisions are in `Docs/ADR/` (binding).

## Process (the main session acts as orchestrator)
- Tracking: GitHub Project https://github.com/users/hrhelena/projects/1. Use the `kanban` skill and its `board.sh` helper.
- Agents (`.claude/agents/`, all with worktree isolation):
  - `product-owner`: ADRs and tickets with spec and Gherkin. Its questions are relayed to the user.
  - `coder`: one Ready ticket, then a PR. Independent tickets may run in parallel.
  - `reviewer`: reviews the PR and posts a verdict comment (a shared account means no formal approve or request-changes).
- Loop: the coder opens a PR, the reviewer reviews, findings go back to the same coder (SendMessage), and this repeats until the verdict is APPROVED.
  Persistent disagreement goes to the user for arbitration. The orchestrator merges approved PRs (squash) after CI is green,
  then moves the ticket to Done.
- Architectural decisions are always confirmed with the user, then recorded as an ADR (the `adr` skill).
- Tickets start in Backlog. The user reviews the backlog before tickets move to Ready.

## Conventions
- Go conventions and tooling: `.claude/skills/go-conventions/SKILL.md`. Tools are project-local and pinned; never install globally.
- `make` is the entry point (`make tools lint test itest build docker kind-up deploy`).
- Branches: `<issue#>-<slug>` (code), `docs/<slug>` (docs). Conventional Commits. Never push to `main` directly.
- Commit trailer: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- `Dataset/` is local-only and gitignored (about 517k files, 150 mailboxes).
