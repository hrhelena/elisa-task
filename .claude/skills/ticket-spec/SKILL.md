---
name: ticket-spec
description: Write a GitHub issue (ticket) with a detailed spec and Gherkin acceptance scenarios, then add it to the kanban board. Use when turning requirements into backlog items.
---

# Writing tickets

Each ticket is one coherent, reviewable unit of work: ideally one PR of under about 500 changed lines, excluding generated code.
Create it with `gh issue create --repo hrhelena/elisa-task --title ... --body-file <file> --label <labels>`.
Then add it to the board with the `kanban` skill: `board.sh add <n> Backlog <P> <Size>`.

Titles use a type prefix: `feat:`, `chore:`, `ci:`, `infra:`, `docs:`, `test:`.
Labels: `enhancement` for features, `documentation` for docs. Create others (`infra`, `ci`) if needed with `gh label create`.

## Body template

````markdown
## Context
Why this ticket exists. Link the requirement in `Docs/task-description.txt` and the relevant ADRs.

## Scope
- Bullet list of what is in scope.

### Out of scope
- What is explicitly not done here (link the follow-up ticket if one exists).

## Technical spec
Concrete detail the coder needs: packages and files to touch, interfaces and signatures, DB schema and
migrations, HTTP request/response shapes with example JSON and status codes, config/env vars with
defaults, error handling for malformed input, logging and metrics to emit.

## Acceptance criteria (Gherkin)
```gherkin
Feature: <feature>

  Scenario: <happy path>
    Given ...
    When ...
    Then ...

  Scenario: <error / edge case>
    ...
```

## Test plan
- Unit tests: which functions and table cases.
- Integration tests: what runs against real Postgres (testcontainers) or a real HTTP server.

## Dependencies
Depends on #N (or "None").

## Definition of done
- [ ] Code plus unit and integration tests; `make lint test` passes; CI is green
- [ ] Every Gherkin scenario is covered by at least one test (name the test in the PR)
- [ ] Docs/README updated if behavior or config changed
````

## Guidance
- Gherkin scenarios must be testable and specific: use real values (`"phillip.allen@enron.com"`, `HTTP 409`), not "some error".
- Always include scenarios for malformed or invalid input, and for concurrency where it applies (for example, `/start` called twice).
- Never leave a decision to the coder silently. If the spec needs a decision that has no ADR, raise it as a question.
