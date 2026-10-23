---
name: adr
description: Write or update an Architecture Decision Record in Docs/ADR using the project's standard template. Use whenever an architectural decision is made or changed.
---

# Architecture Decision Records

ADRs live in `Docs/ADR/` and are named `NNNN-kebab-case-title.md`, with zero-padded sequential numbers
(`ls Docs/ADR` to find the next one). `Docs/ADR/README.md` is the index. Add a row there for every new ADR.

## Rules
- Only write an ADR for a decision the user actually made or approved. Never invent approval.
  If the decision is still open, ask (through the orchestrator) before writing it.
- ADRs are immutable once `Accepted`. To change a decision, write a new ADR. Set the old one's status to
  `Superseded by [NNNN](NNNN-....md)` and link back from the new one.
- Keep each ADR short and concrete: name the libraries, endpoints and numbers.
- ADRs go through a PR like any other change.

## Template (use exactly these sections, in this order)

```markdown
# NNNN. <Title in sentence case>

- **Status:** Proposed | Accepted | Superseded by [NNNN](NNNN-x.md)
- **Date:** YYYY-MM-DD
- **Author:** Helena Rivas (with <agent-name> agent)
- **Deciders:** Helena Rivas
- **Related:** #<issue>, [NNNN](NNNN-x.md)

## Summary

One or two sentences: the decision itself.

## Context

The forces at play: requirements, constraints, data facts (with numbers when known).

## Decision

What we will do, specifically.

## Rationale

Why this option. Include the alternatives considered and why they were rejected (a short pros/cons list or table).

## Consequences

Positive, negative and follow-ups (with issue links where they exist).
```
