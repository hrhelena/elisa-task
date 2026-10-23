---
name: kanban
description: Move issues across the GitHub Project kanban board (Backlog → Ready → In progress → In review → Done) and set Priority/Size. Use whenever a ticket changes state.
allowed-tools: Bash(.claude/skills/kanban/board.sh:*) Bash(gh:*)
---

# Kanban board

Board: https://github.com/users/hrhelena/projects/1 (repo `hrhelena/elisa-task`).
Always use the helper script instead of raw `gh project` calls. It knows the field and option IDs.

```bash
.claude/skills/kanban/board.sh add  <issue#> [Backlog|Ready] [P0|P1|P2] [XS|S|M|L|XL]
.claude/skills/kanban/board.sh move <issue#> "In progress"
.claude/skills/kanban/board.sh set  <issue#> priority P1
.claude/skills/kanban/board.sh list [Ready]
```

## Status lifecycle and who moves the card

| Status      | Meaning                                                    | Moved by          |
|-------------|------------------------------------------------------------|-------------------|
| Backlog     | Drafted by the product owner and not yet approved          | product-owner     |
| Ready       | The user approved it; it can be picked up                  | orchestrator/user |
| In progress | A coding agent is working on it on a branch                | coder             |
| In review   | A PR is open and under review                              | coder             |
| Done        | The PR is merged; the issue closes via `Closes #N`         | orchestrator      |

Rules:
- Only pick up `Ready` tickets, highest priority first, and respect `Depends on #N` in the ticket body.
- If you move a card back (for example, review sends work back), comment on the issue explaining why.
