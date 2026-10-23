# Architecture decision records

Decisions for the Enron email metrics service. Accepted ADRs are binding. To change one, write a new ADR that
supersedes it (see `.claude/skills/adr/SKILL.md`).

| ADR | Title | Status | Date |
|---|---|---|---|
| [0001](0001-use-postgres-as-the-datastore.md) | Use Postgres as the datastore | Accepted | 2026-10-23 |
| [0002](0002-no-full-text-indexing-store-headers-only.md) | No full-text indexing; store parsed headers only | Accepted | 2026-10-23 |
| [0003](0003-count-every-maildir-file-as-one-message.md) | Count every maildir file as one message | Accepted | 2026-10-23 |
| [0004](0004-add-fetch-endpoint-for-dataset-download.md) | Add a POST /fetch endpoint for dataset download | Accepted | 2026-10-23 |
| [0005](0005-no-authentication-or-authorization.md) | No authentication or authorization (extension point only) | Accepted | 2026-10-23 |
| [0006](0006-cloud-native-deployment-with-kustomize-and-kind.md) | Cloud-native deployment: container image, kustomize, kind and managed Postgres | Accepted | 2026-10-23 |
| [0007](0007-tolerate-malformed-dataset-entries.md) | Tolerate malformed dataset entries | Accepted | 2026-10-23 |
| [0008](0008-project-local-pinned-tooling-and-ci.md) | Project-local pinned tooling, pre-commit hooks and GitHub Actions CI | Accepted | 2026-10-23 |
| [0009](0009-extensible-layered-architecture.md) | Extensible layered architecture | Accepted | 2026-10-23 |
| [0010](0010-ingestion-state-in-postgres-with-advisory-lock.md) | Ingestion state in Postgres with advisory-lock single-flight | Proposed | 2026-10-23 |
