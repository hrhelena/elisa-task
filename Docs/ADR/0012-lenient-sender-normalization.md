# 0012. Lenient sender normalization

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#6](https://github.com/hrhelena/elisa-task/issues/6), [#9](https://github.com/hrhelena/elisa-task/issues/9), [0003](0003-count-every-maildir-file-as-one-message.md), [0007](0007-tolerate-malformed-dataset-entries.md), [0011](0011-http-api-contract-and-start-semantics.md)

## Summary

The sender used for `/top-senders` is the `From` address, parsed strictly first and leniently as a fallback, then
lowercased. Broken-but-usable addresses are kept as written; messages with no usable address still count as messages
but are excluded from the ranking and reported as `invalid_sender`.

## Context

A scan of the 517,401 files on 2026-10-23 found:
- Every file has a `From:` line, and addresses are already lowercase.
- About 1,557 values have the form `name <.local@enron.com>` (for example `houston <.ward@enron.com>`), which strict
  RFC 5322 parsing rejects because of the leading dot.
- One value has no domain: `pep <performance.>`.
- `X-From` holds display names (`Phillip K Allen`), not addresses.

## Decision

1. Parse with `net/mail.ParseAddress`. On failure, fall back to the text inside the last `<...>`, or else the first
   whitespace-separated token containing `@`. Trim spaces, quotes and trailing dots, then lowercase.
2. The result is valid when it has exactly one `@`, a non-empty local part and a domain containing a `.`.
   `.ward@enron.com` is valid and kept as written (no repair).
3. No usable address (or no `From` header): the sender is stored as `NULL`. The message counts in `/status` totals,
   is excluded from `/top-senders`, and increments `field_errors.invalid_sender` (or `missing_sender` when the header is absent).
4. `X-From` is stored raw and never used for ranking or repair.

## Rationale

| Option | Pros | Cons |
|---|---|---|
| Lenient, keep as written (chosen) | Predictable, explainable, keeps about 0.3% of files ranked | `.ward@enron.com` appears as its own sender |
| Strict, reject broken forms | Clean addresses only | Drops about 1,557 messages from the ranking for a cosmetic defect |
| Repair from `X-From` | Could recover the real person | Guesswork; display names do not map reliably to addresses |

## Consequences

- Positive: rules fit in a short table test; every ranked sender comes straight from the header.
- Negative: a few odd sender strings (`.ward@enron.com`) can appear in the ranking.
- Follow-ups: #6 implements the rules; the README (#14) explains them.
