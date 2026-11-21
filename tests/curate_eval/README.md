# Curator eval cases

`cases.json` is a list of cases for the memory curator (owner pass + reference pass). Each case:

- `id`: `<lang>-<topic>` (ko/en). `description`: what it checks. `today`: run date.
- `entries`: the memory snapshot. `id` m-id, `bucket` (`core/owner`, `boundaries`, `thread:<key>`,
  `project:<owner/name>`, `general`), `prov` (owner|lane|tool|inference|subagent), `day`,
  `src` (`row=R s=N turn=T`; empty for non-owner entries), `text`. Order for replace = src row id.
- `rows`: window inbox rows from the owner. `id` row id, `conversation` (`slack:th:C0EVAL:NNNN` channel,
  `slack:dm:D0EVAL` / `discord:dm:NNN` DM), `date`, `text`, `reply` (lead reply or null),
  `reads_outside` (true: reply is NOT shown as context).
- `expect` (all keys present, empty list = none expected):
  - `save`: `{row, contains}` a saved sentence from that row containing the substring; optional
    `until` (expiry date) and `bucket` (target bucket, default the row's thread).
  - `no_save`: substrings no saved entry may contain.
  - `replace`: `{old, by}`; `by` may be `new:r<row>` = the entry saved from that row by an earlier
    run of the same case (e.g. a correction saved to its conversation that replaces a core entry
    and moves into core in its place).
  - `conflict`: `{a, b}` pairs reported, memory unchanged; either may be `new:r<row>`.
  - `promote`: m-ids moved into core/owner. `forget`: non-owner m-ids forgotten.
  - `untouched`: m-ids that must keep text, bucket and liveness.
  - Optional `promote_any`: entries saying the same thing; promoting any one is the hit, and they may
    be replaced by one another. Optional `maybe_save`: saves (as in `save`) that are fine but not
    required.
- Ops not listed are not expected; a scorer counts extra replace/promote/forget on owner or boundary
  entries as failures.
