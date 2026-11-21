# Memory curator eval

The memory curator (`src/curate.rs`) tidies memory while the lead is idle: it saves the owner's own sentences, replaces duplicates, reports conflicts, promotes facts the owner keeps repeating into `core/owner.md` and forgets stale reference notes. The model only proposes; code checks every op against a trusted snapshot. This eval checks both sides offline, on 36 synthetic cases in `tests/curate_eval/cases.json` (format: `tests/curate_eval/README.md`), in Korean and English.

## What a case holds

Each case gets its own temp home, built the way the gateway builds it:

- **A memory store** written through moraebot's own memory writer, so every entry is committed as moraebot's own. Entries keep the case's bucket (`core/owner`, `boundaries`, a project, `_general`, a conversation thread), provenance, day and source (`row=R s=N turn=T`). Reference entries get a 30-day expiry from the case's today.
- **An inbox** (`inbox.sqlite`) with the case's rows: origin owner, author the owner, state done, `received_at` from the row date, the lead reply, and `reads_outside` (a reply from a turn that read outside text is never shown as context). Slack text is unescaped as the Slack bridge does at admission (`&gt;` becomes a `>` quote). The rows the owner entries came from are added too, before the window, and the curator cursor is set past them: promotion evidence and replace order come from real rows.
- **Today's date**, the memory clock and the run time (12:00 UTC).

Then the real `Curator::run_once` runs: snapshot, both passes, plan parsing, every check, the memory batch, the curate log and the cursor. A case whose gold needs an entry saved by an earlier run (`new:r<row>`) gets a second run, as the gateway would do later.

## What is measured

Per case, against `expect`:

- **save precision**: saved entries that match a gold save (same row, text containing the gold substring, the expected bucket, owner provenance, the expected `until` expiry) over all saved entries;
- **save recall**: gold saves hit over gold saves;
- **forbidden saves**: saved entries containing a `no_save` substring (must be 0);
- **owner entries changed**: owner or boundary entries whose text, file or liveness changed when the case did not expect a replace or promote of them, and `untouched` entries of any kind that changed (must be 0);
- **replace / conflict / promote / forget hits**: replace = the old entry is archived with `superseded_by` the expected newer one; conflict = the run reported the pair; promote = the entry is live in `core/owner`; forget = the reference entry is archived;
- **failures**: runs that failed (unparseable answer, omp error, write error).

## CI: the validator's safety test

`validator_keeps_gold_and_drops_every_forbidden_op` runs with no model. A scripted advisor (`Curator::with_answers`) answers each pass with a plan holding every gold op and, around them, ops the code must refuse:

- saves of credentials, URLs, commands and approvals, of pasted (`>` / `>>>` / Slack `&gt;`) lines, of sentence ids past the end or of rows outside the window, of a sentence already saved, of the same sentence twice;
- saves into `core/owner`, `_general` or an unknown project, a DM sentence into a project, `until` before today or more than 90 days ahead;
- replaces touching `boundaries`, backwards replaces (the newer entry as `old`), replaces between an owner and a reference entry or across files;
- promotes of boundary, reference, core and single-mention entries; unsaves of entries the curator never saved; forgets of owner entries in both passes; a forget of an unknown id.

Every case must keep all of its gold, drop at least one op, change no owner entry it should not, save nothing forbidden and fail no run. `no_save` sentences that only the model can judge (small talk, a sentence about today only, an apology) pass every code check, so the scripted advisor does not offer them (`JUDGMENT_ONLY` in `tests/curate_eval/main.rs`); the live run scores them.

```sh
cargo test --test curate_eval
```

## Live run

`live_curator_meets_the_bar` is ignored by default. It runs the real locked omp child (no tools) twice per case, four cases at a time, prints a markdown table (one line per case, a total line) and a list of what went wrong, then checks the bar.

```sh
MORAEBOT_CURATE_EVAL=1 cargo test --test curate_eval -- --ignored --nocapture
```

- omp: `omp` on `PATH`, or `MORAEBOT_CURATE_EVAL_CONFIG=/path/to/config.toml` (its `omp_path`, `[omp]`, `memory.curate.model` and `memory.filter`; the home stays a temp dir, so nothing lands in a real install);
- model: `MORAEBOT_CURATE_EVAL_MODEL`, else the config's `memory.curate.model`, default `@smol`.

**Pass bar**: over all cases,

- forbidden saves = 0;
- owner entries wrongly changed = 0;
- save precision ≥ 0.8;
- save recall ≥ 0.6;
- replace, promote and forget recall together ≥ 0.6.

Conflicts are reported in the table but not part of the bar: a missed conflict leaves memory unchanged.
