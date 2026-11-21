//! The offline eval set of the memory curator; how to run it is in `docs/curate-eval.md`.
//! CI drives the real `Curator::run_once` (checks and apply) with a scripted advisor whose
//! plans hold every gold op plus ops the code must refuse. The live run
//! (`live_curator_meets_the_bar`) is ignored and needs `MORAEBOT_CURATE_EVAL=1`.

mod eval;

use std::collections::HashSet;
use std::path::PathBuf;

use eval::{Case, Score, Seeded, Total};
use futures_util::stream::{self, StreamExt};
use moraebot::config::{AccessMode, Config, DiscordConfig, Env, MemoryConfig, OmpConfig, Platform};
use moraebot::curate::RunReport;
use moraebot::memory::{self, Bucket};
use serde_json::{Value, json};

/// `no_save` phrases that only the model can refuse: plain own sentences (small talk, a
/// lasting-looking sentence the case says is not worth keeping) that pass every code check.
/// The scripted advisor does not offer them; the live run scores them.
const JUDGMENT_ONLY: &[&str] = &[
    "미안 잘못 말했어",
    "meetings all afternoon today",
    "응 그렇게 해",
    "고마워 수고했어",
    "lol nice",
    "ok sounds good",
];

/// A config for `home` with the eval owner; omp is never started in CI.
fn base_config(home: PathBuf) -> Config {
    Config {
        messenger: Platform::Discord,
        work_dir: home.clone(),
        omp_path: home.join("omp"),
        home: home.clone(),
        access_mode: AccessMode::OwnerOnly,
        discord: Some(DiscordConfig {
            owner_id: eval::OWNER.parse().expect("owner"),
            channel_id: "2".parse().expect("channel"),
            token_file: home.join("unused.token"),
            access: Default::default(),
        }),
        bridge: None,
        omp: OmpConfig::default(),
        lead: Default::default(),
        memory: MemoryConfig::default(),
    }
}

/// The sentence id of the own sentence of row `row` containing `part`.
fn sentence_id(case: &Case, row: i64, part: &str) -> Option<String> {
    let text = &case.rows.iter().find(|r| r.id == row)?.text;
    memory::own_sentences(text)
        .into_iter()
        .find(|(_, s)| s.contains(part))
        .map(|(n, _)| format!("r{row}.s{n}"))
}

/// Which passes the curator will ask, from the live state of the home.
fn passes(seeded: &Seeded, window: bool, saved: usize) -> (bool, bool) {
    let root = seeded.root();
    let (mut owner, mut reference) = (window || saved > 0, false);
    for (case_id, real) in &seeded.ids {
        let Some(found) = memory::find(&root, real).expect("memory readable") else {
            continue;
        };
        if found.archived || matches!(found.bucket, Bucket::Boundaries) {
            continue;
        }
        let is_owner = seeded
            .case
            .entries
            .iter()
            .any(|e| &e.id == case_id && e.prov == "owner");
        if is_owner {
            owner = true;
        } else if !found.bucket.is_core() {
            reference = true;
        }
    }
    (owner, reference)
}

/// The first run's scripted plans: gold ops, each preceded or followed by ops the code must
/// drop (forbidden saves, owner forgets, boundary replaces, DM to project, pasted and unknown
/// sentences, saves out of range).
fn scripted_first(seeded: &Seeded) -> (Value, Value) {
    let case = &seeded.case;
    let id = |m: &str| seeded.id(m);
    let owner_ids: Vec<String> = case
        .entries
        .iter()
        .filter(|e| e.prov == "owner")
        .map(|e| id(&e.id))
        .collect();
    let boundary_ids: Vec<String> = case
        .entries
        .iter()
        .filter(|e| e.bucket == "boundaries")
        .map(|e| id(&e.id))
        .collect();
    let other_ids: Vec<String> = case
        .entries
        .iter()
        .filter(|e| e.prov != "owner")
        .map(|e| id(&e.id))
        .collect();
    let is_owner = |m: &str| case.entries.iter().any(|e| e.id == m && e.prov == "owner");

    let mut save = Vec::new();
    let mut gold_sentences = HashSet::new();
    for gold in &case.expect.save {
        let sentence = sentence_id(case, gold.row, &gold.contains)
            .unwrap_or_else(|| panic!("{}: no own sentence for {}", case.id, gold.contains));
        gold_sentences.insert(sentence.clone());
        let row = case.rows.iter().find(|r| r.id == gold.row).expect("row");
        // Refused first, so the gold op after them is what lands.
        save.push(json!({"sentence": sentence, "bucket": "core/owner"}));
        save.push(json!({"sentence": sentence, "bucket": "project:_general"}));
        save.push(json!({"sentence": sentence, "bucket": "project:acme/nowhere"}));
        save.push(json!({"sentence": sentence, "until": seeded.today.plus_days(200).to_string()}));
        save.push(json!({"sentence": sentence, "until": seeded.today.plus_days(-1).to_string()}));
        if moraebot::recall::is_dm(&row.conversation) {
            for e in case
                .entries
                .iter()
                .filter(|e| e.bucket.starts_with("project:"))
            {
                save.push(json!({"sentence": sentence, "bucket": e.bucket}));
            }
        }
        let mut pick = json!({"sentence": sentence});
        if let Some(until) = &gold.until {
            pick["until"] = json!(until);
        }
        if let Some(bucket) = &gold.bucket {
            pick["bucket"] = json!(bucket);
        }
        save.push(pick);
        // The same sentence again: saved twice in one plan.
        save.push(json!({"sentence": sentence}));
    }
    for bad in &case.expect.no_save {
        if JUDGMENT_ONLY.contains(&bad.as_str()) {
            continue;
        }
        for row in &case.rows {
            if let Some(sentence) = sentence_id(case, row.id, bad) {
                save.push(json!({"sentence": sentence}));
            }
        }
    }
    for row in &case.rows {
        let own: HashSet<u32> = memory::own_sentences(&row.text)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        let last = own.iter().max().copied().unwrap_or(0) + 4;
        // Pasted lines, sentences past the end, and a row that is not in the window.
        for n in (0..=last).filter(|n| !own.contains(n)) {
            save.push(json!({"sentence": format!("r{}.s{n}", row.id)}));
        }
    }
    save.push(json!({"sentence": "r999999.s0"}));
    save.push(json!({"sentence": "not a sentence"}));

    let mut replace = Vec::new();
    for pair in case
        .expect
        .replace
        .iter()
        .filter(|p| !p.by.starts_with("new:"))
    {
        if is_owner(&pair.old) {
            // Backwards first: `by` is older.
            replace.push(json!({"old": id(&pair.by), "by": id(&pair.old)}));
            replace.push(json!({"old": id(&pair.old), "by": id(&pair.by)}));
        }
    }
    for b in &boundary_ids {
        for o in owner_ids.iter().filter(|o| *o != b) {
            replace.push(json!({"old": b, "by": o}));
            replace.push(json!({"old": o, "by": b}));
        }
    }
    for o in &owner_ids {
        for x in &other_ids {
            replace.push(json!({"old": o, "by": x}));
        }
    }
    let conflict: Vec<Value> = case
        .expect
        .conflict
        .iter()
        .filter(|p| !p.a.starts_with("new:") && !p.b.starts_with("new:"))
        .map(|p| json!({"a": id(&p.a), "b": id(&p.b)}))
        .collect();
    let mut promote: Vec<String> = boundary_ids.clone();
    promote.extend(other_ids.iter().cloned());
    promote.extend(case.expect.promote.iter().map(|m| id(m)));
    promote.extend(
        case.expect
            .untouched
            .iter()
            .filter(|m| is_owner(m))
            .map(|m| id(m)),
    );
    let owner_plan = json!({
        "save": save,
        "replace": replace,
        "conflict": conflict,
        "promote": promote,
        "unsave": owner_ids,
        "forget": owner_ids,
    });

    let mut forget: Vec<String> = owner_ids.clone();
    forget.push("m-999999".to_owned());
    forget.extend(case.expect.forget.iter().map(|m| id(m)));
    let mut ref_replace = Vec::new();
    for pair in case.expect.replace.iter().filter(|p| !is_owner(&p.old)) {
        // Backwards first: `by` is older.
        ref_replace.push(json!({"old": id(&pair.by), "by": id(&pair.old)}));
        ref_replace.push(json!({"old": id(&pair.old), "by": id(&pair.by)}));
    }
    for o in &owner_ids {
        for o2 in owner_ids.iter().filter(|o2| *o2 != o) {
            ref_replace.push(json!({"old": o, "by": o2}));
        }
    }
    let reference_plan = json!({"forget": forget, "replace": ref_replace});
    (owner_plan, reference_plan)
}

/// The answers for the passes the curator will ask.
fn answers(
    seeded: &Seeded,
    window: bool,
    saved: usize,
    owner: Value,
    reference: Value,
) -> Vec<Result<String, String>> {
    let (ask_owner, ask_reference) = passes(seeded, window, saved);
    let mut out = Vec::new();
    if ask_owner {
        out.push(Ok(format!("Here is the plan: {owner}")));
    }
    if ask_reference {
        out.push(Ok(reference.to_string()));
    }
    out
}

/// One case with the scripted advisor: a first run, and a second when a replace needs an
/// entry the first run saved.
async fn run_scripted(case: &Case) -> (Seeded, Vec<RunReport>) {
    let seeded = eval::seed(case, base_config(std::env::temp_dir()));
    let (owner, reference) = scripted_first(&seeded);
    let first = seeded
        .curator()
        .with_answers(answers(&seeded, !case.rows.is_empty(), 0, owner, reference))
        .run_once(seeded.now())
        .await;
    let mut reports = vec![first];
    let new_id = |id: &str| -> Option<String> {
        let row = id.strip_prefix("new:r")?.parse().ok()?;
        seeded.saved_from(&reports, row)
    };
    let resolve = |id: &str| new_id(id).unwrap_or_else(|| seeded.id(id));
    let wants_new = |a: &str, b: &str| a.starts_with("new:") || b.starts_with("new:");
    // A conflict pair offered as a replace first: across files or unlike texts, never applied.
    let replace: Vec<Value> = case
        .expect
        .replace
        .iter()
        .map(|p| (&p.old, &p.by))
        .chain(case.expect.conflict.iter().map(|p| (&p.a, &p.b)))
        .filter(|(old, by)| wants_new(old, by))
        .map(|(old, by)| json!({"old": resolve(old), "by": resolve(by)}))
        .collect();
    let conflict: Vec<Value> = case
        .expect
        .conflict
        .iter()
        .filter(|p| wants_new(&p.a, &p.b))
        .map(|p| json!({"a": resolve(&p.a), "b": resolve(&p.b)}))
        .collect();
    if !replace.is_empty() || !conflict.is_empty() {
        let saved = reports[0].actions.len();
        let owner = json!({"replace": replace, "conflict": conflict});
        let second = seeded
            .curator()
            .with_answers(answers(&seeded, false, saved, owner, json!({})))
            .run_once(seeded.now())
            .await;
        reports.push(second);
    }
    (seeded, reports)
}

#[tokio::test]
async fn validator_keeps_gold_and_drops_every_forbidden_op() {
    let cases = eval::cases();
    assert!(cases.len() >= 30, "the eval set has {} cases", cases.len());
    let mut total = Total::default();
    let mut rows: Vec<(String, Score)> = Vec::new();
    let mut bad = Vec::new();
    for case in &cases {
        let (seeded, reports) = run_scripted(case).await;
        let score = eval::score(&seeded, &reports);
        let dropped: usize = reports.iter().map(|r| r.dropped).sum();
        let perfect = score.failures == 0
            && score.forbidden == 0
            && score.owner_changed == 0
            && score.good_saves == score.saves
            && score.gold_saves_hit == score.gold_saves
            && score.replace.0 == score.replace.1
            && score.conflict.0 == score.conflict.1
            && score.promote.0 == score.promote.1
            && score.forget.0 == score.forget.1
            && dropped > 0;
        if !perfect {
            bad.push(format!(
                "{} (dropped {dropped}): {:?}",
                case.id, score.notes
            ));
        }
        total.add(&score);
        rows.push((case.id.clone(), score));
    }
    println!("{}", eval::table(&rows, &total));
    assert!(bad.is_empty(), "cases off gold:\n{}", bad.join("\n"));
}

/// The config of the live run: `MORAEBOT_CURATE_EVAL_CONFIG` (a moraebot `config.toml`, for its
/// `omp_path`, `[omp]` and `memory.curate.model`) or the `omp` on `PATH`;
/// `MORAEBOT_CURATE_EVAL_MODEL` overrides the model (default `@smol`). The owner is always the
/// eval owner and the home a temp dir, so nothing lands in a real install.
fn live_config(home: PathBuf) -> Config {
    let mut config = base_config(home.clone());
    match std::env::var_os("MORAEBOT_CURATE_EVAL_CONFIG") {
        Some(path) => {
            let loaded =
                Config::load(&PathBuf::from(path), &Env::from_process()).expect("config loads");
            config.omp_path = loaded.omp_path;
            config.omp = loaded.omp;
            config.memory.curate.model = loaded.memory.curate.model;
            config.memory.filter = loaded.memory.filter;
        }
        None => {
            config.omp_path =
                on_path("omp").expect("omp on PATH, or set MORAEBOT_CURATE_EVAL_CONFIG");
        }
    }
    if let Ok(model) = std::env::var("MORAEBOT_CURATE_EVAL_MODEL") {
        config.memory.curate.model = model;
    }
    config
}

fn on_path(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(name))
        .find(|path| path.is_file())
}

#[tokio::test]
#[ignore = "calls a real model; run with MORAEBOT_CURATE_EVAL=1 (docs/curate-eval.md)"]
async fn live_curator_meets_the_bar() {
    if std::env::var("MORAEBOT_CURATE_EVAL").as_deref() != Ok("1") {
        eprintln!("MORAEBOT_CURATE_EVAL is not 1: live curator eval skipped");
        return;
    }
    let cases = eval::cases();
    let scratch = tempfile::tempdir().expect("temp dir");
    let config = live_config(scratch.path().to_path_buf());
    let model = config
        .memory
        .curate
        .model()
        .unwrap_or("omp default")
        .to_owned();
    // Two runs per case, as in the gateway: the second sees what the first saved.
    let scored: Vec<(usize, Score)> = stream::iter(cases.iter().enumerate())
        .map(|(n, case)| {
            let config = config.clone();
            async move {
                let seeded = eval::seed(case, config);
                let mut reports = Vec::new();
                for _ in 0..2 {
                    reports.push(seeded.curator().run_once(seeded.now()).await);
                }
                (n, eval::score(&seeded, &reports))
            }
        })
        .buffer_unordered(4)
        .collect()
        .await;
    let mut scored = scored;
    scored.sort_by_key(|(n, _)| *n);
    let mut total = Total::default();
    let mut rows = Vec::new();
    for (n, score) in scored {
        total.add(&score);
        rows.push((cases[n].id.clone(), score));
    }
    println!(
        "curator eval, model {model}\n\n{}",
        eval::table(&rows, &total)
    );
    for (id, score) in &rows {
        for note in &score.notes {
            println!("- {id}: {note}");
        }
    }
    let misses = total.misses();
    assert!(misses.is_empty(), "below the pass bar: {misses:?}");
}
