//! The curator eval set: cases, seeding a home per case, and scoring a run against the case's
//! expectations. `main.rs` drives it with a scripted advisor (CI) or the real model (live).

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use moraebot::config::{Config, CurateMode};
use moraebot::curate::{CurateOp, Curator, RunReport};
use moraebot::memory::{
    self, Bucket, Day, Found, MemoryHandle, MemoryStore, NewEntry, Prov, Redactor, Source,
};
use moraebot::store::Store;
use serde::Deserialize;
use tempfile::TempDir;

/// The owner id every case's config and rows use.
pub const OWNER: &str = "111";

/// One case of `cases.json` (see its README).
#[derive(Debug, Clone, Deserialize)]
pub struct Case {
    pub id: String,
    pub today: String,
    pub entries: Vec<CaseEntry>,
    pub rows: Vec<CaseRow>,
    pub expect: Expect,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CaseEntry {
    pub id: String,
    pub bucket: String,
    pub prov: String,
    pub day: String,
    pub src: String,
    pub text: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CaseRow {
    pub id: i64,
    pub conversation: String,
    pub date: String,
    pub text: String,
    pub reply: Option<String>,
    pub reads_outside: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Expect {
    pub save: Vec<ExpectSave>,
    pub no_save: Vec<String>,
    pub replace: Vec<Pair>,
    pub conflict: Vec<ConflictPair>,
    pub promote: Vec<String>,
    pub forget: Vec<String>,
    pub untouched: Vec<String>,
    /// Entries saying the same thing: promoting any one of them is the hit, and they may be
    /// replaced by one another (a duplicate folded into the promoted one).
    #[serde(default)]
    pub promote_any: Vec<String>,
    /// Saves that are fine but not required (a repeat of a saved preference, for example).
    #[serde(default)]
    pub maybe_save: Vec<ExpectSave>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExpectSave {
    pub row: i64,
    pub contains: String,
    #[serde(default)]
    pub until: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Pair {
    pub old: String,
    pub by: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConflictPair {
    pub a: String,
    pub b: String,
}

/// Every case of `cases.json`.
pub fn cases() -> Vec<Case> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/curate_eval/cases.json");
    let text = std::fs::read_to_string(path).expect("cases.json readable");
    serde_json::from_str(&text).expect("cases.json parses")
}

/// A row's text as the inbox holds it: Slack text is unescaped at admission, as the Slack
/// bridge does.
pub fn admitted(row: &CaseRow) -> String {
    if row.conversation.starts_with("slack:") {
        moraebot::slack_bridge::slack::unescape_text(&row.text)
    } else {
        row.text.clone()
    }
}

pub fn day(text: &str) -> Day {
    Day::parse(text).unwrap_or_else(|| panic!("not a day: {text}"))
}

/// The case bucket name as a memory bucket.
pub fn bucket(name: &str) -> Bucket {
    match name {
        "core/owner" => Bucket::Owner,
        "boundaries" => Bucket::Boundaries,
        "general" => Bucket::General,
        _ => {
            if let Some(repo) = name.strip_prefix("project:") {
                Bucket::Project(repo.to_owned())
            } else if let Some(key) = name.strip_prefix("thread:") {
                Bucket::Thread(key.to_owned())
            } else {
                panic!("unknown case bucket {name}")
            }
        }
    }
}

fn prov(name: &str) -> Prov {
    match name {
        "owner" => Prov::Owner,
        "lane" => Prov::Lane,
        "tool" => Prov::Tool,
        "inference" => Prov::Inference,
        "subagent" => Prov::Subagent,
        _ => panic!("unknown prov {name}"),
    }
}

/// 12:00 UTC of `today`, in unix seconds.
pub fn noon(today: Day) -> i64 {
    today.days_after(day("1970-01-01")) * 86_400 + 43_200
}

/// What one entry looked like right after seeding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub text: String,
    pub bucket: Bucket,
    pub archived: bool,
}

impl Seen {
    fn of(found: &Found) -> Self {
        Self {
            text: found.entry.text.clone(),
            bucket: found.bucket.clone(),
            archived: found.archived,
        }
    }
}

/// One case's home: its inbox, its memory, and the case-id to real-id map.
pub struct Seeded {
    pub case: Case,
    /// Keeps the home alive.
    pub _dir: TempDir,
    pub config: Config,
    pub store: Arc<Mutex<Store>>,
    pub memory: MemoryHandle,
    /// Case m-id to the id memory gave the entry.
    pub ids: HashMap<String, String>,
    /// Real id to its state after seeding.
    pub before: HashMap<String, Seen>,
    pub today: Day,
}

/// The home of `case`. `config` gives the omp and model settings (live); its home, owner and
/// curate mode are set here.
pub fn seed(case: &Case, mut config: Config) -> Seeded {
    let dir = tempfile::tempdir().expect("temp dir");
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    config.home = home.clone();
    config.work_dir = dir.path().to_path_buf();
    config.memory.curate.mode = CurateMode::On;
    assert_eq!(config.owner_id(), OWNER, "the eval config's owner");
    let today = day(&case.today);

    let db = home.join("inbox.sqlite");
    // Creates the schema; closed again so the rows below can be written with their own ids.
    drop(Store::open(&db).expect("store opens"));
    let window: HashSet<i64> = case.rows.iter().map(|r| r.id).collect();
    // The rows the owner entries came from, so promotion evidence and ordering are real.
    let mut history: Vec<(i64, &CaseEntry)> = case
        .entries
        .iter()
        .filter(|e| e.prov == "owner")
        .filter_map(|e| Some((Source::new(&e.src).ok()?, e)))
        .filter_map(|(src, e)| {
            let row = src
                .as_str()
                .split(' ')
                .find_map(|p| p.strip_prefix("row=")?.parse::<i64>().ok())?;
            (!window.contains(&row)).then_some((row, e))
        })
        .collect();
    history.sort_by_key(|(row, _)| *row);
    history.dedup_by_key(|(row, _)| *row);
    {
        let conn = rusqlite::Connection::open(&db).expect("inbox opens");
        let insert = |id: i64,
                      conversation: &str,
                      on: &str,
                      text: &str,
                      reply: Option<&str>,
                      outside: bool| {
            let at = format!("{on}T10:00:00.000Z");
            conn.execute(
                "INSERT INTO inbox(id, platform, message_id, conversation_key, author_id, text, state, reply, received_at, updated_at, origin, reads_outside)
                 VALUES (?1, 'eval', ?2, ?3, ?4, ?5, 'done', ?6, ?7, ?7, 'owner', ?8)",
                rusqlite::params![id, format!("m{id}"), conversation, OWNER, text, reply, at, i64::from(outside)],
            )
            .expect("row inserted");
        };
        for (row, entry) in &history {
            let conversation = match bucket(&entry.bucket) {
                Bucket::Thread(key) => key,
                _ => "slack:dm:D0EVAL".to_owned(),
            };
            insert(*row, &conversation, &entry.day, &entry.text, None, false);
        }
        for row in &case.rows {
            insert(
                row.id,
                &row.conversation,
                &row.date,
                &admitted(row),
                row.reply.as_deref(),
                row.reads_outside,
            );
        }
    }
    let mut store = Store::open(&db).expect("store opens");
    if let Some((last, _)) = history.last() {
        let first = case.rows.iter().map(|r| r.id).min().unwrap_or(i64::MAX);
        assert!(
            *last < first,
            "{}: history row {last} after the window",
            case.id
        );
        store
            .finish_curate_run(&[], Some(*last), 0, 0, false)
            .expect("cursor set");
    }

    let clock_day = Arc::new(Mutex::new(today));
    let read = Arc::clone(&clock_day);
    let mut mem = MemoryStore::open(
        &home,
        Redactor::default(),
        Arc::new(move || *read.lock().expect("clock")),
    )
    .expect("memory opens");
    let mut ids = HashMap::new();
    for entry in &case.entries {
        *clock_day.lock().expect("clock") = day(&entry.day);
        let owner = entry.prov == "owner";
        let src = if owner {
            Source::new(&entry.src).expect("owner src")
        } else {
            Source::new(&format!("eval={}", entry.id)).expect("src")
        };
        let added = mem
            .add(NewEntry {
                text: entry.text.clone(),
                prov: prov(&entry.prov),
                bucket: bucket(&entry.bucket),
                src,
                expires: (!owner).then(|| today.plus_days(30)),
                why: None,
            })
            .unwrap_or_else(|e| panic!("{}: entry {} not added: {e:?}", case.id, entry.id));
        ids.insert(entry.id.clone(), added.id);
    }
    *clock_day.lock().expect("clock") = today;
    let memory = MemoryHandle::spawn(mem).expect("memory worker");
    let root = memory::memory_dir(&home);
    let before = ids
        .values()
        .map(|id| (id.clone(), Seen::of(&find(&root, id))))
        .collect();
    Seeded {
        case: case.clone(),
        _dir: dir,
        config,
        store: Arc::new(Mutex::new(store)),
        memory,
        ids,
        before,
        today,
    }
}

fn find(root: &Path, id: &str) -> Found {
    memory::find(root, id)
        .expect("memory readable")
        .unwrap_or_else(|| panic!("entry {id} not found"))
}

impl Seeded {
    pub fn root(&self) -> std::path::PathBuf {
        memory::memory_dir(&self.config.home)
    }

    /// The real id of case m-id `id`.
    pub fn id(&self, id: &str) -> String {
        self.ids
            .get(id)
            .unwrap_or_else(|| panic!("{}: unknown case id {id}", self.case.id))
            .clone()
    }

    /// The case entry of real id `id`.
    pub fn entry(&self, real: &str) -> &CaseEntry {
        let case_id = self
            .ids
            .iter()
            .find(|(_, r)| r.as_str() == real)
            .map(|(c, _)| c)
            .expect("seeded id");
        self.case
            .entries
            .iter()
            .find(|e| &e.id == case_id)
            .expect("case entry")
    }

    /// A curator on this home (real omp).
    pub fn curator(&self) -> Curator {
        Curator::new(
            &self.config,
            Arc::clone(&self.store),
            self.memory.clone(),
            Redactor::default(),
        )
    }

    pub fn now(&self) -> i64 {
        noon(self.today)
    }

    /// The real ids saved by `reports`.
    fn saved(&self, reports: &[RunReport]) -> Vec<String> {
        reports
            .iter()
            .flat_map(|r| &r.actions)
            .filter(|a| a.op == CurateOp::Save)
            .map(|a| a.entry_id.clone())
            .collect()
    }

    /// The real id of case id `id`; `new:r<row>` is the entry saved from that row.
    pub fn resolve(&self, reports: &[RunReport], id: &str) -> Option<String> {
        match id.strip_prefix("new:r") {
            Some(row) => self.saved_from(reports, row.parse().ok()?),
            None => Some(self.id(id)),
        }
    }

    /// The id saved from row `row`, if any.
    pub fn saved_from(&self, reports: &[RunReport], row: i64) -> Option<String> {
        let root = self.root();
        self.saved(reports)
            .into_iter()
            .find(|id| find(&root, id).entry.source_row() == Some(row))
    }
}

/// One case's numbers.
#[derive(Debug, Clone, Default)]
pub struct Score {
    pub saves: usize,
    pub good_saves: usize,
    pub gold_saves: usize,
    pub gold_saves_hit: usize,
    pub forbidden: usize,
    pub owner_changed: usize,
    pub replace: (usize, usize),
    pub conflict: (usize, usize),
    pub promote: (usize, usize),
    pub forget: (usize, usize),
    pub failures: usize,
    /// What went wrong, one line each.
    pub notes: Vec<String>,
}

fn hit(n: &mut (usize, usize), ok: bool) {
    n.1 += 1;
    n.0 += usize::from(ok);
}

/// Scores the runs `reports` of `seeded` against its case.
pub fn score(seeded: &Seeded, reports: &[RunReport]) -> Score {
    let case = &seeded.case;
    let root = seeded.root();
    let mut s = Score::default();
    for report in reports {
        if let Some(why) = &report.failed {
            s.failures += 1;
            s.notes.push(format!("run failed: {why}"));
        }
    }
    let rows: HashMap<i64, &CaseRow> = case.rows.iter().map(|r| (r.id, r)).collect();
    let save_ok = |gold: &ExpectSave, found: &Found| {
        let Some(row) = rows.get(&gold.row) else {
            return false;
        };
        let want = gold
            .bucket
            .as_deref()
            .map(bucket)
            .unwrap_or_else(|| Bucket::Thread(row.conversation.clone()));
        found.entry.source_row() == Some(gold.row)
            && found.entry.text.contains(&gold.contains)
            && found.bucket == want
            && found.entry.prov == Prov::Owner
            && gold.until.as_deref().map(day) == found.entry.expires
    };
    let saved: Vec<Found> = seeded
        .saved(reports)
        .iter()
        .map(|id| find(&root, id))
        .collect();
    s.saves = saved.len();
    s.gold_saves = case.expect.save.len();
    for found in &saved {
        let good = case
            .expect
            .save
            .iter()
            .chain(&case.expect.maybe_save)
            .any(|g| save_ok(g, found));
        s.good_saves += usize::from(good);
        if !good {
            s.notes.push(format!(
                "extra save [{}] {}",
                found.bucket.label(),
                found.entry.text
            ));
        }
        for bad in &case.expect.no_save {
            if found.entry.text.contains(bad.as_str()) {
                s.forbidden += 1;
                s.notes
                    .push(format!("forbidden save: {}", found.entry.text));
            }
        }
    }
    for gold in &case.expect.save {
        let ok = saved.iter().any(|f| save_ok(gold, f));
        s.gold_saves_hit += usize::from(ok);
        if !ok {
            s.notes
                .push(format!("missed save r{}: {}", gold.row, gold.contains));
        }
    }
    let actions: Vec<_> = reports.iter().flat_map(|r| &r.actions).collect();
    for pair in &case.expect.replace {
        let old = seeded.id(&pair.old);
        let by = seeded.resolve(reports, &pair.by);
        let found = find(&root, &old);
        let ok = by.is_some() && found.archived && found.entry.superseded_by == by;
        hit(&mut s.replace, ok);
        if !ok {
            s.notes
                .push(format!("missed replace {} by {}", pair.old, pair.by));
        }
    }
    for pair in &case.expect.conflict {
        let (a, b) = (
            seeded.resolve(reports, &pair.a),
            seeded.resolve(reports, &pair.b),
        );
        let ok = actions.iter().any(|x| {
            let (e, o) = (Some(x.entry_id.clone()), x.other_id.clone());
            x.op == CurateOp::Conflict
                && a.is_some()
                && b.is_some()
                && ((e == a && o == b) || (e == b && o == a))
        });
        hit(&mut s.conflict, ok);
        if !ok {
            s.notes
                .push(format!("missed conflict {} {}", pair.a, pair.b));
        }
    }
    for id in &case.expect.promote {
        let found = find(&root, &seeded.id(id));
        let ok = !found.archived && found.bucket == Bucket::Owner;
        hit(&mut s.promote, ok);
        if !ok {
            s.notes.push(format!("missed promote {id}"));
        }
    }
    if !case.expect.promote_any.is_empty() {
        let group: Vec<String> = case
            .expect
            .promote_any
            .iter()
            .map(|id| seeded.id(id))
            .collect();
        let ok = group.iter().any(|id| {
            let found = find(&root, id);
            !found.archived && found.bucket == Bucket::Owner
        });
        hit(&mut s.promote, ok);
        if !ok {
            s.notes.push(format!(
                "missed promote of one of {:?}",
                case.expect.promote_any
            ));
        }
    }
    for id in &case.expect.forget {
        let ok = find(&root, &seeded.id(id)).archived;
        hit(&mut s.forget, ok);
        if !ok {
            s.notes.push(format!("missed forget {id}"));
        }
    }
    // Owner entries (boundaries included) may change only as the case expects; untouched
    // entries of any kind must not change at all.
    let expected_change: HashSet<String> = case
        .expect
        .replace
        .iter()
        .map(|p| seeded.id(&p.old))
        .chain(case.expect.promote.iter().map(|id| seeded.id(id)))
        .chain(case.expect.promote_any.iter().map(|id| seeded.id(id)))
        .collect();
    let untouched: HashSet<String> = case
        .expect
        .untouched
        .iter()
        .map(|id| seeded.id(id))
        .collect();
    for (id, before) in &seeded.before {
        let now = Seen::of(&find(&root, id));
        if &now == before {
            continue;
        }
        let owner = seeded.entry(id).prov == "owner";
        if untouched.contains(id) || (owner && !expected_change.contains(id)) {
            s.owner_changed += 1;
            s.notes.push(format!(
                "{} changed: {:?} -> {:?}",
                seeded.entry(id).id,
                before,
                now
            ));
        }
    }
    s
}

/// Sums of many cases' scores.
#[derive(Debug, Clone, Default)]
pub struct Total {
    pub cases: usize,
    pub saves: usize,
    pub good_saves: usize,
    pub gold_saves: usize,
    pub gold_saves_hit: usize,
    pub forbidden: usize,
    pub owner_changed: usize,
    pub replace: (usize, usize),
    pub conflict: (usize, usize),
    pub promote: (usize, usize),
    pub forget: (usize, usize),
    pub failures: usize,
}

fn add(a: &mut (usize, usize), b: (usize, usize)) {
    a.0 += b.0;
    a.1 += b.1;
}

fn ratio(n: usize, d: usize) -> f64 {
    if d == 0 { 1.0 } else { n as f64 / d as f64 }
}

impl Total {
    pub fn add(&mut self, s: &Score) {
        self.cases += 1;
        self.saves += s.saves;
        self.good_saves += s.good_saves;
        self.gold_saves += s.gold_saves;
        self.gold_saves_hit += s.gold_saves_hit;
        self.forbidden += s.forbidden;
        self.owner_changed += s.owner_changed;
        add(&mut self.replace, s.replace);
        add(&mut self.conflict, s.conflict);
        add(&mut self.promote, s.promote);
        add(&mut self.forget, s.forget);
        self.failures += s.failures;
    }

    pub fn save_precision(&self) -> f64 {
        ratio(self.good_saves, self.saves)
    }

    pub fn save_recall(&self) -> f64 {
        ratio(self.gold_saves_hit, self.gold_saves)
    }

    /// Recall of replace, promote and forget together.
    pub fn change_recall(&self) -> f64 {
        let hits = self.replace.0 + self.promote.0 + self.forget.0;
        let all = self.replace.1 + self.promote.1 + self.forget.1;
        ratio(hits, all)
    }

    /// Where the totals miss the pass bar (docs/curate-eval.md).
    pub fn misses(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.forbidden > 0 {
            out.push(format!("{} forbidden saves", self.forbidden));
        }
        if self.owner_changed > 0 {
            out.push(format!(
                "{} owner entries wrongly changed",
                self.owner_changed
            ));
        }
        if self.save_precision() < 0.8 {
            out.push(format!(
                "save precision {:.2} < 0.80",
                self.save_precision()
            ));
        }
        if self.save_recall() < 0.6 {
            out.push(format!("save recall {:.2} < 0.60", self.save_recall()));
        }
        if self.change_recall() < 0.6 {
            out.push(format!(
                "replace/promote/forget recall {:.2} < 0.60",
                self.change_recall()
            ));
        }
        out
    }
}

fn frac(n: (usize, usize)) -> String {
    format!("{}/{}", n.0, n.1)
}

/// A markdown table: one line per case and a total line.
pub fn table(rows: &[(String, Score)], total: &Total) -> String {
    let mut out = String::from(
        "| case | saves good/all | gold saves hit | forbidden | owner changed | replace | conflict | promote | forget | failures |\n|---|---|---|---|---|---|---|---|---|---|\n",
    );
    for (id, s) in rows {
        out.push_str(&format!(
            "| {id} | {}/{} | {}/{} | {} | {} | {} | {} | {} | {} | {} |\n",
            s.good_saves,
            s.saves,
            s.gold_saves_hit,
            s.gold_saves,
            s.forbidden,
            s.owner_changed,
            frac(s.replace),
            frac(s.conflict),
            frac(s.promote),
            frac(s.forget),
            s.failures
        ));
    }
    out.push_str(&format!(
        "| **total ({} cases)** | precision {:.2} | recall {:.2} | {} | {} | {} | {} | {} | {} | {} |\n",
        total.cases,
        total.save_precision(),
        total.save_recall(),
        total.forbidden,
        total.owner_changed,
        frac(total.replace),
        frac(total.conflict),
        frac(total.promote),
        frac(total.forget),
        total.failures
    ));
    out
}
