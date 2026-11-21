//! Lanes driven through a real `OmpManager` with fake-omp as lead (`FAKE_OMP_MODE=tools`)
//! and as lanes (`FAKE_OMP_LANE_MODE`). Only the lead prompt that should call tools
//! contains the trigger `run-tools`, so each test starts an exact number of lanes. Every
//! test ends with `shutdown_all`, which releases held lanes.

use std::future::Future;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use moraebot::access::{E_ACCESS_NOT_OWNER, Sender};
use moraebot::config::{AccessMode, Config, DiscordConfig, OmpConfig, Platform, Role};
use moraebot::done::{self, E_DONE_SYNTAX, E_DONE_UNMET};
use moraebot::lane::{
    self, E_LANE_ARGS, E_LANE_BUSY, E_LANE_CLOSED, E_LANE_INTERRUPTED, E_LANE_LIMIT,
    E_LANE_STOPPING, E_LANE_UNKNOWN,
};
use moraebot::lane::{E_LANE_IDLE, E_LANE_STOPPED};
use moraebot::lane::{LANE_REPORT_TOOL, LaneNews, REPORT_FOOTER};
use moraebot::lead;
use moraebot::lead::LaneResult;
use moraebot::memory_tools::{MEMORY_SEARCH_TOOL, MEMORY_WRITE_TOOL};
use moraebot::omp::{self, OmpManager, Outcome};
use moraebot::store::{LaneState, Store};
use moraebot::testing::appended;
use moraebot::worktree::{self, E_LANE_GIT, E_LANE_REMOVE, E_LANE_REPO};
use serde_json::{Value, json};

const KEY: &str = "discord:th:111";
const BOUND: Duration = Duration::from_secs(30);

struct Harness {
    dir: tempfile::TempDir,
    config: Config,
    store: Arc<Mutex<Store>>,
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let home = dir.path().join("qa-mbhome");
        let work_dir = dir.path().join("work");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&work_dir).expect("work dir");
        let config = Config {
            messenger: Platform::Discord,
            work_dir,
            omp_path: PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")),
            home: home.clone(),
            access_mode: AccessMode::OwnerOnly,
            discord: Some(DiscordConfig {
                owner_id: 111,
                channel_id: 222,
                token_file: home.join("secrets").join("discord.token"),
                access: Default::default(),
            }),
            bridge: None,
            omp: OmpConfig::default(),
            lead: Default::default(),
            memory: moraebot::config::MemoryConfig::default(),
        };
        let store = Store::open(&home.join("inbox.sqlite")).expect("store");
        Self {
            dir,
            config,
            store: Arc::new(Mutex::new(store)),
        }
    }

    fn log(&self) -> PathBuf {
        self.dir.path().join("fake-omp.log")
    }

    /// A manager whose lead runs `tools` with `calls` (one JSON object per line) and
    /// whose lanes run `lane_mode`.
    fn manager(&self, lane_mode: &str, calls: &[Value]) -> OmpManager {
        let script: Vec<String> = calls.iter().map(Value::to_string).collect();
        OmpManager::new(self.config.clone(), Arc::clone(&self.store)).with_extra_env([
            ("FAKE_OMP_MODE", "tools".to_owned()),
            ("FAKE_OMP_LANE_MODE", lane_mode.to_owned()),
            ("FAKE_OMP_TOOL_CALLS", script.join("\n")),
            ("FAKE_OMP_LOG", self.log().to_string_lossy().into_owned()),
        ])
    }

    fn log_lines(&self) -> Vec<String> {
        std::fs::read_to_string(self.log())
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// `(is_error, text)` of every `host_tool_result` the fake received, in order.
    fn tool_results(&self) -> Vec<(bool, String)> {
        self.log_lines()
            .iter()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|f| f["type"] == "host_tool_result")
            .map(|f| {
                (
                    f["isError"] == true,
                    f["result"]["content"][0]["text"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                )
            })
            .collect()
    }

    /// Indexes (in the log) of the `START` lines of lane children.
    fn lane_starts(&self) -> Vec<usize> {
        self.log_lines()
            .iter()
            .enumerate()
            .filter(|(_, l)| l.starts_with("START pid="))
            .filter(|(_, l)| session_dir_arg(l).contains("/lanes/"))
            .map(|(n, _)| n)
            .collect()
    }

    fn lane(&self, id: i64) -> moraebot::store::LaneRow {
        self.store
            .lock()
            .unwrap()
            .lane(id)
            .unwrap()
            .expect("lane exists")
    }
}

fn start_argv(line: &str) -> Vec<String> {
    let json = line.split_once("argv=").expect("argv in START line").1;
    serde_json::from_str(json).expect("argv json")
}

fn session_dir_arg(line: &str) -> String {
    let argv = start_argv(line);
    let at = argv
        .iter()
        .position(|a| a == "--session-dir")
        .expect("--session-dir");
    argv[at + 1].clone()
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(BOUND, future)
        .await
        .expect("operation finished within bound")
}

fn lane_start(task: &str) -> Value {
    lane_start_done(task, "file .")
}

fn lane_start_done(task: &str, done: &str) -> Value {
    json!({"tool": lane::LANE_START_TOOL, "arguments": {"task": task, "done": done}})
}

/// A check command that appends its own pid to the fake-omp log and keeps that pid as
/// the group leader until it is killed.
const CHECK_ARGV: [&str; 3] = [
    "/bin/sh",
    "-c",
    "echo CHECK pid=$$ >> \"$FAKE_OMP_LOG\"; exec /bin/sleep 600",
];

fn check_done_line() -> String {
    format!("command {}", serde_json::to_string(&CHECK_ARGV).unwrap())
}

/// `n` scripted `lane_done` calls for a lane in mode `tools`.
fn lane_done_calls(n: usize) -> String {
    vec![json!({"tool": done::LANE_DONE_TOOL, "arguments": {"evidence": "e"}}).to_string(); n]
        .join("\n")
}

impl Harness {
    /// A manager whose lead starts one lane (mode `tools`, task with `run-lane-tools`)
    /// with `done`, and whose lane makes `calls` `lane_done` calls.
    fn done_manager(&self, done: &str, calls: usize) -> OmpManager {
        self.manager("tools", &[lane_start_done("run-lane-tools t1", done)])
            .with_extra_env([("FAKE_OMP_LANE_TOOL_CALLS", lane_done_calls(calls))])
    }

    fn marker(&self) -> PathBuf {
        self.config.work_dir.join("marker")
    }

    /// Results of the lane's `lane_done` calls, in log order: chosen by content, since
    /// the lead's lane tools answer with JSON.
    fn lane_tool_results(&self) -> Vec<(bool, String)> {
        self.tool_results()
            .into_iter()
            .filter(|(_, text)| text.starts_with(done::ACCEPTED) || text.starts_with("E_"))
            .collect()
    }

    /// Frames of `kind` logged after the lane `START` line number `nth`.
    fn frames_after_start(&self, nth: usize, kind: &str) -> Vec<Value> {
        let start = self.lane_starts()[nth];
        self.log_lines()[start..]
            .iter()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|f| f["type"] == kind)
            .collect()
    }

    /// Pids the check commands logged (`CHECK pid=<pid>`).
    fn check_pids(&self) -> Vec<u32> {
        self.log_lines()
            .iter()
            .filter_map(|l| l.strip_prefix("CHECK pid="))
            .map(|pid| pid.trim().parse().expect("check pid"))
            .collect()
    }

    /// The pid in the lane `START` line number `nth`.
    fn lane_pid(&self, nth: usize) -> u32 {
        let line = &self.log_lines()[self.lane_starts()[nth]];
        let rest = line.strip_prefix("START pid=").expect("START line");
        rest.split_once(' ')
            .expect("pid then argv")
            .0
            .parse()
            .expect("pid")
    }
}

fn has_done_footer(frame: &Value, line: &str) -> bool {
    frame["message"]
        .as_str()
        .unwrap_or_default()
        .lines()
        .any(|l| l == format!("[moraebot] Done = {line}"))
}

/// Serves exactly `statuses.len()` connections in order on 127.0.0.1: sends each
/// request's first line on the channel and answers with the next status.
fn http_server(statuses: &[u16]) -> (String, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
    let statuses = statuses.to_vec();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for status in statuses {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new((&stream).take(8192));
            let mut first = String::new();
            let _ = reader.read_line(&mut first);
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok_and(|read| read > 2) {
                line.clear();
            }
            let _ = tx.send(first.trim_end().to_owned());
            let _ = (&stream).write_all(
                format!(
                    "HTTP/1.1 {status} QA\r\nLocation: /moved\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            );
        }
    });
    (base, rx)
}

fn process_exists(pid: u32) -> bool {
    // SAFETY: signal 0 only checks that the pid exists.
    let result = unsafe { libc::kill(pid as libc::pid_t, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

/// Prompts the lead with `input` and waits until a lane result row was stored.
async fn prompt_until_lane_result(omp: &OmpManager, message: i64, input: &str) {
    let wake = omp.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    bounded(omp.prompt(KEY, message, Sender::Owner, input)).await;
    bounded(notified).await;
}

fn result_tag(h: &Harness, row: i64) -> lead::Tag {
    lead::parse_tag(&queued_row(h, row).text).expect("tagged result")
}

/// The `lane_id` in a successful `lane_start` result (JSON on its first line).
fn started_id(text: &str) -> i64 {
    let first: Value = serde_json::from_str(text.lines().next().unwrap()).expect("json line");
    first["lane_id"].as_i64().expect("lane_id")
}

fn assert_refused(result: &(bool, String), code: &str) {
    assert!(result.0, "expected an error result: {result:?}");
    assert!(
        result.1.starts_with(&format!("{code}:")),
        "expected {code}: {result:?}"
    );
}

async fn await_running_pids(omp: &OmpManager) {
    let mut pids = omp.lanes().running_pids();
    bounded(pids.wait_for(|p| !p.is_empty()))
        .await
        .expect("pid channel open");
}

/// Waits until the lane children sent at least `n` agent events.
async fn await_lane_events(omp: &OmpManager, n: u64) {
    bounded(omp.lanes().lane_events().wait_for(|seen| *seen >= n))
        .await
        .expect("events channel open");
}

/// The `key=value` words of the line that starts with `lead::STALL_LINE`.
fn stall_fields(text: &str) -> std::collections::HashMap<String, String> {
    let line = text
        .lines()
        .find_map(|l| l.strip_prefix(lead::STALL_LINE))
        .expect("stall line");
    line.split(' ')
        .filter_map(|word| word.split_once('='))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

fn queued_row(h: &Harness, id: i64) -> moraebot::store::InboxRow {
    h.store
        .lock()
        .unwrap()
        .get(id)
        .unwrap()
        .expect("inbox row exists")
}

#[tokio::test]
async fn lane_start_returns_id_and_result_row_wakes_dispatcher() {
    let h = Harness::new();
    let omp = h.manager("ok", &[lane_start("t1")]);
    let wake = omp.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();

    let report = bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    assert_eq!(
        report.main.outcome,
        Outcome::Reply("echo: run-tools go".into())
    );
    let results = h.tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(!results[0].0, "{results:?}");
    assert_eq!(started_id(&results[0].1), 1);

    bounded(notified).await;
    assert_eq!(h.lane(1).state, LaneState::Idle);
    let row = queued_row(&h, 1);
    assert_eq!(row.platform, lane::LANE_PLATFORM);
    assert_eq!(row.message_id, "1:1");
    assert_eq!(row.origin, "lane");
    let tag = lead::parse_tag(&row.text).expect("tagged result");
    assert_eq!(tag.conversation, KEY);
    assert_eq!(tag.lane, Some(1));
    assert_eq!(tag.failed, None);
    assert!(row.text.split_once('\n').unwrap().1.starts_with("echo: t1"));

    let lines = h.log_lines();
    let starts = h.lane_starts();
    assert_eq!(starts.len(), 1);
    let argv = start_argv(&lines[starts[0]]);
    assert!(session_dir_arg(&lines[starts[0]]).ends_with("/lanes/1/session"));
    let overlay = argv.iter().position(|a| a == "--config").expect("--config");
    assert_eq!(
        Path::new(&argv[overlay + 1]),
        moraebot::omp_overlay::overlay_path(&h.config.home)
    );
    assert!(
        !argv.iter().any(|a| a.starts_with("--append-system-prompt")),
        "{argv:?}"
    );
    let tools = h.frames_after_start(0, "set_host_tools");
    assert_eq!(tools.len(), 1, "{tools:?}");
    assert_eq!(tools[0]["tools"].as_array().unwrap().len(), 3);
    assert_eq!(tools[0]["tools"][0]["name"], done::LANE_DONE_TOOL);
    assert_eq!(tools[0]["tools"][1]["name"], LANE_REPORT_TOOL);
    assert_eq!(tools[0]["tools"][2]["name"], MEMORY_SEARCH_TOOL);
    omp.shutdown_all().await;
}

#[tokio::test]
async fn lane_start_does_not_wait_for_the_lane() {
    let h = Harness::new();
    let omp = h.manager("hang", &[lane_start("t1")]);
    let report = bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    assert_eq!(
        report.main.outcome,
        Outcome::Reply("echo: run-tools go".into())
    );
    assert_eq!(h.lane(1).state, LaneState::Running);
    await_running_pids(&omp).await;

    bounded(omp.shutdown_all()).await;
    assert!(omp.lanes().running_pids().borrow().is_empty());
    h.store.lock().unwrap().recover_on_start().unwrap();
    let lane = h.lane(1);
    assert_eq!(lane.state, LaneState::NeedsCheck);
    assert_eq!(lane.error.as_deref(), Some(E_LANE_INTERRUPTED));
    assert_eq!(h.store.lock().unwrap().count().unwrap(), 0, "no result row");
}

#[tokio::test]
async fn fourth_running_lane_is_refused() {
    let h = Harness::new();
    let calls: Vec<Value> = ["t1", "t2", "t3", "t4"].map(lane_start).to_vec();
    let omp = h.manager("hang", &calls);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let results = h.tool_results();
    assert_eq!(results.len(), 4, "{results:?}");
    for (n, result) in results[..3].iter().enumerate() {
        assert!(!result.0, "{result:?}");
        assert_eq!(started_id(&result.1), n as i64 + 1);
    }
    assert_refused(&results[3], E_LANE_LIMIT);
    assert_eq!(h.store.lock().unwrap().lanes().unwrap().len(), 3);
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_message_steers_a_running_lane() {
    let h = Harness::new();
    let omp = h.manager(
        "hang",
        &[
            lane_start("t1"),
            json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}}),
        ],
    );
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let results = h.tool_results();
    assert_eq!(results.len(), 2, "{results:?}");
    assert!(!results[0].0, "{results:?}");
    // The lane is still running, so the text was folded into the turn instead of refused.
    assert!(!results[1].0, "{results:?}");
    let accepted: Value = serde_json::from_str(&results[1].1).expect("json result");
    assert_eq!(accepted["steered"], true, "{results:?}");
    assert_eq!(h.lane(1).prompts, 1, "a steer starts no new prompt");
    bounded(omp.shutdown_all()).await;
    assert!(h.lane_starts().len() <= 1, "no second lane child");
}

/// The text really crosses into the running turn: the lane child receives its own `prompt`
/// frame with `streamingBehavior: steer` carrying the `lane_message` text, while the main
/// prompt's frame carries none, and `prompts` never grows. Polls the child's frame log instead
/// of waiting for the turn to end, so the assertion does not depend on scheduler timing.
#[tokio::test]
async fn lane_message_text_reaches_the_lane_child_as_a_steer() {
    let h = Harness::new();
    let omp = h.manager(
        "steer",
        &[
            lane_start("t1"),
            json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}}),
        ],
    );
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;

    /// The child's `prompt` frames, read from the shared fake-omp log from the lane's start.
    fn prompts(h: &Harness) -> Vec<Value> {
        let Some(start) = h.lane_starts().first().copied() else {
            return Vec::new();
        };
        h.log_lines()[start..]
            .iter()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|f| f["type"] == "prompt")
            .collect()
    }

    // Bounded poll: the steer exists only once the lane child received it.
    let deadline = Instant::now() + BOUND;
    let mut steered = Vec::new();
    while Instant::now() < deadline {
        steered = prompts(&h);
        if steered.iter().any(|f| f["streamingBehavior"] == "steer") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        steered.len(),
        2,
        "one main prompt and one steer: {steered:?}"
    );
    assert_eq!(steered[0]["streamingBehavior"], Value::Null, "{steered:?}");
    assert!(
        steered[0]["message"].as_str().unwrap().contains("t1"),
        "{steered:?}"
    );
    // The steer carries the lane_message text and nothing of the main prompt's footers.
    assert_eq!(steered[1]["streamingBehavior"], "steer", "{steered:?}");
    let steer_message = steered[1]["message"].as_str().expect("steer message");
    assert!(steer_message.contains("t2"), "{steer_message:?}");
    assert!(
        !steer_message.contains("[moraebot] At each big step"),
        "the steer is the bare text, not a second full lane prompt: {steer_message:?}"
    );

    let lane = h.lane(1);
    assert_eq!(lane.prompts, 1, "the steer was not a new prompt");
    assert_eq!(h.lane_starts().len(), 1, "no second lane child");
    bounded(omp.shutdown_all()).await;
}

/// A `lane_message` for a lane whose job already ended is not a steer: the idle lane starts a
/// real second prompt in a new child.
#[tokio::test]
async fn lane_message_after_the_turn_ended_is_not_a_steer() {
    let h = Harness::new();
    let omp = h.manager("ok", &[lane_start("t1")]);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    // The lane is idle now: the text starts a real second prompt, not a steer.
    let m2 = h.manager(
        "ok",
        &[json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}})],
    );
    prompt_until_lane_result(&m2, 2, "run-tools again").await;
    let results = h.tool_results();
    let accepted: Value =
        serde_json::from_str(&results.last().expect("lane_message result").1).expect("json");
    let lane = h.lane(1);
    let starts = h.lane_starts();
    bounded(omp.shutdown_all()).await;
    bounded(m2.shutdown_all()).await;
    assert_eq!(accepted["accepted"], true);
    assert_eq!(accepted["steered"], Value::Null, "{accepted:?}");
    assert_eq!(lane.prompts, 2);
    assert_eq!(starts.len(), 2);
}

/// A steer that omp answers in a run of its own (it reached omp as the main run ended) is part
/// of the lane's result: the job waits for it and stores its newer reply, not the main one.
#[tokio::test]
async fn a_steer_answered_in_its_own_run_is_the_lane_result() {
    let h = Harness::new();
    let omp = h
        .manager(
            "steer",
            &[
                lane_start("t1"),
                json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}}),
            ],
        )
        .with_extra_env([("FAKE_OMP_STEER_CASE", "separate".to_owned())]);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let lane = h.lane(1);
    let row = queued_row(&h, 1);
    let starts = h.lane_starts();
    bounded(omp.shutdown_all()).await;
    assert_eq!(lane.state, LaneState::Idle);
    assert_eq!(lane.prompts, 1, "the steer was not a new prompt");
    assert_eq!(starts.len(), 1, "no second lane child");
    let reply = row.text.split_once('\n').expect("tag line").1;
    assert!(reply.starts_with("echo: t2"), "{reply:?}");
}

/// A `lane_message` the running turn never took (here the main prompt was refused before any
/// `agent_start`, so no steer could join it) is not dropped: it runs as the lane's next prompt.
/// Whether the text arrives while the job runs or after it ended, the outcome is the same.
#[tokio::test]
async fn a_lane_message_the_turn_never_took_runs_as_the_next_prompt() {
    let h = Harness::new();
    let omp = h
        .manager(
            "ok",
            &[
                lane_start("reject-me t1"),
                json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}}),
            ],
        )
        .with_extra_env([("FAKE_OMP_REJECT_TRIGGER", "reject-me".to_owned())]);
    let wake = omp.lane_wake();
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    bounded(async {
        loop {
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let lane = h.lane(1);
            if lane.prompts == 2 && lane.state != LaneState::Running {
                break;
            }
            notified.await;
        }
    })
    .await;
    let second = h.frames_after_start(1, "prompt");
    let starts = h.lane_starts();
    bounded(omp.shutdown_all()).await;
    assert_eq!(starts.len(), 2, "the text ran in the lane's next child");
    assert_eq!(second.len(), 1, "{second:?}");
    assert_eq!(second[0]["streamingBehavior"], Value::Null, "{second:?}");
    assert!(
        second[0]["message"].as_str().unwrap().starts_with("t2"),
        "{second:?}"
    );
    let row = queued_row(&h, 2);
    let reply = row.text.split_once('\n').expect("tag line").1;
    assert!(reply.starts_with("echo: t2"), "{reply:?}");
}

#[tokio::test]
async fn lane_message_resumes_idle_lane_session() {
    let h = Harness::new();
    let m1 = h.manager("ok", &[lane_start("t1")]);
    let wake = m1.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    bounded(m1.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    bounded(notified).await;
    assert_eq!(h.lane(1).state, LaneState::Idle);
    assert_eq!(queued_row(&h, 1).message_id, "1:1");
    bounded(m1.shutdown_all()).await;
    assert!(m1.lanes().running_pids().borrow().is_empty());

    let m2 = h.manager(
        "ok",
        &[json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}})],
    );
    let wake = m2.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    bounded(m2.prompt(KEY, 2, Sender::Owner, "run-tools again")).await;
    let results = h.tool_results();
    let last = results.last().expect("lane_message result");
    assert!(!last.0, "{results:?}");
    let accepted: Value = serde_json::from_str(&last.1).expect("json result");
    assert_eq!(accepted, json!({"lane_id": 1, "accepted": true}));
    bounded(notified).await;

    let second = queued_row(&h, 2);
    assert_eq!(second.message_id, "1:2");
    assert!(
        second
            .text
            .split_once('\n')
            .unwrap()
            .1
            .starts_with("echo: t2")
    );
    let lane = h.lane(1);
    assert_eq!(lane.state, LaneState::Idle);
    assert_eq!(lane.prompts, 2);
    bounded(m2.shutdown_all()).await;
    assert!(m2.lanes().running_pids().borrow().is_empty());

    let session = lane.omp_session_id.expect("lane session stored");
    let lines = h.log_lines();
    let starts = h.lane_starts();
    assert_eq!(starts.len(), 2);
    let after: Vec<Value> = lines[starts[1]..]
        .iter()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .collect();
    let lease = after
        .iter()
        .position(|f| f["type"] == "get_session_lease")
        .expect("lease checked before reopening");
    assert_eq!(after[lease]["sessionIds"], json!([session]));
    assert!(
        after[lease..].iter().any(|f| f["type"] == "open_session"),
        "the session is opened after the lease check"
    );
}

#[tokio::test]
async fn lane_status_lists_lanes_and_rejects_unknown() {
    let h = Harness::new();
    let long_task = "x".repeat(300);
    let status = |args: Value| json!({"tool": lane::LANE_STATUS_TOOL, "arguments": args});
    let omp = h.manager(
        "hang",
        &[
            lane_start("t1"),
            lane_start(&long_task),
            status(json!({})),
            status(json!({"lane_id": 1})),
            status(json!({"lane_id": 99})),
        ],
    );
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let results = h.tool_results();
    assert_eq!(results.len(), 5, "{results:?}");
    let all: Value = serde_json::from_str(&results[2].1).expect("status json");
    let all = all.as_array().expect("array");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0]["lane_id"], 1);
    assert_eq!(all[0]["conversation"], KEY);
    assert_eq!(all[0]["state"], "running");
    assert_eq!(all[0]["prompts"], 1);
    assert_eq!(all[0]["task"], "t1");
    assert_eq!(all[1]["task"].as_str().unwrap().chars().count(), 200);
    let one: Value = serde_json::from_str(&results[3].1).expect("status json");
    assert_eq!(one.as_array().unwrap().len(), 1);
    assert_eq!(one[0]["lane_id"], 1);
    assert_refused(&results[4], E_LANE_UNKNOWN);
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_args_are_validated() {
    let h = Harness::new();
    let call = |tool: &str, args: Value| json!({"tool": tool, "arguments": args});
    let omp = h.manager(
        "ok",
        &[
            call(lane::LANE_START_TOOL, json!({"task": "", "done": "file ."})),
            call(lane::LANE_START_TOOL, json!({})),
            call(lane::LANE_MESSAGE_TOOL, json!({"text": "x"})),
            call(lane::LANE_MESSAGE_TOOL, json!({"lane_id": 1})),
            call(lane::LANE_STATUS_TOOL, json!({"lane_id": "a"})),
        ],
    );
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let results = h.tool_results();
    assert_eq!(results.len(), 5, "{results:?}");
    for result in &results {
        assert_refused(result, E_LANE_ARGS);
    }
    assert!(h.store.lock().unwrap().lanes().unwrap().is_empty());
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_start_after_shutdown_starts_no_child() {
    let h = Harness::new();
    let omp = h.manager("ok", &[lane_start("t1")]);
    bounded(omp.shutdown_all()).await;
    let report = bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    assert_eq!(
        report.main.outcome,
        Outcome::Reply("echo: run-tools go".into())
    );
    let results = h.tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert_refused(&results[0], E_LANE_STOPPING);
    assert!(h.lane_starts().is_empty());
    assert!(h.store.lock().unwrap().lanes().unwrap().is_empty());
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn shutdown_while_lane_child_starts_kills_it() {
    let h = Harness::new();
    let omp = h.manager("hold_open", &[lane_start("t1")]);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    await_running_pids(&omp).await;

    tokio::time::timeout(
        omp::ABORT_JOIN_BOUND + Duration::from_secs(5),
        omp.shutdown_all(),
    )
    .await
    .expect("shutdown finished");
    assert!(omp.lanes().running_pids().borrow().is_empty());
    assert_eq!(h.lane(1).state, LaneState::Running);
    assert_eq!(h.store.lock().unwrap().count().unwrap(), 0, "no result row");
    h.store.lock().unwrap().recover_on_start().unwrap();
    let lane = h.lane(1);
    assert_eq!(lane.state, LaneState::NeedsCheck);
    assert_eq!(lane.error.as_deref(), Some(E_LANE_INTERRUPTED));
}

#[tokio::test]
async fn lane_result_written_when_stop_did_not_kill_it() {
    let h = Harness::new();
    let omp = h.manager("ok", &[lane_start("t1")]);
    let wake = omp.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    await_running_pids(&omp).await;
    omp.lanes().stop_accepting();
    bounded(notified).await;
    assert_eq!(h.lane(1).state, LaneState::Idle);
    assert_eq!(queued_row(&h, 1).message_id, "1:1");
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_failure_comes_back_as_failed_tag() {
    let h = Harness::new();
    let omp = h.manager("error", &[lane_start("t1")]);
    let wake = omp.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    bounded(notified).await;
    let row = queued_row(&h, 1);
    let tag = lead::parse_tag(&row.text).expect("tagged result");
    assert_eq!(tag.lane, Some(1));
    assert_eq!(tag.failed.as_deref(), Some(omp::E_PROMPT_ERROR));
    assert_eq!(h.lane(1).state, LaneState::Failed);
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn duplicate_tool_call_starts_one_lane() {
    let h = Harness::new();
    let dup = json!({
        "tool": lane::LANE_START_TOOL,
        "arguments": {"task": "t1", "done": "file ."},
        "id": "dup"
    });
    let omp = h.manager("hang", &[dup.clone(), dup]);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let results = h.tool_results();
    assert_eq!(results.len(), 2, "{results:?}");
    assert!(!results[0].0 && !results[1].0, "{results:?}");
    assert_eq!(started_id(&results[0].1), started_id(&results[1].1));
    assert_eq!(h.store.lock().unwrap().lanes().unwrap().len(), 1);
    await_running_pids(&omp).await;
    assert_eq!(omp.lanes().running_pids().borrow().len(), 1);
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_start_requires_a_checkable_done_line() {
    let h = Harness::new();
    let start = |args: Value| json!({"tool": lane::LANE_START_TOOL, "arguments": args});
    let omp = h.manager(
        "ok",
        &[
            start(json!({"task": "t1"})),
            start(json!({"task": "t1", "done": "  "})),
            start(json!({"task": "t1", "done": "published p@1"})),
        ],
    );
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let results = h.tool_results();
    assert_eq!(results.len(), 3, "{results:?}");
    assert_refused(&results[0], E_LANE_ARGS);
    assert_refused(&results[1], E_LANE_ARGS);
    assert_refused(&results[2], E_DONE_SYNTAX);
    assert!(h.store.lock().unwrap().lanes().unwrap().is_empty());
    assert!(h.lane_starts().is_empty());
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_with_done_line_gets_lane_done() {
    let h = Harness::new();
    let omp = h.manager("ok", &[lane_start_done("t1", "file .")]);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let tools = h.frames_after_start(0, "set_host_tools");
    assert_eq!(tools.len(), 1, "{tools:?}");
    let names: Vec<&Value> = tools[0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| &t["name"])
        .collect();
    assert_eq!(
        names,
        [done::LANE_DONE_TOOL, LANE_REPORT_TOOL, MEMORY_SEARCH_TOOL]
    );
    assert!(
        !names.contains(&&json!(MEMORY_WRITE_TOOL)),
        "lanes never write memory"
    );
    let prompts = h.frames_after_start(0, "prompt");
    assert_eq!(prompts.len(), 1, "{prompts:?}");
    assert!(has_done_footer(&prompts[0], "file ."), "{prompts:?}");
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_without_done_line_gets_no_tool() {
    let h = Harness::new();
    {
        let mut store = h.store.lock().unwrap();
        let started = store
            .start_lane("toolu_old", KEY, "t1", None, None, 3)
            .unwrap();
        assert_eq!(started, moraebot::store::LaneStart::New(1));
        let text = lead::lane_result(KEY, 1, &lead::LaneResult::Text("old"));
        assert!(
            store
                .finish_lane(1, 1, &text, LaneState::Idle, None)
                .unwrap()
        );
    }
    let omp = h.manager(
        "ok",
        &[json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}})],
    );
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    assert_eq!(h.lane_starts().len(), 1);
    let start = h.lane_starts()[0];
    assert!(
        !h.log_lines()[start..]
            .iter()
            .any(|l| l.contains(done::LANE_DONE_TOOL)),
        "a lane without a done line gets no lane_done"
    );
    let prompts = h.frames_after_start(0, "prompt");
    assert_eq!(prompts.len(), 1, "{prompts:?}");
    assert!(
        !prompts[0]["message"]
            .as_str()
            .unwrap()
            .lines()
            .any(|l| l.starts_with("[moraebot] Done =")),
        "{prompts:?}"
    );
    let row = queued_row(&h, 2);
    assert_eq!(row.message_id, "1:2");
    let tag = lead::parse_tag(&row.text).expect("tagged result");
    assert!(!tag.done);
    assert_eq!(tag.failed, None);
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn accepted_claim_gives_done_tag_and_receipt() {
    let h = Harness::new();
    std::fs::write(h.marker(), "").unwrap();
    let omp = h.done_manager("file marker", 1);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let results = h.lane_tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(!results[0].0, "{results:?}");
    let tag = result_tag(&h, 1);
    assert!(tag.done, "{tag:?}");
    assert_eq!(tag.failed, None);
    let lane = h.lane(1);
    assert_eq!(lane.state, LaneState::Idle);
    assert_eq!(lane.error, None);
    assert_eq!(h.lane_starts().len(), 1);
    assert_eq!(h.frames_after_start(0, "prompt").len(), 1);
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn refused_claim_keeps_the_lane_working_until_it_passes() {
    let h = Harness::new();
    let omp = h.done_manager("file marker", 2).with_extra_env([(
        "FAKE_OMP_TOUCH_ON_TOOL_ERROR",
        h.marker().to_string_lossy().into_owned(),
    )]);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let results = h.lane_tool_results();
    assert_eq!(results.len(), 2, "{results:?}");
    assert_refused(&results[0], E_DONE_UNMET);
    assert!(
        results[0].1.contains("file marker -> E_CHECK_MISSING"),
        "{results:?}"
    );
    assert!(!results[1].0, "{results:?}");
    assert!(result_tag(&h, 1).done);
    assert_eq!(h.lane_starts().len(), 1);
    assert_eq!(
        h.frames_after_start(0, "prompt").len(),
        1,
        "moraebot sent no prompt of its own"
    );
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn done_kinds_accept_a_claim() {
    let h = Harness::new();
    std::fs::write(h.marker(), "ready\n").unwrap();
    let (base, rx) = http_server(&[200]);
    let out = "all-ok";
    let line = format!(
        "file marker contains \"ready\"; command [\"/bin/sh\",\"-c\",\"echo {out}\"] contains \"all-ok\"; url {base}/health"
    );
    let omp = h.done_manager(&line, 1);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let results = h.lane_tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(!results[0].0, "{results:?}");
    assert!(result_tag(&h, 1).done);
    let text = queued_row(&h, 1).text;
    let ok = text.lines().filter(|l| l.ends_with("-> ok")).count();
    assert_eq!(ok, 3, "{text}");
    assert!(has_done_footer(
        &h.frames_after_start(0, "prompt")[0],
        &line
    ));
    assert_eq!(rx.recv_timeout(BOUND).unwrap(), "GET /health HTTP/1.1");
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn done_kinds_refuse_with_their_codes() {
    let h = Harness::new();
    std::fs::write(h.marker(), "wait\n").unwrap();
    let (base, rx) = http_server(&[404]);
    let out = "nope";
    let line = format!(
        "file marker contains \"ready\"; command [\"/bin/sh\",\"-c\",\"echo {out}\"] contains \"all-ok\"; url {base}/health"
    );
    let omp = h.done_manager(&line, 1);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let results = h.lane_tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert_refused(&results[0], E_DONE_UNMET);
    let count = |code: &str| {
        let needle = format!("-> {code}:");
        results[0].1.lines().filter(|l| l.contains(&needle)).count()
    };
    assert_eq!(count(done::E_CHECK_TEXT), 2, "{results:?}");
    assert_eq!(count(done::E_CHECK_STATUS), 1, "{results:?}");
    assert_eq!(result_tag(&h, 1).failed.as_deref(), Some(E_DONE_UNMET));
    assert!(rx.recv_timeout(BOUND).is_ok());
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn no_limit_on_refused_claims() {
    let h = Harness::new();
    let omp = h.done_manager("file marker", 5).with_extra_env([
        (
            "FAKE_OMP_TOUCH_ON_TOOL_ERROR",
            h.marker().to_string_lossy().into_owned(),
        ),
        ("FAKE_OMP_TOUCH_AFTER_ERRORS", "4".to_owned()),
    ]);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let results = h.lane_tool_results();
    assert_eq!(results.len(), 5, "{results:?}");
    for result in &results[..4] {
        assert_refused(result, E_DONE_UNMET);
    }
    assert!(!results[4].0, "{results:?}");
    assert!(result_tag(&h, 1).done);
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn last_failed_claim_reports_unmet() {
    let h = Harness::new();
    let m1 = h.done_manager("file marker", 1);
    prompt_until_lane_result(&m1, 1, "run-tools go").await;
    let tag = result_tag(&h, 1);
    assert_eq!(tag.failed.as_deref(), Some(E_DONE_UNMET));
    let lane = h.lane(1);
    assert_eq!(lane.state, LaneState::Failed);
    assert_eq!(lane.error.as_deref(), Some(E_DONE_UNMET));
    assert_eq!(h.lane_starts().len(), 1);
    assert_eq!(h.frames_after_start(0, "prompt").len(), 1);
    bounded(m1.shutdown_all()).await;

    std::fs::write(h.marker(), "").unwrap();
    let m2 = h
        .manager(
            "tools",
            &[json!({
                "tool": lane::LANE_MESSAGE_TOOL,
                "arguments": {"lane_id": 1, "text": "run-lane-tools again"}
            })],
        )
        .with_extra_env([("FAKE_OMP_LANE_TOOL_CALLS", lane_done_calls(1))]);
    prompt_until_lane_result(&m2, 2, "run-tools again").await;
    assert!(result_tag(&h, 2).done);
    assert_eq!(h.lane(1).prompts, 2);
    let prompts = h.frames_after_start(1, "prompt");
    assert_eq!(prompts.len(), 1, "{prompts:?}");
    assert!(has_done_footer(&prompts[0], "file marker"), "{prompts:?}");
    bounded(m2.shutdown_all()).await;
}

#[tokio::test]
async fn no_claim_gives_the_unchecked_result_tag() {
    let h = Harness::new();
    std::fs::write(h.marker(), "").unwrap();
    let omp = h.manager("ok", &[lane_start_done("t1", "file marker")]);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let tag = result_tag(&h, 1);
    assert!(!tag.done, "{tag:?}");
    assert_eq!(tag.failed, None);
    assert_eq!(h.lane(1).state, LaneState::Idle);
    assert!(h.lane_tool_results().is_empty());
    bounded(omp.shutdown_all()).await;
}

/// Time-limit exception: the 1 s check timeout is the behavior under test.
#[tokio::test]
async fn check_timeout_refuses_the_claim() {
    let h = Harness::new();
    let omp = h
        .done_manager(&check_done_line(), 1)
        .with_check_timeout(Duration::from_secs(1));
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let results = h.lane_tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert_refused(&results[0], E_DONE_UNMET);
    assert!(results[0].1.contains("-> E_CHECK_TIMEOUT:"), "{results:?}");
    assert_eq!(result_tag(&h, 1).failed.as_deref(), Some(E_DONE_UNMET));
    for pid in h.check_pids() {
        assert!(!process_exists(pid), "check {pid} was reaped");
    }
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn shutdown_during_a_check_kills_it_and_writes_no_result() {
    let h = Harness::new();
    let omp = h.done_manager(&check_done_line(), 1);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let mut pids = omp.lanes().running_pids();
    let running = bounded(pids.wait_for(|p| p.len() == 2))
        .await
        .expect("pid channel open")
        .clone();
    let lane_pid = h.lane_pid(0);
    assert!(running.contains(&lane_pid), "{running:?}");
    let check_pid = *running.iter().find(|p| **p != lane_pid).unwrap();

    tokio::time::timeout(
        omp::ABORT_JOIN_BOUND + Duration::from_secs(5),
        omp.shutdown_all(),
    )
    .await
    .expect("shutdown finished");
    assert!(
        !process_exists(check_pid),
        "the check was killed and reaped"
    );
    assert!(omp.lanes().running_pids().borrow().is_empty());
    assert_eq!(h.store.lock().unwrap().count().unwrap(), 0, "no result row");
    assert_eq!(h.lane(1).state, LaneState::Running);
    h.store.lock().unwrap().recover_on_start().unwrap();
    let lane = h.lane(1);
    assert_eq!(lane.state, LaneState::NeedsCheck);
    assert_eq!(lane.error.as_deref(), Some(E_LANE_INTERRUPTED));
}

/// A `lane_done` call refused because moraebot is stopping can reach a lane that is still
/// alive (`stop_accepting` runs before the kills); the reply that ends that turn is still a
/// cut turn, left `running` for `recover_on_start`.
#[tokio::test]
async fn a_claim_refused_for_the_shutdown_leaves_the_turn_for_recovery() {
    let h = Harness::new();
    let release = h.config.work_dir.join("release");
    let argv = [
        "/bin/sh",
        "-c",
        "echo CHECK pid=$$ >> \"$FAKE_OMP_LOG\"; while [ ! -e release ]; do sleep 0.05; done",
    ];
    let omp = h.done_manager(
        &format!("command {}", serde_json::to_string(&argv).unwrap()),
        1,
    );
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let mut pids = omp.lanes().running_pids();
    bounded(pids.wait_for(|p| p.len() == 2))
        .await
        .expect("pid channel open");

    omp.lanes().stop_accepting();
    std::fs::write(&release, "").unwrap();
    bounded(pids.wait_for(|p| p.is_empty()))
        .await
        .expect("pid channel open");

    let results = h.lane_tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert_refused(&results[0], E_LANE_STOPPING);
    assert_eq!(h.store.lock().unwrap().count().unwrap(), 0, "no result row");
    assert_eq!(h.lane(1).state, LaneState::Running);
    bounded(omp.shutdown_all()).await;
}

/// Time-limit exception: the 3 s turn timeout is the behavior under test.
#[tokio::test]
async fn turn_timeout_during_a_check_kills_the_check() {
    let h = Harness::new();
    let omp = h
        .done_manager(&check_done_line(), 1)
        .with_turn_timeout(Duration::from_secs(3));
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    assert_eq!(
        result_tag(&h, 1).failed.as_deref(),
        Some(omp::E_TURN_TIMEOUT)
    );
    let mut pids = omp.lanes().running_pids();
    bounded(pids.wait_for(|p| p.is_empty()))
        .await
        .expect("pid channel open");
    for pid in h.check_pids() {
        assert!(!process_exists(pid), "check {pid} was killed");
    }
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_status_lists_done_line() {
    let h = Harness::new();
    let omp = h.manager(
        "hang",
        &[
            lane_start_done("t1", "file marker"),
            json!({"tool": lane::LANE_STATUS_TOOL, "arguments": {}}),
        ],
    );
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let results = h.tool_results();
    let status = results
        .iter()
        .find(|(_, text)| text.starts_with('['))
        .expect("lane_status result");
    let listed: Value = serde_json::from_str(&status.1).expect("status json");
    assert_eq!(listed[0]["lane_id"], 1);
    assert_eq!(listed[0]["done"], "file marker");
    bounded(omp.shutdown_all()).await;
}

const GIT_ENV: [(&str, &str); 2] = [
    ("GIT_CONFIG_GLOBAL", "/dev/null"),
    ("GIT_CONFIG_NOSYSTEM", "1"),
];
const GIT_REQUIRED: &str = "git is required for the lanes tests";

/// The real `git --version` must run: a missing git fails the test, never skips it.
fn require_git() {
    let out = std::process::Command::new("git")
        .arg("--version")
        .envs(GIT_ENV)
        .output()
        .expect(GIT_REQUIRED);
    assert!(out.status.success(), "{GIT_REQUIRED}");
}

/// Runs `git <args>` in `dir` with `GIT_ENV` and a fixed fake author; returns stdout.
fn git(dir: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git")
        .args(["-c", "user.name=qa", "-c", "user.email=qa@example.invalid"])
        .args(args)
        .current_dir(dir)
        .envs(GIT_ENV)
        .output()
        .expect(GIT_REQUIRED);
    assert!(out.status.success(), "git {args:?}: {out:?}");
    String::from_utf8(out.stdout).expect("utf-8 git output")
}

/// `<tmp>/qa-repo` (outside the work folder) with one commit of `tracked.txt`.
fn qa_repo(h: &Harness) -> PathBuf {
    let repo = h.dir.path().join("qa-repo");
    std::fs::create_dir_all(&repo).expect("qa-repo");
    git(&repo, &["init", "-q", "-b", "main"]);
    std::fs::write(repo.join("tracked.txt"), "x").expect("tracked.txt");
    git(&repo, &["add", "tracked.txt"]);
    git(&repo, &["commit", "-q", "-m", "init"]);
    std::fs::canonicalize(&repo).expect("canonical qa-repo")
}

/// `<tmp>/qa-remote.git`, a local bare repo added to `repo` as `origin` with `main` pushed.
fn qa_remote(h: &Harness, repo: &Path) {
    git(h.dir.path(), &["init", "-q", "--bare", "qa-remote.git"]);
    let remote = h.dir.path().join("qa-remote.git");
    git(
        repo,
        &[
            "remote",
            "add",
            "origin",
            remote.to_str().expect("utf-8 path"),
        ],
    );
    git(repo, &["push", "-q", "origin", "main"]);
}

fn lane_start_repo(task: &str, done: &str, repo: Value) -> Value {
    json!({
        "tool": lane::LANE_START_TOOL,
        "arguments": {"task": task, "done": done, "repo": repo}
    })
}

fn lane_close(lane_id: i64, reason: &str) -> Value {
    json!({"tool": lane::LANE_CLOSE_TOOL, "arguments": {"lane_id": lane_id, "reason": reason}})
}

/// The JSON of a successful `lane_close` result.
fn closed(result: &(bool, String)) -> Value {
    assert!(!result.0, "expected a successful result: {result:?}");
    serde_json::from_str(&result.1).expect("lane_close json")
}

fn cwd_arg(line: &str) -> String {
    let argv = start_argv(line);
    let at = argv.iter().position(|a| a == "--cwd").expect("--cwd");
    argv[at + 1].clone()
}

fn lane_folder(h: &Harness, id: i64) -> PathBuf {
    worktree::folder(&std::fs::canonicalize(&h.config.work_dir).unwrap(), id)
}

/// The `branch ` lines of `git worktree list --porcelain` in `repo`.
fn worktree_branches(repo: &Path) -> Vec<String> {
    git(repo, &["worktree", "list", "--porcelain"])
        .lines()
        .filter(|l| l.starts_with("branch "))
        .map(str::to_owned)
        .collect()
}

/// `--cwd` of every lane child, in start order.
fn lane_cwds(h: &Harness) -> Vec<String> {
    let lines = h.log_lines();
    h.lane_starts()
        .iter()
        .map(|n| cwd_arg(&lines[*n]))
        .collect()
}

/// Waits until the store has `n` lanes that are not running.
async fn wait_finished_lanes(h: &Harness, omp: &OmpManager, n: usize) {
    loop {
        let wake = omp.lane_wake();
        let notified = wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let finished = h
            .store
            .lock()
            .unwrap()
            .lanes()
            .unwrap()
            .iter()
            .filter(|lane| lane.state != LaneState::Running)
            .count();
        if finished >= n {
            return;
        }
        bounded(notified).await;
    }
}

#[tokio::test]
async fn lane_with_repo_works_in_its_own_worktree() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    let omp = h
        .manager(
            "tools",
            &[lane_start_repo(
                "run-lane-tools t1",
                "file tracked.txt",
                json!(repo),
            )],
        )
        .with_extra_env([("FAKE_OMP_LANE_TOOL_CALLS", lane_done_calls(1))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    assert_eq!(
        lane_cwds(&h),
        [lane_folder(&h, 1).to_string_lossy().into_owned()]
    );
    assert!(
        worktree_branches(&repo).contains(&"branch refs/heads/moraebot/lane-1".to_owned()),
        "{:?}",
        worktree_branches(&repo)
    );
    let tag = result_tag(&h, 1);
    assert!(tag.done, "{tag:?}");
    assert_eq!(tag.failed, None);
    assert_eq!(
        h.lane(1).repo.as_deref(),
        Some(repo.to_string_lossy().as_ref())
    );
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn two_lanes_get_separate_worktrees() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    let omp = h
        .manager(
            "ok",
            &[
                lane_start_repo("t1", "file .", json!(repo)),
                lane_start_repo("t2", "file .", json!(repo)),
            ],
        )
        .with_extra_env(GIT_ENV);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    wait_finished_lanes(&h, &omp, 2).await;
    let mut cwds = lane_cwds(&h);
    cwds.sort();
    assert_eq!(
        cwds,
        [1, 2].map(|id| lane_folder(&h, id).to_string_lossy().into_owned())
    );
    let branches = worktree_branches(&repo);
    for id in [1, 2] {
        assert!(
            branches.contains(&format!("branch refs/heads/moraebot/lane-{id}")),
            "{branches:?}"
        );
    }
    for id in [1, 2] {
        assert_eq!(result_tag(&h, id).failed, None);
    }
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_without_repo_keeps_the_shared_work_folder() {
    let h = Harness::new();
    let omp = h.manager("ok", &[lane_start("t1")]).with_extra_env(GIT_ENV);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    assert_eq!(
        lane_cwds(&h),
        [h.config.work_dir.to_string_lossy().into_owned()]
    );
    assert_eq!(h.lane(1).repo, None);
    assert!(!h.config.work_dir.join(worktree::LANES_DIR).exists());
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_start_refuses_a_bad_repo() {
    let h = Harness::new();
    let omp = h
        .manager(
            "ok",
            &[
                lane_start_repo("t", "file .", json!("missing-dir")),
                lane_start_repo("t", "file .", json!("")),
                lane_start_repo("t", "file .", json!(5)),
            ],
        )
        .with_extra_env(GIT_ENV);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let r = h.tool_results();
    assert_eq!(r.len(), 3, "{r:?}");
    assert_refused(&r[0], E_LANE_REPO);
    assert_refused(&r[1], E_LANE_ARGS);
    assert_refused(&r[2], E_LANE_ARGS);
    assert!(h.store.lock().unwrap().lanes().unwrap().is_empty());
    assert!(h.lane_starts().is_empty());
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn repo_that_is_not_a_git_checkout_fails_the_lane() {
    require_git();
    let h = Harness::new();
    let plain = h.dir.path().join("qa-plain");
    std::fs::create_dir_all(&plain).expect("qa-plain");
    let omp = h
        .manager("ok", &[lane_start_repo("t1", "file .", json!(plain))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    assert_eq!(result_tag(&h, 1).failed.as_deref(), Some(E_LANE_REPO));
    let lane = h.lane(1);
    assert_eq!(lane.state, LaneState::Failed);
    assert_eq!(lane.error.as_deref(), Some(E_LANE_REPO));
    assert!(h.lane_starts().is_empty());
    assert!(!lane_folder(&h, 1).exists());
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn missing_git_fails_the_lane() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    let nogit = h.dir.path().join("qa-nogit");
    std::fs::create_dir_all(&nogit).expect("qa-nogit");
    let omp = h
        .manager("ok", &[lane_start_repo("t1", "file .", json!(repo))])
        .with_extra_env(GIT_ENV)
        .with_extra_env([("PATH", nogit.to_string_lossy().into_owned())]);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    assert_eq!(result_tag(&h, 1).failed.as_deref(), Some(E_LANE_GIT));
    assert!(h.lane_starts().is_empty());
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_message_reuses_the_lane_worktree() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    let m1 = h
        .manager("ok", &[lane_start_repo("t1", "file .", json!(repo))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&m1, 1, "run-tools go").await;
    bounded(m1.shutdown_all()).await;

    let m2 = h
        .manager(
            "ok",
            &[json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}})],
        )
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&m2, 2, "run-tools go").await;
    let folder = lane_folder(&h, 1).to_string_lossy().into_owned();
    assert_eq!(lane_cwds(&h), [folder.clone(), folder]);
    assert_eq!(result_tag(&h, 2).failed, None);
    let lane_branches = worktree_branches(&repo)
        .into_iter()
        .filter(|b| b == "branch refs/heads/moraebot/lane-1")
        .count();
    assert_eq!(lane_branches, 1);
    bounded(m2.shutdown_all()).await;
}

#[tokio::test]
async fn lane_status_lists_repo() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    let omp = h
        .manager(
            "hang",
            &[
                lane_start_repo("t1", "file .", json!(repo)),
                json!({"tool": lane::LANE_STATUS_TOOL, "arguments": {"lane_id": 1}}),
            ],
        )
        .with_extra_env(GIT_ENV);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let results = h.tool_results();
    let status = results
        .iter()
        .find(|(_, text)| text.starts_with('['))
        .expect("lane_status result");
    let listed: Value = serde_json::from_str(&status.1).expect("status json");
    assert_eq!(listed[0]["repo"], json!(repo.to_string_lossy()));
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn unpushed_commit_refuses_the_claim_until_pushed() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    qa_remote(&h, &repo);
    std::fs::write(repo.join("tracked.txt"), "second").expect("tracked.txt");
    git(&repo, &["commit", "-q", "-am", "second"]);
    let m1 = h
        .manager(
            "tools",
            &[lane_start_repo(
                "run-lane-tools t1",
                "file tracked.txt",
                json!(repo),
            )],
        )
        .with_extra_env([("FAKE_OMP_LANE_TOOL_CALLS", lane_done_calls(1))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&m1, 1, "run-tools go").await;
    let r = h.lane_tool_results();
    assert_eq!(r.len(), 1, "{r:?}");
    assert_refused(&r[0], E_DONE_UNMET);
    assert!(r[0].1.contains("-> E_CHECK_UNPUSHED:"), "{r:?}");
    assert!(!r[0].1.contains("E_CHECK_UNCOMMITTED"), "{r:?}");
    assert_eq!(result_tag(&h, 1).failed.as_deref(), Some(E_DONE_UNMET));
    let prompts = h.frames_after_start(0, "prompt");
    assert!(
        prompts[0]["message"]
            .as_str()
            .unwrap_or_default()
            .ends_with(worktree::GIT_FOOTER),
        "{prompts:?}"
    );
    bounded(m1.shutdown_all()).await;

    git(
        &lane_folder(&h, 1),
        &["push", "-q", "origin", "moraebot/lane-1"],
    );
    let m2 = h
        .manager(
            "tools",
            &[json!({
                "tool": lane::LANE_MESSAGE_TOOL,
                "arguments": {"lane_id": 1, "text": "run-lane-tools again"}
            })],
        )
        .with_extra_env([("FAKE_OMP_LANE_TOOL_CALLS", lane_done_calls(1))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&m2, 2, "run-tools again").await;
    let tag = result_tag(&h, 2);
    assert!(tag.done, "{tag:?}");
    assert_eq!(tag.failed, None);
    bounded(m2.shutdown_all()).await;
}

#[tokio::test]
async fn uncommitted_change_refuses_the_claim_and_runs_the_done_checks() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    let m1 = h
        .manager("ok", &[lane_start_repo("t1", "file marker", json!(repo))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&m1, 1, "run-tools go").await;
    bounded(m1.shutdown_all()).await;

    std::fs::write(lane_folder(&h, 1).join("tracked.txt"), "changed").expect("tracked.txt");
    let m2 = h
        .manager(
            "tools",
            &[json!({
                "tool": lane::LANE_MESSAGE_TOOL,
                "arguments": {"lane_id": 1, "text": "run-lane-tools again"}
            })],
        )
        .with_extra_env([("FAKE_OMP_LANE_TOOL_CALLS", lane_done_calls(1))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&m2, 2, "run-tools again").await;
    let r = h.lane_tool_results();
    assert_eq!(r.len(), 1, "{r:?}");
    assert_refused(&r[0], E_DONE_UNMET);
    assert!(r[0].1.contains("-> E_CHECK_UNCOMMITTED:"), "{r:?}");
    assert!(r[0].1.contains("file marker -> E_CHECK_MISSING"), "{r:?}");
    assert!(!r[0].1.contains("E_CHECK_UNPUSHED"), "{r:?}");
    assert_eq!(result_tag(&h, 2).failed.as_deref(), Some(E_DONE_UNMET));
    bounded(m2.shutdown_all()).await;
}

#[tokio::test]
async fn lane_without_repo_skips_the_git_checks() {
    require_git();
    let h = Harness::new();
    let work = &h.config.work_dir;
    git(work, &["init", "-q", "-b", "main"]);
    std::fs::write(work.join("tracked.txt"), "x").expect("tracked.txt");
    git(work, &["add", "tracked.txt"]);
    git(work, &["commit", "-q", "-m", "init"]);
    std::fs::write(work.join("tracked.txt"), "changed").expect("tracked.txt");
    let omp = h
        .done_manager("file tracked.txt", 1)
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let r = h.lane_tool_results();
    assert_eq!(r.len(), 1, "{r:?}");
    assert!(!r[0].0, "{r:?}");
    assert!(result_tag(&h, 1).done);
    let prompts = h.frames_after_start(0, "prompt");
    assert!(
        !prompts[0]["message"]
            .as_str()
            .unwrap_or_default()
            .contains(worktree::GIT_FOOTER),
        "{prompts:?}"
    );
    assert!(!lane_folder(&h, 1).exists());
    bounded(omp.shutdown_all()).await;
}

#[tokio::test]
async fn lane_close_closes_an_idle_lane_without_repo() {
    let h = Harness::new();
    let m1 = h.manager("ok", &[lane_start("t1")]);
    prompt_until_lane_result(&m1, 1, "run-tools go").await;
    bounded(m1.shutdown_all()).await;

    let n = h.tool_results().len();
    let m2 = h.manager(
        "ok",
        &[
            lane_close(1, "finished"),
            json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "more"}}),
            json!({"tool": lane::LANE_STATUS_TOOL, "arguments": {"lane_id": 1}}),
        ],
    );
    bounded(m2.prompt(KEY, 2, Sender::Owner, "run-tools go")).await;
    bounded(m2.shutdown_all()).await;
    let r = h.tool_results()[n..].to_vec();
    assert_eq!(r.len(), 3, "{r:?}");
    let result = closed(&r[0]);
    assert_eq!(result["lane_id"], 1);
    assert_eq!(result["closed"], true);
    assert_eq!(result["worktree"], "none");
    assert!(result.get("branch").is_none(), "{result}");
    assert_refused(&r[1], E_LANE_CLOSED);
    let status: Value = serde_json::from_str(&r[2].1).expect("status json");
    assert_eq!(status[0]["closed"], true);
    assert_eq!(status[0]["close_reason"], "finished");
    assert_eq!(h.lane_starts().len(), 1);
    assert!(h.lane(1).closed);
}

#[tokio::test]
async fn lane_close_refuses_running_unknown_and_bad_args() {
    let h = Harness::new();
    let call = |args: Value| json!({"tool": lane::LANE_CLOSE_TOOL, "arguments": args});
    let omp = h.manager(
        "hang",
        &[
            lane_start("t1"),
            call(json!({"reason": "x"})),
            call(json!({"lane_id": 1})),
            call(json!({"lane_id": 1, "reason": "  "})),
            call(json!({"lane_id": "a", "reason": "x"})),
            lane_close(99, "x"),
            lane_close(1, "x"),
        ],
    );
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    bounded(omp.shutdown_all()).await;
    let r = h.tool_results();
    assert_eq!(r.len(), 7, "{r:?}");
    for result in &r[1..=4] {
        assert_refused(result, E_LANE_ARGS);
    }
    assert_refused(&r[5], E_LANE_UNKNOWN);
    assert_refused(&r[6], E_LANE_BUSY);
    assert!(!h.lane(1).closed);
}

#[tokio::test]
async fn lane_stop_and_close_refuse_a_non_owner_turn() {
    let h = Harness::new();
    let stop = json!({"tool": lane::LANE_STOP_TOOL, "arguments": {"lane_id": 1, "reason": "x"}});
    let owner = h.manager("hang", &[lane_start("t1")]);
    bounded(owner.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let chat = h.manager("hang", &[stop, lane_close(1, "x")]);
    bounded(chat.prompt(KEY, 2, Sender::Listed(Role::Chat), "run-tools go")).await;
    bounded(chat.shutdown_all()).await;
    bounded(owner.shutdown_all()).await;
    let r = h.tool_results();
    assert_eq!(r.len(), 3, "{r:?}");
    assert!(!r[0].0, "the owner's lane_start runs: {:?}", r[0]);
    assert_refused(&r[1], E_ACCESS_NOT_OWNER);
    assert_refused(&r[2], E_ACCESS_NOT_OWNER);
    assert!(!h.lane(1).closed);
}

#[tokio::test]
async fn lane_close_after_shutdown_is_refused() {
    let h = Harness::new();
    let m1 = h.manager("ok", &[lane_start("t1")]);
    prompt_until_lane_result(&m1, 1, "run-tools go").await;
    bounded(m1.shutdown_all()).await;

    let n = h.tool_results().len();
    let m2 = h.manager("ok", &[lane_close(1, "x")]);
    bounded(m2.shutdown_all()).await;
    bounded(m2.prompt(KEY, 2, Sender::Owner, "run-tools go")).await;
    let r = h.tool_results()[n..].to_vec();
    assert_eq!(r.len(), 1, "{r:?}");
    assert_refused(&r[0], E_LANE_STOPPING);
    assert!(!h.lane(1).closed);
}

#[tokio::test]
async fn lane_close_removes_a_clean_pushed_worktree_and_keeps_the_branch() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    qa_remote(&h, &repo);
    let m1 = h
        .manager("ok", &[lane_start_repo("t1", "file .", json!(repo))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&m1, 1, "run-tools go").await;
    bounded(m1.shutdown_all()).await;

    let n = h.tool_results().len();
    let m2 = h
        .manager("ok", &[lane_close(1, "finished")])
        .with_extra_env(GIT_ENV);
    bounded(m2.prompt(KEY, 2, Sender::Owner, "run-tools go")).await;
    bounded(m2.shutdown_all()).await;
    let r = h.tool_results()[n..].to_vec();
    assert_eq!(r.len(), 1, "{r:?}");
    let result = closed(&r[0]);
    assert_eq!(result["worktree"], "removed");
    assert_eq!(result["branch"], "moraebot/lane-1");
    assert!(result.get("codes").is_none(), "{result}");
    assert!(!lane_folder(&h, 1).exists());
    assert!(git(&repo, &["branch", "--list", "moraebot/lane-1"]).contains("moraebot/lane-1"));
    assert!(
        !worktree_branches(&repo).contains(&"branch refs/heads/moraebot/lane-1".to_owned()),
        "{:?}",
        worktree_branches(&repo)
    );
}

#[tokio::test]
async fn lane_close_keeps_unpushed_work_and_retries_after_a_push() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    qa_remote(&h, &repo);
    let m1 = h
        .manager("ok", &[lane_start_repo("t1", "file .", json!(repo))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&m1, 1, "run-tools go").await;
    bounded(m1.shutdown_all()).await;
    let folder = lane_folder(&h, 1);
    std::fs::write(folder.join("tracked.txt"), "y").expect("tracked.txt");
    git(&folder, &["commit", "-q", "-am", "lane"]);

    let n = h.tool_results().len();
    let m2 = h
        .manager("ok", &[lane_close(1, "first")])
        .with_extra_env(GIT_ENV);
    bounded(m2.prompt(KEY, 2, Sender::Owner, "run-tools go")).await;
    bounded(m2.shutdown_all()).await;
    let r = h.tool_results()[n..].to_vec();
    assert_eq!(r.len(), 1, "{r:?}");
    let result = closed(&r[0]);
    assert_eq!(result["worktree"], "kept");
    assert_eq!(result["codes"], json!(["E_CHECK_UNPUSHED"]));
    assert_eq!(result["branch"], "moraebot/lane-1");
    assert!(folder.exists());
    assert!(h.lane(1).closed);

    git(&folder, &["push", "-q", "origin", "moraebot/lane-1"]);
    let n = h.tool_results().len();
    let m3 = h
        .manager("ok", &[lane_close(1, "again")])
        .with_extra_env(GIT_ENV);
    bounded(m3.prompt(KEY, 3, Sender::Owner, "run-tools go")).await;
    bounded(m3.shutdown_all()).await;
    let r = h.tool_results()[n..].to_vec();
    assert_eq!(r.len(), 1, "{r:?}");
    assert_eq!(closed(&r[0])["worktree"], "removed");
    assert!(!folder.exists());
    assert_eq!(h.lane(1).close_reason.as_deref(), Some("first"));
}

#[tokio::test]
async fn lane_close_keeps_a_worktree_with_untracked_files() {
    let h = Harness::new();
    let repo = qa_repo(&h);
    let m1 = h
        .manager("ok", &[lane_start_repo("t1", "file .", json!(repo))])
        .with_extra_env(GIT_ENV);
    prompt_until_lane_result(&m1, 1, "run-tools go").await;
    bounded(m1.shutdown_all()).await;
    std::fs::write(lane_folder(&h, 1).join("qa-new.txt"), "n").expect("qa-new.txt");

    let n = h.tool_results().len();
    let m2 = h
        .manager("ok", &[lane_close(1, "finished")])
        .with_extra_env(GIT_ENV);
    bounded(m2.prompt(KEY, 2, Sender::Owner, "run-tools go")).await;
    bounded(m2.shutdown_all()).await;
    let r = h.tool_results()[n..].to_vec();
    assert_eq!(r.len(), 1, "{r:?}");
    let result = closed(&r[0]);
    assert_eq!(result["worktree"], "kept");
    assert_eq!(result["codes"], json!([E_LANE_REMOVE]));
    assert!(lane_folder(&h, 1).join("qa-new.txt").exists());
}

#[tokio::test]
async fn watchdog_reports_a_quiet_lane_once() {
    let h = Harness::new();
    let omp = h.manager("hang", &[lane_start("t1")]);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    await_lane_events(&omp, 1).await;
    let wake = omp.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    let t0 = Instant::now();
    let lanes = omp.lanes();
    let ticks = [
        lanes.watch_tick(t0),
        lanes.watch_tick(t0 + moraebot::config::DEFAULT_STALL_AFTER - Duration::from_secs(1)),
        lanes.watch_tick(t0 + moraebot::config::DEFAULT_STALL_AFTER),
        lanes.watch_tick(t0 + 2 * moraebot::config::DEFAULT_STALL_AFTER),
    ];
    let woke = tokio::time::timeout(BOUND, notified).await.is_ok();
    let state = h.lane(1).state;
    bounded(omp.shutdown_all()).await;

    assert_eq!(ticks, [0, 0, 1, 0]);
    assert!(woke);
    assert_eq!(state, LaneState::Running);
    assert_eq!(h.store.lock().unwrap().count().unwrap(), 1);
    let row = queued_row(&h, 1);
    assert_eq!(row.platform, lane::LANE_PLATFORM);
    assert_eq!(row.origin, "lane");
    assert_eq!(row.message_id, "1:1:stalled:1");
    let tag = lead::parse_tag(&row.text).expect("tagged notice");
    assert_eq!(tag.conversation, KEY);
    assert_eq!(tag.lane, Some(1));
    assert!(tag.stalled);
    assert_eq!(tag.reason.as_deref(), Some(lead::N_LANE_QUIET));
    assert_eq!(tag.failed, None);
    let fields = stall_fields(&row.text);
    assert_eq!(fields["quiet_minutes"], "20");
    assert_eq!(fields["tool_calls"], "0");
    assert_eq!(fields["last_tool"], "-");
}

#[tokio::test]
async fn watchdog_counts_repeated_tool_calls_as_one() {
    let h = Harness::new();
    let omp = h.manager("spin", &[lane_start("t1")]);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    // agent_start and four tool_execution_start frames.
    await_lane_events(&omp, 5).await;
    let t0 = Instant::now();
    let lanes = omp.lanes();
    let ticks = [
        lanes.watch_tick(t0),
        lanes.watch_tick(t0 + moraebot::config::DEFAULT_STALL_AFTER),
    ];
    bounded(omp.shutdown_all()).await;

    assert_eq!(ticks, [0, 1]);
    let row = queued_row(&h, 1);
    assert_eq!(row.message_id, "1:1:stalled:1");
    let tag = lead::parse_tag(&row.text).expect("tagged notice");
    assert_eq!(tag.reason.as_deref(), Some(lead::N_LANE_QUIET));
    let fields = stall_fields(&row.text);
    assert_eq!(fields["tool_calls"], "4");
    assert_eq!(fields["new_calls"], "2");
    assert_eq!(fields["last_tool"], "read");
}

#[tokio::test]
async fn watchdog_is_off_when_the_turn_timeout_is_shorter() {
    let h = Harness::new();
    let omp = h
        .manager("hang", &[lane_start("t1")])
        .with_turn_timeout(moraebot::config::DEFAULT_STALL_AFTER);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    await_lane_events(&omp, 1).await;
    let t0 = Instant::now();
    let lanes = omp.lanes();
    let ticks = [
        lanes.watch_tick(t0),
        lanes.watch_tick(t0 + moraebot::config::DEFAULT_STALL_AFTER),
        lanes.watch_tick(t0 + 2 * moraebot::config::DEFAULT_STALL_AFTER),
    ];
    bounded(omp.shutdown_all()).await;

    assert_eq!(ticks, [0, 0, 0]);
    assert_eq!(h.store.lock().unwrap().count().unwrap(), 0);
}

#[tokio::test]
async fn watchdog_stops_with_the_gateway() {
    let h = Harness::new();
    let omp = h.manager("hang", &[lane_start("t1")]);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    await_lane_events(&omp, 1).await;
    let t0 = Instant::now();
    let first = omp.lanes().watch_tick(t0);
    omp.lanes().stop_accepting();
    let second = omp
        .lanes()
        .watch_tick(t0 + moraebot::config::DEFAULT_STALL_AFTER);
    bounded(omp.shutdown_all()).await;

    assert_eq!(first, 0);
    assert_eq!(second, 0);
    assert_eq!(h.store.lock().unwrap().count().unwrap(), 0);
}

#[tokio::test]
async fn watchdog_ignores_finished_lanes() {
    let h = Harness::new();
    let omp = h.manager("ok", &[lane_start("t1")]);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let t0 = Instant::now();
    let lanes = omp.lanes();
    let ticks = [
        lanes.watch_tick(t0),
        lanes.watch_tick(t0 + moraebot::config::DEFAULT_STALL_AFTER),
    ];
    bounded(omp.shutdown_all()).await;

    assert_eq!(ticks, [0, 0]);
    assert_eq!(h.store.lock().unwrap().count().unwrap(), 1);
}

/// One scripted `lane_report` call with `text`.
fn report_call(text: &str) -> String {
    json!({"tool": lane::LANE_REPORT_TOOL, "arguments": {"text": text}}).to_string()
}

impl Harness {
    /// A manager whose lead starts one lane (mode `tools`) that makes `calls`; with `hold`
    /// the lane writes nothing after its last call's result.
    fn report_manager(&self, calls: &[String], hold: bool) -> OmpManager {
        let omp = self
            .manager("tools", &[lane_start("run-lane-tools t1")])
            .with_extra_env([("FAKE_OMP_LANE_TOOL_CALLS", calls.join("\n"))]);
        if hold {
            omp.with_extra_env([("FAKE_OMP_LANE_HOLD_AFTER_TOOLS", "1".to_owned())])
        } else {
            omp
        }
    }
}

/// Waits until the lanes recorded at least `n` reports.
async fn await_lane_reports(omp: &OmpManager, n: u64) {
    bounded(omp.lanes().lane_reports().wait_for(|seen| *seen >= n))
        .await
        .expect("reports channel open");
}

#[tokio::test]
async fn lane_report_is_news_while_the_lane_runs() {
    let h = Harness::new();
    let omp = h.report_manager(&[report_call("qa step 1")], true);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    await_lane_reports(&omp, 1).await;
    let news = omp.lanes().news();
    let state = h.lane(1).state;
    let tools = h.frames_after_start(0, "set_host_tools");
    bounded(omp.shutdown_all()).await;

    assert_eq!(
        news,
        [LaneNews {
            lane_id: 1,
            seq: 1,
            conversation_key: KEY.into(),
            text: "qa step 1".into(),
        }]
    );
    assert_eq!(state, LaneState::Running);
    assert_eq!(tools.len(), 1, "{tools:?}");
    let names: Vec<&Value> = tools[0]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| &t["name"])
        .collect();
    assert_eq!(
        names,
        [done::LANE_DONE_TOOL, LANE_REPORT_TOOL, MEMORY_SEARCH_TOOL]
    );
    assert!(
        !names.contains(&&json!(MEMORY_WRITE_TOOL)),
        "lanes never write memory"
    );
}

#[tokio::test]
async fn lane_report_of_a_finished_lane_is_not_news() {
    let h = Harness::new();
    let omp = h.report_manager(&[report_call("qa step 1")], false);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let news = omp.lanes().news();
    let results: Vec<(bool, String)> = h
        .tool_results()
        .into_iter()
        .filter(|(_, text)| text.starts_with("{\"recorded\""))
        .collect();
    let prompts = h.frames_after_start(0, "prompt");
    bounded(omp.shutdown_all()).await;

    assert!(news.is_empty(), "{news:?}");
    assert_eq!(results, [(false, r#"{"recorded":true}"#.to_owned())]);
    assert_eq!(prompts.len(), 1, "{prompts:?}");
    assert!(
        prompts[0]["message"]
            .as_str()
            .unwrap()
            .contains(REPORT_FOOTER),
        "{prompts:?}"
    );
}

#[tokio::test]
async fn lane_report_refuses_empty_text() {
    let h = Harness::new();
    let omp = h.report_manager(&[report_call("  ")], false);
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let r = h.lane_tool_results();
    let news = omp.lanes().news();
    bounded(omp.shutdown_all()).await;

    assert_eq!(r.len(), 1, "{r:?}");
    assert_refused(&r[0], E_LANE_ARGS);
    assert!(news.is_empty(), "{news:?}");
}

/// Waits until no lane child, check or git command is running.
async fn await_no_running_pids(omp: &OmpManager) {
    bounded(omp.lanes().running_pids().wait_for(|p| p.is_empty()))
        .await
        .expect("pid channel open");
}

#[tokio::test]
async fn lane_stop_ends_a_running_turn() {
    let h = Harness::new();
    let omp = h.report_manager(&[report_call("qa step 1")], true);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    await_lane_reports(&omp, 1).await;
    let lane_pid = h.lane_pid(0);
    let wake = omp.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    let first = omp.lanes().stop(1, "qa stuck");
    bounded(notified).await;
    await_no_running_pids(&omp).await;
    let again = omp.lanes().stop(1, "qa");
    let alive = process_exists(lane_pid);
    let row = queued_row(&h, 1);
    let lane = h.lane(1);
    bounded(omp.shutdown_all()).await;

    assert_eq!(first, Ok(true));
    assert_eq!(again, Err(E_LANE_IDLE));
    assert!(!alive);
    assert_eq!(row.message_id, "1:1");
    assert_eq!(
        row.text,
        lead::lane_result(KEY, 1, &LaneResult::Stopped { reason: "qa stuck" })
    );
    assert_eq!(lane.state, LaneState::Failed);
    assert_eq!(lane.error.as_deref(), Some(E_LANE_STOPPED));
    assert!(!lane.closed);
    assert_eq!(lane.prompts, 1);
}

#[tokio::test]
async fn lane_message_resumes_a_stopped_lane() {
    let h = Harness::new();
    let m1 = h.report_manager(&[report_call("qa step 1")], true);
    bounded(m1.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    await_lane_reports(&m1, 1).await;
    let wake = m1.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    let stopped = m1.lanes().stop(1, "qa stuck");
    bounded(notified).await;
    bounded(m1.shutdown_all()).await;

    let m2 = h.manager(
        "ok",
        &[json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}})],
    );
    prompt_until_lane_result(&m2, 2, "run-tools again").await;
    let results = h.tool_results();
    let accepted: Value =
        serde_json::from_str(&results.last().expect("lane_message result").1).expect("json");
    let row = queued_row(&h, 2);
    let tag = result_tag(&h, 2);
    let lane = h.lane(1);
    let starts = h.lane_starts();
    let lease = h
        .frames_after_start(1, "get_session_lease")
        .into_iter()
        .next()
        .expect("lease checked before reopening");
    bounded(m2.shutdown_all()).await;

    assert_eq!(stopped, Ok(true));
    assert_eq!(accepted, json!({"lane_id": 1, "accepted": true}));
    assert_eq!(row.message_id, "1:2");
    assert_eq!(tag.failed, None);
    assert!(row.text.split_once('\n').unwrap().1.starts_with("echo: t2"));
    assert_eq!(lane.state, LaneState::Idle);
    assert_eq!(lane.error, None);
    assert_eq!(lane.prompts, 2);
    assert_eq!(starts.len(), 2);
    assert_eq!(
        lease["sessionIds"],
        json!([lane.omp_session_id.clone().expect("session")])
    );
}

#[tokio::test]
async fn lane_stop_kills_a_running_check() {
    let h = Harness::new();
    let omp = h.done_manager(&check_done_line(), 1);
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    let mut pids = omp.lanes().running_pids();
    let running = bounded(pids.wait_for(|p| p.len() == 2))
        .await
        .expect("pid channel open")
        .clone();
    let lane_pid = h.lane_pid(0);
    let check_pid = *running.iter().find(|p| **p != lane_pid).unwrap();
    let wake = omp.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    let first = omp.lanes().stop(1, "qa");
    bounded(notified).await;
    await_no_running_pids(&omp).await;
    let check_alive = process_exists(check_pid);
    let lane_alive = process_exists(lane_pid);
    let tag = result_tag(&h, 1);
    let lane = h.lane(1);
    bounded(omp.shutdown_all()).await;

    assert_eq!(first, Ok(true));
    assert!(!check_alive);
    assert!(!lane_alive);
    assert_eq!(tag.failed.as_deref(), Some(E_LANE_STOPPED));
    assert_eq!(lane.state, LaneState::Failed);
}

fn stop_call(lane_id: Value, reason: &str) -> Value {
    json!({"tool": lane::LANE_STOP_TOOL, "arguments": {"lane_id": lane_id, "reason": reason}})
}

#[tokio::test]
async fn lane_stop_tool_stops_a_lane_through_the_lead() {
    let h = Harness::new();
    let call = |args: Value| json!({"tool": lane::LANE_STOP_TOOL, "arguments": args});
    let omp = h.manager(
        "hold_open",
        &[
            lane_start("t1"),
            stop_call(json!(1), "qa"),
            call(json!({"reason": "x"})),
            stop_call(json!(1), "  "),
            stop_call(json!("a"), "x"),
            stop_call(json!(99), "x"),
        ],
    );
    let wake = omp.lane_wake();
    let notified = wake.notified();
    tokio::pin!(notified);
    notified.as_mut().enable();
    bounded(omp.prompt(KEY, 1, Sender::Owner, "run-tools go")).await;
    bounded(notified).await;
    await_no_running_pids(&omp).await;
    let r = h.tool_results();
    let row = queued_row(&h, 1);
    let lane = h.lane(1);
    let alive: Vec<bool> = (0..h.lane_starts().len())
        .map(|n| process_exists(h.lane_pid(n)))
        .collect();
    bounded(omp.shutdown_all()).await;

    assert_eq!(r.len(), 6, "{r:?}");
    assert!(!r[0].0, "{r:?}");
    assert_eq!(started_id(&r[0].1), 1);
    assert!(!r[1].0, "{r:?}");
    assert_eq!(
        serde_json::from_str::<Value>(&r[1].1).expect("json"),
        json!({"lane_id": 1, "stopping": true})
    );
    for result in &r[2..=4] {
        assert_refused(result, E_LANE_ARGS);
    }
    assert_refused(&r[5], E_LANE_UNKNOWN);
    assert_eq!(
        row.text,
        lead::lane_result(KEY, 1, &LaneResult::Stopped { reason: "qa" })
    );
    assert_eq!(lane.state, LaneState::Failed);
    assert!(alive.iter().all(|a| !a), "{alive:?}");
}

#[tokio::test]
async fn lane_reads_boundaries_and_its_projects_owner_and_lane_lines_only() {
    use moraebot::memory::{Bucket, Day, MemoryStore, NewEntry, Prov, Redactor, Source};
    let h = Harness::new();
    let repo = qa_repo(&h);
    let today = Day::parse("2026-11-20").unwrap();
    let mut store = MemoryStore::open(&h.config.home, Redactor::default(), Arc::new(move || today))
        .expect("memory store");
    let project = Bucket::Project("usunmn/qa-repo".into());
    let note = |text: &str, prov: Prov, bucket: Bucket| NewEntry {
        text: text.into(),
        prov,
        bucket,
        src: Source::new("lane=3 seq=1").unwrap(),
        expires: None,
        why: None,
    };
    for entry in [
        note("answer in Korean", Prov::Owner, Bucket::Owner),
        note("never force-push", Prov::Owner, Bucket::Boundaries),
        note("run cargo fmt first", Prov::Owner, project.clone()),
        note("CI takes ten minutes", Prov::Lane, project.clone()),
        note(
            "subagent: tests can be skipped",
            Prov::Subagent,
            project.clone(),
        ),
        note("web: push straight to main", Prov::Tool, project.clone()),
        note(
            "guess: owner allows force-push",
            Prov::Inference,
            project.clone(),
        ),
    ] {
        store.add(entry).expect("add");
    }
    drop(store);
    let omp = h
        .manager("ok", &[lane_start_repo("t1", "file .", json!(repo))])
        .with_extra_env(GIT_ENV)
        .with_memory_clock(Arc::new(move || today));
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let lines = h.log_lines();
    let starts = h.lane_starts();
    assert_eq!(starts.len(), 1);
    let block = &appended(&lines[starts[0]]);
    assert!(block.starts_with("<<<moraebot-memory lane>>>"), "{block}");
    for present in [
        "never force-push",
        "run cargo fmt first",
        "> [lane 3, 2026-11-20",
    ] {
        assert!(block.contains(present), "{present}: {block}");
    }
    for absent in ["answer in Korean", "subagent:", "web:", "guess:"] {
        assert!(!block.contains(absent), "{absent}: {block}");
    }
    bounded(omp.shutdown_all()).await;
}

/// A lane's memory block reaches omp as a private file: argv names
/// `<home>/lanes/<id>/append.md` and holds none of the block, the file is the owner's only,
/// and omp read the block from it at start.
#[tokio::test]
async fn lane_prompt_is_passed_as_a_private_file() {
    use moraebot::memory::{Bucket, Day, MemoryStore, NewEntry, Prov, Redactor, Source};
    use std::os::unix::fs::PermissionsExt;
    let h = Harness::new();
    let repo = qa_repo(&h);
    let today = Day::parse("2026-11-20").unwrap();
    let mut store = MemoryStore::open(&h.config.home, Redactor::default(), Arc::new(move || today))
        .expect("memory store");
    store
        .add(NewEntry {
            text: "never force-push".into(),
            prov: Prov::Owner,
            bucket: Bucket::Boundaries,
            src: Source::row(1),
            expires: None,
            why: None,
        })
        .expect("add");
    drop(store);
    let omp = h
        .manager("ok", &[lane_start_repo("t1", "file .", json!(repo))])
        .with_extra_env(GIT_ENV)
        .with_memory_clock(Arc::new(move || today));
    prompt_until_lane_result(&omp, 1, "run-tools go").await;
    let lines = h.log_lines();
    let starts = h.lane_starts();
    assert_eq!(starts.len(), 1);
    let start = &lines[starts[0]];
    bounded(omp.shutdown_all()).await;

    let path = omp::lane_append_path(&h.config, 1);
    assert!(path.is_absolute());
    let flags: Vec<String> = start_argv(start)
        .into_iter()
        .filter(|a| a.starts_with("--append-system-prompt"))
        .collect();
    assert_eq!(
        flags,
        [format!("--append-system-prompt={}", path.display())]
    );
    assert!(
        !start
            .split_once(" argv=")
            .unwrap()
            .1
            .contains("never force-push"),
        "{start}"
    );
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&path), 0o600);
    assert_eq!(mode(path.parent().unwrap()), 0o700);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.starts_with("<<<moraebot-memory lane>>>"), "{text}");
    assert!(text.contains("never force-push"), "{text}");
    assert_eq!(appended(start), text, "omp read the file at start");

    // A closed lane never starts again: its prompt file, and the memory text in it, go.
    let m2 = h
        .manager("ok", &[lane_close(1, "finished")])
        .with_extra_env(GIT_ENV);
    bounded(m2.prompt(KEY, 2, Sender::Owner, "run-tools go")).await;
    bounded(m2.shutdown_all()).await;
    assert!(h.lane(1).closed);
    assert!(!path.exists(), "a closed lane keeps no memory text");
}

/// A lane keeps the memory block its first job in this process started with: a memory change
/// between two jobs leaves the second start's system prompt as it was and reaches the lane as
/// an update block in the job's prompt, once; a lane started after the change gets the new
/// block in its system prompt.
#[tokio::test]
async fn lane_memory_block_stays_pinned_across_jobs() {
    use moraebot::memory::{Bucket, Day, MemoryStore, NewEntry, Prov, Redactor, Source};
    use moraebot::pin::UPDATE_OPEN;
    let h = Harness::new();
    let today = Day::parse("2026-11-20").unwrap();
    let add = |text: &str| {
        let mut store =
            MemoryStore::open(&h.config.home, Redactor::default(), Arc::new(move || today))
                .expect("memory store");
        store
            .add(NewEntry {
                text: text.into(),
                prov: Prov::Owner,
                bucket: Bucket::Boundaries,
                src: Source::row(1),
                expires: None,
                why: None,
            })
            .expect("add");
    };
    add("never force-push");
    let m0 = h
        .manager("ok", &[lane_start("t1")])
        .with_memory_clock(Arc::new(move || today));
    prompt_until_lane_result(&m0, 1, "run-tools go").await;
    bounded(m0.shutdown_all()).await;

    let message =
        json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}});
    let omp = h
        .manager("ok", &[message])
        .with_memory_clock(Arc::new(move || today));
    prompt_until_lane_result(&omp, 2, "run-tools go").await;
    add("never delete branches");
    prompt_until_lane_result(&omp, 3, "run-tools go").await;
    prompt_until_lane_result(&omp, 4, "run-tools go").await;
    let lines = h.log_lines();
    let starts = h.lane_starts();
    assert_eq!(starts.len(), 4);
    let blocks: Vec<String> = starts.iter().map(|&n| appended(&lines[n])).collect();
    assert!(blocks[1].contains("never force-push"), "{}", blocks[1]);
    assert!(
        !blocks[1].contains("never delete branches"),
        "{}",
        blocks[1]
    );
    assert_eq!(blocks[2], blocks[1], "the system prompt stays pinned");
    assert_eq!(blocks[3], blocks[1], "the system prompt stays pinned");
    let prompt = |nth: usize| {
        h.frames_after_start(nth, "prompt")[0]["message"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    // The first job after moraebot restarted resumes the lane's session, which may hold an
    // earlier update: it hears that its system prompt holds.
    assert!(
        prompt(1).contains(moraebot::pin::REVERT_HEADER),
        "{}",
        prompt(1)
    );
    let update = prompt(2);
    assert!(update.starts_with("t2"), "{update}");
    assert!(update.contains(UPDATE_OPEN), "{update}");
    assert!(update.contains("never delete branches"), "{update}");
    assert!(!prompt(3).contains(UPDATE_OPEN), "told once: {}", prompt(3));
    bounded(omp.shutdown_all()).await;

    let m2 = h
        .manager("ok", &[lane_start("t3")])
        .with_memory_clock(Arc::new(move || today));
    prompt_until_lane_result(&m2, 5, "run-tools go").await;
    bounded(m2.shutdown_all()).await;
    let lines = h.log_lines();
    let starts = h.lane_starts();
    let fresh = appended(&lines[starts[4]]);
    assert!(fresh.contains("never delete branches"), "{fresh}");
    assert!(!prompt(4).contains(UPDATE_OPEN), "{}", prompt(4));
}

/// A job whose prompt never reached the lane (here refused) did not tell it the memory
/// update: the lane's next job carries it again.
#[tokio::test]
async fn a_memory_update_in_a_refused_prompt_goes_in_again() {
    use moraebot::memory::{Bucket, Day, MemoryStore, NewEntry, Prov, Redactor, Source};
    use moraebot::pin::UPDATE_OPEN;
    let h = Harness::new();
    let today = Day::parse("2026-11-20").unwrap();
    let add = |text: &str| {
        let mut store =
            MemoryStore::open(&h.config.home, Redactor::default(), Arc::new(move || today))
                .expect("memory store");
        store
            .add(NewEntry {
                text: text.into(),
                prov: Prov::Owner,
                bucket: Bucket::Boundaries,
                src: Source::row(1),
                expires: None,
                why: None,
            })
            .expect("add");
    };
    add("never force-push");
    let m0 = h
        .manager("ok", &[lane_start("t1")])
        .with_memory_clock(Arc::new(move || today));
    prompt_until_lane_result(&m0, 1, "run-tools go").await;
    bounded(m0.shutdown_all()).await;
    let message =
        json!({"tool": lane::LANE_MESSAGE_TOOL, "arguments": {"lane_id": 1, "text": "t2"}});
    // Refuses every lane prompt that carries an update (the lead's input never starts with
    // the job text), so each job shows whether it was sent.
    let omp = h
        .manager("ok", &[message])
        .with_memory_clock(Arc::new(move || today))
        .with_extra_env([(
            "FAKE_OMP_REJECT_TRIGGER",
            format!("t2\n\n{UPDATE_OPEN}\n{}", moraebot::pin::UPDATE_HEADER),
        )]);
    prompt_until_lane_result(&omp, 2, "run-tools go").await;
    add("never delete branches");
    prompt_until_lane_result(&omp, 3, "run-tools go").await;
    prompt_until_lane_result(&omp, 4, "run-tools go").await;
    bounded(omp.shutdown_all()).await;
    let prompt = |nth: usize| {
        h.frames_after_start(nth, "prompt")[0]["message"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    assert!(prompt(1).starts_with("t2"), "{}", prompt(1));
    assert!(prompt(2).contains(UPDATE_OPEN), "{}", prompt(2));
    assert!(
        prompt(3).contains(UPDATE_OPEN) && prompt(3).contains("never delete branches"),
        "the refused prompt told nothing: {}",
        prompt(3)
    );
}
