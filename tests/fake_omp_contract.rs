use std::path::Path;
use std::process::Command;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use omp_rpc::{
    Client, ClientOptions, DEFAULT_PROMPT_TIMEOUT, Error, Event, GetSessionLeaseCommand, HostTool,
    HostToolError, OpenSessionCommand, PromptCommand, PromptStatus, PromptTurn, RpcAgentEvent,
    RpcNotification, StreamingBehavior,
};
use serde_json::{Map, Value, json};

const WAIT: Duration = Duration::from_secs(30);

fn spawn_fake(mode: &str, log: Option<&Path>) -> Client {
    spawn_fake_with(mode, log, None, Vec::new())
}

fn spawn_fake_with(
    mode: &str,
    log: Option<&Path>,
    send_file_paths: Option<&str>,
    tools: Vec<HostTool>,
) -> Client {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fake-omp"));
    command
        .args(["--mode", "rpc", "--no-ui"])
        .env("FAKE_OMP_MODE", mode);
    if let Some(log) = log {
        command.env("FAKE_OMP_LOG", log);
    }
    if let Some(paths) = send_file_paths {
        command.env("FAKE_OMP_SEND_FILE", paths);
    }
    let options = ClientOptions {
        tools,
        ..ClientOptions::default()
    };
    let (client, _events) = Client::spawn(command, options).expect("spawn fake-omp");
    client
}

/// A `send_file` host tool that reports every `path` argument on the returned
/// channel and answers with `outcome`.
fn send_file_tool(
    outcome: fn() -> Result<String, HostToolError>,
) -> (HostTool, mpsc::Receiver<String>) {
    let (tx, rx) = mpsc::channel();
    let tx = Mutex::new(tx);
    let parameters: Map<String, Value> = serde_json::from_value(json!({
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "required": ["path"],
    }))
    .expect("schema object");
    let tool = HostTool::new(
        "send_file",
        "Send a file to the owner.",
        parameters,
        move |arguments, _context| {
            let path = arguments
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            tx.lock()
                .expect("sender lock")
                .send(path)
                .expect("report path");
            outcome().map(Into::into)
        },
    );
    (tool, rx)
}

fn queued() -> Result<String, HostToolError> {
    Ok("Queued.".to_owned())
}

fn refused() -> Result<String, HostToolError> {
    Err("E_FILE_PATH: refused".into())
}

fn log_frames(log: &Path) -> Vec<String> {
    std::fs::read_to_string(log)
        .expect("read log")
        .lines()
        .map(str::to_owned)
        .collect()
}

fn host_tool_results(lines: &[String]) -> Vec<Value> {
    lines
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|frame| frame["type"] == "host_tool_result")
        .collect()
}

fn open_session_command() -> OpenSessionCommand {
    OpenSessionCommand {
        session_dir: "fake-session-dir".to_owned(),
        provider: None,
        model_id: None,
    }
}

fn prompt(message: &str) -> PromptCommand {
    PromptCommand {
        message: message.to_owned(),
        images: None,
        streaming_behavior: None,
    }
}

#[test]
fn negotiates_protocol_v2() {
    let client = spawn_fake("ok", None);
    assert_eq!(client.protocol_version(), 2);
}

#[test]
fn open_session_succeeds() {
    let client = spawn_fake("ok", None);
    let opened = client.call(&open_session_command()).expect("open_session");
    assert_eq!(opened.session_id, "fake-session-1");
}

#[test]
fn open_session_failure_surfaces_error_code() {
    let client = spawn_fake("open_session_fail", None);
    match client.call(&open_session_command()) {
        Err(Error::Command { code, .. }) => assert_eq!(code.as_deref(), Some("E_OPEN_REFUSED")),
        other => panic!("expected a command error, got {other:?}"),
    }
}

#[test]
fn prompt_returns_assistant_text() {
    let client = spawn_fake("ok", None);
    let turn = client
        .prompt_and_wait(&prompt("hi"), DEFAULT_PROMPT_TIMEOUT)
        .expect("prompt");
    let result = turn.result.expect("prompt result");
    assert!(matches!(result.status, PromptStatus::Completed));
    assert_eq!(turn.assistant_text.as_deref(), Some("echo: hi"));
}

#[test]
fn not_invoked_prompt_has_no_result() {
    let client = spawn_fake("not_invoked", None);
    let turn = client
        .prompt_and_wait(&prompt("hi"), DEFAULT_PROMPT_TIMEOUT)
        .expect("prompt");
    assert!(turn.result.is_none());
    assert!(turn.assistant_text.is_none());
}

#[test]
fn error_prompt_reports_error_status() {
    let client = spawn_fake("error", None);
    let turn = client
        .prompt_and_wait(&prompt("hi"), DEFAULT_PROMPT_TIMEOUT)
        .expect("prompt");
    let result = turn.result.expect("prompt result");
    assert!(matches!(result.status, PromptStatus::Error));
}

#[test]
fn hung_prompt_times_out() {
    let client = spawn_fake("hang", None);
    match client.prompt_and_wait(&prompt("hi"), Duration::from_secs(2)) {
        Err(Error::Timeout { command }) => assert_eq!(command, "prompt"),
        other => panic!("expected a prompt timeout, got {other:?}"),
    }
}

#[test]
fn lease_reports_requested_sessions_as_not_held() {
    let client = spawn_fake("ok", None);
    let lease = client
        .call(&GetSessionLeaseCommand {
            session_ids: vec!["111".to_owned(), "222".to_owned()],
        })
        .expect("get_session_lease");
    assert!(lease.held.is_empty());
}

#[test]
fn lease_reports_requested_sessions_as_held() {
    let client = spawn_fake("lease_held", None);
    let lease = client
        .call(&GetSessionLeaseCommand {
            session_ids: vec!["111".to_owned()],
        })
        .expect("get_session_lease");
    assert_eq!(lease.held, vec!["111".to_owned()]);
}

#[test]
fn host_tools_registered() {
    let (tool, _rx) = send_file_tool(queued);
    let client = spawn_fake("send_file", None);
    let names = client.set_custom_tools(vec![tool]).expect("set_host_tools");
    assert_eq!(names, vec!["send_file".to_owned()]);
}

#[test]
fn send_file_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = dir.path().join("fake-omp.log");
    let (tool, calls) = send_file_tool(queued);
    let client = spawn_fake_with("send_file", Some(&log), Some("report.txt"), vec![tool]);
    let turn = client
        .prompt_and_wait(&prompt("hi"), DEFAULT_PROMPT_TIMEOUT)
        .expect("prompt");

    assert_eq!(calls.recv_timeout(WAIT).expect("tool call"), "report.txt");
    assert!(calls.try_recv().is_err(), "exactly one call");
    let result = turn.result.expect("prompt result");
    assert!(matches!(result.status, PromptStatus::Completed));
    assert_eq!(turn.assistant_text.as_deref(), Some("echo: hi"));

    let lines = log_frames(&log);
    assert!(
        lines
            .iter()
            .any(|line| line.contains(r#""type":"set_host_tools""#))
    );
    let results = host_tool_results(&lines);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["id"], "host_1");
    assert!(results[0]["isError"].is_null());
}

#[test]
fn send_file_calls_in_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = dir.path().join("fake-omp.log");
    let (tool, calls) = send_file_tool(queued);
    let client = spawn_fake_with("send_file", Some(&log), Some("a.txt\nb.txt"), vec![tool]);
    client
        .prompt_and_wait(&prompt("hi"), DEFAULT_PROMPT_TIMEOUT)
        .expect("prompt");

    assert_eq!(calls.recv_timeout(WAIT).expect("first call"), "a.txt");
    assert_eq!(calls.recv_timeout(WAIT).expect("second call"), "b.txt");

    // The fake logs `CALL host_<n>` right before writing a call and every
    // stdin line it reads, so the log order shows that call 2 was written
    // only after result 1 was read.
    let order: Vec<String> = log_frames(&log)
        .iter()
        .filter_map(|line| {
            if let Some(call) = line.strip_prefix("CALL ") {
                return Some(format!("call {call}"));
            }
            let frame = serde_json::from_str::<Value>(line).ok()?;
            (frame["type"] == "host_tool_result")
                .then(|| format!("result {}", frame["id"].as_str().unwrap_or_default()))
        })
        .collect();
    assert_eq!(
        order,
        [
            "call host_1",
            "result host_1",
            "call host_2",
            "result host_2"
        ]
    );
}

#[test]
fn send_file_error_result_is_logged() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = dir.path().join("fake-omp.log");
    let (tool, _calls) = send_file_tool(refused);
    let client = spawn_fake_with("send_file", Some(&log), Some("../outside"), vec![tool]);
    let turn = client
        .prompt_and_wait(&prompt("hi"), DEFAULT_PROMPT_TIMEOUT)
        .expect("prompt");

    let result = turn.result.expect("prompt result");
    assert!(matches!(result.status, PromptStatus::Completed));
    let results = host_tool_results(&log_frames(&log));
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["isError"], true);
}

#[test]
fn log_records_startup_argv_env_names_and_received_frames() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = dir.path().join("fake-omp.log");
    let client = spawn_fake("ok", Some(&log));
    client
        .prompt_and_wait(&prompt("hi"), DEFAULT_PROMPT_TIMEOUT)
        .expect("prompt");

    let text = std::fs::read_to_string(&log).expect("read log");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.starts_with("START pid="))
            .count(),
        1
    );
    let start = lines
        .iter()
        .find(|line| line.starts_with("START pid="))
        .expect("START line");
    assert!(start.contains(r#"argv=["--mode","rpc","--no-ui"]"#));
    let env = lines
        .iter()
        .find(|line| line.starts_with("ENV "))
        .expect("ENV line");
    let names: Vec<&str> = env.trim_start_matches("ENV ").split_whitespace().collect();
    assert!(names.contains(&"FAKE_OMP_MODE"));
    assert!(!env.contains('='));
    assert!(lines.iter().any(|line| line.contains(r#""type":"prompt""#)));
}

/// Spawns fake-omp with extra arguments and environment and keeps the event
/// receiver.
fn spawn_env(
    args: &[&str],
    envs: &[(&str, &str)],
    tools: Vec<HostTool>,
) -> (Client, mpsc::Receiver<Event>) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fake-omp"));
    command.args(["--mode", "rpc", "--no-ui"]).args(args);
    for (name, value) in envs {
        command.env(name, value);
    }
    let options = ClientOptions {
        tools,
        ..ClientOptions::default()
    };
    Client::spawn(command, options).expect("spawn fake-omp")
}

#[derive(Debug, PartialEq)]
struct RecordedCall {
    tool: String,
    arguments: Value,
    tool_call_id: String,
}

/// Host tools named `names` that report every call, in call order, on the
/// returned channel.
fn recording_tools(names: &[&str]) -> (Vec<HostTool>, mpsc::Receiver<RecordedCall>) {
    let (tx, rx) = mpsc::channel();
    let tx = Arc::new(Mutex::new(tx));
    let parameters: Map<String, Value> =
        serde_json::from_value(json!({"type": "object"})).expect("schema object");
    let tools = names
        .iter()
        .map(|name| {
            let tx = Arc::clone(&tx);
            let tool = (*name).to_owned();
            HostTool::new(
                *name,
                "Records the call.",
                parameters.clone(),
                move |arguments, context| {
                    tx.lock()
                        .expect("sender lock")
                        .send(RecordedCall {
                            tool: tool.clone(),
                            arguments: Value::Object(arguments),
                            tool_call_id: context.tool_call_id().to_owned(),
                        })
                        .expect("report call");
                    Ok("done".to_owned().into())
                },
            )
        })
        .collect();
    (tools, rx)
}

/// Waits for the first event `matches` accepts, bounded by `WAIT` overall.
fn wait_event(events: &mpsc::Receiver<Event>, matches: impl Fn(&Event) -> bool) {
    let deadline = Instant::now() + WAIT;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let event = events.recv_timeout(left).expect("expected event arrived");
        if matches(&event) {
            return;
        }
    }
}

fn is_agent_start(event: &Event) -> bool {
    matches!(
        event,
        Event::Notification(RpcNotification::RpcAgentEvent(RpcAgentEvent::AgentStart(_)))
    )
}

fn is_held(command: &'static str) -> impl Fn(&Event) -> bool {
    move |event| match event {
        Event::Notification(RpcNotification::Unknown(raw)) => {
            raw["type"] == "fake_held" && raw["command"] == command
        }
        _ => false,
    }
}

fn steer(message: &str) -> PromptCommand {
    PromptCommand {
        message: message.to_owned(),
        images: None,
        streaming_behavior: Some(StreamingBehavior::Steer),
    }
}

type Outcome = (&'static str, Result<PromptTurn, Error>);

/// Runs `command` on its own thread and reports its result, labelled, on `tx`.
fn prompt_on_thread(
    client: &Arc<Client>,
    label: &'static str,
    command: PromptCommand,
    tx: &mpsc::Sender<Outcome>,
) {
    let client = Arc::clone(client);
    let tx = tx.clone();
    thread::spawn(move || {
        let result = client.prompt_and_wait(&command, DEFAULT_PROMPT_TIMEOUT);
        let _ = tx.send((label, result));
    });
}

/// Collects one result per label from `rx`, each bounded by `WAIT`.
fn collect(rx: &mpsc::Receiver<Outcome>, count: usize) -> Vec<Outcome> {
    let mut results: Vec<Outcome> = (0..count)
        .map(|_| rx.recv_timeout(WAIT).expect("prompt finished within bound"))
        .collect();
    results.sort_by_key(|(label, _)| *label);
    results
}

fn kill_group(pid: u32) {
    let group = libc::pid_t::try_from(pid).expect("pid fits pid_t");
    // SAFETY: killpg only sends a signal to the fake's own process group.
    let sent = unsafe { libc::killpg(group, libc::SIGKILL) };
    assert_eq!(sent, 0, "process group killed");
}

fn assistant_text(outcome: &Outcome) -> &str {
    let turn = outcome.1.as_ref().expect("prompt completed");
    assert!(matches!(
        turn.result.as_ref().expect("prompt result").status,
        PromptStatus::Completed
    ));
    turn.assistant_text.as_deref().expect("assistant text")
}

fn steer_run(case: &str) -> Vec<Outcome> {
    let (client, events) = spawn_env(
        &[],
        &[("FAKE_OMP_MODE", "steer"), ("FAKE_OMP_STEER_CASE", case)],
        Vec::new(),
    );
    let client = Arc::new(client);
    let (tx, rx) = mpsc::channel();
    prompt_on_thread(&client, "1-main", prompt("m1"), &tx);
    wait_event(&events, is_agent_start);
    prompt_on_thread(&client, "2-steer", steer("m2"), &tx);
    collect(&rx, 2)
}

#[test]
fn lane_session_dir_selects_lane_mode() {
    let dir = tempfile::tempdir().expect("tempdir");
    let modes = [("FAKE_OMP_MODE", "hang"), ("FAKE_OMP_LANE_MODE", "ok")];

    let lane_dir = dir.path().join("lanes/1/session");
    let lane_dir = lane_dir.to_str().expect("utf-8 path");
    let (lane, _events) = spawn_env(&["--session-dir", lane_dir], &modes, Vec::new());
    let turn = lane
        .prompt_and_wait(&prompt("task"), DEFAULT_PROMPT_TIMEOUT)
        .expect("lane prompt");
    assert_eq!(turn.assistant_text.as_deref(), Some("echo: task"));

    let lead_dir = dir.path().join("lead/session");
    let lead_dir = lead_dir.to_str().expect("utf-8 path");
    let (lead, _events) = spawn_env(&["--session-dir", lead_dir], &modes, Vec::new());
    // The lead runs `hang`, which never answers: the 2 s limit is the behavior
    // under test.
    match lead.prompt_and_wait(&prompt("task"), Duration::from_secs(2)) {
        Err(Error::Timeout { command }) => assert_eq!(command, "prompt"),
        other => panic!("expected the lead to hang, got {other:?}"),
    }
}

#[test]
fn tools_mode_calls_each_tool_after_previous_result() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = dir.path().join("fake-omp.log");
    let log_value = log.to_str().expect("utf-8 path");
    let script = concat!(
        r#"{"tool":"lane_start","arguments":{"task":"build it"}}"#,
        "\n",
        r#"{"tool":"lane_status","arguments":{"lane_id":1}}"#,
    );
    let (tools, calls) = recording_tools(&["lane_start", "lane_status"]);
    let (client, _events) = spawn_env(
        &[],
        &[
            ("FAKE_OMP_MODE", "tools"),
            ("FAKE_OMP_TOOL_CALLS", script),
            ("FAKE_OMP_LOG", log_value),
        ],
        tools,
    );
    let turn = client
        .prompt_and_wait(&prompt("run-tools x"), DEFAULT_PROMPT_TIMEOUT)
        .expect("prompt");
    assert_eq!(turn.assistant_text.as_deref(), Some("echo: run-tools x"));

    let first = calls.recv_timeout(WAIT).expect("first call");
    let second = calls.recv_timeout(WAIT).expect("second call");
    assert!(calls.try_recv().is_err(), "exactly two calls");
    assert_eq!(first.tool, "lane_start");
    assert_eq!(first.arguments, json!({"task": "build it"}));
    assert_eq!(second.tool, "lane_status");
    assert_eq!(second.arguments, json!({"lane_id": 1}));

    let order: Vec<String> = log_frames(&log)
        .iter()
        .filter_map(|line| {
            if let Some(call) = line.strip_prefix("CALL ") {
                return Some(format!("call {call}"));
            }
            let frame = serde_json::from_str::<Value>(line).ok()?;
            (frame["type"] == "host_tool_result")
                .then(|| format!("result {}", frame["id"].as_str().unwrap_or_default()))
        })
        .collect();
    assert_eq!(
        order,
        [
            "call host_1",
            "result host_1",
            "call host_2",
            "result host_2"
        ]
    );
}

#[test]
fn tools_mode_without_trigger_answers_plain() {
    let script = r#"{"tool":"lane_start","arguments":{"task":"build it"}}"#;
    let (tools, calls) = recording_tools(&["lane_start", "lane_status"]);
    let (client, _events) = spawn_env(
        &[],
        &[("FAKE_OMP_MODE", "tools"), ("FAKE_OMP_TOOL_CALLS", script)],
        tools,
    );
    let turn = client
        .prompt_and_wait(&prompt("lane done"), DEFAULT_PROMPT_TIMEOUT)
        .expect("prompt");
    assert_eq!(turn.assistant_text.as_deref(), Some("echo: lane done"));
    // The fake writes every call before its final reply, so an empty channel
    // after the reply is conclusive.
    assert!(
        calls.try_recv().is_err(),
        "no tool call without the trigger"
    );
}

#[test]
fn lane_fake_runs_the_lane_tool_script() {
    let dir = tempfile::tempdir().expect("tempdir");
    let lane_script = r#"{"tool":"lane_done","arguments":{"evidence":"built it"}}"#;
    let lead_script = r#"{"tool":"other_tool","arguments":{}}"#;
    let lane_dir = dir.path().join("lanes/1/session");
    let lane_dir = lane_dir.to_str().expect("utf-8 path");
    let (tools, calls) = recording_tools(&["lane_done", "other_tool"]);
    let (lane, _events) = spawn_env(
        &["--session-dir", lane_dir],
        &[
            ("FAKE_OMP_LANE_MODE", "tools"),
            ("FAKE_OMP_LANE_TOOL_CALLS", lane_script),
            ("FAKE_OMP_TOOL_CALLS", lead_script),
        ],
        tools,
    );
    let turn = lane
        .prompt_and_wait(&prompt("run-lane-tools x"), DEFAULT_PROMPT_TIMEOUT)
        .expect("lane prompt");
    assert_eq!(
        turn.assistant_text.as_deref(),
        Some("echo: run-lane-tools x")
    );
    let call = calls.recv_timeout(WAIT).expect("lane_done call");
    assert_eq!(call.tool, "lane_done");
    assert_eq!(call.arguments, json!({"evidence": "built it"}));
    assert!(calls.try_recv().is_err(), "exactly one call");

    // The lead trigger does nothing in a lane; the fake writes every call
    // before its final reply, so an empty channel after the reply is conclusive.
    let turn = lane
        .prompt_and_wait(&prompt("run-tools y"), DEFAULT_PROMPT_TIMEOUT)
        .expect("lane prompt");
    assert_eq!(turn.assistant_text.as_deref(), Some("echo: run-tools y"));
    assert!(calls.try_recv().is_err(), "no call on the lead trigger");

    // A fake without `/lanes/` keeps the lead script.
    let (tools, calls) = recording_tools(&["lane_done", "other_tool"]);
    let (lead, _events) = spawn_env(
        &[],
        &[
            ("FAKE_OMP_MODE", "tools"),
            ("FAKE_OMP_LANE_TOOL_CALLS", lane_script),
            ("FAKE_OMP_TOOL_CALLS", lead_script),
        ],
        tools,
    );
    lead.prompt_and_wait(&prompt("run-tools z"), DEFAULT_PROMPT_TIMEOUT)
        .expect("lead prompt");
    let call = calls.recv_timeout(WAIT).expect("other_tool call");
    assert_eq!(call.tool, "other_tool");
    assert!(calls.try_recv().is_err(), "exactly one call");
}

#[test]
fn touch_after_the_nth_tool_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let marker = dir.path().join("marker");
    let lane_dir = dir.path().join("lanes/1/session");
    let lane_dir = lane_dir.to_str().expect("utf-8 path");
    let script = concat!(
        r#"{"tool":"lane_done","arguments":{"evidence":"one"}}"#,
        "\n",
        r#"{"tool":"lane_done","arguments":{"evidence":"two"}}"#,
        "\n",
        r#"{"tool":"lane_done","arguments":{"evidence":"three"}}"#,
    );
    // Each call reports whether the marker exists when the call is read; the
    // first two are refused, the third is accepted.
    let (tx, seen) = mpsc::channel();
    let tx = Mutex::new(tx);
    let seen_marker = marker.clone();
    let handled = std::sync::atomic::AtomicUsize::new(0);
    let parameters: Map<String, Value> =
        serde_json::from_value(json!({"type": "object"})).expect("schema object");
    let tool = HostTool::new(
        "lane_done",
        "Records whether the marker exists.",
        parameters,
        move |_arguments, _context| {
            tx.lock()
                .expect("sender lock")
                .send(seen_marker.exists())
                .expect("report marker");
            let call = handled.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            if call < 3 {
                Err("E_TEST_REFUSED: refused".into())
            } else {
                Ok("done".to_owned().into())
            }
        },
    );
    let marker_value = marker.to_str().expect("utf-8 path");
    let (client, _events) = spawn_env(
        &["--session-dir", lane_dir],
        &[
            ("FAKE_OMP_LANE_MODE", "tools"),
            ("FAKE_OMP_LANE_TOOL_CALLS", script),
            ("FAKE_OMP_TOUCH_ON_TOOL_ERROR", marker_value),
            ("FAKE_OMP_TOUCH_AFTER_ERRORS", "2"),
        ],
        vec![tool],
    );
    let turn = client
        .prompt_and_wait(&prompt("run-lane-tools"), DEFAULT_PROMPT_TIMEOUT)
        .expect("lane prompt");
    assert_eq!(turn.assistant_text.as_deref(), Some("echo: run-lane-tools"));
    let seen: Vec<bool> = (0..3)
        .map(|_| seen.recv_timeout(WAIT).expect("call handled"))
        .collect();
    assert_eq!(seen, [false, false, true]);
    assert!(marker.exists(), "marker stays after the final reply");
}

#[test]
fn steer_hold_keeps_both_prompts_open() {
    let (client, events) = spawn_env(
        &[],
        &[("FAKE_OMP_MODE", "steer"), ("FAKE_OMP_STEER_CASE", "hold")],
        Vec::new(),
    );
    let pid = client.pid().expect("child pid");
    let client = Arc::new(client);
    let (tx, rx) = mpsc::channel();
    prompt_on_thread(&client, "1-main", prompt("m1"), &tx);
    wait_event(&events, is_agent_start);
    prompt_on_thread(&client, "2-steer", steer("m2"), &tx);
    wait_event(&events, is_held("prompt"));
    assert!(rx.try_recv().is_err(), "both prompts still open");

    kill_group(pid);
    for (label, result) in collect(&rx, 2) {
        assert!(
            matches!(result, Err(Error::Closed)),
            "{label} ended by the kill, got {result:?}"
        );
    }
}

#[test]
fn lane_hold_open_never_answers_open_session() {
    let dir = tempfile::tempdir().expect("tempdir");
    let lane_dir = dir.path().join("lanes/1/session");
    let lane_dir = lane_dir.to_str().expect("utf-8 path");
    let (client, events) = spawn_env(
        &["--session-dir", lane_dir],
        &[("FAKE_OMP_LANE_MODE", "hold_open")],
        Vec::new(),
    );
    let pid = client.pid().expect("child pid");
    let lease = client
        .call(&GetSessionLeaseCommand {
            session_ids: vec!["111".to_owned()],
        })
        .expect("get_session_lease is answered");
    assert!(lease.held.is_empty());

    let client = Arc::new(client);
    let (tx, rx) = mpsc::channel();
    let opener = Arc::clone(&client);
    thread::spawn(move || {
        let _ = tx.send(opener.call(&open_session_command()));
    });
    wait_event(&events, is_held("open_session"));
    assert!(rx.try_recv().is_err(), "open_session still open");

    kill_group(pid);
    let result = rx.recv_timeout(WAIT).expect("open_session finished");
    assert!(
        matches!(result, Err(Error::Closed)),
        "open_session ended by the kill, got {result:?}"
    );
}

#[test]
fn steer_merged_reports_both_on_one_run() {
    let results = steer_run("merged");
    assert_eq!(assistant_text(&results[0]), "echo: m1 + m2");
    assert_eq!(assistant_text(&results[1]), "echo: m1 + m2");
}

#[test]
fn steer_separate_reports_two_runs() {
    let results = steer_run("separate");
    assert_eq!(assistant_text(&results[0]), "echo: m1");
    assert_eq!(assistant_text(&results[1]), "echo: m2");
}

#[test]
fn tools_mode_uses_script_id_as_tool_call_id() {
    let script = concat!(
        r#"{"tool":"t","arguments":{},"id":"dup"}"#,
        "\n",
        r#"{"tool":"t","arguments":{},"id":"dup"}"#,
        "\n",
        r#"{"tool":"t","arguments":{}}"#,
    );
    let (tools, calls) = recording_tools(&["t"]);
    let (client, _events) = spawn_env(
        &[],
        &[("FAKE_OMP_MODE", "tools"), ("FAKE_OMP_TOOL_CALLS", script)],
        tools,
    );
    let mut ids = Vec::new();
    for text in ["run-tools x", "run-tools y"] {
        client
            .prompt_and_wait(&prompt(text), DEFAULT_PROMPT_TIMEOUT)
            .expect("prompt");
        let turn: Vec<String> = (0..3)
            .map(|_| calls.recv_timeout(WAIT).expect("tool call").tool_call_id)
            .collect();
        assert_eq!(turn[..2], ["dup", "dup"]);
        assert!(turn[2].starts_with("toolu_"), "default id: {}", turn[2]);
        ids.push(turn[2].clone());
    }
    assert_ne!(ids[0], ids[1], "default ids never repeat");
}

#[test]
fn envelope_line_is_not_echoed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = dir.path().join("fake-omp.log");
    let client = spawn_fake("ok", Some(&log));
    let envelope = "[moraebot conversation=discord:th:111 message=7]\nhello";
    for (message, reply) in [
        (envelope, "echo: hello"),
        ("[note]\nhello", "echo: [note]\nhello"),
        ("hello", "echo: hello"),
    ] {
        let turn = client
            .prompt_and_wait(&prompt(message), DEFAULT_PROMPT_TIMEOUT)
            .expect("prompt");
        assert_eq!(turn.assistant_text.as_deref(), Some(reply));
    }
    let first = log_frames(&log)
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|frame| frame["type"] == "prompt")
        .expect("logged prompt frame");
    assert_eq!(first["message"], envelope);
}

/// The locked children's overlay turns omp's Auto-QA tool off: with it `get_state` lists no
/// tool; without its `dev: autoqa: false` lines (or without any overlay) it lists
/// `report_issue`, and moraebot would refuse the child.
#[test]
fn the_locked_overlay_turns_off_the_auto_qa_tool() {
    use omp_rpc::GetStateCommand;
    let dir = tempfile::tempdir().unwrap();
    let tools = |overlay: Option<&str>| -> Vec<String> {
        let path = dir.path().join("overlay.yml");
        let mut args = Vec::new();
        if let Some(text) = overlay {
            std::fs::write(&path, text).unwrap();
            args = vec!["--config", path.to_str().unwrap()];
        }
        let (client, _events) = spawn_env(&args, &[], Vec::new());
        let state = client.call(&GetStateCommand {}).expect("get_state");
        state.dump_tools.into_iter().map(|t| t.name).collect()
    };
    let overlay = moraebot::chat::CHAT_OVERLAY;
    assert!(overlay.contains("dev:\n  autoqa: false\n"));
    assert_eq!(tools(Some(overlay)), Vec::<String>::new());
    let without = overlay.replace("dev:\n  autoqa: false\n", "");
    assert_eq!(tools(Some(&without)), ["report_issue"]);
    assert_eq!(tools(None), ["report_issue"]);
}
