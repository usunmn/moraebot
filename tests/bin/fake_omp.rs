//! fake-omp: a scripted stand-in for `omp --mode rpc`, used by contract tests.
//!
//! Reads JSONL commands on stdin and answers on stdout. The behaviour is
//! selected by FAKE_OMP_MODE (default `ok`). When the `--session-dir` argument
//! contains `/lanes/`, FAKE_OMP_LANE_MODE (default `ok`) is used instead, so a
//! lead and its lanes run different scripts from one environment. When
//! FAKE_OMP_LOG is set, the process appends a trace line at startup
//! (`START pid=<pid> prompt=<json> argv=<json>`; `prompt` is the appended system prompt
//! resolved like omp 18.22.2 does it, once, at start: a one-line `--append-system-prompt`
//! value names a file that is read, a value with a line break or an unreadable file is the
//! text itself, `null` without the option), a line with the environment variable NAMES
//! only (`ENV <names>`), a line with the number of entries in the `--cwd`
//! folder at startup (`CWD_ENTRIES <pid> <n>`; `-1` when it cannot be read or
//! there is no `--cwd`), and then every received stdin line. An answered `open_session`
//! also logs `SESSION_ENTRIES <pid> <n>`, the entries in its `sessionDir` (`-1` when it
//! cannot be read).
//!
//! `get_state` answers a full session state whose `dumpTools` lists one tool
//! per comma-separated name in FAKE_OMP_DUMP_TOOLS (unset or empty: none), plus
//! `report_issue` like omp's Auto-QA (`dev.autoqa`, on by default, even with `--no-tools`)
//! unless the overlay file named by `--config <path>` has a top-level `dev:` block with an
//! indented `autoqa: false` line.
//!
//! Envelope: a prompt whose first line starts with `[moraebot ` and ends with
//! `]` is echoed without that line (and its newline); any other message is
//! echoed whole. The log keeps the full frame. With FAKE_OMP_REPLY set, an
//! echoed prompt is answered with that text instead; FAKE_OMP_REPLY_FILE=<path> wins over it
//! and is read for each answer, so a test can change the reply between calls. Without it, a summarizer
//! child (its `--session-dir` contains `/summarizer/`) answers each numbered
//! note line `<n>. <note>` with `<n>. echo: <note>`, like moraebot's
//! summarizer is asked to.
//!
//! Host tools: `set_host_tools` is acknowledged in every mode with the tool
//! names of the frame. Mode `send_file` answers a prompt with one
//! `host_tool_call` for `send_file` per line of FAKE_OMP_SEND_FILE (unset or
//! empty: one call with an empty path). Mode `tools` answers a prompt whose
//! body contains FAKE_OMP_TOOL_TRIGGER (default `run-tools`) with one
//! `host_tool_call` per JSON line of FAKE_OMP_TOOL_CALLS
//! (`{"tool": "<name>", "arguments": {...}, "id": "<toolCallId>"}`; without
//! `id` the call id is `toolu_<pid>_<n>`, `n` counting calls over the child's
//! life; a lead fake writes a `tool_execution_start` frame with the same
//! `toolCallId`, `toolName` and `args` just before each call, like omp); a line
//! `{"frame": {...}}` instead writes that frame as is (for example a
//! `tool_execution_start` event) and goes straight on to the next line, waiting
//! for nothing. Every other prompt is answered like `ok`. In both modes each next
//! call, and the final reply, is written only after the matching
//! `host_tool_result` frame arrived on stdin, so no transition depends on
//! time. With FAKE_OMP_LOG set, each call also leaves a `CALL host_<n>` line
//! just before it is written, which orders the calls against the received
//! `host_tool_result` lines.
//!
//! Lane tool script: a lane fake (its `--session-dir` contains `/lanes/`) in
//! mode `tools` reads FAKE_OMP_LANE_TOOL_TRIGGER (default `run-lane-tools`) and
//! FAKE_OMP_LANE_TOOL_CALLS (same line format as FAKE_OMP_TOOL_CALLS) instead of
//! FAKE_OMP_TOOL_TRIGGER and FAKE_OMP_TOOL_CALLS, which only a lead fake reads.
//! With FAKE_OMP_LANE_HOLD_AFTER_TOOLS set, a lane fake in mode `tools` writes nothing
//! after the last scripted call's `host_tool_result` (no final reply), like `hang`.
//!
//! End of a lead tool turn: FAKE_OMP_TOOLS_END=error ends a lead fake's tool turn
//! (after its last scripted call) with `agent_end` and an error `prompt_result`;
//! `hang` writes nothing more, like `hang`. Unset or anything else: the echo reply.
//!
//! Reply gate: with FAKE_OMP_REPLY_GATE=<path> set, a prompt answered like `ok` writes its
//! reply only once `<path>` exists (`agent_start` comes at once).
//!
//! Usage: with FAKE_OMP_USAGE set (a JSON object such as
//! `{"input":10,"output":5,"cacheRead":900,"cacheWrite":0}`), every assistant message a run ends
//! with carries it as its `usage`, in `message_end` and `agent_end` alike.
//!
//! Refused prompt: a prompt whose message contains FAKE_OMP_REJECT_TRIGGER (unset
//! or empty: none) gets an error `response` (code `E_PROMPT_REFUSED`) in every
//! mode, and no run.
//!
//! Touch on a refused call: FAKE_OMP_TOUCH_ON_TOOL_ERROR=<path> with
//! FAKE_OMP_TOUCH_AFTER_ERRORS=<n> (default 1) creates `<path>` empty (parents
//! are not created) when the n-th `host_tool_result` frame with
//! `"isError": true` arrives, counted over the child's life, before the next
//! call or the final reply is written. It applies to any fake, lead or lane.
//!
//! Steer: in mode `steer` the first prompt answers `agent_start` and waits; with
//! FAKE_OMP_STEER_FRAME set (a JSON frame), that frame is written as is between the
//! prompt's response and `agent_start` (for example an `auto_compaction_end` after the
//! main prompt was accepted, seen before the steer gate opens). A
//! later prompt with `"streamingBehavior":"steer"` is acknowledged and handled
//! by FAKE_OMP_STEER_CASE: `merged` (one run answers both), `separate` (two
//! runs, one answer each), `hold` (writes `{"type":"fake_held",...}` and then
//! nothing more), `tools` (runs the FAKE_OMP_TOOL_CALLS script as host tool
//! calls, then one run answers both, like `merged`), anything else (an error
//! `prompt_result` for both). Other
//! prompts are answered like `ok`; other modes answer a steer frame like any
//! prompt.
//!
//! Notes in mode `steer`: a steer whose message's first line starts with `[moraebot ` and
//! has no `conversation=` (a moraebot note, not an owner input) is acknowledged and held while
//! the main prompt waits; whenever the main prompt is answered (any case but `hold`), every
//! held note gets a `completed` result after it. With FAKE_OMP_STEER_CASE=note the first note
//! itself ends the run instead: one run answers the main prompt and the note
//! (`echo: <main> + <note body>`).
//!
//! Tool events in mode `steer`: FAKE_OMP_STEER_EVENTS (one JSON frame per non-empty line) is
//! written after the first prompt's `agent_start`, once FAKE_OMP_EVENTS_GATE=<path> exists
//! when that is set, so a test decides when the lead's tool events come.
//!
//! Lane mode `hold_open` answers `open_session` with a `fake_held` marker
//! instead of a response and then writes nothing more. A held fake is released
//! only by stdin closing or a kill.
//!
//! Lane mode `spin` answers a prompt like `hang`, then writes four
//! `tool_execution_start` frames (three identical `bash` calls with args
//! `{"command":"ls"}`, then one `read` call with args `{"path":"qa.txt"}`) and
//! nothing more.

use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write};
use std::path::Path;

use serde_json::{Value, json};

const SESSION_ID: &str = "fake-session-1";
/// This fake is a moraebot memory summarizer child.
static SUMMARIZER: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
/// Whether `get_state` lists omp's Auto-QA tool (`report_issue`); see [`autoqa`].
static AUTOQA: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
const DEFAULT_TOOL_TRIGGER: &str = "run-tools";
const DEFAULT_LANE_TOOL_TRIGGER: &str = "run-lane-tools";

/// One host tool call still to be offered to the host.
struct PlannedCall {
    tool: String,
    arguments: Value,
    tool_call_id: Option<String>,
    /// Written as is instead of a host tool call.
    frame: Option<Value>,
}

/// A host-tool turn in progress: the prompt it answers and the calls still to
/// be offered to the host.
struct ToolTurn {
    prompt_id: Value,
    message: String,
    remaining: VecDeque<PlannedCall>,
    calls_made: usize,
    /// A steer answered together with the main prompt once the calls are done.
    steer: Option<(Value, String)>,
}

/// The main prompt of a `steer` turn, waiting for its steer.
struct SteerTurn {
    prompt_id: Value,
    message: String,
}

struct Fake {
    mode: String,
    /// The `--session-dir` contains `/lanes/`: this fake is a lane child.
    lane: bool,
    /// `host_tool_result` frames with `isError` true seen over the child's life.
    tool_errors: usize,
    /// Host tool calls written over the child's life (default call ids).
    total_calls: usize,
    tool_turn: Option<ToolTurn>,
    steer_turn: Option<SteerTurn>,
    /// `steer` mode: the first prompt was taken.
    first_prompt_seen: bool,
    /// Set by a `hold` case: nothing more is written.
    held: bool,
    /// `steer` mode: the ids of notes held until the main prompt is answered.
    notes: Vec<Value>,
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let lane = is_lane(&argv);
    SUMMARIZER
        .set(arg_value(&argv, "--session-dir").is_some_and(|dir| dir.contains("/summarizer/")))
        .expect("set once");
    AUTOQA.set(autoqa(&argv)).expect("set once");
    let mode = select_mode(lane);
    let log = std::env::var_os("FAKE_OMP_LOG").map(std::path::PathBuf::from);
    if let Some(path) = &log {
        let json = serde_json::to_string(&argv).expect("argv is serializable");
        let prompt = argv
            .iter()
            .find_map(|a| a.strip_prefix("--append-system-prompt="))
            .map(|value| {
                if value.contains('\n') {
                    value.to_owned()
                } else {
                    std::fs::read_to_string(value).unwrap_or_else(|_| value.to_owned())
                }
            });
        let prompt = serde_json::to_string(&prompt).expect("prompt is serializable");
        log_line(
            path,
            &format!(
                "START pid={} prompt={prompt} argv={json}",
                std::process::id()
            ),
        );
        let mut names: Vec<String> = std::env::vars().map(|(name, _)| name).collect();
        names.sort();
        log_line(path, &format!("ENV {}", names.join(" ")));
        let entries = arg_value(&argv, "--cwd")
            .and_then(|cwd| std::fs::read_dir(cwd).ok())
            .map_or(-1, |dir| dir.count() as i64);
        log_line(
            path,
            &format!("CWD_ENTRIES {} {entries}", std::process::id()),
        );
    }

    let stdout = io::stdout();
    let mut out = stdout.lock();
    send(
        &mut out,
        &json!({
            "type": "ready",
            "protocolVersion": 1,
            "supportedProtocolVersions": [1, 2],
            "maxFrameBytes": 1_048_576,
            "maxReassembledFrameBytes": 67_108_864,
        }),
    );
    send(
        &mut out,
        &json!({"type": "available_commands_update", "commands": []}),
    );

    let mut fake = Fake {
        mode,
        lane,
        tool_errors: 0,
        total_calls: 0,
        tool_turn: None,
        steer_turn: None,
        first_prompt_seen: false,
        held: false,
        notes: Vec::new(),
    };
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if let Some(path) = &log {
            log_line(path, &line);
        }
        if fake.held {
            continue;
        }
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        fake.handle(&frame, &mut out);
    }
}

/// The value of `flag` given as `<flag> <value>` or `<flag>=<value>`.
fn arg_value<'a>(argv: &'a [String], flag: &str) -> Option<&'a str> {
    argv.iter().enumerate().find_map(|(index, arg)| {
        if arg == flag {
            argv.get(index + 1).map(String::as_str)
        } else {
            arg.strip_prefix(flag)
                .and_then(|rest| rest.strip_prefix('='))
        }
    })
}

/// Whether the `--session-dir` argument contains `/lanes/` (a lane child).
fn is_lane(argv: &[String]) -> bool {
    arg_value(argv, "--session-dir").is_some_and(|dir| dir.contains("/lanes/"))
}

/// Whether omp's Auto-QA tool is on: not when the `--config` overlay sets `dev: autoqa: false`.
fn autoqa(argv: &[String]) -> bool {
    let Some(overlay) = arg_value(argv, "--config").and_then(|p| std::fs::read_to_string(p).ok())
    else {
        return true;
    };
    let mut in_dev = false;
    for line in overlay.lines() {
        if !line.starts_with(' ') {
            in_dev = line.trim_end() == "dev:";
        } else if in_dev && line.trim() == "autoqa: false" {
            return false;
        }
    }
    true
}

/// A full `get_state` answer; `dumpTools` from FAKE_OMP_DUMP_TOOLS, plus `report_issue` when
/// Auto-QA is on.
fn session_state(autoqa: bool) -> Value {
    let mut tools: Vec<Value> = std::env::var("FAKE_OMP_DUMP_TOOLS")
        .unwrap_or_default()
        .split(',')
        .filter(|name| !name.is_empty())
        .map(|name| json!({"name": name, "description": "fake tool"}))
        .collect();
    if autoqa {
        tools.push(json!({"name": "report_issue", "description": "fake Auto-QA tool"}));
    }
    json!({
        "sessionId": SESSION_ID,
        "isStreaming": false,
        "isCompacting": false,
        "steeringMode": "all",
        "followUpMode": "all",
        "interruptMode": "immediate",
        "autoCompactionEnabled": false,
        "fastModeEnabled": false,
        "fastModeActive": false,
        "slowModeSupported": false,
        "slowModeEnabled": false,
        "messageCount": 0,
        "queuedMessageCount": 0,
        "hasPendingAsyncWork": false,
        "isSettled": true,
        "queuedMessages": {"steering": [], "followUp": []},
        "todoPhases": [],
        "systemPrompt": [],
        "dumpTools": tools,
    })
}

/// FAKE_OMP_LANE_MODE for a lane child, FAKE_OMP_MODE otherwise.
fn select_mode(lane: bool) -> String {
    let variable = if lane {
        "FAKE_OMP_LANE_MODE"
    } else {
        "FAKE_OMP_MODE"
    };
    std::env::var(variable).unwrap_or_else(|_| "ok".to_owned())
}

impl Fake {
    fn handle(&mut self, frame: &Value, out: &mut impl Write) {
        let id = frame.get("id").cloned().unwrap_or(Value::Null);
        let command = frame
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let mode = self.mode.as_str();
        match command {
            "negotiate_protocol" => respond_ok(out, &id, command, json!({"protocolVersion": 2})),
            "open_session" if mode == "open_session_fail" => respond_err(
                out,
                &id,
                command,
                "open_session refused by fake-omp",
                "E_OPEN_REFUSED",
            ),
            "open_session" if mode == "hold_open" => self.hold(out, command),
            "open_session" => {
                log_session_entries(frame);
                respond_ok(
                    out,
                    &id,
                    command,
                    json!({
                        "cancelled": false,
                        "resumed": false,
                        "sessionId": SESSION_ID,
                        "sessionFile": "fake-session.jsonl",
                    }),
                )
            }
            "get_state" => respond_ok(out, &id, command, session_state(*AUTOQA.get().unwrap())),
            "get_session_lease" => {
                let requested = frame
                    .get("sessionIds")
                    .cloned()
                    .unwrap_or_else(|| json!([]));
                let (held, unknown) = if mode == "lease_held" {
                    (requested, json!([]))
                } else {
                    (json!([]), requested)
                };
                respond_ok(out, &id, command, json!({"held": held, "unknown": unknown}));
            }
            "set_host_tools" => {
                let names: Vec<Value> = frame
                    .get("tools")
                    .and_then(Value::as_array)
                    .map(|tools| {
                        tools
                            .iter()
                            .filter_map(|tool| tool.get("name"))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                respond_ok(out, &id, command, json!({"toolNames": names}));
            }
            "prompt" if refused(frame) => respond_err(
                out,
                &id,
                command,
                "prompt refused by fake-omp",
                "E_PROMPT_REFUSED",
            ),
            "prompt" if mode == "send_file" => {
                let calls = send_file_calls();
                self.start_tool_turn(&id, frame, calls, out);
            }
            "prompt" if mode == "tools" => {
                let (trigger_variable, default_trigger, calls_variable) = if self.lane {
                    (
                        "FAKE_OMP_LANE_TOOL_TRIGGER",
                        DEFAULT_LANE_TOOL_TRIGGER,
                        "FAKE_OMP_LANE_TOOL_CALLS",
                    )
                } else {
                    (
                        "FAKE_OMP_TOOL_TRIGGER",
                        DEFAULT_TOOL_TRIGGER,
                        "FAKE_OMP_TOOL_CALLS",
                    )
                };
                let trigger =
                    std::env::var(trigger_variable).unwrap_or_else(|_| default_trigger.to_owned());
                if prompt_message(frame).contains(&trigger) {
                    let calls = scripted_calls(calls_variable);
                    self.start_tool_turn(&id, frame, calls, out);
                } else {
                    prompt("ok", &id, frame, out);
                }
            }
            "prompt" if mode == "steer" => self.steer_prompt(&id, frame, out),
            "prompt" => prompt(mode, &id, frame, out),
            "host_tool_result" => {
                if frame.get("isError").and_then(Value::as_bool) == Some(true) {
                    self.tool_errors += 1;
                    touch_on_tool_error(self.tool_errors);
                }
                if let Some(active) = self.tool_turn.take() {
                    self.tool_turn = self.advance_tool_turn(active, out);
                }
            }
            _ => respond_err(out, &id, command, "unknown command", "E_UNKNOWN_COMMAND"),
        }
    }

    /// Writes the `fake_held` marker for `command`; nothing is written after it.
    fn hold(&mut self, out: &mut impl Write, command: &str) {
        send(out, &json!({"type": "fake_held", "command": command}));
        self.held = true;
    }

    fn start_tool_turn(
        &mut self,
        id: &Value,
        frame: &Value,
        calls: VecDeque<PlannedCall>,
        out: &mut impl Write,
    ) {
        respond_ok(out, id, "prompt", json!({"agentInvoked": true}));
        send(out, &json!({"type": "agent_start"}));
        let turn = ToolTurn {
            prompt_id: id.clone(),
            message: prompt_message(frame).to_owned(),
            remaining: calls,
            calls_made: 0,
            steer: None,
        };
        self.tool_turn = self.advance_tool_turn(turn, out);
    }

    /// Emits the next host tool call, or finishes the turn when none is left.
    /// Returns the turn while it still waits for a `host_tool_result`.
    fn advance_tool_turn(&mut self, mut turn: ToolTurn, out: &mut impl Write) -> Option<ToolTurn> {
        let call = loop {
            match turn.remaining.pop_front() {
                Some(PlannedCall {
                    frame: Some(frame), ..
                }) => send(out, &frame),
                Some(call) => break call,
                None => {
                    if self.lane && std::env::var_os("FAKE_OMP_LANE_HOLD_AFTER_TOOLS").is_some() {
                        return None;
                    }
                    match &turn.steer {
                        Some((steer_id, steer_message)) => {
                            let text = format!("echo: {} + {steer_message}", turn.message);
                            end_run(&text, json!({}), out);
                            completed(&turn.prompt_id, out);
                            completed(steer_id, out);
                        }
                        None => {
                            let end = std::env::var("FAKE_OMP_TOOLS_END").unwrap_or_default();
                            match end.as_str() {
                                "error" if !self.lane => {
                                    send(out, &json!({"type": "agent_end", "messages": []}));
                                    prompt_error(&turn.prompt_id, out);
                                }
                                "hang" if !self.lane => {}
                                _ => finish_echo(&turn.prompt_id, &turn.message, out),
                            }
                        }
                    }
                    return None;
                }
            }
        };
        turn.calls_made += 1;
        self.total_calls += 1;
        let n = turn.calls_made;
        let tool_call_id = call.tool_call_id.unwrap_or_else(|| {
            if self.mode == "send_file" {
                format!("toolu_{n}")
            } else {
                format!("toolu_{}_{}", std::process::id(), self.total_calls)
            }
        });
        if let Some(path) = std::env::var_os("FAKE_OMP_LOG") {
            log_line(Path::new(&path), &format!("CALL host_{n}"));
        }
        if !self.lane {
            // Like omp, a lead's call starts with `tool_execution_start` for the same id.
            send(
                out,
                &json!({
                    "type": "tool_execution_start",
                    "toolCallId": tool_call_id,
                    "toolName": call.tool,
                    "args": call.arguments,
                }),
            );
        }
        send(
            out,
            &json!({
                "type": "host_tool_call",
                "id": format!("host_{n}"),
                "toolCallId": tool_call_id,
                "toolName": call.tool,
                "arguments": call.arguments,
            }),
        );
        Some(turn)
    }

    fn steer_prompt(&mut self, id: &Value, frame: &Value, out: &mut impl Write) {
        let is_steer = frame.get("streamingBehavior").and_then(Value::as_str) == Some("steer");
        if is_steer {
            if self.steer_turn.is_some() && is_note(frame) {
                respond_ok(out, id, "prompt", json!({"agentInvoked": true}));
                self.notes.push(id.clone());
                if std::env::var("FAKE_OMP_STEER_CASE").as_deref() == Ok("note") {
                    let main = self.steer_turn.take().expect("checked above");
                    let text = format!("echo: {} + {}", main.message, prompt_message(frame));
                    end_run(&text, json!({}), out);
                    completed(&main.prompt_id, out);
                    self.complete_notes(out);
                }
                return;
            }
            if let Some(main) = self.steer_turn.take() {
                respond_ok(out, id, "prompt", json!({"agentInvoked": true}));
                self.answer_steer(&main, id, prompt_message(frame), out);
                if !self.held && self.tool_turn.is_none() {
                    self.complete_notes(out);
                }
                return;
            }
        } else if !self.first_prompt_seen {
            self.first_prompt_seen = true;
            respond_ok(out, id, "prompt", json!({"agentInvoked": true}));
            if let Ok(frame) = std::env::var("FAKE_OMP_STEER_FRAME") {
                let frame: Value =
                    serde_json::from_str(&frame).expect("FAKE_OMP_STEER_FRAME is a JSON frame");
                send(out, &frame);
            }
            send(out, &json!({"type": "agent_start"}));
            if let Ok(script) = std::env::var("FAKE_OMP_STEER_EVENTS") {
                await_file("FAKE_OMP_EVENTS_GATE");
                for line in script.lines().filter(|line| !line.trim().is_empty()) {
                    let frame: Value =
                        serde_json::from_str(line).expect("FAKE_OMP_STEER_EVENTS lines are JSON");
                    send(out, &frame);
                }
            }
            self.steer_turn = Some(SteerTurn {
                prompt_id: id.clone(),
                message: prompt_message(frame).to_owned(),
            });
            return;
        }
        prompt("ok", id, frame, out);
    }

    /// Answers every held note with a `completed` result.
    fn complete_notes(&mut self, out: &mut impl Write) {
        for note in std::mem::take(&mut self.notes) {
            completed(&note, out);
        }
    }

    fn answer_steer(
        &mut self,
        main: &SteerTurn,
        steer_id: &Value,
        steer_message: &str,
        out: &mut impl Write,
    ) {
        let case = std::env::var("FAKE_OMP_STEER_CASE").unwrap_or_default();
        match case.as_str() {
            "merged" => {
                let text = format!("echo: {} + {steer_message}", main.message);
                end_run(&text, json!({}), out);
                completed(&main.prompt_id, out);
                completed(steer_id, out);
            }
            "separate" => {
                end_run(
                    &format!("echo: {}", main.message),
                    json!({"yielded": true, "isTerminal": false}),
                    out,
                );
                completed(&main.prompt_id, out);
                send(out, &json!({"type": "agent_start"}));
                end_run(&format!("echo: {steer_message}"), json!({}), out);
                completed(steer_id, out);
            }
            "hold" => self.hold(out, "prompt"),
            "tools" => {
                let turn = ToolTurn {
                    prompt_id: main.prompt_id.clone(),
                    message: main.message.clone(),
                    remaining: scripted_calls("FAKE_OMP_TOOL_CALLS"),
                    calls_made: 0,
                    steer: Some((steer_id.clone(), steer_message.to_owned())),
                };
                self.tool_turn = self.advance_tool_turn(turn, out);
            }
            _ => {
                send(out, &json!({"type": "agent_end", "messages": []}));
                prompt_error(&main.prompt_id, out);
                prompt_error(steer_id, out);
            }
        }
    }
}

/// The prompt's first line is a moraebot note's tag: `[moraebot ` without `conversation=`.
fn is_note(frame: &Value) -> bool {
    let message = frame
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let first = message.lines().next().unwrap_or_default();
    first.starts_with("[moraebot ") && first.ends_with(']') && !first.contains("conversation=")
}

/// The prompt's message contains FAKE_OMP_REJECT_TRIGGER (unset or empty: never).
fn refused(frame: &Value) -> bool {
    std::env::var("FAKE_OMP_REJECT_TRIGGER")
        .is_ok_and(|trigger| !trigger.is_empty() && prompt_message(frame).contains(&trigger))
}

/// FAKE_OMP_SEND_FILE as `send_file` calls (unset or empty: one empty path).
fn send_file_calls() -> VecDeque<PlannedCall> {
    let script = std::env::var("FAKE_OMP_SEND_FILE").unwrap_or_default();
    let paths: Vec<String> = if script.is_empty() {
        vec![String::new()]
    } else {
        script.lines().map(str::to_owned).collect()
    };
    paths
        .into_iter()
        .map(|path| PlannedCall {
            tool: "send_file".to_owned(),
            arguments: json!({"path": path}),
            tool_call_id: None,
            frame: None,
        })
        .collect()
}

/// Creates FAKE_OMP_TOUCH_ON_TOOL_ERROR when `errors`, the count of refused
/// calls so far, reaches FAKE_OMP_TOUCH_AFTER_ERRORS (default 1).
fn touch_on_tool_error(errors: usize) {
    let Some(path) = std::env::var_os("FAKE_OMP_TOUCH_ON_TOOL_ERROR") else {
        return;
    };
    let after = std::env::var("FAKE_OMP_TOUCH_AFTER_ERRORS")
        .map(|value| {
            value
                .parse::<usize>()
                .expect("FAKE_OMP_TOUCH_AFTER_ERRORS is a number")
        })
        .unwrap_or(1);
    if errors == after {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(path)
            .expect("create FAKE_OMP_TOUCH_ON_TOOL_ERROR");
    }
}

/// The script in `variable`: one JSON object per non-empty line.
fn scripted_calls(variable: &str) -> VecDeque<PlannedCall> {
    let script = std::env::var(variable).unwrap_or_default();
    script
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let call: Value = serde_json::from_str(line).expect("tool script line is JSON");
            if let Some(frame) = call.get("frame") {
                return PlannedCall {
                    tool: String::new(),
                    arguments: Value::Null,
                    tool_call_id: None,
                    frame: Some(frame.clone()),
                };
            }
            PlannedCall {
                tool: call
                    .get("tool")
                    .and_then(Value::as_str)
                    .expect("tool script line has a tool")
                    .to_owned(),
                arguments: call.get("arguments").cloned().unwrap_or_else(|| json!({})),
                tool_call_id: call.get("id").and_then(Value::as_str).map(str::to_owned),
                frame: None,
            }
        })
        .collect()
}

fn prompt(mode: &str, id: &Value, frame: &Value, out: &mut impl Write) {
    if mode == "not_invoked" {
        respond_ok(out, id, "prompt", json!({"agentInvoked": false}));
        return;
    }
    respond_ok(out, id, "prompt", json!({"agentInvoked": true}));
    send(out, &json!({"type": "agent_start"}));
    match mode {
        "hang" => {}
        "spin" => spin(out),
        "exit1" => std::process::exit(1),
        "error" => {
            send(out, &json!({"type": "agent_end", "messages": []}));
            prompt_error(id, out);
        }
        _ => {
            await_reply_gate();
            finish_echo(id, prompt_message(frame), out)
        }
    }
}

/// FAKE_OMP_REPLY_GATE=<path>: a plain prompt's reply is written only once `<path>` exists, so a
/// test decides when an answer comes (the recall advisor's late picks) instead of a timer.
fn await_reply_gate() {
    await_file("FAKE_OMP_REPLY_GATE");
}

/// Waits until the file named by `variable` exists (unset: returns at once).
fn await_file(variable: &str) {
    let Some(gate) = std::env::var_os(variable) else {
        return;
    };
    while !std::path::Path::new(&gate).exists() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// Mode `spin`: three identical `bash` tool calls, then one `read` call.
fn spin(out: &mut impl Write) {
    for id in ["spin-1", "spin-2", "spin-3"] {
        send(
            out,
            &json!({"type": "tool_execution_start", "toolCallId": id, "toolName": "bash", "args": {"command": "ls"}}),
        );
    }
    send(
        out,
        &json!({"type": "tool_execution_start", "toolCallId": "spin-4", "toolName": "read", "args": {"path": "qa.txt"}}),
    );
}

/// The prompt's message without a leading `[moraebot ...]` envelope line.
fn prompt_message(frame: &Value) -> &str {
    let message = frame
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let (first, rest) = message.split_once('\n').unwrap_or((message, ""));
    if first.starts_with("[moraebot ") && first.ends_with(']') {
        rest
    } else {
        message
    }
}

fn finish_echo(id: &Value, message: &str, out: &mut impl Write) {
    let from_file =
        std::env::var_os("FAKE_OMP_REPLY_FILE").and_then(|path| std::fs::read_to_string(path).ok());
    let text = from_file
        .map_or_else(|| std::env::var("FAKE_OMP_REPLY"), Ok)
        .unwrap_or_else(|_| {
            if SUMMARIZER.get().copied().unwrap_or(false) {
                message
                    .lines()
                    .map(|line| match line.split_once(". ") {
                        Some((n, note)) => format!("{n}. echo: {note}"),
                        None => format!("echo: {line}"),
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            } else {
                format!("echo: {message}")
            }
        });
    end_run(&text, json!({}), out);
    completed(id, out);
}

/// Writes `message_end` and `agent_end` for one assistant `text`; `extra`
/// fields are added to the `agent_end` frame. The message carries FAKE_OMP_USAGE as its usage.
fn end_run(text: &str, extra: Value, out: &mut impl Write) {
    let mut assistant = json!({
        "role": "assistant",
        "content": [{"type": "text", "text": text}],
        "stopReason": "stop",
        "timestamp": 0,
    });
    if let Some(usage) = std::env::var("FAKE_OMP_USAGE")
        .ok()
        .and_then(|usage| serde_json::from_str::<Value>(&usage).ok())
    {
        assistant["usage"] = usage;
    }
    send(out, &json!({"type": "message_end", "message": assistant}));
    let mut end = json!({"type": "agent_end", "messages": [assistant]});
    if let (Some(end), Value::Object(extra)) = (end.as_object_mut(), extra) {
        end.extend(extra);
    }
    send(out, &end);
}

fn completed(id: &Value, out: &mut impl Write) {
    send(
        out,
        &json!({
            "type": "prompt_result",
            "id": id,
            "agentInvoked": true,
            "status": "completed",
            "sessionSettled": true,
        }),
    );
}

fn prompt_error(id: &Value, out: &mut impl Write) {
    send(
        out,
        &json!({
            "type": "prompt_result",
            "id": id,
            "agentInvoked": true,
            "status": "error",
            "sessionSettled": true,
            "error": {"message": "fake provider failure", "retryable": false},
        }),
    );
}

fn respond_ok(out: &mut impl Write, id: &Value, command: &str, data: Value) {
    send(
        out,
        &json!({"id": id, "type": "response", "command": command, "success": true, "data": data}),
    );
}

fn respond_err(out: &mut impl Write, id: &Value, command: &str, error: &str, code: &str) {
    send(
        out,
        &json!({
            "id": id,
            "type": "response",
            "command": command,
            "success": false,
            "error": error,
            "code": code,
        }),
    );
}

fn send(out: &mut impl Write, frame: &Value) {
    writeln!(out, "{frame}")
        .and_then(|()| out.flush())
        .expect("write frame to stdout");
}

/// Appends `line` and its newline in one `write` call: a lead and its lanes share the
/// log, and `writeln!` on a `File` writes the text and the newline separately, so
/// another child's line could land between them and merge two lines into one.
/// With FAKE_OMP_LOG set, logs `SESSION_ENTRIES <pid> <n>`: the entries in the `open_session`
/// frame's `sessionDir` (`-1` when it cannot be read).
fn log_session_entries(frame: &Value) {
    let Some(path) = std::env::var_os("FAKE_OMP_LOG") else {
        return;
    };
    let entries = frame
        .get("sessionDir")
        .and_then(Value::as_str)
        .and_then(|dir| std::fs::read_dir(dir).ok())
        .map_or(-1, |dir| dir.count() as i64);
    log_line(
        Path::new(&path),
        &format!("SESSION_ENTRIES {} {entries}", std::process::id()),
    );
}

fn log_line(path: &Path, line: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open FAKE_OMP_LOG");
    file.write_all(format!("{line}\n").as_bytes())
        .expect("write FAKE_OMP_LOG");
}
