//! Lanes: separate headless omp children started by the lead through host tools. Messenger-agnostic.
//!
//! The lead calls `lane_start`, `lane_message` and `lane_status` (registered on the lead
//! child next to `send_file`). A lane is its own omp RPC child whose appended system prompt
//! is only its memory block, passed as a private file (`omp::write_append_file`); it runs one
//! prompt on a worker thread and closes.
//!
//! `lane_message` on a running lane steers instead of waiting: the text goes into the job's
//! steer inbox, and the job's own thread forwards it as `prompt{streamingBehavior:"steer"}`
//! into the turn already in progress, the way the lead's own steer works. A steer only goes out
//! after the turn's first `agent_start` (omp admits a steer into a run that is already
//! streaming); texts that arrive earlier wait for it. omp answers a steer inside the main
//! prompt's run (or, when it reached omp as that run ended, in a run of its own); the job waits
//! for every steer it sent, so the child never outlives the job, and the newest reply is the
//! result. The lane still writes one result row and `prompts` does not grow. A text the turn
//! never took (it came before the turn could take a steer, or after the turn ended) is not
//! dropped: the inbox lives until the job's child is removed under `inner`, and what is left
//! in it then runs as the lane's next prompt, the way `lane_message` starts one on an idle
//! lane. A steer carries the bare text, without the
//! lane's footers (`lane_done`, `lane_report`, git). A lane with a done line gets
//! exactly one host tool, `lane_done`: the lane claims its work is done, moraebot runs
//! the Done checks inside that call and refuses a claim that fails them. Its result is
//! stored as one queued inbox row (`platform = LANE_PLATFORM`) in the same transaction as
//! the lane's new state, and the dispatcher is woken; the lead answers it in a normal
//! turn. Handlers never wait for lane work.
//!
//! Lock order is claim -> `inner` -> store everywhere; nothing takes a claim lock while
//! holding `inner`. A handler holds `inner` from the `stopping` check until the new
//! worker is registered, and a worker writes its result and removes its own registration
//! under one hold of `inner`, so at most one worker per lane is ever registered and
//! `shutdown_all` sees every lane child that exists. If omp ever ended a turn while a
//! `lane_done` call was still running, `run_prompt` waits on the claim lock for that one
//! check; it holds no lock while it waits, so this is not a deadlock. An `Activity` lock is
//! taken last and nothing is locked while it is held.
//!
//! Shutdown kills every lane child first, then every running check, then waits until
//! each killed check was reaped. `stopping` is set before those kills, so a `lane_done`
//! call refused for the shutdown can still reach a live lane, which may then end its turn
//! with a reply; that refusal marks the job's claim `Cut`, so a turn cut during a check is
//! always left for `recover_on_start`.
//!
//! A lane started with `repo` works in its own git worktree,
//! `<work dir>/.moraebot-lanes/lane-<id>` on branch `moraebot/lane-<id>`, created before
//! its first child starts and reused by later prompts; its git commands are tracked and
//! killed on shutdown like Done checks. Its `lane_done` claims first read the worktree's git
//! state: uncommitted tracked changes, and commits on no remote when it has one, refuse the
//! claim like a failing Done check.
//!
//! The lead closes a lane that is not running with `lane_close`: the store marks it closed
//! (no more `lane_message`), and a lane with a repo has its worktree removed only when nothing
//! would be lost; the branch always stays.
//!
//! While a job runs, its omp agent events are counted in an `Activity` (a new tool call, by
//! name and args, is progress); `watch_tick` reports a job with no event, or no progress, for
//! `omp.stall_after_minutes` (`stall_after`) to the lead once per episode as a lane inbox row.
//! moraebot never stops a lane for it.
//!
//! Every lane job also gets `lane_report`: the lane leaves a one-line progress note, cleaned by
//! `news_text` and kept as the job's newest report in its `Activity`. `news` lists the reports
//! not yet handed on for their lane; while the lane's conversation is quiet the dispatcher gives
//! each one to the lead (a lane inbox row, or a note in the running turn of that conversation)
//! and calls `news_posted`. Reports live in memory only.
//!
//! The lead stops a running job with the host tool `lane_stop` (`stop`): the job is marked stopped, its child's process
//! group and the checks it started are killed the way `shutdown_all` kills them, and from then
//! on the job starts no child and no check. The worker then stores a `failed
//! code=E_LANE_STOPPED` result and the lane is `failed`, open for `lane_message`. A job that
//! ended on its own before the kill keeps its own result.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use omp_rpc::{
    Event, HostTool, HostToolContext, HostToolError, PromptCommand, RpcAgentEvent, RpcNotification,
    ToolLoadMode,
};
use serde_json::{Map, Value, json};
use tokio::sync::{Notify, watch};

use crate::access::{E_ACCESS_NOT_OWNER, Sender};
use crate::config::Config;
use crate::done::{self, DoneLine};
use crate::lead::{self, LaneResult, Stalled};
use crate::memory;
use crate::omp::{
    self, ABORT_JOIN_BOUND, E_OMP_SPAWN, EMPTY_REPLY, GroupKill, OpenedChild, Outcome, TurnContext,
    lock,
};
use crate::outcome;
use crate::secret;
use crate::store::{LaneClose, LaneMessage, LaneStart, LaneState, Store};
use crate::worktree::{self, Cleanup, WorktreeError};

/// Most lanes that may run a turn at the same time.
pub const MAX_RUNNING_LANES: usize = 3;
/// How often the watchdog looks at running lanes.
pub const WATCH_TICK: Duration = Duration::from_secs(30);
/// Inbox `platform` of rows that carry a lane's result to the lead.
pub const LANE_PLATFORM: &str = "lane";
/// Host tool that starts a new lane.
pub const LANE_START_TOOL: &str = "lane_start";
/// Host tool that sends a follow-up to an idle lane.
pub const LANE_MESSAGE_TOOL: &str = "lane_message";
/// Host tool that lists lanes and their state.
pub const LANE_STATUS_TOOL: &str = "lane_status";
/// Host tool that closes a lane the lead decided is finished.
pub const LANE_CLOSE_TOOL: &str = "lane_close";
/// Host tool that stops a running lane's current turn.
pub const LANE_STOP_TOOL: &str = "lane_stop";
/// Host tool a lane calls to leave a one-line progress note.
pub const LANE_REPORT_TOOL: &str = "lane_report";

/// A lane tool call had missing or invalid arguments.
pub const E_LANE_ARGS: &str = "E_LANE_ARGS";
/// No lane has the given id.
pub const E_LANE_UNKNOWN: &str = "E_LANE_UNKNOWN";
/// The lane is still running a turn.
pub const E_LANE_BUSY: &str = "E_LANE_BUSY";
/// `MAX_RUNNING_LANES` lanes are already running.
pub const E_LANE_LIMIT: &str = "E_LANE_LIMIT";
/// The lane was running when moraebot stopped or restarted.
pub const E_LANE_INTERRUPTED: &str = "E_LANE_INTERRUPTED";
/// moraebot is shutting down and starts no new lane.
pub const E_LANE_STOPPING: &str = "E_LANE_STOPPING";
/// The lane was closed with `lane_close` and takes no more work.
pub const E_LANE_CLOSED: &str = "E_LANE_CLOSED";
/// The lead stopped the lane's running turn with `lane_stop`.
pub const E_LANE_STOPPED: &str = "E_LANE_STOPPED";
/// The lane has no running turn to stop.
pub const E_LANE_IDLE: &str = "E_LANE_IDLE";

/// `lane_status` cuts each task to this many characters.
const STATUS_TASK_CHARS: usize = 200;

/// How often a running lane job looks for a text `lane_message` steered into its live turn.
/// Short enough that a steer joins the turn promptly, long enough not to spin.
const STEER_POLL: Duration = Duration::from_millis(200);

/// A lane progress note is cut to this many characters.
pub const REPORT_CHARS: usize = 200;

/// A `lane_stop` reason is cut to this many characters.
pub const STOP_REASON_CHARS: usize = 200;

/// Appended to every lane prompt: ask for progress notes.
pub const REPORT_FOOTER: &str = "\n[moraebot] At each big step (a milestone reached, a blocker hit), call lane_report with one short line for the owner. moraebot hands the newest line to the lead later, only while the conversation is quiet, and the lead tells the owner. Keep paths, keys and personal data out of it.";

/// At most this many posted-text hashes are kept per lane; a full set is cleared before the next one.
pub const POSTED_PER_LANE: usize = 64;

/// One progress note to post: the newest unposted report of a running lane job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneNews {
    pub lane_id: i64,
    /// The job (prompt) whose report this is.
    pub seq: i64,
    /// The conversation that started the lane.
    pub conversation_key: String,
    /// Already cleaned by `news_text`.
    pub text: String,
}

impl LaneNews {
    /// The message id of this note's lane inbox row, `<lane>:<seq>:news:<text hash>`: the same
    /// note of the same job is stored once.
    pub fn message_id(&self) -> String {
        format!(
            "{}:{}:news:{:016x}",
            self.lane_id,
            self.seq,
            text_hash(&self.text)
        )
    }
}

/// One registered lane worker: its child's process group once spawned, and its thread.
struct LaneChild {
    kill: Option<GroupKill>,
    worker: JoinHandle<()>,
    /// The prompt (job) this worker runs.
    seq: i64,
    /// When this job's worker started, for the status answer.
    started: Instant,
    /// The lane's conversation, for notices.
    conversation_key: String,
    /// Counted by the child's event callback, read by `watch_tick`.
    activity: Arc<Mutex<Activity>>,
    /// Set once by `stop`: the cleaned reason. A stopped job starts no child or check.
    stop: Option<String>,
    /// The job's steer inbox. It goes away with this entry, which the worker removes under
    /// `inner` after the result is written; what the turn never took is then still in the
    /// worker's receiver and runs as the lane's next prompt.
    steer: SteerSender,
}

/// The running job's steer inbox: `lane_message` sends here while the lane is running, and
/// the job's own thread forwards each text into the live omp turn as a steer.
type SteerSender = mpsc::Sender<SteerText>;

/// One mid-run text handed to the running job.
struct SteerText {
    text: String,
}

/// Whether the turn's first `agent_start` has arrived. A steer may only go out after it: omp
/// admits a steer into a run that is already streaming, and the lead's own steer gate waits
/// for the same signal ([`crate::omp`]'s `note_agent_start`). Set by the job's event callback.
#[derive(Default)]
struct SteerGate {
    started: bool,
}

/// What a running job's omp agent events showed. The event callback only counts;
/// `stall` (called by `watch_tick` with the tick's time) keeps the times.
#[derive(Default)]
struct Activity {
    events: u64,
    /// New tool calls: (name, args) pairs not seen before in this job.
    progress: u64,
    tool_calls: u64,
    calls: HashSet<u64>,
    /// The last agent event was `AutoRetryStart`.
    retrying: bool,
    last_tool: Option<String>,
    seen: Option<Seen>,
    /// Notices sent for this job; the current episode's number once reported.
    episode: u32,
    reported: bool,
    /// The newest `lane_report` text of this job, cleaned.
    report: Option<String>,
}

/// The counters as of the last tick that saw them change, and when.
struct Seen {
    events: u64,
    progress: u64,
    event_at: Instant,
    progress_at: Instant,
}

/// One stall to report.
struct Stall {
    reason: &'static str,
    quiet: Duration,
    idle: Duration,
    episode: u32,
}

impl Activity {
    /// Counts one event of the lane child; `false` for frames that are not agent events.
    fn note(&mut self, event: &Event) -> bool {
        let Event::Notification(RpcNotification::RpcAgentEvent(agent)) = event else {
            return false;
        };
        self.events += 1;
        self.retrying = matches!(agent, RpcAgentEvent::AutoRetryStart(_));
        if let RpcAgentEvent::ToolExecutionStart(start) = agent {
            self.tool_calls += 1;
            self.last_tool = Some(start.tool_name.clone());
            let mut hasher = DefaultHasher::new();
            start.tool_name.hash(&mut hasher);
            start.args.as_ref().map(Value::to_string).hash(&mut hasher);
            if self.calls.insert(hasher.finish()) {
                self.progress += 1;
            }
        }
        true
    }

    /// Updates the times from the counters and returns the stall to report (no event, or no
    /// new tool call, for `after`), once per episode; the first call only starts the clock.
    fn stall(&mut self, now: Instant, after: Duration) -> Option<Stall> {
        let Some(seen) = self.seen.as_mut() else {
            self.seen = Some(Seen {
                events: self.events,
                progress: self.progress,
                event_at: now,
                progress_at: now,
            });
            return None;
        };
        if seen.events != self.events {
            seen.events = self.events;
            seen.event_at = now;
        }
        if seen.progress != self.progress {
            seen.progress = self.progress;
            seen.progress_at = now;
            self.reported = false;
        }
        if self.reported {
            return None;
        }
        let quiet = now.saturating_duration_since(seen.event_at);
        let idle = now.saturating_duration_since(seen.progress_at);
        let reason = if quiet >= after {
            if self.retrying {
                lead::N_LANE_RETRYING
            } else {
                lead::N_LANE_QUIET
            }
        } else if idle >= after {
            lead::N_LANE_SPINNING
        } else {
            return None;
        };
        self.reported = true;
        self.episode += 1;
        Some(Stall {
            reason,
            quiet,
            idle,
            episode: self.episode,
        })
    }
}

#[derive(Default)]
struct LaneSet {
    /// Set by `stop_accepting`: no lane starts and no child is kept from then on.
    stopping: bool,
    children: HashMap<i64, LaneChild>,
    /// Running check and git pids -> `(lane id, seq)` of the job that started it.
    checks: HashMap<u32, (i64, i64)>,
    /// Hashes of the report texts posted per lane with a running job (at most `POSTED_PER_LANE` each).
    posted: HashMap<i64, HashSet<u64>>,
}

/// One lane prompt for a worker thread.
struct Job {
    id: i64,
    seq: i64,
    conversation_key: String,
    text: String,
    previous_session: Option<String>,
    done: Option<DoneLine>,
    /// The canonical git checkout of the lane's worktree; `None` = the shared work folder.
    repo: Option<PathBuf>,
}

/// Where a job's Done checks run (`work`) and write their output (`out`).
struct CheckDirs {
    work: PathBuf,
    out: PathBuf,
    /// Whether `work` is the lane's own worktree, whose git state is checked before the Done
    /// signals.
    git: bool,
}

/// What the lane's `lane_done` calls of one job established; the last call decides.
enum Claim {
    None,
    Failed {
        receipt: String,
    },
    Accepted {
        receipt: String,
    },
    /// A call was refused because moraebot is stopping: the turn is cut, whatever it replies.
    Cut,
}

/// A lane prompt that ran, with its claim state applied.
enum LaneOutcome {
    Plain(Outcome),
    Done {
        text: String,
        receipt: String,
    },
    Unmet {
        text: String,
        receipt: String,
    },
    /// The lead stopped the job and the kill ended it.
    Stopped {
        reason: String,
    },
}

pub struct LaneManager {
    config: Config,
    store: Arc<Mutex<Store>>,
    wake: Arc<Notify>,
    turn: Arc<Mutex<Option<TurnContext>>>,
    pub(crate) turn_timeout: Duration,
    /// A running job with no omp agent event, or no new tool call, for this long is reported
    /// to the lead (`omp.stall_after_minutes`).
    pub(crate) stall_after: Duration,
    /// Kill deadline of one Done check command.
    pub(crate) check_timeout: Duration,
    inner: Mutex<LaneSet>,
    pids: watch::Sender<Vec<u32>>,
    /// Count of agent events received from lane children (tests wait on it).
    events: watch::Sender<u64>,
    /// Count of recorded lane reports (tests wait on it).
    reports: watch::Sender<u64>,
    /// The time of the last watchdog tick; the dispatcher posts lane news after each.
    ticks: watch::Sender<Option<Instant>>,
    pub(crate) extra_env: Vec<(OsString, OsString)>,
    /// Today's date for a job's memory block (expired entries are left out).
    pub(crate) memory_clock: memory::Clock,
    /// Per lane id: the memory block its child was first started with in this process (kept
    /// in memory only, never read back from `append.md`) and the notice last put in a job
    /// prompt about a later difference, so the lane's system prompt and cache stay stable.
    pins: Mutex<HashMap<i64, LanePin>>,
    /// The memory writer that saves failed lane results ([`crate::outcome`]); `None` saves
    /// nothing.
    pub(crate) outcome_memory: Option<memory::MemoryHandle>,
}

/// The memory block a lane is pinned to and what the lane was last told about it.
struct LanePin {
    block: String,
    /// The update block the lane took last ([`crate::pin::next_notice`]).
    told: crate::pin::Held,
}

fn refused(code: &str, reason: &str) -> HostToolError {
    format!("{code}: {reason}").into()
}

fn schema(value: Value) -> Map<String, Value> {
    serde_json::from_value(value).expect("lane tool schema is a JSON object")
}

fn text_hash(text: &str) -> u64 {
    let mut h = DefaultHasher::new();
    text.hash(&mut h);
    h.finish()
}

/// The canonical work folder, the root of lane worktrees.
fn lanes_root(config: &Config) -> PathBuf {
    std::fs::canonicalize(&config.work_dir)
        .or_else(|_| std::path::absolute(&config.work_dir))
        .unwrap_or_else(|_| config.work_dir.clone())
}

/// `raw` made safe to show the owner with [`news_text`], with the paths a `lane_report` note
/// loses: the lanes root and the work folder become `.`, `$HOME` (real and canonical) `~`.
pub fn clean_note(raw: &str, config: &Config) -> String {
    let root = lanes_root(config);
    let work = std::path::absolute(&config.work_dir).ok();
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let home_real = home.as_deref().and_then(|h| std::fs::canonicalize(h).ok());
    let mut paths = vec![(root.as_path(), ".")];
    paths.extend(work.as_deref().map(|p| (p, ".")));
    paths.extend(home_real.as_deref().map(|p| (p, "~")));
    paths.extend(home.as_deref().map(|p| (p, "~")));
    news_text(raw, &paths)
}

/// A lane's progress note made safe to post: secrets become `[secret]`, each path in `paths`
/// becomes its mark (in order), `@` becomes U+FF20 so no mention fires, whitespace runs become
/// one space, and the result is cut to `REPORT_CHARS` characters.
pub fn news_text(raw: &str, paths: &[(&Path, &str)]) -> String {
    let mut rest = raw;
    let mut text = String::new();
    while let Some(found) = secret::find_secret(rest) {
        text.push_str(&rest[..found.start]);
        text.push_str("[secret]");
        rest = &rest[found.end..];
    }
    text.push_str(rest);

    for &(path, mark) in paths {
        if !path.is_absolute() || path.parent().is_none() {
            continue;
        }
        let path = path.to_string_lossy();
        let path = path.trim_end_matches('/');
        let mut out = String::new();
        let mut last = 0;
        for (at, _) in text.match_indices(path) {
            let end = at + path.len();
            if text[end..]
                .chars()
                .next()
                .is_none_or(|c| c == '/' || c.is_whitespace() || matches!(c, '"' | '\'' | '`'))
            {
                out.push_str(&text[last..at]);
                out.push_str(mark);
                last = end;
            }
        }
        out.push_str(&text[last..]);
        text = out;
    }

    text = text.replace('@', "\u{FF20}");
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    text.chars().take(REPORT_CHARS).collect()
}

impl LaneManager {
    /// `turn` is the lead's turn context; `turn_timeout` the configured turn limit.
    pub(crate) fn new(
        config: Config,
        store: Arc<Mutex<Store>>,
        turn: Arc<Mutex<Option<TurnContext>>>,
        turn_timeout: Duration,
    ) -> Self {
        Self {
            stall_after: config.omp.stall_after(),
            config,
            store,
            wake: Arc::default(),
            turn,
            turn_timeout,
            check_timeout: done::CHECK_TIMEOUT,
            inner: Mutex::default(),
            pids: watch::channel(Vec::new()).0,
            events: watch::channel(0).0,
            reports: watch::channel(0).0,
            ticks: watch::channel(None).0,
            extra_env: Vec::new(),
            memory_clock: memory::system_clock(),
            pins: Mutex::default(),
            outcome_memory: None,
        }
    }

    /// Notified after a lane result row was stored.
    pub(crate) fn wake(&self) -> Arc<Notify> {
        Arc::clone(&self.wake)
    }

    /// Pids of lane children and of Done check commands that are spawned and not yet
    /// reaped.
    pub fn running_pids(&self) -> watch::Receiver<Vec<u32>> {
        self.pids.subscribe()
    }

    /// Agent events received from lane children since start; a test waits on it before it
    /// ticks.
    pub fn lane_events(&self) -> watch::Receiver<u64> {
        self.events.subscribe()
    }

    /// Lane reports recorded since start; a test waits on it before it reads `news`.
    pub fn lane_reports(&self) -> watch::Receiver<u64> {
        self.reports.subscribe()
    }

    /// Changes after every watchdog tick, carrying that tick's time.
    pub fn watch_ticks(&self) -> watch::Receiver<Option<Instant>> {
        self.ticks.subscribe()
    }

    /// The newest report of each running job that was not posted for its lane yet, in lane id
    /// order; posted hashes of lanes without a running job are forgotten first.
    pub fn news(&self) -> Vec<LaneNews> {
        let mut guard = lock(&self.inner);
        let set = &mut *guard;
        set.posted.retain(|id, _| set.children.contains_key(id));
        let mut news: Vec<LaneNews> = set
            .children
            .iter()
            .filter_map(|(id, child)| {
                let text = lock(&child.activity).report.clone()?;
                if set
                    .posted
                    .get(id)
                    .is_some_and(|p| p.contains(&text_hash(&text)))
                {
                    return None;
                }
                Some(LaneNews {
                    lane_id: *id,
                    seq: child.seq,
                    conversation_key: child.conversation_key.clone(),
                    text,
                })
            })
            .collect();
        news.sort_by_key(|n| n.lane_id);
        news
    }

    /// Records that `text` was posted for `lane_id`; `news` never lists it again for that lane.
    pub fn news_posted(&self, lane_id: i64, text: &str) {
        let mut set = lock(&self.inner);
        let posted = set.posted.entry(lane_id).or_default();
        if posted.len() >= POSTED_PER_LANE {
            posted.clear();
        }
        posted.insert(text_hash(text));
    }

    /// When the current job of each running lane started, by lane id.
    pub fn running_since(&self) -> HashMap<i64, Instant> {
        lock(&self.inner)
            .children
            .iter()
            .map(|(id, child)| (*id, child.started))
            .collect()
    }

    /// Counts `event` of a lane child in its job's `activity`.
    fn note_event(&self, activity: &Mutex<Activity>, gate: &Mutex<SteerGate>, event: &Event) {
        let agent = lock(activity).note(event);
        if agent {
            self.events.send_modify(|n| *n += 1);
        }
        // A steer may only join a run that is already streaming, so the gate opens on the
        // turn's first `agent_start`, exactly as the lead's steer gate does.
        if omp::opens_steer_gate(event) {
            lock(gate).started = true;
        }
    }

    /// One watchdog tick at `now`: queues a notice for each running job that looks stalled
    /// and wakes the dispatcher; returns the number of notices written. Off when the turn
    /// limit is `stall_after` or less and while stopping.
    pub fn watch_tick(&self, now: Instant) -> usize {
        if self.turn_timeout <= self.stall_after {
            return 0;
        }
        let mut written = 0;
        {
            let set = lock(&self.inner);
            if set.stopping {
                return 0;
            }
            for (id, child) in &set.children {
                let (stall, text) = {
                    let mut activity = lock(&child.activity);
                    let Some(stall) = activity.stall(now, self.stall_after) else {
                        continue;
                    };
                    let text = lead::lane_stalled(
                        &child.conversation_key,
                        *id,
                        &Stalled {
                            reason: stall.reason,
                            quiet_minutes: stall.quiet.as_secs() / 60,
                            idle_minutes: stall.idle.as_secs() / 60,
                            tool_calls: activity.tool_calls,
                            new_calls: activity.progress,
                            last_tool: activity.last_tool.as_deref(),
                        },
                    );
                    (stall, text)
                };
                match lock(&self.store).stall_notice(*id, child.seq, stall.episode, &text) {
                    Ok(true) => {
                        written += 1;
                        tracing::warn!(
                            code = omp::W_TURN_STALLED,
                            lane = id,
                            seq = child.seq,
                            reason = stall.reason,
                            episode = stall.episode,
                            "lane looks stalled"
                        );
                    }
                    Ok(false) => {}
                    Err(e) => {
                        tracing::error!(
                            lane = id,
                            code = e.code(),
                            "could not store a stalled-lane notice"
                        );
                    }
                }
            }
        }
        if written > 0 {
            self.wake.notify_one();
        }
        written
    }

    /// Runs `watch_tick` every `WATCH_TICK` (first tick at once) until the task is aborted.
    pub async fn watch(&self) {
        let mut tick = tokio::time::interval(WATCH_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let now = tokio::time::Instant::now().into_std();
            self.watch_tick(now);
            self.ticks.send_replace(Some(now));
        }
    }

    /// From now on `lane_start` and `lane_message` refuse with `E_LANE_STOPPING`.
    pub fn stop_accepting(&self) {
        lock(&self.inner).stopping = true;
    }

    /// Stops lane `id`'s running job for the lead: marks it stopped with the cleaned `reason`
    /// (one line, at most `STOP_REASON_CHARS` characters), kills its child's process group and
    /// every check it started, and returns at once: `true`, or `false` when the job was already
    /// stopped (nothing is killed again). The worker writes the result. Refuses with the code
    /// `E_LANE_ARGS`, `E_LANE_STOPPING`, `E_LANE_UNKNOWN`, `E_LANE_CLOSED` or `E_LANE_IDLE`.
    pub fn stop(&self, id: i64, reason: &str) -> Result<bool, &'static str> {
        let reason: String = reason
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(STOP_REASON_CHARS)
            .collect();
        if reason.is_empty() {
            return Err(E_LANE_ARGS);
        }
        let mut guard = lock(&self.inner);
        let set = &mut *guard;
        if set.stopping {
            return Err(E_LANE_STOPPING);
        }
        if let Some(child) = set.children.get_mut(&id) {
            if child.stop.is_some() {
                return Ok(false);
            }
            child.stop = Some(reason);
            if let Some(group) = &child.kill {
                group.kill();
            }
            let seq = child.seq;
            for (pid, owner) in &set.checks {
                if *owner == (id, seq) {
                    omp::kill_group(*pid);
                }
            }
            tracing::info!(lane = id, seq, "lane stop requested");
            return Ok(true);
        }
        // Still under `inner` (lock order inner -> store): no job can start meanwhile.
        match lock(&self.store).lane(id) {
            Ok(None) => Err(E_LANE_UNKNOWN),
            Ok(Some(lane)) if lane.closed => Err(E_LANE_CLOSED),
            Ok(Some(_)) => Err(E_LANE_IDLE),
            Err(e) => Err(e.code()),
        }
    }

    /// Stops accepting, then kills every lane child (including one still starting), then
    /// every running check, waits until each killed check was reaped, and joins each
    /// worker; every wait is bounded by `ABORT_JOIN_BOUND`. A lane the kill ended keeps
    /// its `running` row for `recover_on_start`.
    pub async fn shutdown_all(&self) {
        let mut pids = self.pids.subscribe();
        let (children, checks): (Vec<(i64, LaneChild)>, Vec<u32>) = {
            let mut set = lock(&self.inner);
            set.stopping = true;
            (
                set.children.drain().collect(),
                set.checks.keys().copied().collect(),
            )
        };
        for (_, child) in &children {
            if let Some(group) = &child.kill {
                group.kill();
            }
        }
        // Children first: a check's refusal can never reach a live lane.
        for pid in &checks {
            omp::kill_group(*pid);
        }
        if !checks.is_empty() {
            let reaped = pids.wait_for(|running| !running.iter().any(|p| checks.contains(p)));
            if !matches!(
                tokio::time::timeout(ABORT_JOIN_BOUND, reaped).await,
                Ok(Ok(_))
            ) {
                tracing::error!(checks = checks.len(), "a done check did not stop in time");
            }
        }
        for (id, LaneChild { kill, worker, .. }) in children {
            let joined = tokio::task::spawn_blocking(move || worker.join());
            match tokio::time::timeout(ABORT_JOIN_BOUND, joined).await {
                Ok(Ok(Ok(()))) => {
                    // The worker reaped its child: never signal that pid again.
                    if let Some(mut group) = kill {
                        group.disarm();
                    }
                }
                Ok(_) => tracing::error!(lane = id, "lane worker thread panicked"),
                Err(_) => tracing::error!(lane = id, "lane worker did not stop in time"),
            }
        }
    }

    /// The five lane tools for the lead child.
    pub fn host_tools(self: &Arc<Self>) -> Vec<HostTool> {
        let start = Arc::clone(self);
        let message = Arc::clone(self);
        let status = Arc::clone(self);
        let close = Arc::clone(self);
        let stop = Arc::clone(self);
        vec![
            HostTool::new(
                LANE_START_TOOL,
                "Start a lane: a separate agent that works on `task` in the background, in \
                 the shared work folder, or with `repo` in its own git worktree. `task` must \
                 be self-contained; the lane does not see \
                 the conversation. `done` is the Done line moraebot checks when the lane \
                 claims it is done. Returns the lane id at once; the lane's result comes \
                 back to you later as a new message.",
                schema(json!({
                    "type": "object",
                    "properties": {
                        "task": {"type": "string", "description": "The self-contained task."},
                        "done": {
                            "type": "string",
                            "description": "Done line: `command [...]` and/or `file <path>`, joined with `;`."
                        },
                        "repo": {
                            "type": "string",
                            "description": "Optional: a git checkout (absolute, ~/..., or relative to the work folder). The lane then works in its own worktree of it."
                        }
                    },
                    "required": ["task", "done"],
                    "additionalProperties": false
                })),
                move |arguments, ctx| start.lane_start(&arguments, ctx.tool_call_id()),
            )
            .load_mode(ToolLoadMode::Essential),
            HostTool::new(
                LANE_MESSAGE_TOOL,
                "Send more work to a lane. A running lane takes the text as a steer: omp folds it \
                 into the turn already in progress and the lane answers it in that same result; a \
                 text that turn can no longer take runs as the lane's next prompt. An idle, failed \
                 or stopped lane starts a new prompt instead. Either way the answer comes back to \
                 you later as a new message.",
                schema(json!({
                    "type": "object",
                    "properties": {
                        "lane_id": {"type": "integer"},
                        "text": {"type": "string"}
                    },
                    "required": ["lane_id", "text"],
                    "additionalProperties": false
                })),
                move |arguments, _| message.lane_message(&arguments),
            )
            .load_mode(ToolLoadMode::Essential),
            HostTool::new(
                LANE_STATUS_TOOL,
                "List lanes with their state, or one lane when `lane_id` is given.",
                schema(json!({
                    "type": "object",
                    "properties": {
                        "lane_id": {"type": "integer"}
                    },
                    "additionalProperties": false
                })),
                move |arguments, _| status.lane_status(&arguments),
            )
            .load_mode(ToolLoadMode::Essential),
            HostTool::new(
                LANE_CLOSE_TOOL,
                "Close a lane you decided is finished or no longer needed. A running lane refuses; a \
                 closed lane takes no more lane_message. For a lane with `repo`, moraebot removes its \
                 worktree folder only when every change is committed and pushed (with no remote, \
                 committed) and git can remove it cleanly; otherwise the folder is kept and the result \
                 lists codes. The branch always stays. Calling it again on a closed lane retries the \
                 folder cleanup.",
                schema(json!({
                    "type": "object",
                    "properties": {
                        "lane_id": {"type": "integer"},
                        "reason": {"type": "string", "description": "Why the lane is closed, in a few words."}
                    },
                    "required": ["lane_id", "reason"],
                    "additionalProperties": false
                })),
                move |arguments, ctx| close.lane_close(&arguments, ctx),
            )
            .load_mode(ToolLoadMode::Essential),
            HostTool::new(
                LANE_STOP_TOOL,
                "Stop a running lane's current turn at once: moraebot ends the lane's agent; files it \
                 already wrote stay, work in progress may be lost. Returns at once; the lane's result \
                 comes back to you later as failed code=E_LANE_STOPPED. The lane stays open: give it \
                 more work with lane_message after that result, or close it with lane_close. A lane \
                 that is not running refuses.",
                schema(json!({
                    "type": "object",
                    "properties": {
                        "lane_id": {"type": "integer"},
                        "reason": {"type": "string", "description": "Why the lane is stopped, in a few words."}
                    },
                    "required": ["lane_id", "reason"],
                    "additionalProperties": false
                })),
                move |arguments, _| stop.lane_stop(&arguments),
            )
            .load_mode(ToolLoadMode::Essential),
        ]
    }

    fn lane_start(
        self: &Arc<Self>,
        arguments: &Map<String, Value>,
        tool_call_id: &str,
    ) -> Result<omp_rpc::HostToolOutput, HostToolError> {
        let task = arguments
            .get("task")
            .and_then(Value::as_str)
            .filter(|task| !task.trim().is_empty())
            .ok_or_else(|| refused(E_LANE_ARGS, "task must be a non-empty string"))?;
        let line = arguments
            .get("done")
            .and_then(Value::as_str)
            .filter(|line| !line.trim().is_empty())
            .ok_or_else(|| refused(E_LANE_ARGS, "done must be a non-empty string"))?;
        let line = DoneLine::parse(line)
            .map_err(|error| refused(done::E_DONE_SYNTAX, &error.to_string()))?;
        let canonical = line.to_string();
        let repo = self.repo_arg(arguments)?;
        let repo_text = repo
            .as_deref()
            .map(|repo| repo.to_string_lossy().into_owned());
        let conversation_key = lock(&self.turn)
            .as_ref()
            .map(|turn| turn.conversation_key.clone())
            .ok_or_else(|| refused(E_LANE_ARGS, "no active turn"))?;
        let mut set = lock(&self.inner);
        if set.stopping {
            return Err(refused(E_LANE_STOPPING, "the gateway is stopping"));
        }
        let started = lock(&self.store)
            .start_lane(
                tool_call_id,
                &conversation_key,
                task,
                Some(&canonical),
                repo_text.as_deref(),
                MAX_RUNNING_LANES,
            )
            .map_err(|e| refused(e.code(), &e.to_string()))?;
        let id = match started {
            LaneStart::New(id) => {
                self.spawn_worker(
                    &mut set,
                    Job {
                        id,
                        seq: 1,
                        conversation_key,
                        text: task.to_owned(),
                        previous_session: None,
                        done: Some(line),
                        repo,
                    },
                );
                id
            }
            LaneStart::Existing(id) => id,
            LaneStart::Limit => {
                return Err(refused(
                    E_LANE_LIMIT,
                    &format!("{MAX_RUNNING_LANES} lanes are already running"),
                ));
            }
        };
        Ok(format!(
            "{}\nThe lane's result comes back to you as a new message.",
            json!({ "lane_id": id })
        )
        .into())
    }

    /// `lane_start`'s optional `repo`: missing or `null` is `None`; otherwise a non-empty
    /// string naming an existing folder (`~/` from `HOME`, absolute, else relative to the
    /// work folder), returned canonical.
    fn repo_arg(&self, arguments: &Map<String, Value>) -> Result<Option<PathBuf>, HostToolError> {
        let repo = match arguments.get("repo") {
            None | Some(Value::Null) => return Ok(None),
            Some(value) => value
                .as_str()
                .filter(|repo| !repo.trim().is_empty())
                .ok_or_else(|| refused(E_LANE_ARGS, "repo must be a non-empty string"))?,
        };
        let not_a_folder = || refused(worktree::E_LANE_REPO, "repo is not an existing folder");
        let path = match repo.strip_prefix("~/") {
            Some(rest) => {
                PathBuf::from(std::env::var_os("HOME").ok_or_else(not_a_folder)?).join(rest)
            }
            None if Path::new(repo).is_absolute() => PathBuf::from(repo),
            None => self.config.work_dir.join(repo),
        };
        let canonical = std::fs::canonicalize(path).map_err(|_| not_a_folder())?;
        if !canonical.is_dir() {
            return Err(not_a_folder());
        }
        Ok(Some(canonical))
    }

    fn lane_message(
        self: &Arc<Self>,
        arguments: &Map<String, Value>,
    ) -> Result<omp_rpc::HostToolOutput, HostToolError> {
        let id = arguments
            .get("lane_id")
            .and_then(Value::as_i64)
            .ok_or_else(|| refused(E_LANE_ARGS, "lane_id must be an integer"))?;
        let text = arguments
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .ok_or_else(|| refused(E_LANE_ARGS, "text must be a non-empty string"))?;
        let mut set = lock(&self.inner);
        if set.stopping {
            return Err(refused(E_LANE_STOPPING, "the gateway is stopping"));
        }
        // A running lane takes the text into its steer inbox. The worker holds the receiver
        // until it removes this entry under `inner`, so the text either joins the live turn or
        // runs as the lane's next prompt; it is never dropped and never runs twice.
        if let Some(child) = set.children.get(&id).filter(|child| child.stop.is_none())
            && child
                .steer
                .send(SteerText {
                    text: text.to_owned(),
                })
                .is_ok()
        {
            tracing::info!(lane = id, "steered into the running lane");
            return Ok(json!({ "lane_id": id, "accepted": true, "steered": true })
                .to_string()
                .into());
        }
        let job = self.next_job(id, text)?;
        self.spawn_worker(&mut set, job);
        Ok(json!({ "lane_id": id, "accepted": true })
            .to_string()
            .into())
    }

    /// The next prompt of lane `id` with `text`, numbered in the store, under the caller's
    /// hold of `inner` (lock order inner -> store). Refuses like `lane_message`.
    fn next_job(&self, id: i64, text: &str) -> Result<Job, HostToolError> {
        let mut store = lock(&self.store);
        let store_err = |e: crate::store::StoreError| refused(e.code(), &e.to_string());
        let lane = store
            .lane(id)
            .map_err(store_err)?
            .ok_or_else(|| refused(E_LANE_UNKNOWN, "no lane has this id"))?;
        let line = lane
            .done
            .as_deref()
            .map(DoneLine::parse)
            .transpose()
            .map_err(|error| refused(done::E_DONE_SYNTAX, &error.to_string()))?;
        match store
            .message_lane(id, MAX_RUNNING_LANES)
            .map_err(store_err)?
        {
            LaneMessage::Seq(seq) => Ok(Job {
                id,
                seq,
                conversation_key: lane.conversation_key,
                text: text.to_owned(),
                previous_session: lane.omp_session_id,
                done: line,
                repo: lane.repo.map(PathBuf::from),
            }),
            LaneMessage::Unknown => Err(refused(E_LANE_UNKNOWN, "no lane has this id")),
            LaneMessage::Busy => Err(refused(
                E_LANE_BUSY,
                "the lane is still running; wait for its result",
            )),
            LaneMessage::Limit => Err(refused(
                E_LANE_LIMIT,
                &format!("{MAX_RUNNING_LANES} lanes are already running"),
            )),
            LaneMessage::Closed => Err(refused(E_LANE_CLOSED, "the lane is closed")),
        }
    }

    fn lane_status(
        &self,
        arguments: &Map<String, Value>,
    ) -> Result<omp_rpc::HostToolOutput, HostToolError> {
        let wanted = match arguments.get("lane_id") {
            None | Some(Value::Null) => None,
            Some(value) => Some(
                value
                    .as_i64()
                    .ok_or_else(|| refused(E_LANE_ARGS, "lane_id must be an integer"))?,
            ),
        };
        let lanes = lock(&self.store)
            .lanes()
            .map_err(|e| refused(e.code(), &e.to_string()))?;
        let listed: Vec<Value> = lanes
            .into_iter()
            .filter(|lane| wanted.is_none_or(|id| lane.id == id))
            .map(|lane| {
                json!({
                    "lane_id": lane.id,
                    "conversation": lane.conversation_key,
                    "state": lane.state.as_str(),
                    "error": lane.error,
                    "prompts": lane.prompts,
                    "done": lane.done,
                    "repo": lane.repo,
                    "task": lane.task.chars().take(STATUS_TASK_CHARS).collect::<String>(),
                    "closed": lane.closed,
                    "close_reason": lane.close_reason,
                })
            })
            .collect();
        if wanted.is_some() && listed.is_empty() {
            return Err(refused(E_LANE_UNKNOWN, "no lane has this id"));
        }
        Ok(Value::Array(listed).to_string().into())
    }

    /// Starts the worker thread for `job` and registers it, under the caller's hold of
    /// `inner`. When no thread can be started, the prompt is reported as failed.
    fn spawn_worker(self: &Arc<Self>, set: &mut LaneSet, job: Job) {
        let (id, seq) = (job.id, job.seq);
        let key = job.conversation_key.clone();
        let manager = Arc::clone(self);
        let activity = Arc::new(Mutex::new(Activity::default()));
        let worker_activity = Arc::clone(&activity);
        // `lane_message` sends a mid-run text here; the worker forwards it into the live turn,
        // or runs what the turn never took as the lane's next prompt.
        let (steer_tx, steer_rx) = mpsc::channel::<SteerText>();
        match std::thread::Builder::new()
            .name(format!("omp-lane-{id}"))
            .spawn(move || manager.run_worker(job, worker_activity, steer_rx))
        {
            Ok(worker) => {
                set.children.insert(
                    id,
                    LaneChild {
                        kill: None,
                        worker,
                        seq,
                        started: Instant::now(),
                        conversation_key: key.clone(),
                        activity,
                        stop: None,
                        steer: steer_tx,
                    },
                );
                tracing::info!(lane = id, seq, "lane started");
            }
            Err(e) => {
                tracing::warn!(lane = id, seq, error = %e, "could not start a lane worker");
                let outcome = Outcome::Failed {
                    code: E_OMP_SPAWN,
                    detail: e.to_string(),
                };
                if self.write_result(set, id, seq, &key, Err(outcome)) {
                    self.wake.notify_one();
                }
            }
        }
    }

    /// One lane prompt: open the child, send the prompt, map the result, close the
    /// child; then the final section under `inner`.
    fn run_worker(
        self: Arc<Self>,
        job: Job,
        activity: Arc<Mutex<Activity>>,
        steer_rx: mpsc::Receiver<SteerText>,
    ) {
        let mut pid = None;
        let mut untaken = Vec::new();
        let outcome = self.run_prompt(&job, &mut pid, &activity, &steer_rx, &mut untaken);
        let written = {
            let mut set = lock(&self.inner);
            let written = self.write_result(&set, job.id, job.seq, &job.conversation_key, outcome);
            // A check outlives no job: `close` does not join a running tool handler.
            for (check, owner) in &set.checks {
                if *owner == (job.id, job.seq) {
                    omp::kill_group(*check);
                }
            }
            let mut stopped = false;
            if let Some(mut child) = set.children.remove(&job.id) {
                stopped = child.stop.is_some();
                if let Some(group) = child.kill.as_mut() {
                    // The child was reaped by `close` above.
                    group.disarm();
                }
            }
            if let Some(pid) = pid {
                self.pids.send_modify(|pids| pids.retain(|p| *p != pid));
            }
            // The inbox went with the entry and `lane_message` sends only under `inner`, so
            // nothing more can arrive: these are all the texts the turn never took.
            untaken.extend(steer_rx.try_iter().map(|steer| steer.text));
            if !untaken.is_empty() {
                self.run_untaken(&mut set, job.id, untaken, written && !stopped);
            }
            written
        };
        if written {
            self.wake.notify_one();
        }
    }

    /// Runs the `lane_message` texts lane `id`'s finished job never took (they came before its
    /// turn could take a steer, or after it ended), joined in arrival order, as the lane's next
    /// prompt, the way `lane_message` starts one on an idle lane. A job the lead stopped, one
    /// the shutdown ended (`settled` false) or a refused next prompt starts nothing; the texts
    /// are then logged by count and size only.
    fn run_untaken(
        self: &Arc<Self>,
        set: &mut LaneSet,
        id: i64,
        texts: Vec<String>,
        settled: bool,
    ) {
        let (count, chars) = (
            texts.len(),
            texts.iter().map(|t| t.chars().count()).sum::<usize>(),
        );
        if !settled || set.stopping {
            tracing::warn!(
                lane = id,
                count,
                chars,
                "lane_message texts not run: the job was stopped"
            );
            return;
        }
        match self.next_job(id, &texts.join("\n\n")) {
            Ok(job) => {
                tracing::info!(
                    lane = id,
                    count,
                    "lane_message texts the turn never took run as the next prompt"
                );
                self.spawn_worker(set, job);
            }
            Err(error) => {
                tracing::warn!(lane = id, count, chars, %error, "lane_message texts not run");
            }
        }
    }

    /// Registers `pid` (a check or git command) as running for job `(id, seq)`; `false`
    /// when moraebot is stopping, the call was cancelled, the job is no longer current or the
    /// job was stopped.
    fn track(&self, pid: u32, (id, seq): (i64, i64), cancelled: bool) -> bool {
        let mut set = lock(&self.inner);
        let current = set
            .children
            .get(&id)
            .is_some_and(|child| child.seq == seq && child.stop.is_none());
        if set.stopping || cancelled || !current {
            return false;
        }
        set.checks.insert(pid, (id, seq));
        self.pids.send_modify(|pids| pids.push(pid));
        true
    }

    /// Registers `pid` (a `lane_close` git command of lane `id`) under seq 0, which no job
    /// uses; `false` when moraebot is stopping or the call was cancelled.
    fn track_close(&self, pid: u32, id: i64, cancelled: bool) -> bool {
        let mut set = lock(&self.inner);
        if set.stopping || cancelled {
            return false;
        }
        set.checks.insert(pid, (id, 0));
        self.pids.send_modify(|pids| pids.push(pid));
        true
    }

    /// Refuses a management tool unless the running lead turn is the owner's.
    fn owner_turn(&self, action: &str) -> Result<(), HostToolError> {
        let owner = lock(&self.turn)
            .as_ref()
            .is_some_and(|turn| turn.sender == Sender::Owner);
        if owner {
            return Ok(());
        }
        Err(refused(
            E_ACCESS_NOT_OWNER,
            &format!("only the owner may {action}"),
        ))
    }

    /// Closes a lane that is not running, then removes its worktree when nothing would be
    /// lost. Owner turns only. No git command runs while `inner` is held.
    fn lane_close(
        &self,
        arguments: &Map<String, Value>,
        ctx: &HostToolContext,
    ) -> Result<omp_rpc::HostToolOutput, HostToolError> {
        self.owner_turn("close a lane")?;
        let id = arguments
            .get("lane_id")
            .and_then(Value::as_i64)
            .ok_or_else(|| refused(E_LANE_ARGS, "lane_id must be an integer"))?;
        let reason = arguments
            .get("reason")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|reason| !reason.is_empty())
            .ok_or_else(|| refused(E_LANE_ARGS, "reason must be a non-empty string"))?;
        let busy = || {
            refused(
                E_LANE_BUSY,
                "the lane is still running; wait for its result",
            )
        };
        let repo = {
            let set = lock(&self.inner);
            if set.stopping {
                return Err(refused(E_LANE_STOPPING, "the gateway is stopping"));
            }
            if set.children.contains_key(&id) {
                return Err(busy());
            }
            match lock(&self.store)
                .close_lane(id, reason)
                .map_err(|e| refused(e.code(), &e.to_string()))?
            {
                LaneClose::Closed { repo } => {
                    lock(&self.pins).remove(&id);
                    repo
                }
                LaneClose::Unknown => return Err(refused(E_LANE_UNKNOWN, "no lane has this id")),
                LaneClose::Busy => return Err(busy()),
            }
        };
        // The prompt file holds memory text (boundaries, project notes). A closed lane never
        // starts again, so the file goes now and an entry the owner forgets later does not
        // stay readable in it.
        let append = omp::lane_append_path(&self.config, id);
        if let Err(err) = std::fs::remove_file(&append)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(lane = id, error = %err, "lane prompt file not removed");
        }
        let (word, codes, detail, branch) = match repo {
            None => ("none", Vec::new(), None, None),
            Some(repo) => {
                let folder = worktree::folder(&self.lanes_root(), id);
                let git_dir = self
                    .config
                    .home
                    .join("lanes")
                    .join(id.to_string())
                    .join("git");
                let cleanup = match omp::private_dir(&git_dir) {
                    Err(e) => Cleanup::Kept {
                        codes: vec![worktree::E_LANE_WORKTREE],
                        detail: done::short(&e.to_string()),
                    },
                    Ok(()) => {
                        let env = omp::check_env(&self.extra_env);
                        let opts = done::RunOpts {
                            out_dir: &git_dir,
                            env: &env,
                            timeout: worktree::GIT_TIMEOUT,
                        };
                        worktree::remove(
                            Path::new(&repo),
                            &folder,
                            &opts,
                            |pid| self.track_close(pid, id, ctx.is_cancelled()),
                            |pid| self.untrack(pid),
                        )
                        .unwrap_or_else(|done::Stopped| Cleanup::Kept {
                            codes: vec![E_LANE_STOPPING],
                            detail: "the gateway is stopping".to_owned(),
                        })
                    }
                };
                let branch = Some(worktree::branch(id));
                match cleanup {
                    Cleanup::Missing => ("missing", Vec::new(), None, branch),
                    Cleanup::Removed => ("removed", Vec::new(), None, branch),
                    Cleanup::Kept { codes, detail } => ("kept", codes, Some(detail), branch),
                }
            }
        };
        tracing::info!(lane = id, worktree = word, codes = ?codes, "lane closed");
        let mut result = json!({ "lane_id": id, "closed": true, "worktree": word });
        if let Some(branch) = branch {
            result["branch"] = json!(branch);
        }
        if let Some(detail) = detail {
            result["codes"] = json!(codes);
            result["detail"] = json!(detail);
        }
        Ok(result.to_string().into())
    }

    /// Handles one `lane_stop` call: checks the turn's sender and the arguments and calls
    /// `stop`; never waits.
    fn lane_stop(
        &self,
        arguments: &Map<String, Value>,
    ) -> Result<omp_rpc::HostToolOutput, HostToolError> {
        self.owner_turn("stop a lane")?;
        let id = arguments
            .get("lane_id")
            .and_then(Value::as_i64)
            .ok_or_else(|| refused(E_LANE_ARGS, "lane_id must be an integer"))?;
        let reason = arguments
            .get("reason")
            .and_then(Value::as_str)
            .ok_or_else(|| refused(E_LANE_ARGS, "reason must be a non-empty string"))?;
        self.stop(id, reason).map_err(|code| {
            let why = match code {
                E_LANE_ARGS => "reason must be a non-empty string",
                E_LANE_STOPPING => "the gateway is stopping",
                E_LANE_UNKNOWN => "no lane has this id",
                E_LANE_CLOSED => "the lane is closed",
                E_LANE_IDLE => "the lane is not running; its result already came",
                _ => "could not read the lane",
            };
            refused(code, why)
        })?;
        Ok(json!({ "lane_id": id, "stopping": true })
            .to_string()
            .into())
    }

    /// Forgets `pid` once its command was reaped.
    fn untrack(&self, pid: u32) {
        let mut set = lock(&self.inner);
        set.checks.remove(&pid);
        self.pids.send_modify(|pids| pids.retain(|p| *p != pid));
    }

    /// `<work dir>` canonical (else absolute, else as given): the base of lane worktrees.
    fn lanes_root(&self) -> PathBuf {
        lanes_root(&self.config)
    }

    /// The folder job `job` works in: the work folder, or for a lane with a repo its
    /// worktree, made sure of by `worktree::ensure` before any child starts.
    fn job_cwd(&self, job: &Job) -> Result<PathBuf, Outcome> {
        let Some(repo) = &job.repo else {
            return Ok(self.config.work_dir.clone());
        };
        let (id, seq) = (job.id, job.seq);
        let folder = worktree::folder(&self.lanes_root(), id);
        let git_dir = self
            .config
            .home
            .join("lanes")
            .join(id.to_string())
            .join("git");
        let fail = |code: &'static str, detail: String| {
            tracing::warn!(lane = id, seq, code, "lane worktree is not ready");
            Outcome::Failed { code, detail }
        };
        omp::private_dir(&git_dir).map_err(|e| fail(worktree::E_LANE_WORKTREE, e.to_string()))?;
        let env = omp::check_env(&self.extra_env);
        let opts = done::RunOpts {
            out_dir: &git_dir,
            env: &env,
            timeout: worktree::GIT_TIMEOUT,
        };
        match worktree::ensure(
            repo,
            &folder,
            id,
            &opts,
            |pid| self.track(pid, (id, seq), false),
            |pid| self.untrack(pid),
        ) {
            Ok(()) => Ok(folder),
            Err(WorktreeError::Failed { code, detail }) => Err(fail(code, detail)),
            Err(WorktreeError::Stopped) => {
                Err(fail(E_LANE_STOPPING, "the gateway is stopping".to_owned()))
            }
        }
    }

    /// The `lane_done` tool of one job: its handler checks `line` in `work_dir` and
    /// records the claim.
    fn lane_done_tool(
        self: &Arc<Self>,
        job: &Job,
        line: &DoneLine,
        claim: &Arc<Mutex<Claim>>,
        work_dir: &Path,
    ) -> HostTool {
        let (id, seq) = (job.id, job.seq);
        let check_dir = self
            .config
            .home
            .join("lanes")
            .join(id.to_string())
            .join("check")
            .join(seq.to_string());
        if omp::private_dir(&check_dir).is_err() {
            tracing::warn!(
                lane = id,
                code = done::E_CHECK_SPAWN,
                "could not create the lane's check folder"
            );
        }
        let dirs = CheckDirs {
            work: work_dir.to_path_buf(),
            out: check_dir,
            git: job.repo.is_some(),
        };
        let manager = Arc::clone(self);
        let line = line.clone();
        let claim = Arc::clone(claim);
        HostTool::new(
            done::LANE_DONE_TOOL,
            "Claim that your work is done. moraebot runs this lane's Done checks now; a \
             refusal lists what still fails.",
            schema(json!({
                "type": "object",
                "properties": {
                    "evidence": {
                        "type": "string",
                        "description": "What you did and how you checked it."
                    }
                },
                "required": ["evidence"],
                "additionalProperties": false
            })),
            move |arguments, ctx| {
                manager.lane_done(&arguments, ctx, (id, seq), &line, &dirs, &claim)
            },
        )
        .load_mode(ToolLoadMode::Essential)
    }

    /// Handles one `lane_done` call on its own client thread. The claim lock is held for
    /// the whole call, so parallel calls of one lane run one after another.
    fn lane_done(
        &self,
        arguments: &Map<String, Value>,
        ctx: &HostToolContext,
        (id, seq): (i64, i64),
        line: &DoneLine,
        dirs: &CheckDirs,
        claim: &Mutex<Claim>,
    ) -> Result<omp_rpc::HostToolOutput, HostToolError> {
        let evidence = arguments
            .get("evidence")
            .and_then(Value::as_str)
            .filter(|evidence| !evidence.trim().is_empty())
            .ok_or_else(|| refused(E_LANE_ARGS, "evidence must be a non-empty string"))?;
        let mut claim = lock(claim);
        let Ok(checks) = self.claim_checks((id, seq), ctx, line, dirs) else {
            // `stopping` never resets, so `write_result` leaves this job for recovery.
            if lock(&self.inner).stopping {
                *claim = Claim::Cut;
            }
            return Err(refused(E_LANE_STOPPING, "the gateway is stopping"));
        };
        let receipt = done::receipt(evidence, &checks);
        if done::all_passed(&checks) {
            tracing::info!(
                lane = id,
                seq,
                signals = checks.len(),
                "lane done claim accepted"
            );
            *claim = Claim::Accepted { receipt };
            return Ok(format!(
                "{}: every Done check passed. End your turn now with a short summary of \
                 what you did.",
                done::ACCEPTED
            )
            .into());
        }
        let failing: Vec<&str> = checks.iter().filter_map(|check| check.code).collect();
        tracing::info!(
            lane = id,
            seq,
            signals = checks.len(),
            codes = ?failing,
            "lane done claim refused"
        );
        *claim = Claim::Failed { receipt };
        Err(refused(done::E_DONE_UNMET, &done::not_done(&checks)))
    }

    /// Runs one `lane_done` call's git checks (worktree lanes), then its Done checks;
    /// `Stopped` when moraebot is stopping, the call was cancelled or the job was stopped.
    fn claim_checks(
        &self,
        (id, seq): (i64, i64),
        ctx: &HostToolContext,
        line: &DoneLine,
        dirs: &CheckDirs,
    ) -> Result<Vec<done::Check>, done::Stopped> {
        if lock(&self.inner).stopping {
            return Err(done::Stopped);
        }
        let env = omp::check_env(&self.extra_env);
        let mut checks = Vec::new();
        if dirs.git {
            let opts = done::RunOpts {
                out_dir: &dirs.out,
                env: &env,
                timeout: worktree::GIT_TIMEOUT,
            };
            checks = worktree::git_checks(
                &dirs.work,
                &opts,
                |pid| self.track(pid, (id, seq), ctx.is_cancelled()),
                |pid| self.untrack(pid),
            )?;
        }
        checks.extend(done::run(
            line,
            &dirs.work,
            &dirs.out,
            &env,
            self.check_timeout,
            |pid| self.track(pid, (id, seq), ctx.is_cancelled()),
            |pid| self.untrack(pid),
        )?);
        if lock(&self.inner).stopping {
            return Err(done::Stopped);
        }
        Ok(checks)
    }

    /// The `lane_report` tool of one job: its handler records the job's newest report.
    fn lane_report_tool(self: &Arc<Self>, job: &Job, activity: &Arc<Mutex<Activity>>) -> HostTool {
        let (id, seq) = (job.id, job.seq);
        let manager = Arc::clone(self);
        let activity = Arc::clone(activity);
        HostTool::new(
            LANE_REPORT_TOOL,
            "Leave a one-line progress note for the owner at a big step: a milestone reached or \
             a blocker hit. moraebot keeps only your newest note and posts it later in the \
             owner's conversation while it is quiet. Returns at once.",
            schema(json!({
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "One short line."
                    }
                },
                "required": ["text"],
                "additionalProperties": false
            })),
            move |arguments, _| manager.lane_report(&arguments, (id, seq), &activity),
        )
        .load_mode(ToolLoadMode::Essential)
    }

    /// Handles one `lane_report` call: records the cleaned text as the job's newest report.
    /// It never posts and never waits.
    fn lane_report(
        &self,
        arguments: &Map<String, Value>,
        (id, seq): (i64, i64),
        activity: &Mutex<Activity>,
    ) -> Result<omp_rpc::HostToolOutput, HostToolError> {
        let raw = arguments
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let text = clean_note(raw, &self.config);
        if text.is_empty() {
            return Err(refused(E_LANE_ARGS, "text must be a non-empty line"));
        }
        let chars = text.chars().count();
        lock(activity).report = Some(text);
        self.reports.send_modify(|n| *n += 1);
        tracing::info!(lane = id, seq, chars, "lane report recorded");
        Ok(json!({"recorded": true}).to_string().into())
    }

    /// Returns the block to start lane `id` with (pinned on its first job in this process) and
    /// the notice for this job's prompt with what the lane holds once it took it
    /// ([`crate::pin::next_notice`]); the caller commits that with [`Self::pin_told`]. A lane
    /// pinned for a job that `resumes` an earlier session may hold an earlier update, so its
    /// first notice is sent as if what it holds were unknown.
    fn pin_memory(
        &self,
        id: i64,
        current: String,
        resumes: bool,
    ) -> (String, Option<(String, crate::pin::Held)>) {
        let mut pins = lock(&self.pins);
        let pin = pins.entry(id).or_insert_with(|| LanePin {
            block: current.clone(),
            told: if resumes {
                crate::pin::Held::Unknown
            } else {
                crate::pin::Held::Nothing
            },
        });
        let notice = crate::pin::next_notice(&pin.block, &current, pin.told);
        (pin.block.clone(), notice)
    }

    /// Records that lane `id` took the notice [`Self::pin_memory`] returned.
    fn pin_told(&self, id: i64, told: crate::pin::Held) {
        if let Some(pin) = lock(&self.pins).get_mut(&id) {
            pin.told = told;
        }
    }

    /// `Err` is a failure before the prompt was sent (the child did not open). The child's
    /// system prompt gets the lane's pinned memory block ([`Self::pin_memory`]), passed as the
    /// private file `<home>/lanes/<id>/append.md`; a later change goes in the job prompt.
    fn run_prompt(
        self: &Arc<Self>,
        job: &Job,
        pid: &mut Option<u32>,
        activity: &Arc<Mutex<Activity>>,
        steer_rx: &mpsc::Receiver<SteerText>,
        untaken: &mut Vec<String>,
    ) -> Result<LaneOutcome, Outcome> {
        let cwd = self.job_cwd(job)?;
        let name = format!("lane-{}", job.id);
        let session_dir = omp::lane_session_dir(&self.config, job.id);
        let memory_dir = self.config.memory_dir();
        let projects = job
            .repo
            .as_deref()
            .map(|repo| memory::lane_projects(&memory_dir, repo))
            .unwrap_or_default();
        let current = memory::lane_block(&memory_dir, &projects, (self.memory_clock)());
        let (memory, notice) = self.pin_memory(job.id, current, job.previous_session.is_some());
        // Written again before every start: another lane's shell may have edited it.
        crate::omp_overlay::write_overlay(&self.config.home).map_err(|e| Outcome::Failed {
            code: crate::omp_overlay::E_OMP_OVERLAY,
            detail: e.to_string(),
        })?;
        let command = omp::private_dir(&session_dir)
            .and_then(|()| {
                omp::write_append_file(&omp::lane_append_path(&self.config, job.id), &memory)
            })
            .and_then(|append| {
                omp::child_command(
                    &self.config,
                    omp::omp_args(&self.config, &cwd, &session_dir, append.as_deref()),
                    &omp::stderr_log_path(&self.config, &name),
                    &self.extra_env,
                )
            })
            .map_err(|e| Outcome::Failed {
                code: E_OMP_SPAWN,
                detail: e.to_string(),
            })?;
        let claim = Arc::new(Mutex::new(Claim::None));
        // Opens on the turn's first `agent_start`: a steer may only join a streaming run.
        let gate = Arc::new(Mutex::new(SteerGate::default()));
        let mut tools = match &job.done {
            Some(line) => vec![self.lane_done_tool(job, line, &claim, &cwd)],
            None => Vec::new(),
        };
        tools.push(self.lane_report_tool(job, activity));
        let searchable = [memory::Bucket::Boundaries, memory::Bucket::General]
            .into_iter()
            .chain(projects.into_iter().map(memory::Bucket::Project))
            .collect();
        tools.push(crate::memory_tools::lane_search_tool(
            memory_dir,
            Arc::clone(&self.memory_clock),
            searchable,
        ));
        let OpenedChild {
            client, session_id, ..
        } = omp::open_child(
            &name,
            command,
            tools,
            session_dir.to_string_lossy().into_owned(),
            job.previous_session.clone(),
            |group| {
                let mut set = lock(&self.inner);
                if set.stopping {
                    return false;
                }
                let Some(child) = set.children.get_mut(&job.id) else {
                    return false;
                };
                if child.stop.is_some() {
                    return false;
                }
                if let Some(spawned) = group.pid() {
                    *pid = Some(spawned);
                    self.pids.send_modify(|pids| pids.push(spawned));
                }
                child.kill = Some(group);
                true
            },
            {
                let watcher = Arc::clone(self);
                let activity = Arc::clone(activity);
                let gate = Arc::clone(&gate);
                move |event: &Event| watcher.note_event(&activity, &gate, event)
            },
        )?;
        if let Err(e) = lock(&self.store).set_lane_session_id(job.id, &session_id) {
            tracing::warn!(
                lane = job.id,
                code = e.code(),
                "could not store the lane's omp session id"
            );
        }
        let mut message = job.text.clone();
        if let Some((notice, _)) = &notice {
            message += "\n\n";
            message += notice;
        }
        if let Some(line) = &job.done {
            message += &done::lane_footer(line);
        }
        message += REPORT_FOOTER;
        if job.done.is_some() && job.repo.is_some() {
            message += worktree::GIT_FOOTER;
        }
        let prompt = PromptCommand {
            message,
            images: None,
            streaming_behavior: None,
        };
        // The main prompt runs on its own thread so a steer can join the turn already in
        // progress, exactly as `serve_lead` does for the lead's turn: concurrent waits on one
        // client are allowed, and a steer prompt is answered inside the main prompt's result.
        let client = Arc::new(client);
        let main_client = Arc::clone(&client);
        let turn_timeout = self.turn_timeout;
        let (main_tx, main_rx) = mpsc::channel();
        let started = std::thread::Builder::new()
            .name(format!("omp-lane-main-{}", job.id))
            .spawn(move || {
                let turn = main_client.prompt_and_wait(&prompt, turn_timeout);
                // Released before the result is sent: once every wait answered, the job alone
                // holds the client and can close it.
                drop(main_client);
                let _ = main_tx.send(turn);
            });
        let outcome = match started {
            Ok(_) => {
                let (outcome, mut left) =
                    self.drain_steers(&client, steer_rx, &main_rx, &gate, *pid, job.id);
                untaken.append(&mut left);
                // Only a prompt the lane took told it the notice.
                if let Some((_, told)) = notice
                    && !omp::rejected(&outcome)
                {
                    self.pin_told(job.id, told);
                }
                outcome
            }
            Err(e) => Outcome::Failed {
                code: E_OMP_SPAWN,
                detail: e.to_string(),
            },
        };
        let replied = match (&outcome, &job.done) {
            (Outcome::Reply(text), Some(_)) => Some(text.clone()),
            (Outcome::NoAgent, Some(_)) => Some(EMPTY_REPLY.to_owned()),
            _ => None,
        };
        let outcome = match replied {
            None => LaneOutcome::Plain(outcome),
            Some(text) => match &*lock(&claim) {
                Claim::None => LaneOutcome::Plain(outcome),
                Claim::Accepted { receipt } => LaneOutcome::Done {
                    text,
                    receipt: receipt.clone(),
                },
                Claim::Failed { receipt } => LaneOutcome::Unmet {
                    text,
                    receipt: receipt.clone(),
                },
                Claim::Cut => LaneOutcome::Plain(Outcome::NeedsCheck {
                    code: E_LANE_STOPPING,
                    detail: "a lane_done call was refused for the shutdown".to_owned(),
                }),
            },
        };
        match Arc::try_unwrap(client) {
            Ok(client) => omp::close(&name, client),
            // Not expected: every wait drops its clone before it reports, and all were waited for.
            Err(_) => tracing::warn!(lane = job.id, "lane client still shared at close"),
        }
        Ok(outcome)
    }

    /// Waits for the main prompt, forwarding every text `lane_message` sent meanwhile into the
    /// live turn as an omp steer, then waits for each forwarded steer's own result, and returns
    /// the turn's outcome.
    ///
    /// A steer only goes out once the turn's first `agent_start` opened the gate: omp admits a
    /// steer into a run that is already streaming. omp answers a steer inside the main run, or
    /// in a run of its own when it reached omp as that run ended; either way its wait ends with
    /// that run, so every steer is waited for (at most the turn limit after the main prompt,
    /// then the child is killed) and the child never outlives the job. A newer reply replaces
    /// the main one. Texts that never went out (the gate never opened, or no thread could be
    /// started for them) are returned next to it; the worker runs them as the lane's next prompt.
    fn drain_steers(
        &self,
        client: &Arc<omp_rpc::Client>,
        steer_rx: &mpsc::Receiver<SteerText>,
        main_rx: &mpsc::Receiver<Result<omp_rpc::PromptTurn, omp_rpc::Error>>,
        gate: &Mutex<SteerGate>,
        pid: Option<u32>,
        id: i64,
    ) -> (Outcome, Vec<String>) {
        let turn_timeout = self.turn_timeout;
        let mut untaken = Vec::new();
        let (done_tx, done_rx) = mpsc::channel();
        let mut held: Vec<String> = Vec::new();
        let main = loop {
            if lock(gate).started {
                held.extend(steer_rx.try_iter().map(|steer| steer.text));
                for text in std::mem::take(&mut held) {
                    let steer = PromptCommand {
                        message: text.clone(),
                        images: None,
                        streaming_behavior: Some(omp_rpc::StreamingBehavior::Steer),
                    };
                    let steer_client = Arc::clone(client);
                    let done = done_tx.clone();
                    let started = std::thread::Builder::new()
                        .name(format!("omp-lane-steer-{id}"))
                        .spawn(move || {
                            let turn = steer_client.prompt_and_wait(&steer, turn_timeout);
                            drop(steer_client);
                            let _ = done.send(turn);
                        });
                    if let Err(e) = started {
                        tracing::warn!(lane = id, error = %e, "could not start a lane steer");
                        untaken.push(text);
                    }
                }
            }
            match main_rx.recv_timeout(STEER_POLL) {
                Ok(turn) => break turn,
                Err(mpsc::RecvTimeoutError::Disconnected) => break Err(omp_rpc::Error::Closed),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        };
        untaken.append(&mut held);
        // Only the steer threads hold a sender now: the channel closes when the last one reports.
        drop(done_tx);
        let main_timed_out = matches!(main, Err(omp_rpc::Error::Timeout { .. }));
        let mut outcome = omp::map_result(main);
        let kill = |pid: Option<u32>| {
            tracing::warn!(
                lane = id,
                "a lane steer outlived the turn limit; killing the lane child"
            );
            if let Some(pid) = pid {
                omp::kill_group(pid);
            }
        };
        if main_timed_out {
            // Never let the child outlive the turn limit by waiting for a steer.
            kill(pid);
        }
        let deadline = Instant::now() + turn_timeout;
        loop {
            match done_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(turn) => {
                    let steered = omp::map_result(turn);
                    if matches!(outcome, Outcome::Reply(_)) && matches!(steered, Outcome::Reply(_))
                    {
                        outcome = steered;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    kill(pid);
                    // The kill ends every wait still open.
                    done_rx.iter().for_each(drop);
                    outcome = Outcome::NeedsCheck {
                        code: omp::E_TURN_TIMEOUT,
                        detail: "a steer outlived the turn time limit".to_owned(),
                    };
                    break;
                }
            }
        }
        (outcome, untaken)
    }

    /// Stores the lane prompt's result row and the lane's new state, unless the shutdown
    /// kill ended the prompt (then the lane stays `running` for `recover_on_start`). A
    /// prompt that ended on its own is written even while stopping. Returns whether a
    /// row was written.
    /// A stopped job that the kill ended gets the `E_LANE_STOPPED` result.
    fn write_result(
        &self,
        set: &LaneSet,
        id: i64,
        seq: i64,
        conversation_key: &str,
        outcome: Result<LaneOutcome, Outcome>,
    ) -> bool {
        let cut = matches!(
            &outcome,
            Err(_) | Ok(LaneOutcome::Plain(Outcome::NeedsCheck { .. }))
        );
        let killed = set.stopping && cut;
        if killed {
            tracing::info!(
                lane = id,
                seq,
                "lane stopped by shutdown; left for recovery"
            );
            return false;
        }
        let stopped = set
            .children
            .get(&id)
            .filter(|child| child.seq == seq && cut)
            .and_then(|child| child.stop.clone());
        let outcome = match stopped {
            Some(reason) => LaneOutcome::Stopped { reason },
            None => outcome.unwrap_or_else(LaneOutcome::Plain),
        };
        let (result, state, code) = match &outcome {
            LaneOutcome::Plain(plain) => {
                let (result, state) = match plain {
                    Outcome::Reply(text) => (LaneResult::Text(text), LaneState::Idle),
                    Outcome::NoAgent => (LaneResult::Text(EMPTY_REPLY), LaneState::Idle),
                    Outcome::Failed { code, .. } => (LaneResult::Failed(code), LaneState::Failed),
                    Outcome::NeedsCheck { code, .. } => {
                        (LaneResult::Failed(code), LaneState::NeedsCheck)
                    }
                };
                (result, state, plain.code())
            }
            LaneOutcome::Done { text, receipt } => {
                (LaneResult::Done { text, receipt }, LaneState::Idle, None)
            }
            LaneOutcome::Unmet { text, receipt } => (
                LaneResult::Unmet { text, receipt },
                LaneState::Failed,
                Some(done::E_DONE_UNMET),
            ),
            LaneOutcome::Stopped { reason } => (
                LaneResult::Stopped { reason },
                LaneState::Failed,
                Some(E_LANE_STOPPED),
            ),
        };
        let text = lead::lane_result(conversation_key, id, &result);
        let finished = lock(&self.store).finish_lane(id, seq, &text, state, code);
        match finished {
            Ok(written) => {
                tracing::info!(
                    lane = id,
                    seq,
                    state = state.as_str(),
                    code,
                    "lane finished"
                );
                if written {
                    self.remember_outcome(id, seq, &outcome, code);
                }
                written
            }
            Err(e) => {
                tracing::error!(
                    lane = id,
                    seq,
                    code = e.code(),
                    "could not store the lane result"
                );
                false
            }
        }
    }

    /// Saves result `seq` of lane `id` as a reference entry of the lane's project when it
    /// failed with `code` (error, unmet done check or stop); a successful result saves nothing.
    fn remember_outcome(&self, id: i64, seq: i64, outcome: &LaneOutcome, code: Option<&str>) {
        let (Some(memory), Some(code)) = (&self.outcome_memory, code) else {
            return;
        };
        let failure = match outcome {
            LaneOutcome::Plain(
                Outcome::Failed { detail, .. } | Outcome::NeedsCheck { detail, .. },
            ) => outcome::Failure::Error(detail),
            LaneOutcome::Unmet { receipt, .. } => outcome::Failure::Unmet(receipt),
            LaneOutcome::Stopped { reason } => outcome::Failure::Stopped(reason),
            LaneOutcome::Plain(Outcome::Reply(_) | Outcome::NoAgent) | LaneOutcome::Done { .. } => {
                return;
            }
        };
        let lane = match lock(&self.store).lane(id) {
            Ok(Some(lane)) => lane,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(
                    lane = id,
                    code = e.code(),
                    "lane row not read for outcome memory"
                );
                return;
            }
        };
        let projects = lane
            .repo
            .as_deref()
            .map(|repo| memory::lane_projects(&self.config.memory_dir(), Path::new(repo)))
            .unwrap_or_default();
        let entry = outcome::entry(id, seq, &projects, &lane.task, code, &failure);
        if let Err(e) = memory.queue_add(entry) {
            tracing::warn!(
                lane = id,
                seq,
                code = e.code,
                "lane outcome not saved to memory"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_STALL_AFTER as STALL_AFTER;
    use omp_rpc::{
        AgentStartEvent, AutoRetryEndEvent, AutoRetryStartEvent, ToolExecutionStartEvent,
    };

    const TICK: Duration = Duration::from_secs(30);

    fn agent(event: RpcAgentEvent) -> Event {
        Event::Notification(RpcNotification::RpcAgentEvent(event))
    }

    fn agent_start() -> Event {
        agent(RpcAgentEvent::AgentStart(AgentStartEvent {
            resumed_tool_call_ids: None,
        }))
    }

    fn tool_start(name: &str, args: Value) -> Event {
        agent(RpcAgentEvent::ToolExecutionStart(ToolExecutionStartEvent {
            tool_call_id: "call-1".into(),
            tool_name: name.into(),
            args: Some(args),
            intent: None,
        }))
    }

    fn retry_start() -> Event {
        agent(RpcAgentEvent::AutoRetryStart(AutoRetryStartEvent {
            attempt: 1,
            max_attempts: 3,
            delay_ms: 0,
            error_message: "qa".into(),
            error_id: None,
        }))
    }

    fn retry_end() -> Event {
        agent(RpcAgentEvent::AutoRetryEnd(AutoRetryEndEvent {
            success: true,
            attempt: 1,
            final_error: None,
            retry_errors: Vec::new(),
        }))
    }

    #[test]
    fn stall_reports_a_quiet_lane_once_per_episode() {
        let base = Instant::now();
        let mut a = Activity::default();
        a.note(&agent_start());
        assert!(a.stall(base, STALL_AFTER).is_none());
        assert!(
            a.stall(base + STALL_AFTER - Duration::from_secs(1), STALL_AFTER)
                .is_none()
        );
        let first = a
            .stall(base + STALL_AFTER, STALL_AFTER)
            .expect("first episode");
        assert_eq!(first.reason, lead::N_LANE_QUIET);
        assert_eq!(first.episode, 1);
        assert_eq!(first.quiet, STALL_AFTER);
        assert!(a.stall(base + 2 * STALL_AFTER, STALL_AFTER).is_none());
        a.note(&tool_start("bash", json!({"command": "ls"})));
        assert!(
            a.stall(base + 2 * STALL_AFTER + TICK, STALL_AFTER)
                .is_none()
        );
        let second = a
            .stall(base + 3 * STALL_AFTER + TICK, STALL_AFTER)
            .expect("second episode");
        assert_eq!(second.episode, 2);
        assert_eq!(second.reason, lead::N_LANE_QUIET);
    }

    #[test]
    fn stall_reports_spinning_when_calls_repeat() {
        let base = Instant::now();
        let mut a = Activity::default();
        a.note(&tool_start("bash", json!({"command": "ls"})));
        assert!(a.stall(base, STALL_AFTER).is_none());
        for k in 1..=40u32 {
            a.note(&tool_start("bash", json!({"command": "ls"})));
            let r = a.stall(base + k * TICK, STALL_AFTER);
            if k < 40 {
                assert!(r.is_none(), "no stall at tick {k}");
            } else {
                let r = r.expect("spinning at tick 40");
                assert_eq!(r.reason, lead::N_LANE_SPINNING);
                assert_eq!(r.episode, 1);
            }
        }
        assert_eq!(a.tool_calls, 41);
        assert_eq!(a.progress, 1);
        assert_eq!(a.last_tool.as_deref(), Some("bash"));
    }

    #[test]
    fn stall_counts_a_new_call_as_progress() {
        let base = Instant::now();
        let mut a = Activity::default();
        a.note(&tool_start("bash", json!({"command": "ls"})));
        assert!(a.stall(base, STALL_AFTER).is_none());
        for k in 1..=40u32 {
            let args = if k == 20 {
                json!({"command": "pwd"})
            } else {
                json!({"command": "ls"})
            };
            a.note(&tool_start("bash", args));
            assert!(
                a.stall(base + k * TICK, STALL_AFTER).is_none(),
                "no stall at tick {k}"
            );
        }
        let mut first = None;
        for k in 41..=60u32 {
            a.note(&tool_start("bash", json!({"command": "ls"})));
            if let Some(stall) = a.stall(base + k * TICK, STALL_AFTER) {
                first = Some((k, stall.reason));
                break;
            }
        }
        assert_eq!(first, Some((60, lead::N_LANE_SPINNING)));
        assert_eq!(a.progress, 2);
    }

    #[test]
    fn stall_names_an_open_provider_retry() {
        let base = Instant::now();
        let mut open = Activity::default();
        open.note(&agent_start());
        open.note(&retry_start());
        assert!(open.stall(base, STALL_AFTER).is_none());
        let stall = open.stall(base + STALL_AFTER, STALL_AFTER).expect("stall");
        assert_eq!(stall.reason, lead::N_LANE_RETRYING);

        let mut ended = Activity::default();
        ended.note(&agent_start());
        ended.note(&retry_start());
        ended.note(&retry_end());
        assert!(ended.stall(base, STALL_AFTER).is_none());
        let stall = ended.stall(base + STALL_AFTER, STALL_AFTER).expect("stall");
        assert_eq!(stall.reason, lead::N_LANE_QUIET);
    }

    #[test]
    fn note_counts_agent_events_only() {
        let mut a = Activity::default();
        let frame = Event::Undecodable {
            raw: json!({}),
            error: "qa".into(),
        };
        assert!(!a.note(&frame));
        assert_eq!(a.events, 0);
        assert!(a.note(&agent_start()));
        assert_eq!(a.events, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn watch_reports_on_its_own_tick() {
        use crate::config::{AccessMode, DiscordConfig, OmpConfig, Platform};

        let dir = tempfile::tempdir().expect("tempdir");
        let home = dir.path().join("qa-mbhome");
        let work_dir = dir.path().join("work");
        std::fs::create_dir_all(&home).expect("home dir");
        std::fs::create_dir_all(&work_dir).expect("work dir");
        let config = Config {
            messenger: Platform::Discord,
            work_dir,
            omp_path: PathBuf::from("/qa/omp"),
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
            memory: crate::config::MemoryConfig::default(),
        };
        let mut store = Store::open(&home.join("inbox.sqlite")).expect("store");
        let started = store
            .start_lane(
                "call-1",
                "discord:th:111",
                "t1",
                Some("file ."),
                None,
                MAX_RUNNING_LANES,
            )
            .expect("start lane");
        assert!(matches!(started, LaneStart::New(1)));
        let manager = Arc::new(LaneManager::new(
            config,
            Arc::new(Mutex::new(store)),
            Arc::default(),
            Duration::from_secs(365 * 24 * 60 * 60),
        ));
        let activity = Arc::new(Mutex::new(Activity::default()));
        lock(&activity).note(&agent_start());
        let (steer, _steer_rx) = mpsc::channel();
        lock(&manager.inner).children.insert(
            1,
            LaneChild {
                kill: None,
                worker: std::thread::spawn(|| {}),
                seq: 1,
                started: Instant::now(),
                conversation_key: "discord:th:111".into(),
                activity,
                stop: None,
                steer,
            },
        );
        let wake = manager.wake();
        let notified = wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let start = tokio::time::Instant::now();
        let task = tokio::spawn({
            let m = Arc::clone(&manager);
            async move { m.watch().await }
        });
        let woke = tokio::time::timeout(STALL_AFTER + 2 * WATCH_TICK, notified)
            .await
            .is_ok();
        let elapsed = start.elapsed();
        task.abort();
        let _ = task.await;
        let child = lock(&manager.inner).children.remove(&1).expect("child");
        child.worker.join().expect("worker joins");
        let (count, row) = {
            let store = lock(&manager.store);
            (store.count().expect("count"), store.get(1).expect("get"))
        };

        assert!(woke);
        assert!(elapsed >= STALL_AFTER, "elapsed {elapsed:?}");
        assert!(elapsed <= STALL_AFTER + WATCH_TICK, "elapsed {elapsed:?}");
        assert_eq!(count, 1);
        assert_eq!(row.expect("row").message_id, "1:1:stalled:1");
    }

    #[test]
    fn news_text_cleans_secrets_paths_and_mentions() {
        let paths = [
            (Path::new("/qa/work"), "."),
            (Path::new("/qa/qa-user"), "~"),
        ];
        let at = '\u{FF20}';
        let cases: [(String, String); 11] = [
            ("built /qa/work/src/a.rs".into(), "built ./src/a.rs".into()),
            (
                "read /qa/qa-user/notes.txt".into(),
                "read ~/notes.txt".into(),
            ),
            (
                "ping @everyone and <@111>".into(),
                format!("ping {at}everyone and <{at}111>"),
            ),
            (
                format!("key password={}", "q".repeat(12)),
                "key [secret]".into(),
            ),
            (
                format!("use sk-{} now", "q".repeat(24)),
                "use [secret] now".into(),
            ),
            ("a\n\n  b\tc ".into(), "a b c".into()),
            ("x".repeat(500), "x".repeat(REPORT_CHARS)),
            ("  \n ".into(), String::new()),
            ("/qa/workshop".into(), "/qa/workshop".into()),
            ("/qa/qa-userx/a".into(), "/qa/qa-userx/a".into()),
            ("in '/qa/work' and /qa/work".into(), "in '.' and .".into()),
        ];
        for (i, (input, expected)) in cases.iter().enumerate() {
            assert_eq!(news_text(input, &paths), *expected, "case {i}");
        }

        assert_eq!(news_text("/ only", &[(Path::new("/"), ".")]), "/ only");
        assert_eq!(
            news_text("network ok", &[(Path::new("work"), ".")]),
            "network ok"
        );
        assert_eq!(
            news_text("built /qa/work/a.rs", &[(Path::new("/qa/work/"), ".")]),
            "built ./a.rs"
        );
        assert_eq!(news_text("a b", &[(Path::new(""), ".")]), "a b");
    }

    /// A manager with a Discord config under `dir` (home `qa-mbhome`, work dir `work`); its
    /// omp path is never run.
    fn qa_manager(dir: &Path) -> Arc<LaneManager> {
        use crate::config::{AccessMode, DiscordConfig, OmpConfig, Platform};

        let home = dir.join("qa-mbhome");
        let work_dir = dir.join("work");
        std::fs::create_dir_all(&home).expect("home dir");
        std::fs::create_dir_all(&work_dir).expect("work dir");
        let config = Config {
            messenger: Platform::Discord,
            work_dir,
            omp_path: PathBuf::from("/qa/omp"),
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
            memory: crate::config::MemoryConfig::default(),
        };
        let store = Store::open(&home.join("inbox.sqlite")).expect("store");
        Arc::new(LaneManager::new(
            config,
            Arc::new(Mutex::new(store)),
            Arc::default(),
            Duration::from_secs(365 * 24 * 60 * 60),
        ))
    }

    /// Registers a dummy running job for lane `id` in conversation `key`; returns its activity.
    fn qa_child(manager: &LaneManager, id: i64, key: &str) -> Arc<Mutex<Activity>> {
        let activity = Arc::new(Mutex::new(Activity::default()));
        let (steer, _steer_rx) = mpsc::channel();
        lock(&manager.inner).children.insert(
            id,
            LaneChild {
                kill: None,
                worker: std::thread::spawn(|| {}),
                seq: 1,
                started: Instant::now(),
                conversation_key: key.into(),
                activity: Arc::clone(&activity),
                stop: None,
                steer,
            },
        );
        activity
    }

    /// Removes the dummy job of lane `id` and joins its worker.
    fn qa_drop_child(manager: &LaneManager, id: i64) {
        let child = lock(&manager.inner).children.remove(&id).expect("child");
        child.worker.join().expect("worker joins");
    }

    #[test]
    fn news_lists_unposted_reports_of_running_lanes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let m = qa_manager(dir.path());
        let a1 = qa_child(&m, 1, "discord:th:111");
        let a2 = qa_child(&m, 2, "discord:th:222");
        assert!(m.news().is_empty());

        lock(&a1).report = Some("qa step 1".into());
        assert_eq!(
            m.news(),
            [LaneNews {
                lane_id: 1,
                seq: 1,
                conversation_key: "discord:th:111".into(),
                text: "qa step 1".into(),
            }]
        );
        m.news_posted(1, "qa step 1");
        assert!(m.news().is_empty());

        lock(&a1).report = Some("qa step 2".into());
        let news = m.news();
        assert_eq!(news.len(), 1);
        assert_eq!((news[0].lane_id, news[0].text.as_str()), (1, "qa step 2"));

        lock(&a1).report = Some("qa step 1".into());
        assert!(m.news().is_empty());

        lock(&a2).report = Some("qa b".into());
        assert_eq!(
            m.news(),
            [LaneNews {
                lane_id: 2,
                seq: 1,
                conversation_key: "discord:th:222".into(),
                text: "qa b".into(),
            }]
        );
        qa_drop_child(&m, 2);
        assert!(m.news().is_empty());
        qa_drop_child(&m, 1);
    }

    #[test]
    fn lane_report_records_the_cleaned_text() {
        let dir = tempfile::tempdir().expect("tempdir");
        let m = qa_manager(dir.path());
        let a = qa_child(&m, 1, "discord:th:111");
        let root = m.lanes_root();
        let args = schema(json!({"text": format!("wrote {}/a.rs\n@here", root.display())}));
        assert!(m.lane_report(&args, (1, 1), &a).is_ok());
        let expected = format!("wrote ./a.rs {}here", '\u{FF20}');
        assert_eq!(lock(&a).report.as_deref(), Some(expected.as_str()));
        assert_eq!(*m.lane_reports().borrow(), 1);

        assert!(
            m.lane_report(&schema(json!({"text": "  \n "})), (1, 1), &a)
                .is_err()
        );
        assert!(m.lane_report(&schema(json!({})), (1, 1), &a).is_err());
        assert_eq!(lock(&a).report.as_deref(), Some(expected.as_str()));
        assert_eq!(*m.lane_reports().borrow(), 1);
        qa_drop_child(&m, 1);
    }

    #[test]
    fn news_forgets_posted_texts_of_finished_lanes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let m = qa_manager(dir.path());
        let _a1 = qa_child(&m, 1, "discord:th:111");
        for i in 0..=POSTED_PER_LANE {
            m.news_posted(1, &format!("qa {i}"));
        }
        let full = lock(&m.inner).posted[&1].len();
        m.news_posted(9, "qa x");
        let keys = |m: &LaneManager| {
            let mut keys: Vec<i64> = lock(&m.inner).posted.keys().copied().collect();
            keys.sort_unstable();
            keys
        };
        let before = keys(&m);
        m.news();
        let running = keys(&m);
        qa_drop_child(&m, 1);
        m.news();
        let after = lock(&m.inner).posted.is_empty();

        assert_eq!(full, 1);
        assert_eq!(before, [1, 9]);
        assert_eq!(running, [1]);
        assert!(after);
    }

    #[tokio::test(start_paused = true)]
    async fn watch_publishes_each_tick() {
        let dir = tempfile::tempdir().expect("tempdir");
        let m = qa_manager(dir.path());
        let mut ticks = m.watch_ticks();
        let task = tokio::spawn({
            let m = Arc::clone(&m);
            async move { m.watch().await }
        });
        let first_seen = tokio::time::timeout(2 * WATCH_TICK, ticks.changed()).await;
        let first = ticks.borrow_and_update().expect("tick time");
        let second_seen = tokio::time::timeout(2 * WATCH_TICK, ticks.changed()).await;
        let second = ticks.borrow_and_update().expect("tick time");
        task.abort();
        let _ = task.await;

        assert!(matches!(first_seen, Ok(Ok(()))));
        assert!(matches!(second_seen, Ok(Ok(()))));
        assert_eq!(second.duration_since(first), WATCH_TICK);
    }

    #[test]
    fn stop_marks_a_running_job_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let m = qa_manager(dir.path());
        let started = lock(&m.store)
            .start_lane(
                "call-1",
                "discord:th:111",
                "t1",
                Some("file ."),
                None,
                MAX_RUNNING_LANES,
            )
            .expect("start lane");
        qa_child(&m, 1, "discord:th:111");
        qa_child(&m, 2, "discord:th:222");
        let first = m.stop(1, "qa  stuck\n now");
        let tracked = m.track(4242, (1, 1), false);
        let second = m.stop(1, "other");
        let long = m.stop(2, &"x".repeat(300));
        let reason1 = lock(&m.inner).children[&1].stop.clone();
        let reason2_chars = lock(&m.inner).children[&2]
            .stop
            .as_ref()
            .map(|r| r.chars().count());
        let checks_empty = lock(&m.inner).checks.is_empty();
        qa_drop_child(&m, 1);
        qa_drop_child(&m, 2);

        assert!(matches!(started, LaneStart::New(1)));
        assert_eq!(first, Ok(true));
        assert!(!tracked);
        assert_eq!(second, Ok(false));
        assert_eq!(long, Ok(true));
        assert_eq!(reason1.as_deref(), Some("qa stuck now"));
        assert_eq!(reason2_chars, Some(STOP_REASON_CHARS));
        assert!(checks_empty);
    }

    #[test]
    fn stop_refuses_unknown_idle_closed_and_stopping() {
        let dir = tempfile::tempdir().expect("tempdir");
        let m = qa_manager(dir.path());
        let (started1, finished1, started2, finished2, closed2) = {
            let mut store = lock(&m.store);
            let started1 = store
                .start_lane(
                    "call-1",
                    "discord:th:111",
                    "t1",
                    Some("file ."),
                    None,
                    MAX_RUNNING_LANES,
                )
                .expect("start lane 1");
            let finished1 = store
                .finish_lane(1, 1, "r", LaneState::Idle, None)
                .expect("finish lane 1");
            let started2 = store
                .start_lane(
                    "call-2",
                    "discord:th:222",
                    "t2",
                    Some("file ."),
                    None,
                    MAX_RUNNING_LANES,
                )
                .expect("start lane 2");
            let finished2 = store
                .finish_lane(2, 1, "r", LaneState::Idle, None)
                .expect("finish lane 2");
            let closed2 = store.close_lane(2, "qa").expect("close lane 2");
            (started1, finished1, started2, finished2, closed2)
        };
        let got = [
            m.stop(1, "  \n "),
            m.stop(99, "qa"),
            m.stop(1, "qa"),
            m.stop(2, "qa"),
        ];
        m.stop_accepting();
        let late = m.stop(1, "qa");

        assert!(matches!(started1, LaneStart::New(1)));
        assert!(finished1);
        assert!(matches!(started2, LaneStart::New(2)));
        assert!(finished2);
        assert!(matches!(closed2, LaneClose::Closed { .. }));
        assert_eq!(
            got,
            [
                Err(E_LANE_ARGS),
                Err(E_LANE_UNKNOWN),
                Err(E_LANE_IDLE),
                Err(E_LANE_CLOSED)
            ]
        );
        assert_eq!(late, Err(E_LANE_STOPPING));
    }

    #[test]
    fn stop_kills_the_running_checks_of_the_job() {
        use std::os::unix::process::CommandExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let m = qa_manager(dir.path());
        qa_child(&m, 1, "discord:th:111");
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("600")
            .process_group(0)
            .spawn()
            .expect("sleep");
        let pid = child.id();
        lock(&m.inner).checks.insert(pid, (1, 1));
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(child.wait());
        });
        let stopped = m.stop(1, "qa");
        let got = rx.recv_timeout(Duration::from_secs(30));
        if got.is_err() {
            // Cleanup before any assertion: a failing run leaves no `sleep` behind.
            omp::kill_group(pid);
            let _ = rx.recv_timeout(Duration::from_secs(30));
        }
        lock(&m.inner).checks.remove(&pid);
        qa_drop_child(&m, 1);

        assert_eq!(stopped, Ok(true));
        assert!(matches!(got, Ok(Ok(status)) if !status.success()));
    }

    #[test]
    fn write_result_reports_a_stopped_job() {
        let dir = tempfile::tempdir().expect("tempdir");
        let m = qa_manager(dir.path());
        let started: Vec<LaneStart> = (1..=3)
            .map(|n| {
                lock(&m.store)
                    .start_lane(
                        &format!("call-{n}"),
                        "discord:th:111",
                        &format!("t{n}"),
                        Some("file ."),
                        None,
                        MAX_RUNNING_LANES,
                    )
                    .expect("start lane")
            })
            .collect();
        for n in 1..=3 {
            qa_child(&m, n, "discord:th:111");
        }
        for n in 1..=2 {
            lock(&m.inner).children.get_mut(&n).unwrap().stop = Some("qa".into());
        }
        let lost = || {
            Ok(LaneOutcome::Plain(Outcome::NeedsCheck {
                code: omp::E_CHILD_LOST,
                detail: "qa".into(),
            }))
        };
        let written = {
            let set = lock(&m.inner);
            [
                m.write_result(&set, 1, 1, "discord:th:111", lost()),
                m.write_result(
                    &set,
                    2,
                    1,
                    "discord:th:111",
                    Ok(LaneOutcome::Plain(Outcome::Reply("qa done".into()))),
                ),
                m.write_result(&set, 3, 1, "discord:th:111", lost()),
            ]
        };
        let (rows, lanes): (Vec<String>, Vec<(LaneState, Option<String>)>) = {
            let store = lock(&m.store);
            (1..=3)
                .map(|n| {
                    let row = store.get(n).expect("get").expect("row").text;
                    let lane = store.lane(n).expect("lane").expect("lane row");
                    (row, (lane.state, lane.error))
                })
                .unzip()
        };
        for n in 1..=3 {
            qa_drop_child(&m, n);
        }

        assert!(
            started
                .iter()
                .zip(1..)
                .all(|(s, n)| matches!(s, LaneStart::New(id) if *id == n))
        );
        assert_eq!(written, [true, true, true]);
        assert_eq!(
            rows[0],
            lead::lane_result("discord:th:111", 1, &LaneResult::Stopped { reason: "qa" })
        );
        assert_eq!(
            lanes[0],
            (LaneState::Failed, Some(E_LANE_STOPPED.to_owned()))
        );
        assert_eq!(
            rows[1],
            lead::lane_result("discord:th:111", 2, &LaneResult::Text("qa done"))
        );
        assert_eq!(lanes[1], (LaneState::Idle, None));
        assert_eq!(
            rows[2],
            lead::lane_result("discord:th:111", 3, &LaneResult::Failed(omp::E_CHILD_LOST))
        );
        assert_eq!(
            lanes[2],
            (LaneState::NeedsCheck, Some(omp::E_CHILD_LOST.to_owned()))
        );
    }

    #[tokio::test]
    async fn only_failed_stopped_or_unmet_lane_results_are_remembered_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut m = qa_manager(dir.path());
        let today = memory::Day::from_ymd(2026, 11, 22).expect("day");
        let store = memory::MemoryStore::open(
            &dir.path().join("memory"),
            memory::Redactor::default(),
            Arc::new(move || today),
        )
        .expect("memory store");
        let handle = memory::MemoryHandle::spawn(store).expect("spawn");
        Arc::get_mut(&mut m).expect("not shared").outcome_memory = Some(handle.clone());
        for (n, task) in [
            (1, "fix the build\nmore"),
            (2, "write docs"),
            (3, "run the migration"),
            (4, "ship it"),
        ] {
            lock(&m.store)
                .start_lane(&format!("call-{n}"), "discord:th:111", task, None, None, 4)
                .expect("start lane");
        }
        let failed = || {
            Err(Outcome::Failed {
                code: omp::E_OMP_SPAWN,
                detail: "spawn failed\nsecond line".into(),
            })
        };
        {
            let set = lock(&m.inner);
            assert!(m.write_result(&set, 1, 1, "discord:th:111", failed()));
            m.write_result(&set, 1, 1, "discord:th:111", failed());
            assert!(m.write_result(
                &set,
                2,
                1,
                "discord:th:111",
                Ok(LaneOutcome::Plain(Outcome::Reply("done".into())))
            ));
            assert!(m.write_result(
                &set,
                3,
                1,
                "discord:th:111",
                Ok(LaneOutcome::Stopped {
                    reason: "owner changed plans\nmore".into()
                })
            ));
            assert!(
                m.write_result(
                    &set,
                    4,
                    1,
                    "discord:th:111",
                    Ok(LaneOutcome::Unmet {
                        text: "all done, trust me".into(),
                        receipt:
                            "evidence: x\n- file a -> ok\n- cmd make test -> E_CHECK_EXIT: exit 2"
                                .into(),
                    })
                )
            );
        }
        // The worker takes jobs in order: once this add is saved, the queued ones are too.
        handle
            .add(memory::NewEntry {
                text: "barrier".into(),
                prov: memory::Prov::Owner,
                bucket: memory::Bucket::Owner,
                src: memory::Source::row(1),
                expires: None,
                why: None,
            })
            .await
            .expect("barrier");
        let saved = handle.entries(&memory::Bucket::General).expect("entries");
        let mut texts: Vec<&str> = saved.iter().map(|e| e.text.as_str()).collect();
        texts.sort_unstable();
        assert_eq!(
            texts,
            [
                format!(
                    "Lane 1 result failed with {}. Task: fix the build. Error: spawn failed",
                    omp::E_OMP_SPAWN
                ),
                format!(
                    "Lane 3 result failed with {E_LANE_STOPPED}. Task: run the migration. Stopped: owner changed plans"
                ),
                format!(
                    "Lane 4 result failed with {}. Task: ship it. Failed checks: cmd make test",
                    done::E_DONE_UNMET
                ),
            ]
        );
    }
}
