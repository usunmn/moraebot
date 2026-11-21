//! Core dispatcher: inbound event -> access check -> store -> one global worker -> omp -> reply.
//!
//! Order of an inbound message: drop authors that are neither the owner nor listed in the
//! active messenger's `allow` list (nothing stored); drop a listed bot's message once its
//! conversation reached `max_bot_replies` in the window (no limit when unset; bot
//! messages posted directly in the channel count per channel); answer `/`
//! messages (except the owner's `/new`, see below) and empty messages without attachments with
//! one fixed notice (never stored, never forwarded), refuse more than [`files::MAX_FILES`] attachments (nothing stored,
//! nothing fetched); hand a listed `chat` sender's message to the chat runtime
//! ([`crate::chat`]: never stored, never steered, never seen by the lead or its tools);
//! pick the conversation key, store the row FIRST (a duplicate stops here), and only then
//! open a thread for a message posted directly in the channel.
//!
//! A chat message seen before (a redelivery, see [`ChatManager::first_delivery`]) stops
//! there. A chat message posted directly in the channel gets its own thread first (no
//! thread: one [`N_THREAD_FAILED`] notice). Its turn then runs on its own task: its
//! attachments within their size limit are fetched into the chat folder under the home
//! (never the work folder), handed to the chat child with the prompt and removed; the
//! reply, or one notice, goes to its conversation.
//!
//! Attachments are forwarded: they are fetched only after the row is stored and its thread
//! opened, outside the admission lock, into `<work_dir>/.moraebot-inbox/<key>/<row>/`, each
//! under the stall watchdog. The row stays out of the FIFO until every file is saved; one
//! failed file fails the row with one notice. The saved paths are listed in the prompt.
//!
//! A row that starts its own thread (key `<platform>:th:<its own message id>`) is run only
//! once that thread is known (`known_threads`). A row left without a thread by a crash
//! between admission and thread creation gets one more creation attempt at startup (or
//! at dequeue); if that fails it becomes `failed` with one notice in the channel, and it
//! never reaches omp.
//!
//! One lead omp session serves every conversation. Each owner row reaches it as
//! [`lead::owner_input`] (a tag line naming its conversation and row, then the text with
//! the saved-files block); a lane-result row (`origin = 'lane'`) carries its stored text
//! and skips the owner check. Every reply goes to `reply_target(row.conversation_key)`.
//!
//! Owner new-session command ([`crate::new_session`]): an owner row without attachments whose
//! whole text is `새 세션`, `새 세션 시작`, `새 세션으로 시작`, `new session` or `/new` is checked first in
//! that slot, before the memory commands. With attachments it gets
//! one fixed notice ([`N_NEW_SESSION_FILES`]) and nothing is stored, the same shape as the
//! `/` notice. On arrival it is only queued, never steered
//! and never claimed: it waits in the FIFO, so a running turn ends first, and the store refuses
//! to steer later rows of its conversation past it. The worker answers it without the lead:
//! while lanes run it asks for `새 세션 확인` / `new session confirm` within
//! [`new_session::CONFIRM_WINDOW`] and changes nothing (the question lives in memory only, and
//! the owner's next message in that conversation closes it: only a confirmation arriving while
//! it is open counts, judged on arrival);
//! otherwise [`OmpManager::new_session`] closes the lead child, moves its session folder
//! aside and clears its session row, and the reply names the backup. Rows queued behind it run
//! on the new lead.
//!
//! Owner memory commands ([`crate::memory_commands`]): once a memory store is attached
//! ([`Dispatcher::with_memory`]), an owner row without attachments is checked right after
//! admission (and its thread), before any steer: a remember / show / correct / forget /
//! restore request is claimed, answered by moraebot itself as the row's reply and never
//! reaches the lead, also while a lead turn of its conversation is running. Forget only asks;
//! the owner's next message in that conversation answers (yes forgets, no keeps, anything
//! else drops the question and is handled as usual). The question lives in memory only. A
//! command row queued without that check (a race with the worker, a restart) is answered
//! by the worker the same way; lane-result rows are never checked.
//!
//! Owner corrections ([`crate::lessons`]): when the worker turns an owner row into a lead
//! input and `[memory.self]` is on, a row whose text matches a correction pattern is queued
//! as an owner entry in `self/observations.md` (the turn never waits for it); at every third
//! similar correction the input gets a short block asking the lead to offer a standing rule.
//!
//! Owner status questions ([`crate::status`]), unless `lead.direct_status = false`: in the same
//! slot, right after the memory check,
//! an owner row without attachments whose whole text is a status question ("뭐 하고 있어",
//! "status", ...) is claimed and answered by moraebot from the lead's turn state
//! ([`OmpManager::lead_status`]) and the running lanes, also while a lead turn runs. The lead
//! never sees it, and a status row queued without that check is answered by the worker the
//! same way.
//!
//! A message of the conversation whose lead turn is running is steered into that turn
//! once its files are saved, only when it comes from the same sender as the turn, when the store accepts the claim ([`Store::steer_into`]);
//! otherwise it is queued. A turn and its steered row form one group: one reply on the
//! main row, or one outcome for both (`needs_check` wins over `failed`).
//!
//! One worker serves the global FIFO with one lead turn in flight. A reply is stored
//! (`reply_pending`) before it is sent and the row becomes `done` only after every chunk
//! was accepted; a failed send is retried after the adapter reconnects and at startup.
//! Interrupted work (`needs_check`) is never re-prompted; its conversation gets one
//! notice, also across restarts (`notice_sent`), and its older queued rows are held: they
//! are marked `needs_check` / [`E_DISPATCH_HELD`] in the same store transaction as the
//! interruption, so the hold survives a restart. Memory saves are never posted: a reply
//! carries only the lead's text (the memory writer logs each save).
//!
//! Files the agent queued with `send_file` are stored with the reply and uploaded after
//! its text, each marked sent or skipped on its own, so a resend sends only what is
//! missing. A file the messenger refuses, that cannot be read, that ran past
//! [`files::UPLOAD_TIMEOUT`] or that reached [`files::MAX_UPLOAD_ATTEMPTS`] is skipped;
//! the owner gets one notice per reply.
//!
//! At startup, an onboarding request left by `moraebot init` or `moraebot onboarding` starts
//! the first meeting and posts one fixed greeting: in the owner's DM (in the configured
//! channel when the DM fails), or in the channel when the request says so.
//!
//! Lane news: after every lane watchdog tick, the newest progress note of each running lane
//! not handed on yet goes to the lead for every conversation that had no owner message,
//! delivered reply or handed-on news for [`NEWS_AFTER`]; the lead tells the owner. While the
//! lead runs a turn of the lane's conversation the note is sent into that turn
//! ([`NoteKind::News`]); it counts as handed on only once the lead took it, and a note omp
//! refused, or that a cut turn never answered, becomes the row below at the next step;
//! otherwise it is stored as a dedup'd lane inbox row
//! ([`lead::lane_news`]) that waits for the lead like a lane result. A news row whose lane no
//! longer runs when its turn comes is dropped ([`E_LANE_NEWS_STALE`]). Only while the lead
//! cannot start ([`OmpManager::lead_down`]) is the raw [`news_line`] posted instead. A note
//! that could not be handed on is tried again on the next tick.
//!
//! Busy lead turn: after every lane watchdog tick, a lead turn that has run for
//! `lead.handoff_after_minutes` while an owner row of its conversation waits (steered into it
//! or queued) gets one [`NoteKind::Busy`] note asking it to hand off and answer
//! ([`OmpManager::lead_busy`], log code `W_LEAD_BUSY`). Nothing is killed.
//!
//! Stalled lead turn: after every lane watchdog tick, a lead turn that sent no omp agent event
//! for `omp.stall_after_minutes` gets one [`N_TURN_STALLED`] notice in its conversation
//! ([`OmpManager::lead_stalled`]); after new progress a later stall gets a new one. The turn
//! keeps running: nothing is killed, restarted or interrupted.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc, watch};
use tokio::task::{self, JoinSet};

use crate::access::{BotReplies, Decision, DropReason, E_ACCESS_NOT_OWNER, Policy, Sender};
use crate::adapter::{
    AdapterError, ConversationKey, E_ADAPTER_UPLOAD_DENIED, InboundAttachment, InboundEvent,
    MessengerAdapter, split_reply,
};
use crate::chat::{self, ChatFile, ChatManager};
use crate::config::{Config, SelfConfig};
use crate::files::{self, OutFile, SavedFile};
use crate::gateway::{E_GATEWAY_TASK, GatewayError};
use crate::lane::{LaneManager, LaneNews};
use crate::lead;
use crate::lessons;
use crate::memory::MemoryHandle;
use crate::memory_commands::{self, Command};
use crate::new_session;
use crate::omp::{
    ABORT_JOIN_BOUND, E_COST_CAP, E_LEAD_NEW_SESSION, GroupReport, NoteKind, OmpManager, Outcome,
    key_slug, lock,
};
use crate::omp_overlay::{OverlayCheck, W_OMP_VERSION};
use crate::onboarding::{self, E_ONBOARDING_REQUEST, GreetTarget};
use crate::status;
pub use crate::store::E_DISPATCH_HELD;
use crate::store::{Admitted, InboxRow, LaneRow, LaneState, State, Store, StoreError};
use crate::store::{LogLevel, LogRow};

/// Empty message without attachments, or `/` message: not forwarded to omp.
pub const N_UNSUPPORTED_INPUT: &str = "N_UNSUPPORTED_INPUT";
/// A thread could not be opened for a channel message; the row is `failed`.
pub const N_THREAD_FAILED: &str = "N_THREAD_FAILED";
/// The turn failed; the row is `failed`.
pub const N_FAILED: &str = "N_FAILED";
/// The configured cost limit stopped the turn.
pub const N_COST_CAP: &str = "N_COST_CAP";
/// The turn was interrupted (lost child, timeout, held session); the row is `needs_check`.
pub const N_INTERRUPTED: &str = "N_INTERRUPTED";
/// omp handled the message without running the agent.
pub const N_NO_AGENT: &str = "N_NO_AGENT";
/// Appended to a reply that was cut at [`MAX_REPLY_MESSAGES`].
pub const N_REPLY_TRUNCATED: &str = "N_REPLY_TRUNCATED";
/// Too many attachments, or one could not be fetched or saved; the message was not run.
pub const N_FILE_NOT_RECEIVED: &str = "N_FILE_NOT_RECEIVED";
/// The messenger refused the download (missing permission); the message was not run.
pub const N_FILE_NOT_ALLOWED: &str = "N_FILE_NOT_ALLOWED";
/// A file the agent queued was skipped or its upload failed; posted once per reply.
pub const N_FILE_NOT_SENT: &str = "N_FILE_NOT_SENT";
/// A lane was running when the gateway stopped; it was not resumed.
pub const N_LANE_INTERRUPTED: &str = "N_LANE_INTERRUPTED";
/// The one greeting after an onboarding request.
pub const N_ONBOARDING_HELLO: &str = "N_ONBOARDING_HELLO";
/// The running lead turn showed no progress for `omp.stall_after_minutes`; it keeps running.
pub const N_TURN_STALLED: &str = "N_TURN_STALLED";
/// omp is not the version moraebot's memory settings were checked against; sent to the owner
/// once per omp version.
pub const N_OMP_VERSION: &str = "N_OMP_VERSION";
/// The owner's new-session command came with attachments; nothing was stored or run.
pub const N_NEW_SESSION_FILES: &str = "N_NEW_SESSION_FILES";

/// Stored row error: the conversation key has no reply target.
pub const E_DISPATCH_KEY: &str = "E_DISPATCH_KEY";
/// Stored row error: a lane news row reached its turn after its lane stopped running; it was
/// dropped without a lead turn or a notice.
pub const E_LANE_NEWS_STALE: &str = "E_LANE_NEWS_STALE";
/// Log code: a notice skipped its channel fallback because there is no channel (a bridge
/// whose hello names no `home`).
pub const W_NO_HOME_CHANNEL: &str = "W_NO_HOME_CHANNEL";

/// Longer replies are cut with a note.
pub const MAX_REPLY_MESSAGES: usize = 10;

/// A conversation must be this quiet before lane news is posted in it.
pub const NEWS_AFTER: Duration = Duration::from_secs(20 * 60);

/// The fixed English text of a notice code.
pub fn notice_text(code: &str) -> &'static str {
    match code {
        N_UNSUPPORTED_INPUT => {
            "Messages starting with \"/\" and empty messages are not sent to the agent."
        }
        N_THREAD_FAILED => {
            "Could not open a thread for this message, so it was not run. Send it again if needed."
        }
        N_FAILED => "The task failed and did not finish.",
        N_COST_CAP => "The cost limit was reached. Continue in a new thread.",
        N_INTERRUPTED => {
            "The task was interrupted. Part of it may already have run. Send it again if needed."
        }
        N_NO_AGENT => "omp handled this message without running the agent.",
        N_REPLY_TRUNCATED => "[Reply cut: it was longer than 10 messages.]",
        N_FILE_NOT_RECEIVED => {
            "A file could not be received, so the message was not run. At most 10 files per \
             message. Send it again if needed."
        }
        N_FILE_NOT_ALLOWED => {
            "moraebot is not allowed to download this file, so the message was not run. Check \
             the bot's file permission (README: Upgrading) and send it again."
        }
        N_FILE_NOT_SENT => {
            "A file from the agent could not be sent, or sending it was interrupted. If this \
             keeps happening, check the bot's file permission (README: Upgrading)."
        }
        N_LANE_INTERRUPTED => {
            "A background task was interrupted by a restart and was not resumed. Ask again if it \
             is still needed."
        }
        N_NEW_SESSION_FILES => {
            "The new session command takes no attachments, so nothing was done. Send it alone."
        }
        N_ONBOARDING_HELLO => {
            "Hi! I'm your moraebot agent, and this is our first chat. When you reply, I'll ask \
             what to call me, how I should talk to you, and a little about you. Reply skip if \
             you'd rather not."
        }
        N_OMP_VERSION => {
            "The installed omp is not the version moraebot's memory settings were checked \
             against. Run `moraebot check-config` for the details."
        }
        _ => "Something went wrong.",
    }
}

/// The [`N_TURN_STALLED`] notice text for a stall of `minutes`.
pub fn stalled_text(minutes: u64) -> String {
    format!(
        "No progress for {minutes} minutes. The task may be stuck. It keeps running and nothing \
         was stopped."
    )
}

/// The messages to post for a reply: at most [`MAX_REPLY_MESSAGES`] chunks of at most
/// `limit` characters (the adapter's limit), never blank; a longer reply ends with the
/// cut note.
pub fn reply_messages(text: &str, limit: usize) -> Vec<String> {
    let mut chunks: Vec<String> = split_reply(text, limit)
        .into_iter()
        .filter(|chunk| !chunk.trim().is_empty())
        .collect();
    if chunks.len() <= MAX_REPLY_MESSAGES {
        return chunks;
    }
    chunks.truncate(MAX_REPLY_MESSAGES);
    let note = notice_text(N_REPLY_TRUNCATED);
    let room = limit - note.chars().count() - 1;
    let last = chunks.pop().unwrap_or_default();
    let head: String = last.chars().take(room).collect();
    chunks.push(format!("{head}\n{note}"));
    chunks
}

/// The channel a conversation's replies go to: the thread id or the DM channel id.
pub fn reply_target(key: &str) -> Option<&str> {
    key.split_once(":th:")
        .or_else(|| key.split_once(":dm:"))
        .map(|(_, id)| id)
        .filter(|id| !id.is_empty())
}

/// One lane's note as posted while the lead cannot start.
pub fn news_line(lane_id: i64, text: &str) -> String {
    format!("[lane {lane_id}] {text}")
}

/// Whether the row opened its own thread: its key is `<platform>:th:<its message id>`.
fn is_thread_starter(row: &InboxRow) -> bool {
    row.conversation_key == ConversationKey::thread(&row.platform, &row.message_id).as_str()
}

/// The notice for a row whose files could not be received.
fn file_notice(code: &str) -> &'static str {
    if code == files::E_FILE_DENIED {
        N_FILE_NOT_ALLOWED
    } else {
        N_FILE_NOT_RECEIVED
    }
}

/// The declared attachment metadata stored at admission: name, size and mime only, never
/// the reference (a signed or token-gated URL).
fn declared_json(attachments: &[InboundAttachment]) -> String {
    let declared: Vec<serde_json::Value> = attachments
        .iter()
        .map(|a| serde_json::json!({"name": a.name, "size": a.size, "mime": a.mime}))
        .collect();
    serde_json::Value::Array(declared).to_string()
}

/// The out-file list stored with a reply for the files `send_file` queued; `None` when
/// there are none.
fn out_files_json(files: Vec<PathBuf>) -> Option<String> {
    if files.is_empty() {
        return None;
    }
    let entries: Vec<OutFile> = files
        .into_iter()
        .map(|path| OutFile {
            name: path
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "file".to_owned()),
            path,
            sent: false,
            skipped: false,
            attempting: false,
            attempts: 0,
        })
        .collect();
    Some(serde_json::to_string(&entries).expect("out-file list serializes to JSON"))
}

/// The reply text of one group member; `None` when it did not reply.
fn reply_text(outcome: &Outcome) -> Option<&str> {
    match outcome {
        Outcome::Reply(text) => Some(text),
        Outcome::NoAgent => Some(notice_text(N_NO_AGENT)),
        Outcome::Failed { .. } | Outcome::NeedsCheck { .. } => None,
    }
}

/// The group's one reply: the texts in send order, consecutive equal texts once, joined
/// by a blank line.
fn combine_replies<'a>(texts: impl IntoIterator<Item = &'a str>) -> String {
    let mut kept: Vec<&str> = Vec::new();
    for text in texts {
        if kept.last() != Some(&text) {
            kept.push(text);
        }
    }
    kept.join("\n\n")
}

/// Resolves at the adapter's next reconnect; never, when it has no reconnect signal.
async fn next_reconnect(reconnects: &mut Option<watch::Receiver<u64>>) {
    let Some(rx) = reconnects else {
        return std::future::pending().await;
    };
    if rx.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// What happened to one inbound event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Handled {
    Dropped(DropReason),
    /// Answered with this notice code; nothing stored.
    Notice(&'static str),
    Duplicate,
    /// Stored, but the thread could not be opened; the row is `failed`.
    ThreadFailed(i64),
    /// The message's files were refused or not received, so it is not run; carries the
    /// notice code. A count refusal stores nothing; otherwise the row is `failed` (or kept
    /// held under its earlier notice when it was held meanwhile).
    FilesRejected(&'static str),
    Queued(i64),
    /// Stored and sent into the running lead turn of its conversation; answered in that
    /// turn's reply.
    Steered(i64),
    /// A listed chat sender's message, handed to the chat runtime on its own task; nothing
    /// stored.
    Chat,
    /// An owner memory command, answered by moraebot itself as the row's reply; never sent to
    /// the lead.
    Memory(i64),
    /// An owner status question, answered by moraebot itself as the row's reply; never sent to
    /// the lead.
    Status(i64),
}

/// The current time in unix seconds.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// What an owner row asks of the memory store.
enum MemoryAct {
    Run(Command),
    /// The owner's yes (`true`) or no to the forget question about this id.
    Confirm(String, bool),
    /// The recent-recalls command, in the owner's language.
    Recalls(status::Lang),
}

/// How often a conversation saw an owner message, a delivered reply or posted news, and
/// the tick time that count was last seen to change.
#[derive(Default)]
struct Talk {
    said: u64,
    seen: Option<(u64, Instant)>,
}

/// Lane news notes sent into a running lead turn. A sent note is only queued: it counts as
/// handed on once the lead took it. Until it settles its message id is in `in_flight`, so the
/// same note is not offered twice; the turn's end pushes it to `settled` with whether the lead
/// took it, and the next news step finishes it.
#[derive(Default)]
struct NewsNotes {
    in_flight: std::collections::HashSet<String>,
    settled: Vec<(LaneNews, bool)>,
}

/// What [`Dispatcher::hand_news`] did with a note.
enum Handed {
    /// Stored as a lane row, or posted while the lead cannot start: handed on now.
    Now,
    /// Sent into the running turn; it settles when the turn ends.
    Sent,
    /// Not handed on; the next news step tries again.
    No,
}

pub struct Dispatcher {
    owner_id: String,
    /// Who may talk to moraebot, from the active messenger's config.
    access: Policy,
    /// Replies to listed bots per conversation, capped by `max_bot_replies`.
    bot_replies: Mutex<BotReplies>,
    /// Running chat turns; joined by [`Self::chats_done`] and at shutdown.
    chats: Mutex<tokio::task::JoinSet<()>>,
    adapter: Arc<dyn MessengerAdapter>,
    store: Arc<Mutex<Store>>,
    omp: OmpManager,
    /// Absolute work folder: received files are saved under it, sent files must be in it.
    work_dir: PathBuf,
    /// The moraebot home: never a source of sent files.
    home: PathBuf,
    /// The configured channel: the onboarding greeting goes there when asked, or when the
    /// owner's DM fails. Empty for a bridge whose hello names no `home`.
    channel_id: String,
    /// Held while a row is admitted and its thread opened, and while the worker dequeues,
    /// so the worker never picks a row whose thread does not exist yet.
    admission: tokio::sync::Mutex<()>,
    /// Rows whose reply send failed in this process; re-sent by [`Self::resend_pending`].
    pending: Mutex<Vec<i64>>,
    wake: Notify,
    /// Talk per conversation key, for lane news.
    talk: Mutex<HashMap<String, Talk>>,
    /// Lane news sent into a running turn as notes, until each is settled.
    news_notes: Arc<Mutex<NewsNotes>>,
    /// Owner status questions are answered by moraebot itself (`lead.direct_status`).
    direct_status: bool,
    /// The memory store; without it no message is a memory command.
    memory: Option<MemoryHandle>,
    /// The id each conversation was last asked to confirm forgetting.
    forget_asked: Mutex<HashMap<String, String>>,
    /// When each conversation was last asked to confirm a new session while lanes ran.
    session_asked: Mutex<HashMap<String, Instant>>,
    /// Command rows that confirmed their conversation's open new-session question on arrival.
    session_confirmed: Mutex<std::collections::HashSet<i64>>,
    /// Owner rows whose correction capture ran on arrival ([`lessons::capture`]) and that were
    /// queued, with the note it gave: the worker reuses it instead of capturing the row again.
    captured: Mutex<HashMap<i64, Option<String>>>,
    /// What the start check found about omp; a warning is sent to the owner once per version.
    omp_check: Option<OverlayCheck>,
    /// `[memory.self]`: owner corrections are captured with it ([`lessons::capture`]).
    self_config: SelfConfig,
    /// `memory.recall.mode` and the mid-task mode, for the status answer.
    recall_modes: (&'static str, &'static str),
}

impl Dispatcher {
    pub fn new(
        config: &Config,
        adapter: Arc<dyn MessengerAdapter>,
        store: Arc<Mutex<Store>>,
        omp: OmpManager,
    ) -> Self {
        let channel_id = adapter.home_target().unwrap_or_else(|| config.channel_id());
        Self {
            owner_id: config.owner_id(),
            access: Policy::from_config(config),
            bot_replies: Mutex::new(BotReplies::from_config(config)),
            chats: Mutex::default(),
            adapter,
            store,
            omp,
            work_dir: std::path::absolute(&config.work_dir)
                .unwrap_or_else(|_| config.work_dir.clone()),
            home: config.home.clone(),
            channel_id,
            admission: tokio::sync::Mutex::new(()),
            pending: Mutex::new(Vec::new()),
            wake: Notify::new(),
            talk: Mutex::default(),
            news_notes: Arc::default(),
            direct_status: config.lead.direct_status(),
            memory: None,
            forget_asked: Mutex::default(),
            session_asked: Mutex::default(),
            session_confirmed: Mutex::default(),
            captured: Mutex::default(),
            omp_check: None,
            self_config: config.memory.self_file.clone(),
            recall_modes: (
                config.memory.recall_mode().as_str(),
                config.memory.recall_mid_task().as_str(),
            ),
        }
    }

    /// Turns on the owner memory commands, served by `memory`.
    pub fn with_memory(mut self, memory: MemoryHandle) -> Self {
        self.memory = Some(memory);
        self
    }

    /// What the start check found about omp: [`Self::startup`] tells the owner about a version
    /// warning once per omp version.
    pub fn with_omp_check(mut self, check: OverlayCheck) -> Self {
        self.omp_check = Some(check);
        self
    }

    fn store(&self) -> MutexGuard<'_, Store> {
        lock(&self.store)
    }

    /// Startup recovery: interrupted rows become `needs_check` (never re-prompted) and the
    /// queued rows of their conversations are held, each conversation with unannounced
    /// rows gets one notice, rows that failed for their files get their pending notice, a
    /// reply whose file upload was cut gets one file notice (the file is skipped), stored
    /// replies are re-sent. A row cut off by a crash and one marked by a graceful shutdown
    /// are treated the same. A lane a restart interrupted gets one notice in its
    /// conversation. Then an onboarding request gets its one greeting, and last an omp
    /// version warning is sent once per version ([`N_OMP_VERSION`]).
    pub async fn startup(&self) -> Result<(), StoreError> {
        let recovered = self.store().recover_on_start()?;
        self.post_needs_check_notices().await?;
        self.post_lane_notices().await?;
        self.post_failed_file_notices().await?;
        for id in recovered.interrupted_uploads {
            let Some(row) = self.store().get(id)? else {
                continue;
            };
            tracing::warn!(row = id, "file upload was cut by a restart, file skipped");
            if let Some(target) = reply_target(&row.conversation_key) {
                let mut notice_sent = row.notice_sent;
                self.file_not_sent(id, target, &mut notice_sent).await?;
            }
        }
        let starters = self.store().queued_thread_starters()?;
        for row in starters {
            if !self
                .store()
                .is_known_thread(&row.platform, &row.message_id)?
            {
                self.open_missing_thread(&row).await?;
            }
        }
        for row in recovered.reply_pending {
            self.send_reply(row.id).await?;
        }
        self.greet_if_requested().await?;
        self.tell_omp_version().await
    }

    /// Sends [`N_OMP_VERSION`] when the start check warned and no `W_OMP_VERSION` log row
    /// names this omp version: in the owner's DM, or in the configured channel when the DM
    /// fails. The log row is written only after a send, so a notice that reached nobody is
    /// tried again at the next start. Log rows are pruned after 30 days, so a lasting mismatch
    /// is mentioned at most once a month.
    async fn tell_omp_version(&self) -> Result<(), StoreError> {
        let Some(check) = self.omp_check.as_ref().filter(|c| c.warning.is_some()) else {
            return Ok(());
        };
        if self.store().memory_logged(W_OMP_VERSION, &check.version)? {
            return Ok(());
        }
        let sent =
            self.notify_in_dm(N_OMP_VERSION).await || self.notify_in_channel(N_OMP_VERSION).await;
        if !sent {
            tracing::warn!(omp = %check.version, "omp version notice not sent");
            return Ok(());
        }
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        self.store().memory_log(&LogRow {
            at,
            level: LogLevel::Warn,
            code: W_OMP_VERSION,
            job_id: None,
            omp_version: Some(check.version.clone()),
            detail: Some(check.version.clone()),
        })
    }

    /// Starts the onboarding and posts [`N_ONBOARDING_HELLO`] once when a request file is
    /// present: in the owner's DM unless the request says `channel`, and in the configured
    /// channel when the DM cannot be opened or the send fails. The file is removed before the
    /// greeting; when it cannot be removed nothing is sent (no greeting at every start). A
    /// greeting that failed in both places is not retried.
    async fn greet_if_requested(&self) -> Result<(), StoreError> {
        let path = onboarding::request_file(&self.home);
        if std::fs::symlink_metadata(&path).is_err() {
            return Ok(());
        }
        let target = onboarding::read_request(&self.home);
        self.store().start_onboarding()?;
        if std::fs::remove_file(&path).is_err() {
            tracing::warn!(
                code = E_ONBOARDING_REQUEST,
                "onboarding request not removed; no greeting"
            );
            return Ok(());
        }
        let place = match target {
            GreetTarget::Dm if self.greet_in_dm().await => Some(GreetTarget::Dm),
            _ => self
                .notify_in_channel(N_ONBOARDING_HELLO)
                .await
                .then_some(GreetTarget::Channel),
        };
        if let Some(place) = place {
            tracing::info!(place = place.as_str(), "onboarding greeting sent");
        }
        Ok(())
    }

    /// Opens the owner's DM and sends the greeting there; false when either step fails.
    async fn greet_in_dm(&self) -> bool {
        self.notify_in_dm(N_ONBOARDING_HELLO).await
    }

    /// Sends notice `code` in the configured channel; false when the send fails or there is
    /// no channel, which logs [`W_NO_HOME_CHANNEL`] instead of sending to an empty target.
    async fn notify_in_channel(&self, code: &'static str) -> bool {
        if self.channel_id.is_empty() {
            tracing::warn!(
                code = W_NO_HOME_CHANNEL,
                notice = code,
                "no home channel; channel fallback skipped"
            );
            return false;
        }
        self.notify(&self.channel_id, code).await
    }

    /// Opens the owner's DM and sends notice `code` there; false when either step fails.
    async fn notify_in_dm(&self, code: &'static str) -> bool {
        match self.adapter.open_dm(&self.owner_id).await {
            Ok(target) => self.notify(&target, code).await,
            Err(err) => {
                tracing::warn!(code = err.code, notice = code, "owner DM not opened");
                false
            }
        }
    }

    /// Runs startup, then serves inbound events and the worker until `inbound` closes.
    /// The worker, the lane watchdog and the lane news never end on their own: when one does
    /// (or panics), the dispatcher shuts down with [`GatewayError::TaskEnded`], so the
    /// process exits non-zero and its service manager restarts it.
    pub async fn run(
        self: Arc<Self>,
        inbound: mpsc::Receiver<InboundEvent>,
    ) -> Result<(), GatewayError> {
        let reconnects = self.adapter.reconnects();
        self.startup().await?;
        let mut tasks = Supervised::default();
        tasks.spawn("worker", {
            let this = Arc::clone(&self);
            async move { this.work_loop(reconnects).await }
        });
        tasks.spawn("lane watchdog", {
            let this = Arc::clone(&self);
            async move { this.omp.lanes().watch().await }
        });
        tasks.spawn("lane news", {
            let this = Arc::clone(&self);
            async move { this.news_loop().await }
        });
        self.serve(inbound, tasks).await
    }

    /// Serves inbound events until `inbound` closes or one of `tasks` ends, then stops the
    /// tasks and shuts down.
    async fn serve(
        &self,
        mut inbound: mpsc::Receiver<InboundEvent>,
        mut tasks: Supervised,
    ) -> Result<(), GatewayError> {
        let ended = loop {
            tokio::select! {
                event = inbound.recv() => match event {
                    Some(event) => {
                        let message_id = event.message_id.clone();
                        match self.handle_event(event).await {
                            // Every handled outcome is acked; a store error is not, so a
                            // spooling bridge delivers the message again.
                            Ok(_) => self.adapter.ack_inbound(&message_id).await,
                            Err(err) => tracing::error!(code = err.code(), error = %err, "inbound event not handled"),
                        }
                    }
                    None => break None,
                },
                name = tasks.ended() => break Some(name),
            }
        };
        tasks.set.shutdown().await;
        self.shutdown().await;
        match ended {
            Some(name) => Err(GatewayError::TaskEnded(name)),
            None => Ok(()),
        }
    }

    /// Stops the lead child, even in the middle of a turn, and every lane child.
    pub async fn shutdown(&self) {
        self.omp.shutdown_all().await;
        let chats = std::mem::take(&mut *lock(&self.chats));
        if tokio::time::timeout(ABORT_JOIN_BOUND, chats.join_all())
            .await
            .is_err()
        {
            tracing::error!("a chat turn did not stop in time");
        }
    }

    /// Waits until every chat turn started so far has sent its reply or notice.
    pub async fn chats_done(&self) {
        let chats = std::mem::take(&mut *lock(&self.chats));
        chats.join_all().await;
    }

    /// Runs a listed chat sender's message on its own task (see the module docs). A
    /// channel post first gets its own thread, so its conversation continues there.
    async fn start_chat(
        &self,
        event: InboundEvent,
        key: ConversationKey,
        new_thread: bool,
    ) -> Handled {
        let chat = self.omp.chat();
        if !chat.first_delivery(&event.message_id) {
            tracing::info!(message_id = %event.message_id, "chat message redelivered, not run again");
            return Handled::Duplicate;
        }
        if new_thread
            && let Err(err) = self
                .adapter
                .create_thread(&event.channel_id, &event.message_id, &event.text)
                .await
        {
            tracing::warn!(message_id = %event.message_id, code = err.code, "chat thread not opened");
            chat.forget_delivery(&event.message_id);
            self.notify(&event.channel_id, N_THREAD_FAILED).await;
            return Handled::Notice(N_THREAD_FAILED);
        }
        let Some(target) = reply_target(key.as_str()).map(str::to_owned) else {
            return Handled::Notice(N_FAILED);
        };
        let turn = ChatTurn {
            adapter: Arc::clone(&self.adapter),
            chat,
            key: key.as_str().to_owned(),
            target,
            event,
        };
        let mut chats = lock(&self.chats);
        // Reap finished turns so the set holds running ones only.
        while let Some(done) = chats.try_join_next() {
            if let Err(e) = done {
                tracing::error!(error = %e, "a chat turn panicked");
            }
        }
        chats.spawn(turn.run());
        Handled::Chat
    }

    /// `Some(conversation key)` while a lead turn that accepts a steer runs.
    pub fn running_turn(&self) -> watch::Receiver<Option<String>> {
        self.omp.running_turn()
    }

    /// Serves the queue and re-sends failed replies after each adapter reconnect. Both run
    /// on this one task, so a reconnect seen during a turn is handled right after it, once
    /// the turn's failed send was remembered.
    async fn work_loop(&self, mut reconnects: Option<watch::Receiver<u64>>) {
        let lane_wake = self.omp.lane_wake();
        loop {
            if let Some(rx) = reconnects.as_mut()
                && rx.has_changed().unwrap_or(false)
            {
                rx.borrow_and_update();
                self.resend_after_reconnect().await;
            }
            let idle = match self.process_next().await {
                Ok(more) => !more,
                Err(err) => {
                    tracing::error!(code = err.code(), error = %err, "worker step failed");
                    true
                }
            };
            if idle {
                tokio::select! {
                    biased;
                    () = next_reconnect(&mut reconnects) => self.resend_after_reconnect().await,
                    () = self.wake.notified() => {}
                    () = lane_wake.notified() => {}
                }
            }
        }
    }

    async fn resend_after_reconnect(&self) {
        match self.resend_pending().await {
            Ok(delivered) => tracing::info!(delivered, "pending replies re-sent after reconnect"),
            Err(err) => {
                tracing::error!(code = err.code(), error = %err, "pending replies not re-sent")
            }
        }
    }

    /// Access check, bot reply limit, chat notice, input filter, file count, store-first
    /// admission, thread creation, then the attachment downloads outside the admission lock.
    pub async fn handle_event(&self, event: InboundEvent) -> Result<Handled, StoreError> {
        let sender = match self.access.allow(&event) {
            Decision::Allow(sender) => sender,
            Decision::Drop(reason) => {
                tracing::info!(
                    message_id = %event.message_id,
                    author_id = %event.author_id,
                    code = reason.code(),
                    "inbound message dropped"
                );
                return Ok(Handled::Dropped(reason));
            }
        };

        let platform = self.adapter.platform();
        let (key, new_thread) = match (&event.thread_id, event.is_dm) {
            (Some(thread), true) => (ConversationKey::thread(platform, thread), false),
            (None, true) => (ConversationKey::dm(platform, &event.channel_id), false),
            (Some(thread), false) => (ConversationKey::thread(platform, thread), false),
            (None, false) => (ConversationKey::thread(platform, &event.message_id), true),
        };
        // A message posted directly in the channel opens its own thread, so its key is new
        // every time: bot messages there count under the channel instead.
        let bot_key = if new_thread {
            format!("{platform}:ch:{}", event.channel_id)
        } else {
            key.as_str().to_owned()
        };
        if event.author_is_bot && !lock(&self.bot_replies).take(&bot_key, Instant::now()) {
            tracing::info!(
                message_id = %event.message_id,
                author_id = %event.author_id,
                conversation = bot_key.as_str(),
                code = DropReason::BotLimit.code(),
                "bot reply limit reached, message dropped"
            );
            return Ok(Handled::Dropped(DropReason::BotLimit));
        }

        let here = event.thread_id.as_deref().unwrap_or(&event.channel_id);
        let text = event.text.trim();
        let owner_command = sender == Sender::Owner && new_session::request(text).is_some();
        if (text.starts_with('/') && !owner_command)
            || (text.is_empty() && event.attachments.is_empty())
        {
            tracing::info!(message_id = %event.message_id, code = N_UNSUPPORTED_INPUT, "input not forwarded");
            self.notify(here, N_UNSUPPORTED_INPUT).await;
            return Ok(Handled::Notice(N_UNSUPPORTED_INPUT));
        }
        if owner_command && !event.attachments.is_empty() {
            tracing::info!(message_id = %event.message_id, code = N_NEW_SESSION_FILES, "new session with attachments not stored");
            self.notify(here, N_NEW_SESSION_FILES).await;
            return Ok(Handled::Notice(N_NEW_SESSION_FILES));
        }
        if event.attachments.len() > files::MAX_FILES {
            tracing::info!(
                message_id = %event.message_id,
                count = event.attachments.len(),
                code = files::E_FILE_TOO_MANY,
                "too many attachments, message not stored"
            );
            self.notify(here, N_FILE_NOT_RECEIVED).await;
            return Ok(Handled::FilesRejected(N_FILE_NOT_RECEIVED));
        }
        if let Sender::Listed(role) = sender {
            tracing::info!(
                message_id = %event.message_id,
                author_id = %event.author_id,
                role = role.as_str(),
                conversation = key.as_str(),
                "chat message handed to the chat runtime"
            );
            return Ok(self.start_chat(event, key, new_thread).await);
        }

        let attachments_json =
            (!event.attachments.is_empty()).then(|| declared_json(&event.attachments));
        let id = {
            let _gate = self.admission.lock().await;
            // A DM thread already exists (a root DM is its own thread), so it is never opened.
            if event.is_dm
                && let Some(thread) = &event.thread_id
            {
                self.store().remember_thread(platform, thread)?;
            }
            let admitted = self.store().admit_with_files(
                platform,
                &event.message_id,
                key.as_str(),
                &event.channel_id,
                &event.author_id,
                &event.text,
                attachments_json.as_deref(),
            )?;
            let id = match admitted {
                Admitted::Duplicate => {
                    tracing::info!(message_id = %event.message_id, "duplicate message ignored");
                    return Ok(Handled::Duplicate);
                }
                Admitted::New(id) => id,
            };
            self.talked(key.as_str());

            if new_thread {
                match self
                    .adapter
                    .create_thread(&event.channel_id, &event.message_id, &event.text)
                    .await
                {
                    Ok(thread) => self.store().remember_thread(platform, &thread)?,
                    Err(err) => {
                        self.thread_failed(id, err.code, &event.channel_id).await?;
                        return Ok(Handled::ThreadFailed(id));
                    }
                }
            }
            id
        };

        if sender == Sender::Owner {
            // Any owner message closes this conversation's new-session question; only a
            // confirmation that arrives while it is open counts.
            let question = lock(&self.session_asked).remove(key.as_str());
            if let Some(asked) = new_session::request(&event.text) {
                // Queued for the worker, never steered: a running turn ends first.
                lock(&self.forget_asked).remove(key.as_str());
                let open = question.is_some_and(|at| {
                    Instant::now().saturating_duration_since(at) < new_session::CONFIRM_WINDOW
                });
                if asked.confirm && open {
                    lock(&self.session_confirmed).insert(id);
                }
                tracing::info!(row = id, conversation = key.as_str(), "new session queued");
                self.wake.notify_one();
                return Ok(Handled::Queued(id));
            }
            if event.attachments.is_empty() {
                if let Some(handled) = self
                    .refresh_command(id, key.as_str(), &event.text, true)
                    .await?
                {
                    return Ok(handled);
                }
                if let Some(handled) = self
                    .memory_command(id, key.as_str(), &event.text, true)
                    .await?
                {
                    return Ok(handled);
                }
                if let Some(handled) = self
                    .status_command(id, key.as_str(), &event.text, true)
                    .await?
                {
                    return Ok(handled);
                }
            } else {
                lock(&self.forget_asked).remove(key.as_str());
            }
        }

        let mut saved: Vec<SavedFile> = Vec::new();
        if !event.attachments.is_empty() {
            let fetched = self.fetch_files(id, &key, &event.attachments).await;
            let _gate = self.admission.lock().await;
            match fetched {
                Ok(files) => {
                    let json = serde_json::to_string(&files)
                        .expect("saved-file metadata serializes to JSON");
                    self.store().files_saved(id, &json)?;
                    tracing::info!(row = id, count = files.len(), "attachments saved");
                    saved = files;
                }
                Err(err) => {
                    tracing::warn!(row = id, code = err.code, "attachments not received");
                    let notice = file_notice(err.code);
                    let failed = self.store().files_failed(id, err.code)?;
                    if failed
                        && let Some(target) = reply_target(key.as_str())
                        && self.notify(target, notice).await
                    {
                        self.store().mark_notice_sent(id)?;
                    }
                    return Ok(Handled::FilesRejected(notice));
                }
            }
        }
        // The same body the worker would send for this row, with the correction note. The
        // capture runs once per row: it is kept for the worker (before the steer, so a worker
        // that takes the row at once finds it) and dropped when the row is steered.
        let mut body = lead::owner_input(
            key.as_str(),
            id,
            &files::prompt_with_files(&event.text, &saved),
        );
        if sender == Sender::Owner
            && let Some(memory) = &self.memory
        {
            let note = lessons::capture(memory, &self.self_config, id, &event.text);
            if let Some(note) = &note {
                body = format!("{body}\n\n{note}");
            }
            lock(&self.captured).insert(id, note);
        }
        let mut claim_error = None;
        let steered = self.omp.try_steer(key.as_str(), id, sender, &body, |main| {
            self.store().steer_into(id, main).unwrap_or_else(|err| {
                claim_error = Some(err);
                false
            })
        });
        if steered {
            lock(&self.captured).remove(&id);
            return Ok(Handled::Steered(id));
        }
        tracing::info!(row = id, conversation = key.as_str(), "message queued");
        self.wake.notify_one();
        match claim_error {
            Some(err) => Err(err),
            None => Ok(Handled::Queued(id)),
        }
    }

    /// Fetches every attachment of row `id` into its own inbox folder, one after another,
    /// each under the stall watchdog with a fresh progress record. The folder is created
    /// here (0700 levels); adapters never create directories.
    async fn fetch_files(
        &self,
        id: i64,
        key: &ConversationKey,
        attachments: &[InboundAttachment],
    ) -> Result<Vec<SavedFile>, AdapterError> {
        let dir = files::inbox_dir(&self.work_dir, &key_slug(key.as_str()), id);
        files::create_private_dir_all(&dir)?;
        let mut saved = Vec::with_capacity(attachments.len());
        for (index, attachment) in attachments.iter().enumerate() {
            let name = files::sanitize_name(&attachment.name, index + 1);
            let path = dir.join(&name);
            let progress = files::Progress::new();
            let bytes = files::with_stall_timeout(
                self.adapter.fetch_attachment(attachment, &path, &progress),
                &progress,
            )
            .await?;
            tracing::info!(row = id, index, bytes, mime = ?attachment.mime, "attachment saved");
            saved.push(SavedFile {
                path,
                name,
                mime: attachment.mime.clone(),
                bytes,
            });
        }
        Ok(saved)
    }

    /// Serves the oldest queued row: claims it (a row a steer claimed first is skipped),
    /// runs one lead turn for it and settles the turn's group. Returns `false` when the
    /// queue is empty.
    pub async fn process_next(&self) -> Result<bool, StoreError> {
        let row = {
            let _gate = self.admission.lock().await;
            let Some(row) = self.store().next_queued()? else {
                return Ok(false);
            };
            if !self.store().claim(row.id)? {
                return Ok(true);
            }
            row
        };
        // Taken whatever happens to the row below, so no capture is left behind.
        let captured = lock(&self.captured).remove(&row.id);
        let key = row.conversation_key.as_str();
        let from_owner = row.origin == "owner";
        if !from_owner && self.stale_news(&row)? {
            return Ok(true);
        }

        if from_owner && row.author_id != self.owner_id {
            tracing::warn!(
                row = row.id,
                code = E_ACCESS_NOT_OWNER,
                "stored author is not the owner"
            );
            self.store()
                .mark(row.id, State::Failed, Some(E_ACCESS_NOT_OWNER))?;
            return Ok(true);
        }
        let Some(target) = reply_target(key) else {
            self.store()
                .mark(row.id, State::Failed, Some(E_DISPATCH_KEY))?;
            return Ok(true);
        };
        if is_thread_starter(&row)
            && !self
                .store()
                .is_known_thread(&row.platform, &row.message_id)?
            && !self.open_missing_thread(&row).await?
        {
            return Ok(true);
        }

        if from_owner && row.attachments.is_none() {
            if self.new_session_command(row.id, key, &row.text).await? {
                return Ok(true);
            }
            if self
                .refresh_command(row.id, key, &row.text, false)
                .await?
                .is_some()
            {
                return Ok(true);
            }
            if self
                .memory_command(row.id, key, &row.text, false)
                .await?
                .is_some()
            {
                return Ok(true);
            }
            if self
                .status_command(row.id, key, &row.text, false)
                .await?
                .is_some()
            {
                return Ok(true);
            }
        }

        let prompt = if from_owner {
            let saved: Vec<SavedFile> = match row.attachments.as_deref().map(serde_json::from_str) {
                None => Vec::new(),
                Some(Ok(saved)) => saved,
                Some(Err(_)) => {
                    tracing::warn!(
                        row = row.id,
                        code = files::E_FILE_INCOMPLETE,
                        "stored file list unreadable"
                    );
                    self.store()
                        .mark(row.id, State::Failed, Some(files::E_FILE_INCOMPLETE))?;
                    if self.notify(target, N_FILE_NOT_RECEIVED).await {
                        self.store().mark_notice_sent(row.id)?;
                    }
                    return Ok(true);
                }
            };
            let mut input =
                lead::owner_input(key, row.id, &files::prompt_with_files(&row.text, &saved));
            // A row queued after an arrival capture reuses it; one queued before a restart is
            // captured now (the same source saves nothing twice).
            let note = match captured {
                Some(note) => note,
                None => self.memory.as_ref().and_then(|memory| {
                    lessons::capture(memory, &self.self_config, row.id, &row.text)
                }),
            };
            if let Some(note) = note {
                input = format!("{input}\n\n{note}");
            }
            match self
                .store()
                .onboarding_turn(row.id)?
                .filter(|n| (1..=onboarding::MAX_TURNS).contains(n))
            {
                Some(n) => onboarding::mark_input(&input, n),
                None => input,
            }
        } else {
            // A lane result row stores the lead input `finish_lane` built.
            row.text.clone()
        };

        let typing = match self.adapter.start_typing(target).await {
            Ok(handle) => Some(handle),
            Err(err) => {
                tracing::warn!(row = row.id, code = err.code, "typing indicator failed");
                None
            }
        };
        // Only owner rows are stored and run. A lane-result row acts for the owner who
        // started the lane.
        let group = self.omp.prompt(key, row.id, Sender::Owner, &prompt).await;
        drop(typing);
        self.settle_group(row.id, target, group).await?;
        Ok(true)
    }

    /// Answers owner row `id` of conversation `key` when its `text` is a memory command (or,
    /// on `arrival`, the answer to this conversation's forget question): the reply is stored
    /// and sent like a lead reply and the row ends `done`. On `arrival` the row is still
    /// `queued` and is claimed first (a row the worker took meanwhile is left to it), and any
    /// other message drops the forget question. The worker passes `arrival = false` for its
    /// already claimed row: it never answers a forget question, because the row may predate
    /// it. Returns `None` when the row is not for the memory store.
    async fn memory_command(
        &self,
        id: i64,
        key: &str,
        text: &str,
        arrival: bool,
    ) -> Result<Option<Handled>, StoreError> {
        let Some(memory) = &self.memory else {
            return Ok(None);
        };
        let asked = if arrival {
            lock(&self.forget_asked).get(key).cloned()
        } else {
            None
        };
        let act = asked
            .and_then(|target| {
                memory_commands::confirmation(text).map(|yes| MemoryAct::Confirm(target, yes))
            })
            .or_else(|| memory_commands::parse(text).map(MemoryAct::Run))
            .or_else(|| memory_commands::recent_recalls_request(text).map(MemoryAct::Recalls));
        let Some(act) = act else {
            if arrival {
                lock(&self.forget_asked).remove(key);
            }
            return Ok(None);
        };
        if arrival {
            let _gate = self.admission.lock().await;
            if !self.store().claim(id)? {
                return Ok(None);
            }
            lock(&self.forget_asked).remove(key);
        }
        let reply = match act {
            MemoryAct::Confirm(target, yes) => memory_commands::forget(memory, &target, yes).await,
            MemoryAct::Recalls(lang) => {
                let rows = self
                    .store()
                    .recent_recalls(memory_commands::RECENT_RECALLS)?;
                memory_commands::recent_recalls(memory, lang, &rows, unix_now())
            }
            MemoryAct::Run(command) => {
                let answer = memory_commands::run(memory, key, id, command).await;
                if let Some(target) = answer.confirm_forget {
                    lock(&self.forget_asked).insert(key.to_owned(), target);
                }
                answer.reply
            }
        };
        tracing::info!(row = id, conversation = key, "memory command answered");
        self.store().finish_direct(id, &reply)?;
        self.send_reply(id).await?;
        Ok(Some(Handled::Memory(id)))
    }

    /// Answers the worker's claimed owner row `id` of conversation `key` when its whole `text` is
    /// the new-session command ([`new_session::request`]): while lanes run and the row did not
    /// confirm an open question on arrival (`session_confirmed`), it asks and changes nothing
    /// (a row queued without that check, after a restart, asks again); otherwise the
    /// lead gets a new session ([`OmpManager::new_session`]). The answer is stored and sent like a
    /// lead reply and the row ends `done`. Returns whether the row was the command.
    async fn new_session_command(
        &self,
        id: i64,
        key: &str,
        text: &str,
    ) -> Result<bool, StoreError> {
        let Some(asked) = new_session::request(text) else {
            return Ok(false);
        };
        let confirmed = lock(&self.session_confirmed).remove(&id);
        let lanes = self
            .store()
            .lanes()?
            .iter()
            .filter(|lane| lane.state == LaneState::Running)
            .count();
        let reply = if lanes > 0 && !confirmed {
            lock(&self.session_asked).insert(key.to_owned(), Instant::now());
            tracing::info!(
                row = id,
                conversation = key,
                lanes,
                "new session needs a confirmation"
            );
            new_session::confirm_text(asked.lang, lanes)
        } else {
            match self.omp.new_session().await {
                Ok(backup) => {
                    let shown = backup.map(|path| self.omp.clean_note(&path.to_string_lossy()));
                    new_session::opened_text(asked.lang, shown.as_deref())
                }
                Err(err) => {
                    tracing::error!(code = E_LEAD_NEW_SESSION, error = %err, "lead session folder not moved");
                    new_session::failed_text(asked.lang, E_LEAD_NEW_SESSION)
                }
            }
        };
        self.store().finish_direct(id, &reply)?;
        self.send_reply(id).await?;
        Ok(true)
    }

    /// Answers owner row `id` of conversation `key` when its whole `text` is a status question
    /// ([`status::request`]): the answer is stored and sent like a lead reply and the row ends
    /// `done`, so the lead never sees it. On `arrival` the row is still `queued` and is claimed
    /// first (a row the worker took meanwhile is left to it); the worker passes `arrival =
    /// false` for its already claimed row. Returns `None` when the row is not a status question.
    async fn status_command(
        &self,
        id: i64,
        key: &str,
        text: &str,
        arrival: bool,
    ) -> Result<Option<Handled>, StoreError> {
        let Some(lang) = status::request(text).filter(|_| self.direct_status) else {
            return Ok(None);
        };
        if arrival {
            let _gate = self.admission.lock().await;
            if !self.store().claim(id)? {
                return Ok(None);
            }
        }
        let reply = self.status_reply(lang, key)?;
        tracing::info!(row = id, conversation = key, "status question answered");
        self.store().finish_direct(id, &reply)?;
        self.send_reply(id).await?;
        Ok(Some(Handled::Status(id)))
    }

    /// Answers owner row `id` of conversation `key` when its whole `text` is the refresh command
    /// ([`memory_commands::refresh_request`]): the lead's system prompt is set again before its
    /// next turn ([`OmpManager::refresh_memory`]), and the short answer is stored and sent like a
    /// status answer. On `arrival` the row is claimed first, like [`Self::status_command`], and
    /// an open forget question of the conversation is dropped. Returns `None` when the row is not
    /// the command.
    async fn refresh_command(
        &self,
        id: i64,
        key: &str,
        text: &str,
        arrival: bool,
    ) -> Result<Option<Handled>, StoreError> {
        let Some(lang) = memory_commands::refresh_request(text) else {
            return Ok(None);
        };
        if arrival {
            lock(&self.forget_asked).remove(key);
            let _gate = self.admission.lock().await;
            if !self.store().claim(id)? {
                return Ok(None);
            }
        }
        self.omp.refresh_memory();
        tracing::info!(row = id, conversation = key, "memory refresh requested");
        self.store()
            .finish_direct(id, memory_commands::refresh_text(lang))?;
        self.send_reply(id).await?;
        Ok(Some(Handled::Memory(id)))
    }

    /// The status answer for conversation `key`: the lead's turn and the running lanes.
    fn status_reply(&self, lang: status::Lang, key: &str) -> Result<String, StoreError> {
        let now = Instant::now();
        let lead = self.omp.lead_status(now);
        let since = self.lanes().running_since();
        let running: Vec<LaneRow> = self
            .store()
            .lanes()?
            .into_iter()
            .filter(|lane| lane.state == LaneState::Running)
            .collect();
        let (mine, others): (Vec<LaneRow>, Vec<LaneRow>) = running
            .into_iter()
            .partition(|lane| lane.conversation_key == key);
        let lanes: Vec<status::LaneLine> = mine
            .iter()
            .map(|lane| status::LaneLine {
                id: lane.id,
                running: since
                    .get(&lane.id)
                    .map(|started| now.saturating_duration_since(*started)),
                task: &lane.task,
            })
            .collect();
        Ok(status::reply(
            lang,
            key,
            lead.as_ref(),
            &self.omp.lead_cache(),
            Some(&self.recall_status()?),
            &lanes,
            others.len(),
            &|text| self.omp.clean_note(text),
        ))
    }

    /// The recall advisor's modes and recent use, for the status answer.
    fn recall_status(&self) -> Result<status::RecallStatus, StoreError> {
        let now = unix_now();
        let store = self.store();
        Ok(status::RecallStatus {
            mode: self.recall_modes.0,
            mid: self.recall_modes.1,
            calls_last_hour: store.recall_calls_since(now - 3600)?,
            last_delivered: store.last_delivered_recall()?.map(|at| now - at),
        })
    }

    /// Drops lane news row `row` when its lane no longer runs: the note is out of date and the
    /// lane's result follows it. Returns whether the row was dropped.
    fn stale_news(&self, row: &InboxRow) -> Result<bool, StoreError> {
        let Some(lane) = lead::parse_tag(&row.text)
            .filter(|tag| tag.news)
            .and_then(|tag| tag.lane)
        else {
            return Ok(false);
        };
        let running = self
            .store()
            .lane(lane)?
            .is_some_and(|lane| lane.state == LaneState::Running);
        if running {
            return Ok(false);
        }
        tracing::info!(
            row = row.id,
            lane,
            code = E_LANE_NEWS_STALE,
            "stale lane news dropped"
        );
        self.store()
            .mark(row.id, State::Failed, Some(E_LANE_NEWS_STALE))?;
        Ok(true)
    }

    /// Stores the outcome of a lead turn and its steered row, if any: one reply on the
    /// main row (the steered row `done`), else every member `needs_check` with the
    /// conversation's notice, else every member `failed` with one notice.
    async fn settle_group(
        &self,
        main_id: i64,
        target: &str,
        group: GroupReport,
    ) -> Result<(), StoreError> {
        let GroupReport {
            main,
            steered,
            trace,
        } = group;
        let steered_id = steered.as_ref().map(|(id, _)| *id);
        let mut ids = vec![main_id];
        ids.extend(steered_id);
        let outcomes: Vec<&Outcome> = std::iter::once(&main.outcome)
            .chain(steered.as_ref().map(|(_, outcome)| outcome))
            .collect();

        if let Some(code) = outcomes.iter().find_map(|outcome| match outcome {
            Outcome::NeedsCheck { code, .. } => Some(*code),
            _ => None,
        }) {
            tracing::warn!(row = main_id, steered = ?steered_id, code, "turn interrupted");
            self.store().interrupt_group(&ids, code, &trace)?;
            self.post_needs_check_notices().await?;
            return Ok(());
        }
        if let Some(code) = outcomes.iter().find_map(|outcome| match outcome {
            Outcome::Failed { code, .. } => Some(*code),
            _ => None,
        }) {
            tracing::warn!(row = main_id, steered = ?steered_id, code, "turn failed");
            self.store().fail_group(&ids, code, &trace)?;
            let notice = if code == E_COST_CAP {
                N_COST_CAP
            } else {
                N_FAILED
            };
            self.notify(target, notice).await;
            return Ok(());
        }

        let reply = combine_replies(outcomes.iter().filter_map(|outcome| reply_text(outcome)));
        let out_files = out_files_json(main.files);
        self.store()
            .finish_group(main_id, &reply, out_files.as_deref(), steered_id, &trace)?;
        self.send_reply(main_id).await?;
        Ok(())
    }

    /// Opens the thread of a queued thread-starter row whose creation never completed.
    /// Returns whether the row may run; otherwise it is `failed` and announced.
    async fn open_missing_thread(&self, row: &InboxRow) -> Result<bool, StoreError> {
        let Some(channel) = self.store().channel_of(&row.conversation_key)? else {
            self.store()
                .mark(row.id, State::Failed, Some(E_DISPATCH_KEY))?;
            return Ok(false);
        };
        match self
            .adapter
            .create_thread(&channel, &row.message_id, &row.text)
            .await
        {
            Ok(thread) => {
                self.store().remember_thread(&row.platform, &thread)?;
                tracing::info!(row = row.id, "missing thread opened");
                Ok(true)
            }
            Err(err) => {
                self.thread_failed(row.id, err.code, &channel).await?;
                Ok(false)
            }
        }
    }

    /// Marks a row whose thread could not be opened `failed` and posts one notice in the
    /// channel; `notice_sent` records the delivery.
    async fn thread_failed(&self, id: i64, code: &str, channel: &str) -> Result<(), StoreError> {
        tracing::warn!(row = id, code, "thread creation failed");
        self.store().mark(id, State::Failed, Some(code))?;
        if self.notify(channel, N_THREAD_FAILED).await {
            self.store().mark_notice_sent(id)?;
        }
        Ok(())
    }

    /// Re-sends replies whose earlier send failed (call after a reconnect). Returns how many
    /// were delivered.
    pub async fn resend_pending(&self) -> Result<usize, StoreError> {
        let ids = std::mem::take(&mut *lock(&self.pending));
        let mut delivered = 0;
        for id in ids {
            let Some(row) = self.store().get(id)? else {
                continue;
            };
            if row.state == State::ReplyPending && self.send_reply(row.id).await? {
                delivered += 1;
            }
        }
        Ok(delivered)
    }

    /// Sends a stored reply: its text chunks unless already delivered (`text_sent`), then
    /// each outbound file not yet sent or skipped. The row becomes `done` only when the text
    /// was accepted and every file is sent or skipped. A failed text send or a transient
    /// upload error keeps the row `reply_pending` and remembers it for a resend.
    async fn send_reply(&self, id: i64) -> Result<bool, StoreError> {
        let Some(row) = self.store().get(id)? else {
            return Ok(false);
        };
        let Some(target) = reply_target(&row.conversation_key) else {
            self.store().mark(id, State::Failed, Some(E_DISPATCH_KEY))?;
            return Ok(false);
        };
        if !row.text_sent {
            let reply = row.reply.as_deref().unwrap_or_default();
            for chunk in reply_messages(reply, self.adapter.message_limit()) {
                if let Err(err) = self.adapter.send(target, &chunk).await {
                    tracing::warn!(
                        row = id,
                        code = err.code,
                        "reply send failed, kept as reply_pending"
                    );
                    self.remember_pending(id);
                    return Ok(false);
                }
            }
            self.store().mark_text_sent(id)?;
            self.talked(&row.conversation_key);
        }

        let out_files: Vec<OutFile> = match row.out_files.as_deref().map(serde_json::from_str) {
            None => Vec::new(),
            Some(Ok(out_files)) => out_files,
            Some(Err(_)) => return Err(StoreError::Invalid("stored out-file list is not valid")),
        };
        let mut notice_sent = row.notice_sent;
        for (index, file) in out_files.iter().enumerate() {
            if file.sent || file.skipped {
                continue;
            }
            let path = match files::check_outbound(&file.path, &self.work_dir, &self.home) {
                Ok((path, _)) => path,
                Err(err) => {
                    tracing::warn!(row = id, index, code = err.code, "outbound file skipped");
                    self.store().mark_file_skipped(id, index)?;
                    self.file_not_sent(id, target, &mut notice_sent).await?;
                    continue;
                }
            };
            if file.attempts >= files::MAX_UPLOAD_ATTEMPTS {
                tracing::warn!(
                    row = id,
                    index,
                    attempts = file.attempts,
                    "outbound file skipped after its last attempt"
                );
                self.store().mark_file_skipped(id, index)?;
                self.file_not_sent(id, target, &mut notice_sent).await?;
                continue;
            }
            self.store().mark_file_attempting(id, index)?;
            let sent = tokio::time::timeout(
                files::UPLOAD_TIMEOUT,
                self.adapter.send_file(target, &path, &file.name),
            )
            .await;
            if let Ok(Ok(_)) = sent {
                self.store().mark_file_sent(id, index)?;
                tracing::info!(row = id, index, "outbound file sent");
                continue;
            }
            self.store().clear_file_attempting(id, index)?;
            let code = match &sent {
                Ok(Err(err)) => err.code,
                _ => files::E_FILE_UPLOAD_TIMEOUT,
            };
            tracing::warn!(row = id, index, code, "outbound file not sent");
            self.file_not_sent(id, target, &mut notice_sent).await?;
            if code == E_ADAPTER_UPLOAD_DENIED || code == files::E_FILE_UPLOAD_TIMEOUT {
                self.store().mark_file_skipped(id, index)?;
                continue;
            }
            self.remember_pending(id);
            return Ok(false);
        }
        self.store().mark_done(id)?;
        Ok(true)
    }

    /// Remembers a `reply_pending` row for the next reconnect resend.
    fn remember_pending(&self, id: i64) {
        let mut pending = lock(&self.pending);
        if !pending.contains(&id) {
            pending.push(id);
        }
    }

    /// Posts the reply's one [`N_FILE_NOT_SENT`] unless it was already posted (`notice_sent`).
    async fn file_not_sent(
        &self,
        id: i64,
        target: &str,
        notice_sent: &mut bool,
    ) -> Result<(), StoreError> {
        if !*notice_sent && self.notify(target, N_FILE_NOT_SENT).await {
            self.store().mark_notice_sent(id)?;
            *notice_sent = true;
        }
        Ok(())
    }

    /// Posts the pending notice of each row that failed for its files: in its thread when
    /// that thread is known, else in the conversation's channel.
    async fn post_failed_file_notices(&self) -> Result<(), StoreError> {
        let rows = self.store().failed_notices()?;
        for row in rows {
            let error = row.error.as_deref().unwrap_or_default();
            let thread_known = match reply_target(&row.conversation_key) {
                Some(thread) => self.store().is_known_thread(&row.platform, thread)?,
                None => false,
            };
            let target = if thread_known {
                reply_target(&row.conversation_key).map(str::to_owned)
            } else {
                self.store().channel_of(&row.conversation_key)?
            };
            let Some(target) = target else {
                continue;
            };
            if self.notify(&target, file_notice(error)).await {
                self.store().mark_notice_sent(row.id)?;
            }
        }
        Ok(())
    }

    /// Posts one interrupted notice per conversation with unannounced `needs_check` rows.
    async fn post_needs_check_notices(&self) -> Result<(), StoreError> {
        let rows = self.store().needs_check_notices()?;
        let mut groups: Vec<(String, Vec<i64>)> = Vec::new();
        for row in rows {
            match groups
                .iter_mut()
                .find(|(key, _)| *key == row.conversation_key)
            {
                Some((_, ids)) => ids.push(row.id),
                None => groups.push((row.conversation_key, vec![row.id])),
            }
        }
        for (key, ids) in groups {
            let Some(target) = reply_target(&key) else {
                continue;
            };
            if self.notify(target, N_INTERRUPTED).await {
                let store = self.store();
                for id in ids {
                    store.mark_notice_sent(id)?;
                }
            }
        }
        Ok(())
    }

    /// Posts one [`N_LANE_INTERRUPTED`] per lane a restart interrupted, in the
    /// conversation that started it; `notice_sent` records the delivery.
    async fn post_lane_notices(&self) -> Result<(), StoreError> {
        let lanes = self.store().lane_notices()?;
        for lane in lanes {
            let Some(target) = reply_target(&lane.conversation_key) else {
                continue;
            };
            tracing::warn!(
                lane = lane.id,
                code = N_LANE_INTERRUPTED,
                "lane was interrupted by a restart"
            );
            if self.notify(target, N_LANE_INTERRUPTED).await {
                self.store().mark_lane_notice_sent(lane.id)?;
            }
        }
        Ok(())
    }

    /// Counts one owner message, delivered reply or posted news in `key`; no clock is read.
    fn talked(&self, key: &str) {
        lock(&self.talk).entry(key.to_owned()).or_default().said += 1;
    }

    /// Conversations whose talk is tracked for lane news; quiet ones are dropped at each news
    /// step.
    pub fn talk_len(&self) -> usize {
        lock(&self.talk).len()
    }

    /// The lane manager, for lane news.
    pub fn lanes(&self) -> &LaneManager {
        self.omp.lanes()
    }

    /// One news step at tick time `now`: notes settled since the last step are finished first
    /// (a note the lead took is marked handed on; a refused or cut one is stored as a lane row
    /// instead), every conversation's quiet clock is updated, then each lane of `news` whose
    /// conversation is quiet for [`NEWS_AFTER`] gets its note handed on by [`Self::hand_news`].
    /// Handed-on texts are marked and count as talk. Returns the number of notes handed on in
    /// this step; a note just sent into a turn counts once it settled.
    pub async fn post_news(&self, news: Vec<LaneNews>, now: Instant) -> usize {
        let mut handed = self.settle_news_notes();
        let due: Vec<LaneNews> = {
            let in_flight = lock(&self.news_notes).in_flight.clone();
            let mut talk = lock(&self.talk);
            for t in talk.values_mut() {
                if t.seen.is_none_or(|(count, _)| count != t.said) {
                    t.seen = Some((t.said, now));
                }
            }
            talk.retain(|key, t| {
                news.iter().any(|n| n.conversation_key == *key)
                    || t.seen
                        .is_some_and(|(_, since)| now.saturating_duration_since(since) < NEWS_AFTER)
            });
            news.into_iter()
                .filter(|n| {
                    reply_target(&n.conversation_key).is_some()
                        && !in_flight.contains(&n.message_id())
                        && talk.get(&n.conversation_key).is_none_or(|t| {
                            t.seen.is_some_and(|(_, since)| {
                                now.saturating_duration_since(since) >= NEWS_AFTER
                            })
                        })
                })
                .collect()
        };

        for n in due {
            let Some(target) = reply_target(&n.conversation_key) else {
                continue;
            };
            match self.hand_news(target, &n).await {
                Handed::Now => {
                    self.news_handed(&n);
                    handed += 1;
                }
                Handed::Sent | Handed::No => {}
            }
        }
        handed
    }

    /// Finishes the lane news notes whose turn ended: a note the lead took is handed on; one
    /// omp refused, or that a cut turn never answered, is stored as a lane row so it waits for
    /// its turn (a note whose row cannot be stored is offered again while its lane runs).
    /// Returns the number handed on.
    fn settle_news_notes(&self) -> usize {
        let settled = {
            let mut notes = lock(&self.news_notes);
            let settled = std::mem::take(&mut notes.settled);
            for (n, _) in &settled {
                notes.in_flight.remove(&n.message_id());
            }
            settled
        };
        let mut handed = 0;
        for (n, taken) in settled {
            if taken {
                tracing::info!(lane = n.lane_id, "lane news taken by the running turn");
            } else {
                tracing::warn!(
                    lane = n.lane_id,
                    "lane news note not delivered; queued as a row"
                );
            }
            if taken || self.queue_news_row(&n) {
                self.news_handed(&n);
                handed += 1;
            }
        }
        handed
    }

    /// Marks lane note `n` handed on: never offered again for its lane, and talk in its
    /// conversation.
    fn news_handed(&self, n: &LaneNews) {
        self.lanes().news_posted(n.lane_id, &n.text);
        self.talked(&n.conversation_key);
    }

    /// Hands lane note `n` to the lead: a [`NoteKind::News`] note when the running turn is of
    /// the note's conversation ([`Handed::Sent`], settled when the turn ends), else a lane inbox
    /// row that waits for the lead (a turn of another conversation finishes first). While the
    /// lead cannot start, the raw [`news_line`] is posted in `target` instead.
    async fn hand_news(&self, target: &str, n: &LaneNews) -> Handed {
        if let Some((turn, key)) = self.omp.open_turn()
            && key == n.conversation_key
        {
            let notes = Arc::clone(&self.news_notes);
            let note = n.clone();
            let on_settled = Box::new(move |taken: bool| lock(&notes).settled.push((note, taken)));
            lock(&self.news_notes).in_flight.insert(n.message_id());
            if self.omp.notes().send(
                turn,
                NoteKind::News,
                &lead::lane_news_note(n.lane_id, &n.text),
                Some(on_settled),
            ) {
                tracing::info!(lane = n.lane_id, "lane news sent into the running turn");
                return Handed::Sent;
            }
            lock(&self.news_notes).in_flight.remove(&n.message_id());
        }
        if self.omp.lead_down() {
            return match self
                .adapter
                .send(target, &news_line(n.lane_id, &n.text))
                .await
            {
                Ok(_) => {
                    tracing::info!(lane = n.lane_id, "lead is down; lane news posted");
                    Handed::Now
                }
                Err(err) => {
                    tracing::warn!(lane = n.lane_id, code = err.code, "lane news send failed");
                    Handed::No
                }
            };
        }
        if self.queue_news_row(n) {
            Handed::Now
        } else {
            Handed::No
        }
    }

    /// Stores lane note `n` as a lane inbox row for the lead and wakes the worker. Returns
    /// whether it is handed on: a new row, a row that already exists, or a lane that stopped
    /// meanwhile (its result follows) all count.
    fn queue_news_row(&self, n: &LaneNews) -> bool {
        let input = lead::lane_news(&n.conversation_key, n.lane_id, &n.text);
        let stored = self.store().news_notice(n.lane_id, &n.message_id(), &input);
        match stored {
            Ok(queued) => {
                if queued {
                    tracing::info!(lane = n.lane_id, "lane news queued for the lead");
                    self.wake.notify_one();
                }
                true
            }
            Err(err) => {
                tracing::error!(lane = n.lane_id, code = err.code(), "lane news not stored");
                false
            }
        }
    }

    /// One busy check at tick time `now` ([`OmpManager::lead_busy`]): an owner row of the
    /// running turn's conversation that is still `queued` counts as waiting. Returns whether a
    /// note was sent.
    pub fn post_busy_note(&self, now: Instant) -> bool {
        self.omp.lead_busy(now, |key| {
            self.store()
                .has_queued_owner_row(key)
                .unwrap_or_else(|err| {
                    tracing::error!(code = err.code(), "queued owner rows not read");
                    false
                })
        })
    }

    /// Posts lane news after every lane watchdog tick until the task is aborted.
    async fn news_loop(&self) {
        let mut ticks = self.lanes().watch_ticks();
        while ticks.changed().await.is_ok() {
            let now = *ticks.borrow_and_update();
            if let Some(now) = now {
                self.post_news(self.lanes().news(), now).await;
                self.post_stall_notice(now).await;
                self.post_busy_note(now);
            }
        }
    }

    /// One lead stall check at tick time `now`: when [`OmpManager::lead_stalled`] reports the
    /// running turn, its conversation gets one [`N_TURN_STALLED`] notice. Returns whether the
    /// messenger accepted one. The turn is left running.
    pub async fn post_stall_notice(&self, now: Instant) -> bool {
        let Some(key) = self.omp.lead_stalled(now) else {
            return false;
        };
        let Some(target) = reply_target(&key) else {
            return false;
        };
        let text = stalled_text(self.omp.stall_after().as_secs() / 60);
        match self.adapter.send(target, &text).await {
            Ok(_) => true,
            Err(err) => {
                tracing::warn!(
                    notice = N_TURN_STALLED,
                    code = err.code,
                    "notice send failed"
                );
                false
            }
        }
    }

    /// Sends a fixed notice; returns whether the messenger accepted it.
    async fn notify(&self, channel: &str, code: &'static str) -> bool {
        send_notice(self.adapter.as_ref(), channel, code).await
    }
}

/// The background tasks of [`Dispatcher::run`], each with a name for the log.
#[derive(Default)]
struct Supervised {
    set: JoinSet<()>,
    names: HashMap<task::Id, &'static str>,
}

impl Supervised {
    fn spawn(&mut self, name: &'static str, task: impl Future<Output = ()> + Send + 'static) {
        let id = self.set.spawn(task).id();
        self.names.insert(id, name);
    }

    /// Waits until a task returns or panics, logs it at error level and returns its name.
    /// Never resolves while the set is empty.
    async fn ended(&mut self) -> &'static str {
        let Some(joined) = self.set.join_next_with_id().await else {
            return std::future::pending().await;
        };
        let (id, panic) = match joined {
            Ok((id, ())) => (id, None),
            Err(err) => (err.id(), Some(err)),
        };
        let name = self.names.get(&id).copied().unwrap_or("unknown");
        match panic {
            None => tracing::error!(code = E_GATEWAY_TASK, task = name, "background task ended"),
            Some(err) => {
                tracing::error!(code = E_GATEWAY_TASK, task = name, error = %err, "background task failed");
            }
        }
        name
    }
}

/// Sends a fixed notice; returns whether the messenger accepted it.
async fn send_notice(adapter: &dyn MessengerAdapter, channel: &str, code: &'static str) -> bool {
    match adapter.send(channel, notice_text(code)).await {
        Ok(_) => true,
        Err(err) => {
            tracing::warn!(notice = code, code = err.code, "notice send failed");
            false
        }
    }
}

/// One chat sender's message, answered by the chat runtime on its own task.
struct ChatTurn {
    adapter: Arc<dyn MessengerAdapter>,
    chat: Arc<ChatManager>,
    key: String,
    target: String,
    event: InboundEvent,
}

impl ChatTurn {
    /// Fetches the attachments (one failed file: one notice, no turn), runs the chat turn
    /// and posts its reply, or one notice when it did not reply.
    async fn run(self) {
        let dir = self.chat.files_dir(&self.key, &self.event.message_id);
        let files = match self.fetch(&dir).await {
            Ok(files) => files,
            Err(err) => {
                chat::remove_files(&dir);
                tracing::warn!(conversation = %self.key, code = err.code, "chat attachments not received");
                send_notice(self.adapter.as_ref(), &self.target, file_notice(err.code)).await;
                return;
            }
        };
        let typing = match self.adapter.start_typing(&self.target).await {
            Ok(handle) => Some(handle),
            Err(err) => {
                tracing::warn!(code = err.code, "typing indicator failed");
                None
            }
        };
        let outcome = self.chat.reply(&self.key, &self.event.text, &files).await;
        drop(typing);
        let notice = match outcome {
            Outcome::Reply(text) => {
                for chunk in reply_messages(&text, self.adapter.message_limit()) {
                    if let Err(err) = self.adapter.send(&self.target, &chunk).await {
                        tracing::warn!(conversation = %self.key, code = err.code, "chat reply not sent");
                        return;
                    }
                }
                tracing::info!(conversation = %self.key, "chat reply sent");
                return;
            }
            Outcome::NoAgent => N_NO_AGENT,
            Outcome::Failed { code, .. } | Outcome::NeedsCheck { code, .. } => {
                tracing::warn!(conversation = %self.key, code, "chat turn failed");
                if code == E_COST_CAP {
                    N_COST_CAP
                } else {
                    N_FAILED
                }
            }
        };
        send_notice(self.adapter.as_ref(), &self.target, notice).await;
    }

    /// Saves every attachment within its [`chat::size_limit`] (by declared size) under
    /// `dir` (0700 levels) under the stall watchdog, reads it back and removes `dir`.
    /// Larger ones are not fetched and only named; a download that streams past the
    /// limit anyway stops with `E_FILE_TOO_LARGE`.
    async fn fetch(&self, dir: &std::path::Path) -> Result<Vec<ChatFile>, AdapterError> {
        if self.event.attachments.is_empty() {
            return Ok(Vec::new());
        }
        files::create_private_dir_all(dir)?;
        let mut saved = Vec::with_capacity(self.event.attachments.len());
        for (index, attachment) in self.event.attachments.iter().enumerate() {
            let name = files::sanitize_name(&attachment.name, index + 1);
            let limit = chat::size_limit(attachment.mime.as_deref());
            if attachment.size > limit {
                saved.push((name, attachment.mime.clone(), attachment.size, None));
                continue;
            }
            let path = dir.join(&name);
            let progress = files::Progress::with_limit(limit);
            files::with_stall_timeout(
                self.adapter.fetch_attachment(attachment, &path, &progress),
                &progress,
            )
            .await?;
            saved.push((name, attachment.mime.clone(), attachment.size, Some(path)));
        }
        Ok(chat::take_files(dir, saved))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Env;
    use crate::testing::FakeAdapter;

    const BOUND: Duration = Duration::from_secs(10);

    fn dispatcher(dir: &tempfile::TempDir) -> Dispatcher {
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).expect("home");
        let env = Env {
            home_dir: Some(dir.path().to_path_buf()),
            moraebot_home: None,
            path: None,
        };
        let text = format!(
            "owner_id = 111\ntoken_file = \"{}\"\nomp_path = \"{}\"\nhome = \"{}\"\nwork_dir = \"{}\"\n[discord]\nchannel_id = 222\n",
            dir.path().join("token").display(),
            dir.path().join("omp").display(),
            home.display(),
            dir.path().join("work").display(),
        );
        let config = Config::from_toml(&text, &env).expect("loads");
        let store = Arc::new(Mutex::new(
            Store::open(&home.join("inbox.sqlite")).expect("store"),
        ));
        let omp = OmpManager::new(config.clone(), Arc::clone(&store));
        Dispatcher::new(&config, Arc::new(FakeAdapter::new()), store, omp)
    }

    #[tokio::test]
    async fn a_supervised_task_that_returns_stops_the_dispatcher_with_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dispatcher = dispatcher(&dir);
        // The sender stays open, so only the ended task can stop `serve`.
        let (_events, inbound) = mpsc::channel(1);
        let mut tasks = Supervised::default();
        tasks.spawn("forever", std::future::pending());
        tasks.spawn("short", async {});
        let result = tokio::time::timeout(BOUND, dispatcher.serve(inbound, tasks))
            .await
            .expect("serve stopped");
        assert!(
            matches!(result, Err(GatewayError::TaskEnded("short"))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn a_panicked_task_is_reported_by_name() {
        let mut tasks = Supervised::default();
        tasks.spawn("forever", std::future::pending());
        tasks.spawn("boom", async {
            panic!("supervised task panics on purpose")
        });
        let name = tokio::time::timeout(BOUND, tasks.ended())
            .await
            .expect("a task ended");
        assert_eq!(name, "boom");
    }

    #[tokio::test]
    async fn a_closed_inbound_stops_the_dispatcher_cleanly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dispatcher = dispatcher(&dir);
        let (events, inbound) = mpsc::channel(1);
        drop(events);
        let mut tasks = Supervised::default();
        tasks.spawn("forever", std::future::pending());
        let result = tokio::time::timeout(BOUND, dispatcher.serve(inbound, tasks))
            .await
            .expect("serve stopped");
        assert!(result.is_ok(), "{result:?}");
    }
}
