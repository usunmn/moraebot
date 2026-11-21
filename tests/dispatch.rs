use std::future::Future;
use std::io::{BufRead, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use moraebot::access::{DropReason, E_ACCESS_NOT_OWNER};
use moraebot::adapter::{
    AdapterError, AttachmentRef, E_ADAPTER_THREAD, E_ADAPTER_UPLOAD, E_ADAPTER_UPLOAD_DENIED,
    InboundAttachment, InboundEvent, MessengerAdapter, SendResult, TypingHandle,
};
use moraebot::chat;
use moraebot::config::{
    AccessMode, AllowEntry, Config, DiscordConfig, OmpConfig, Platform, Role, SenderKind,
};
use moraebot::config::{DEFAULT_HANDOFF_AFTER, DEFAULT_STALL_AFTER};
use moraebot::dispatch::{
    Dispatcher, E_DISPATCH_HELD, Handled, MAX_REPLY_MESSAGES, N_FAILED, N_FILE_NOT_ALLOWED,
    N_FILE_NOT_RECEIVED, N_FILE_NOT_SENT, N_INTERRUPTED, N_LANE_INTERRUPTED, N_NEW_SESSION_FILES,
    N_OMP_VERSION, N_ONBOARDING_HELLO, N_REPLY_TRUNCATED, N_THREAD_FAILED, N_UNSUPPORTED_INPUT,
    notice_text, reply_messages,
};
use moraebot::dispatch::{NEWS_AFTER, news_line, stalled_text};
use moraebot::files::{self, OutFile, SavedFile};
use moraebot::gateway::Gateway;
use moraebot::hints;
use moraebot::lane::MAX_RUNNING_LANES;
use moraebot::lane::{LANE_REPORT_TOOL, LaneNews, WATCH_TICK};
use moraebot::lead::{self, LaneResult};
use moraebot::memory::{Bucket, Day, MemoryHandle, MemoryStore, Redactor};
use moraebot::memory_commands;
use moraebot::new_session;
use moraebot::omp::{self, E_CHILD_LOST, E_OPEN_SESSION, OmpManager};
use moraebot::omp_overlay::{OverlayCheck, W_OMP_VERSION};
use moraebot::onboarding::{self, GreetTarget};
use moraebot::status::Lang;
use moraebot::store::{
    Admitted, LaneStart, LaneState, OnboardingRow, OnboardingState, State, Store,
};
use moraebot::testing::{Call, FakeAdapter, appended};
use serde_json::Value;
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

const LIMIT: usize = 2000;
const OWNER: &str = "111";
const STRANGER: &str = "222";
const CHANNEL: &str = "333";
const OWNER_DM: &str = "dm-111";
const FRIEND: &str = "666";
const PEER_BOT: &str = "777";
const BOUND: Duration = Duration::from_secs(30);
const TOKEN_SENTINEL: &str = "TOKEN_SENTINEL_9f3a";

/// The config of a test home under `root` (created, with its token file).
fn test_config(root: &Path, omp_path: PathBuf) -> Config {
    let home = root.join("home");
    let work_dir = root.join("work");
    std::fs::create_dir_all(home.join("secrets")).expect("home");
    std::fs::create_dir_all(&work_dir).expect("work dir");
    let token_file = home.join("secrets").join("discord.token");
    std::fs::write(&token_file, TOKEN_SENTINEL).expect("token file");
    std::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o600))
        .expect("token mode");
    Config {
        messenger: Platform::Discord,
        work_dir,
        omp_path,
        home,
        access_mode: AccessMode::OwnerOnly,
        discord: Some(DiscordConfig {
            owner_id: OWNER.parse().unwrap(),
            channel_id: CHANNEL.parse().unwrap(),
            token_file,
            access: Default::default(),
        }),
        bridge: None,
        omp: OmpConfig::default(),
        lead: Default::default(),
        memory: moraebot::config::MemoryConfig::default(),
    }
}

struct Harness {
    root: PathBuf,
    config: Config,
    store: Arc<Mutex<Store>>,
    adapter: Arc<FakeAdapter>,
    dispatcher: Arc<Dispatcher>,
    chat: Arc<chat::ChatManager>,
}

impl Harness {
    fn open(root: &Path, mode: &str) -> Self {
        Self::open_with(root, mode, PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")))
    }

    /// Like [`Self::open`], but every `create_thread` call is recorded and then fails.
    fn open_failing_threads(root: &Path) -> Self {
        Self::build(
            root,
            "ok",
            PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")),
            true,
            Vec::new(),
        )
    }

    fn open_with(root: &Path, mode: &str, omp_path: PathBuf) -> Self {
        Self::build(root, mode, omp_path, false, Vec::new())
    }

    /// fake-omp in mode `send_file`: every turn calls the `send_file` host tool once per
    /// path, then replies `echo: <message>`.
    fn open_sending(root: &Path, paths: &[PathBuf]) -> Self {
        let script = paths
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("\n");
        Self::build(
            root,
            "send_file",
            PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")),
            false,
            vec![("FAKE_OMP_SEND_FILE", script)],
        )
    }

    fn build(
        root: &Path,
        mode: &str,
        omp_path: PathBuf,
        fail_threads: bool,
        extra_env: Vec<(&'static str, String)>,
    ) -> Self {
        Self::with_config(
            root,
            test_config(root, omp_path),
            mode,
            fail_threads,
            extra_env,
        )
    }

    /// `access_mode = "allowlist"` with [`FRIEND`] (user) and [`PEER_BOT`] (bot) listed as
    /// chat, and `max_bot_replies`.
    fn open_allowlist(root: &Path, max_bot_replies: Option<u32>) -> Self {
        Self::open_allowlist_with(root, max_bot_replies, "tools", Vec::new())
    }

    /// Like [`Self::open_allowlist`] with fake-omp mode `mode` and `extra_env`.
    fn open_allowlist_with(
        root: &Path,
        max_bot_replies: Option<u32>,
        mode: &str,
        extra_env: Vec<(&'static str, String)>,
    ) -> Self {
        let config = allowlist_config(root, max_bot_replies);
        Self::with_config(root, config, mode, false, extra_env)
    }

    /// Like [`Self::open_allowlist`], but every `create_thread` call is recorded and then
    /// fails.
    fn open_allowlist_failing_threads(root: &Path) -> Self {
        Self::with_config(
            root,
            allowlist_config(root, None),
            "tools",
            true,
            Vec::new(),
        )
    }

    fn with_config(
        root: &Path,
        config: Config,
        mode: &str,
        fail_threads: bool,
        extra_env: Vec<(&'static str, String)>,
    ) -> Self {
        Self::with_lead(root, config, mode, fail_threads, extra_env, |omp| omp)
    }

    /// Like [`Self::with_config`]; `lead` finishes the lead's manager first.
    fn with_lead(
        root: &Path,
        config: Config,
        mode: &str,
        fail_threads: bool,
        extra_env: Vec<(&'static str, String)>,
        lead: impl FnOnce(OmpManager) -> OmpManager,
    ) -> Self {
        Self::assemble(root, config, mode, extra_env, lead, |adapter| {
            if fail_threads {
                Arc::new(ThreadFailAdapter(adapter))
            } else {
                adapter
            }
        })
    }

    /// Like [`Self::with_lead`]; `wrap` turns the recording [`FakeAdapter`] into the adapter
    /// the dispatcher gets.
    fn assemble(
        root: &Path,
        config: Config,
        mode: &str,
        extra_env: Vec<(&'static str, String)>,
        lead: impl FnOnce(OmpManager) -> OmpManager,
        wrap: impl FnOnce(Arc<FakeAdapter>) -> Arc<dyn MessengerAdapter>,
    ) -> Self {
        let home = config.home.clone();
        let store = Arc::new(Mutex::new(
            Store::open(&home.join("inbox.sqlite")).expect("store"),
        ));
        let mut env = vec![
            ("FAKE_OMP_MODE", mode.to_owned()),
            (
                "FAKE_OMP_LOG",
                root.join("fake-omp.log").to_string_lossy().into_owned(),
            ),
        ];
        env.extend(extra_env);
        let omp = lead(OmpManager::new(config.clone(), Arc::clone(&store)).with_extra_env(env));
        let chat = omp.chat();
        let adapter = Arc::new(FakeAdapter::new());
        let as_dyn = wrap(Arc::clone(&adapter));
        let dispatcher = Arc::new(Dispatcher::new(&config, as_dyn, Arc::clone(&store), omp));
        Self {
            root: root.to_owned(),
            config,
            store,
            adapter,
            dispatcher,
            chat,
        }
    }

    async fn handle(&self, event: InboundEvent) -> Handled {
        bounded(self.dispatcher.handle_event(event))
            .await
            .expect("handle event")
    }

    async fn step(&self) -> bool {
        bounded(self.dispatcher.process_next())
            .await
            .expect("worker step")
    }

    async fn drain(&self) {
        while self.step().await {}
    }

    fn row(&self, id: i64) -> moraebot::store::InboxRow {
        self.store
            .lock()
            .unwrap()
            .get(id)
            .unwrap()
            .expect("row exists")
    }

    fn count(&self) -> i64 {
        self.store.lock().unwrap().count().unwrap()
    }

    fn log_lines(&self) -> Vec<String> {
        std::fs::read_to_string(self.root.join("fake-omp.log"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// Every prompt frame, its `message` turned into the body (see [`prompt_frame`]).
    fn prompts(&self) -> Vec<Value> {
        self.log_lines()
            .iter()
            .filter_map(|l| prompt_frame(l))
            .collect()
    }

    /// Every child start: its pid and argv.
    fn start_argvs(&self) -> Vec<(u32, Vec<String>)> {
        self.log_lines()
            .iter()
            .filter_map(|l| {
                let rest = l.strip_prefix("START pid=")?;
                let pid = rest.split_whitespace().next()?;
                let (_, argv) = rest.split_once(" argv=")?;
                Some((pid.parse().ok()?, serde_json::from_str(argv).ok()?))
            })
            .collect()
    }

    /// The `--cwd` entry count child `pid` logged at startup.
    fn cwd_entries(&self, pid: u32) -> Option<i64> {
        self.log_lines().iter().find_map(|l| {
            let (logged, n) = l.strip_prefix("CWD_ENTRIES ")?.split_once(' ')?;
            if logged.parse::<u32>().ok()? == pid {
                n.parse().ok()
            } else {
                None
            }
        })
    }

    async fn chats_done(&self) {
        bounded(self.dispatcher.chats_done()).await;
    }

    fn starts(&self) -> usize {
        self.log_lines()
            .iter()
            .filter(|l| l.starts_with("START pid="))
            .count()
    }

    fn sends(&self) -> Vec<(String, String)> {
        self.adapter
            .calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Send { channel, text } => Some((channel, text)),
                _ => None,
            })
            .collect()
    }

    fn threads_created(&self) -> usize {
        self.adapter
            .calls()
            .iter()
            .filter(|call| matches!(call, Call::CreateThread { .. }))
            .count()
    }

    async fn close(self) {
        self.dispatcher.shutdown().await;
    }

    /// Stores a channel message exactly as `handle_event` does, without opening its
    /// thread: the state left by a crash between admission and `create_thread`.
    fn admit_without_thread(&self, message_id: &str, text: &str) -> i64 {
        let admitted = self
            .store
            .lock()
            .unwrap()
            .admit(
                "fake",
                message_id,
                &format!("fake:th:{message_id}"),
                CHANNEL,
                OWNER,
                text,
            )
            .unwrap();
        match admitted {
            moraebot::store::Admitted::New(id) => id,
            other => panic!("expected New, got {other:?}"),
        }
    }
}

/// `access_mode = "allowlist"` with [`FRIEND`] (user) and [`PEER_BOT`] (bot) listed as
/// chat, and `max_bot_replies`.
fn allowlist_config(root: &Path, max_bot_replies: Option<u32>) -> Config {
    let mut config = test_config(root, PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")));
    config.access_mode = AccessMode::Allowlist;
    let discord = config.discord.as_mut().expect("discord section");
    discord.access.allow = vec![
        AllowEntry {
            id: FRIEND.parse().unwrap(),
            kind: SenderKind::User,
            role: Role::Chat,
        },
        AllowEntry {
            id: PEER_BOT.parse().unwrap(),
            kind: SenderKind::Bot,
            role: Role::Chat,
        },
    ];
    discord.access.max_bot_replies = max_bot_replies;
    config
}

/// A [`FakeAdapter`] whose `create_thread` is recorded and then fails.
struct ThreadFailAdapter(Arc<FakeAdapter>);

#[async_trait]
impl MessengerAdapter for ThreadFailAdapter {
    fn platform(&self) -> &str {
        self.0.platform()
    }

    fn message_limit(&self) -> usize {
        self.0.message_limit()
    }

    fn behavior_hints(&self) -> &str {
        self.0.behavior_hints()
    }

    async fn start(&self) -> Result<mpsc::Receiver<InboundEvent>, AdapterError> {
        self.0.start().await
    }

    async fn send(&self, channel: &str, text: &str) -> Result<SendResult, AdapterError> {
        self.0.send(channel, text).await
    }

    async fn create_thread(
        &self,
        channel_id: &str,
        from_message_id: &str,
        name: &str,
    ) -> Result<String, AdapterError> {
        self.0
            .create_thread(channel_id, from_message_id, name)
            .await?;
        Err(AdapterError {
            code: E_ADAPTER_THREAD,
            message: "injected thread failure".to_owned(),
        })
    }

    async fn start_typing(&self, channel: &str) -> Result<TypingHandle, AdapterError> {
        self.0.start_typing(channel).await
    }

    fn reconnects(&self) -> Option<watch::Receiver<u64>> {
        self.0.reconnects()
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(BOUND, future)
        .await
        .expect("finished within bound")
}

fn event(message_id: &str, author_id: &str, thread_id: Option<&str>, text: &str) -> InboundEvent {
    InboundEvent {
        message_id: message_id.into(),
        author_id: author_id.into(),
        author_is_bot: false,
        channel_id: CHANNEL.into(),
        thread_id: thread_id.map(str::to_owned),
        is_dm: false,
        text: text.into(),
        attachments: Vec::new(),
    }
}

fn channel_msg(message_id: &str, text: &str) -> InboundEvent {
    event(message_id, OWNER, None, text)
}

fn queued(handled: Handled) -> i64 {
    match handled {
        Handled::Queued(id) => id,
        other => panic!("expected Queued, got {other:?}"),
    }
}

/// Shell stand-in for an omp child that dies with exit code 1 mid-turn: it forwards the
/// client's frames to fake-omp (mode `hang`), closes fake-omp's stdin right after the
/// prompt frame so it exits, then exits 1 itself. Written into the test's temp dir.
fn exit1_wrapper(root: &Path) -> PathBuf {
    let script = root.join("omp-exit1.sh");
    let fake = env!("CARGO_BIN_EXE_fake-omp");
    let body = format!(
        "#!/bin/sh\n\
         fifo=\"$(dirname \"$0\")/omp-exit1.fifo\"\n\
         rm -f \"$fifo\" && mkfifo \"$fifo\" || exit 2\n\
         '{fake}' \"$@\" < \"$fifo\" &\n\
         child=$!\n\
         exec 3>\"$fifo\"\n\
         while IFS= read -r line; do\n\
           printf '%s\\n' \"$line\" >&3\n\
           case \"$line\" in *'\"type\":\"prompt\"'*) break ;; esac\n\
         done\n\
         exec 3>&-\n\
         wait \"$child\"\n\
         exit 1\n"
    );
    std::fs::write(&script, body).expect("write wrapper");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    script
}

#[tokio::test]
async fn round_trip_creates_thread_and_replies() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let mid = "1300000000000000010";
    let id = queued(h.handle(channel_msg(mid, "hello")).await);
    assert!(h.step().await);

    assert_eq!(
        h.adapter.calls(),
        vec![
            Call::CreateThread {
                channel_id: CHANNEL.into(),
                from_message_id: mid.into(),
                name: "hello".into(),
            },
            Call::StartTyping {
                channel: mid.into()
            },
            Call::Send {
                channel: mid.into(),
                text: "echo: hello".into()
            },
        ]
    );
    let row = h.row(id);
    assert_eq!(row.state, State::Done);
    assert_eq!(row.conversation_key, format!("fake:th:{mid}"));
    assert_eq!(row.reply.as_deref(), Some("echo: hello"));
    assert!(!h.step().await);
    h.close().await;
}

#[tokio::test]
async fn crash_before_thread_creation_opens_thread_at_startup_then_runs_once() {
    let dir = tempfile::tempdir().unwrap();
    let mid = "1300000000000000140";
    let id = {
        let h = Harness::open(dir.path(), "ok");
        let id = h.admit_without_thread(mid, "hello");
        h.close().await;
        id
    };

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.threads_created(), 1);
    assert!(h.prompts().is_empty(), "startup itself never prompts");
    h.drain().await;

    assert_eq!(
        h.adapter.calls(),
        vec![
            Call::CreateThread {
                channel_id: CHANNEL.into(),
                from_message_id: mid.into(),
                name: "hello".into(),
            },
            Call::StartTyping {
                channel: mid.into()
            },
            Call::Send {
                channel: mid.into(),
                text: "echo: hello".into()
            },
        ]
    );
    assert_eq!(h.prompts().len(), 1);
    assert_eq!(h.row(id).state, State::Done);
    h.close().await;
}

#[tokio::test]
async fn crash_before_thread_creation_and_failed_retry_marks_failed_with_one_notice() {
    let dir = tempfile::tempdir().unwrap();
    let mid = "1300000000000000141";
    let id = {
        let h = Harness::open(dir.path(), "ok");
        let id = h.admit_without_thread(mid, "hello");
        h.close().await;
        id
    };

    let h = Harness::open_failing_threads(dir.path());
    bounded(h.dispatcher.startup()).await.unwrap();
    h.drain().await;
    let row = h.row(id);
    assert_eq!(row.state, State::Failed);
    assert_eq!(row.error.as_deref(), Some(E_ADAPTER_THREAD));
    assert!(row.notice_sent);
    assert_eq!(h.threads_created(), 1);
    assert_eq!(
        h.sends(),
        vec![(CHANNEL.to_owned(), notice_text(N_THREAD_FAILED).to_owned())]
    );
    assert!(h.prompts().is_empty());
    assert_eq!(h.starts(), 0);
    h.close().await;

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(!h.step().await);
    assert!(
        h.adapter.calls().is_empty(),
        "no second thread attempt and no second notice"
    );
    assert!(h.prompts().is_empty());
    assert_eq!(h.row(id).state, State::Failed);
    h.close().await;
}

#[tokio::test]
async fn worker_never_prompts_a_thread_starter_without_its_thread() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_failing_threads(dir.path());
    let mid = "1300000000000000142";
    let id = h.admit_without_thread(mid, "hello");
    assert!(h.step().await);
    assert!(!h.step().await);

    let row = h.row(id);
    assert_eq!(row.state, State::Failed);
    assert_eq!(row.error.as_deref(), Some(E_ADAPTER_THREAD));
    assert!(row.notice_sent);
    assert!(h.prompts().is_empty());
    assert_eq!(h.starts(), 0);
    assert_eq!(
        h.adapter.calls(),
        vec![
            Call::CreateThread {
                channel_id: CHANNEL.into(),
                from_message_id: mid.into(),
                name: "hello".into(),
            },
            Call::Send {
                channel: CHANNEL.into(),
                text: notice_text(N_THREAD_FAILED).into()
            },
        ]
    );
    h.close().await;
}

#[tokio::test]
async fn thread_creation_failure_on_arrival_marks_failed_with_one_notice() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_failing_threads(dir.path());
    let handled = h.handle(channel_msg("1300000000000000143", "hello")).await;
    let Handled::ThreadFailed(id) = handled else {
        panic!("expected ThreadFailed, got {handled:?}")
    };
    assert!(!h.step().await);

    let row = h.row(id);
    assert_eq!(row.state, State::Failed);
    assert_eq!(row.error.as_deref(), Some(E_ADAPTER_THREAD));
    assert!(row.notice_sent);
    assert!(h.prompts().is_empty());
    assert_eq!(
        h.sends(),
        vec![(CHANNEL.to_owned(), notice_text(N_THREAD_FAILED).to_owned())]
    );
    h.close().await;
}

#[tokio::test]
async fn run_loop_serves_injected_events() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let inbound = h.adapter.start().await.unwrap();
    let runner = tokio::spawn(Arc::clone(&h.dispatcher).run(inbound));
    h.adapter
        .inject(channel_msg("1300000000000000011", "one"))
        .await;
    h.adapter
        .inject(channel_msg("1300000000000000012", "two"))
        .await;
    bounded(h.adapter.wait_for_send(2)).await;
    let texts: Vec<String> = h.sends().into_iter().map(|(_, text)| text).collect();
    assert_eq!(texts, ["echo: one", "echo: two"]);
    runner.abort();
    let _ = runner.await;
    h.close().await;
}

#[tokio::test]
async fn message_in_thread_reuses_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let thread = "1300000000000000020";
    let first = queued(h.handle(channel_msg(thread, "first")).await);
    h.drain().await;
    let second = queued(
        h.handle(event("1300000000000000021", OWNER, Some(thread), "second"))
            .await,
    );
    h.drain().await;

    assert_eq!(h.threads_created(), 1);
    assert_eq!(h.starts(), 1, "same fake-omp child serves both messages");
    assert_eq!(
        h.row(first).conversation_key,
        h.row(second).conversation_key
    );
    assert_eq!(h.row(second).state, State::Done);
    assert_eq!(
        h.sends(),
        vec![
            (thread.to_owned(), "echo: first".to_owned()),
            (thread.to_owned(), "echo: second".to_owned()),
        ]
    );
    h.close().await;
}

#[tokio::test]
async fn non_owner_never_stored_or_sent() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let stranger = h
        .handle(event("1300000000000000030", STRANGER, None, "hello"))
        .await;
    assert_eq!(stranger, Handled::Dropped(DropReason::NotOwner));
    let stranger_slash = h
        .handle(event("1300000000000000031", STRANGER, None, "/model x"))
        .await;
    assert_eq!(stranger_slash, Handled::Dropped(DropReason::NotOwner));
    let mut bot = event("1300000000000000032", OWNER, None, "hello");
    bot.author_is_bot = true;
    assert_eq!(h.handle(bot).await, Handled::Dropped(DropReason::Bot));

    assert!(!h.step().await);
    assert_eq!(h.count(), 0);
    assert!(h.adapter.calls().is_empty());
    assert!(h.prompts().is_empty());
    assert_eq!(h.starts(), 0);
}

fn bot_event(message_id: &str, author_id: &str, thread_id: Option<&str>) -> InboundEvent {
    let mut bot = event(message_id, author_id, thread_id, "hello");
    bot.author_is_bot = true;
    bot
}

/// The chat child's start: the one START whose argv carries `--no-tools`.
fn chat_start(h: &Harness) -> (u32, Vec<String>) {
    let mut chats: Vec<(u32, Vec<String>)> = h
        .start_argvs()
        .into_iter()
        .filter(|(_, argv)| argv.iter().any(|a| a == "--no-tools"))
        .collect();
    assert_eq!(chats.len(), 1, "exactly one chat child started");
    chats.remove(0)
}

fn arg_after<'a>(argv: &'a [String], flag: &str) -> &'a str {
    let index = argv.iter().position(|a| a == flag).expect("flag present");
    &argv[index + 1]
}

fn alive(pid: u32) -> bool {
    std::process::Command::new("/bin/kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .expect("run kill -0")
        .success()
}

#[tokio::test]
async fn listed_bot_passes_unlisted_bot_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    let unlisted = bot_event("1300000000000000040", "888", None);
    assert_eq!(h.handle(unlisted).await, Handled::Dropped(DropReason::Bot));
    let as_user = event("1300000000000000041", PEER_BOT, None, "hello");
    assert_eq!(
        h.handle(as_user).await,
        Handled::Dropped(DropReason::NotOwner)
    );
    assert!(h.adapter.calls().is_empty());

    let listed = bot_event("1300000000000000042", PEER_BOT, None);
    assert_eq!(h.handle(listed).await, Handled::Chat);
    h.chats_done().await;
    assert_eq!(h.threads_created(), 1, "a channel post gets its own thread");
    assert_eq!(
        h.sends(),
        vec![("1300000000000000042".to_owned(), "echo: hello".to_owned())]
    );
    assert!(!h.step().await);
    assert_eq!(h.count(), 0);
    h.close().await;
}

#[tokio::test]
async fn chat_child_has_no_tools_and_an_empty_cwd() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    let thread = "1300000000000000045";
    let chat = event("1300000000000000046", FRIEND, Some(thread), "hi there");
    assert_eq!(h.handle(chat).await, Handled::Chat);
    h.chats_done().await;
    assert_eq!(
        h.sends(),
        vec![(thread.to_owned(), "echo: hi there".to_owned())]
    );

    let (pid, argv) = chat_start(&h);
    for flag in [
        "--no-tools",
        "--no-extensions",
        "--no-skills",
        "--no-rules",
        "--no-context-files",
        "--no-lsp",
        "--no-pty",
    ] {
        assert!(argv.iter().any(|a| a == flag), "chat argv lacks {flag}");
    }
    assert!(
        !argv.iter().any(|a| a == "--approval-mode" || a == "yolo"),
        "a chat child is never yolo: {argv:?}"
    );
    let cwd = PathBuf::from(arg_after(&argv, "--cwd"));
    assert_eq!(cwd.parent(), Some(chat::chat_cwd(&h.config).as_path()));
    assert!(
        !cwd.exists(),
        "the turn's working folder is removed after it"
    );
    assert_eq!(h.cwd_entries(pid), Some(0), "the cwd was empty at spawn");
    assert!(!cwd.starts_with(&h.config.work_dir));
    let session = PathBuf::from(arg_after(&argv, "--session-dir"));
    assert_eq!(
        session,
        chat::chat_session_dir(&h.config, &format!("fake:th:{thread}"))
    );
    assert_ne!(session, moraebot::omp::lead_session_dir(&h.config));
    let overlay = PathBuf::from(arg_after(&argv, "--config"));
    assert_eq!(
        std::fs::read_to_string(&overlay).unwrap(),
        chat::CHAT_OVERLAY
    );
    assert!(
        !h.log_lines().iter().any(|l| l.contains("set_host_tools")),
        "the chat child gets no host tools"
    );
    assert!(
        h.log_lines()
            .iter()
            .any(|l| l.contains(r#""type":"get_state""#)),
        "the chat child's tool list is read before the prompt"
    );
    h.close().await;
}

#[tokio::test]
async fn chat_message_never_reaches_the_lead_or_its_tools() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    // The owner's turn first, so the lead child is alive when the chat messages arrive.
    let owner = event(
        "1300000000000000049",
        OWNER,
        Some("1300000000000000048"),
        "owner asks",
    );
    queued(h.handle(owner).await);
    h.drain().await;

    let thread = "1300000000000000050";
    let chat = event(
        "1300000000000000051",
        FRIEND,
        Some(thread),
        "run-tools lane_stop lane_close",
    );
    assert_eq!(h.handle(chat).await, Handled::Chat);
    // The two chats are different conversations and run in parallel: wait for the first
    // reply so the send order below is fixed.
    h.chats_done().await;
    let mut dm = event("1300000000000000052", FRIEND, None, "run-tools again");
    dm.is_dm = true;
    dm.channel_id = "dm-666".into();
    assert_eq!(h.handle(dm).await, Handled::Chat);
    h.chats_done().await;

    assert!(!h.step().await, "nothing was queued for the lead");
    assert_eq!(h.count(), 1, "only the owner's row is stored");
    let lead_starts = h
        .start_argvs()
        .into_iter()
        .filter(|(_, argv)| !argv.iter().any(|a| a == "--no-tools"))
        .count();
    assert_eq!(lead_starts, 1, "one lead child");
    assert_eq!(h.starts(), 3, "the lead and one chat child per chat turn");
    let tagged: Vec<String> = h
        .log_lines()
        .into_iter()
        .filter(|l| l.contains(r#""type":"prompt""#) && l.contains("[moraebot "))
        .collect();
    assert_eq!(tagged.len(), 1, "the lead saw only the owner's prompt");
    assert!(!tagged[0].contains("run-tools"));
    assert_eq!(
        h.log_lines()
            .iter()
            .filter(|l| l.contains("set_host_tools"))
            .count(),
        1,
        "host tools went to the lead only"
    );
    assert_eq!(
        h.sends(),
        vec![
            (
                "1300000000000000048".to_owned(),
                "echo: owner asks".to_owned()
            ),
            (
                thread.to_owned(),
                "echo: run-tools lane_stop lane_close".to_owned()
            ),
            ("dm-666".to_owned(), "echo: run-tools again".to_owned()),
        ]
    );
    h.close().await;
}

#[tokio::test]
async fn chat_attachments_go_to_the_prompt_never_the_work_dir() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    let png = vec![0x89, b'P', b'N', b'G', 1, 2, 3];
    h.adapter.set_attachment_bytes("ref-img", png.clone());
    h.adapter
        .set_attachment_bytes("ref-txt", b"guest notes".to_vec());
    let mut image = attachment("pic.png", "ref-img", png.len() as u64);
    image.mime = Some("image/png".into());
    let thread = "1300000000000000055";
    let chat = with_files(
        event("1300000000000000056", FRIEND, Some(thread), "see files"),
        vec![image, attachment("notes.txt", "ref-txt", 11)],
    );
    assert_eq!(h.handle(chat).await, Handled::Chat);
    h.chats_done().await;

    let chat_dir = h.config.home.join("chat");
    let fetched = fetches(&h);
    assert_eq!(fetched.len(), 2);
    for dest in fetched {
        assert!(
            dest.starts_with(&chat_dir),
            "fetched under the chat folder: {dest:?}"
        );
    }
    assert!(!h.config.work_dir.join(files::INBOX_DIR).exists());
    assert!(
        !chat::chat_files_dir(
            &h.config,
            &format!("fake:th:{thread}"),
            "1300000000000000056"
        )
        .exists(),
        "attachments are removed after the turn"
    );
    let frame: Value = h
        .log_lines()
        .iter()
        .find(|l| l.contains(r#""type":"prompt""#))
        .map(|l| serde_json::from_str(l).unwrap())
        .expect("chat prompt frame");
    assert_eq!(frame["images"][0]["data"], "iVBORwECAw==");
    assert_eq!(frame["images"][0]["mimeType"], "image/png");
    let message = frame["message"].as_str().unwrap();
    assert!(message.starts_with("see files\n\n[Attached files]"));
    assert!(message.contains("- 1-pic.png (image/png, 7 bytes): attached as an image"));
    assert!(message.contains("- 2-notes.txt (text/plain, 11 bytes):\n```\nguest notes\n```"));
    let sends = h.sends();
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].0, thread);
    h.close().await;
}

#[tokio::test]
async fn chat_turn_refused_when_the_child_lists_tools() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist_with(
        dir.path(),
        None,
        "ok",
        vec![("FAKE_OMP_DUMP_TOOLS", "bash,read".to_owned())],
    );
    let thread = "1300000000000000057";
    let chat = event("1300000000000000058", FRIEND, Some(thread), "hello");
    assert_eq!(h.handle(chat).await, Handled::Chat);
    h.chats_done().await;
    assert!(
        !h.log_lines()
            .iter()
            .any(|l| l.contains(r#""type":"prompt""#)),
        "no prompt reaches a child with tools"
    );
    assert_eq!(
        h.sends(),
        vec![(thread.to_owned(), notice_text(N_FAILED).to_owned())]
    );
    h.close().await;
}

#[tokio::test]
async fn shutdown_kills_a_running_chat_child() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist_with(dir.path(), None, "hang", Vec::new());
    let mut tap = LogTap::create(&dir.path().join("fake-omp.log"));
    let thread = "1300000000000000047";
    let chat = event("1300000000000000059", FRIEND, Some(thread), "hold");
    assert_eq!(h.handle(chat).await, Handled::Chat);
    let start = tap.next_line().await;
    let pid: u32 = start
        .strip_prefix("START pid=")
        .and_then(|rest| rest.split_once(' '))
        .and_then(|(pid, _)| pid.parse().ok())
        .expect("START line first");
    assert!(start.contains("--no-tools"));
    // A chat prompt carries no moraebot tag, so it is read here rather than by `next_prompt`.
    let prompt: Value = loop {
        let line = tap.next_line().await;
        if let Ok(frame) = serde_json::from_str::<Value>(&line)
            && frame["type"] == "prompt"
        {
            break frame;
        }
    };
    assert_eq!(prompt["message"], "hold");

    bounded(h.dispatcher.shutdown()).await;
    assert!(!alive(pid), "the chat child was killed and reaped");
    assert_eq!(
        h.sends(),
        vec![(thread.to_owned(), notice_text(N_FAILED).to_owned())]
    );
}

#[tokio::test]
async fn chat_conversation_locks_are_dropped_after_their_turns() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    let thread = "1300000000000000100";
    for (id, text) in [
        ("1300000000000000101", "one"),
        ("1300000000000000102", "two"),
    ] {
        assert_eq!(
            h.handle(event(id, FRIEND, Some(thread), text)).await,
            Handled::Chat
        );
    }
    let other = event(
        "1300000000000000103",
        FRIEND,
        Some("1300000000000000104"),
        "three",
    );
    assert_eq!(h.handle(other).await, Handled::Chat);
    h.chats_done().await;
    assert_eq!(h.sends().len(), 3);
    assert_eq!(h.chat.open_conversations(), 0, "no lock outlives its turns");
    h.close().await;
}

#[tokio::test]
async fn redelivered_chat_message_runs_once() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    let post = event("1300000000000000105", FRIEND, None, "hello once");
    assert_eq!(h.handle(post.clone()).await, Handled::Chat);
    assert_eq!(h.handle(post).await, Handled::Duplicate);
    h.chats_done().await;
    assert_eq!(
        h.threads_created(),
        1,
        "no second thread for the redelivery"
    );
    assert_eq!(h.starts(), 1, "one chat child");
    assert_eq!(
        h.sends(),
        vec![(
            "1300000000000000105".to_owned(),
            "echo: hello once".to_owned()
        )]
    );
    h.close().await;
}

#[tokio::test]
async fn chat_children_are_capped() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist_with(dir.path(), None, "hang", Vec::new());
    let mut tap = LogTap::create(&dir.path().join("fake-omp.log"));
    let threads = [
        "1300000000000000110",
        "1300000000000000111",
        "1300000000000000112",
    ];
    for (n, thread) in threads.iter().enumerate() {
        let id = format!("130000000000000012{n}");
        assert_eq!(
            h.handle(event(&id, FRIEND, Some(thread), "hold")).await,
            Handled::Chat
        );
    }
    // Every turn task ran up to its first wait before any child could start, so once
    // two children took their prompt the third turn is waiting for a slot.
    let mut prompts = 0;
    while prompts < chat::MAX_CHAT_CHILDREN {
        let line = tap.next_line().await;
        if serde_json::from_str::<Value>(&line).is_ok_and(|frame| frame["type"] == "prompt") {
            prompts += 1;
        }
    }
    assert_eq!(h.chat.running(), chat::MAX_CHAT_CHILDREN);

    bounded(h.dispatcher.shutdown()).await;
    assert_eq!(h.chat.running(), 0);
    let mut sends = h.sends();
    sends.sort();
    let failed = notice_text(N_FAILED).to_owned();
    assert_eq!(
        sends,
        threads
            .iter()
            .map(|t| (t.to_string(), failed.clone()))
            .collect::<Vec<_>>(),
        "two killed turns and the waiting one each get one notice"
    );
}

#[tokio::test]
async fn oversized_chat_attachment_is_named_not_fetched() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    h.adapter.set_attachment_bytes("ref-big", vec![b'a'; 16]);
    let size = chat::MAX_INLINE_TEXT_BYTES as u64 + 1;
    let thread = "1300000000000000113";
    let chat = with_files(
        event("1300000000000000114", FRIEND, Some(thread), "big file"),
        vec![attachment("big.txt", "ref-big", size)],
    );
    assert_eq!(h.handle(chat).await, Handled::Chat);
    h.chats_done().await;
    assert!(
        fetches(&h).is_empty(),
        "an oversized file is never downloaded"
    );
    let message = h
        .log_lines()
        .iter()
        .find(|l| l.contains(r#""type":"prompt""#))
        .map(|l| {
            serde_json::from_str::<Value>(l).unwrap()["message"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .expect("chat prompt frame");
    assert!(message.contains(&format!(
        "- 1-big.txt (text/plain, {size} bytes): too large to show"
    )));
    assert_eq!(h.sends().len(), 1);
    h.close().await;
}

#[tokio::test]
async fn chat_attachment_streaming_past_its_limit_is_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    let served = chat::MAX_INLINE_TEXT_BYTES + 1;
    h.adapter
        .set_attachment_bytes("ref-lie", vec![b'a'; served]);
    let thread = "1300000000000000115";
    let chat = with_files(
        event("1300000000000000116", FRIEND, Some(thread), "small file"),
        vec![attachment("lie.txt", "ref-lie", 5)],
    );
    assert_eq!(h.handle(chat).await, Handled::Chat);
    h.chats_done().await;
    let fetched = fetches(&h);
    assert_eq!(
        fetched.len(),
        1,
        "the declared size passed, so it was fetched"
    );
    assert!(!fetched[0].exists(), "nothing over the limit stays on disk");
    assert_eq!(h.starts(), 0, "no chat turn runs on a refused file");
    assert_eq!(
        h.sends(),
        vec![(
            thread.to_owned(),
            notice_text(N_FILE_NOT_RECEIVED).to_owned()
        )]
    );
    h.close().await;
}

#[tokio::test]
async fn chat_message_whose_thread_failed_is_answered_on_redelivery() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist_failing_threads(dir.path());
    let post = event("1300000000000000117", FRIEND, None, "hello");
    assert_eq!(
        h.handle(post.clone()).await,
        Handled::Notice(N_THREAD_FAILED)
    );
    assert_eq!(
        h.handle(post).await,
        Handled::Notice(N_THREAD_FAILED),
        "the redelivery is tried again, not dropped as a duplicate"
    );
    assert_eq!(h.threads_created(), 2);
    h.close().await;
}

#[tokio::test]
async fn bot_reply_limit_caps_a_thread_when_set() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), Some(2));
    let thread = "1300000000000000060";
    let mut handled = Vec::new();
    for n in 61..=63 {
        let id = format!("13000000000000000{n}");
        handled.push(h.handle(bot_event(&id, PEER_BOT, Some(thread))).await);
    }
    assert_eq!(
        handled,
        vec![
            Handled::Chat,
            Handled::Chat,
            Handled::Dropped(DropReason::BotLimit),
        ]
    );
    let other = bot_event("1300000000000000064", PEER_BOT, Some("1300000000000000065"));
    assert_eq!(h.handle(other).await, Handled::Chat);
    let person = event("1300000000000000066", FRIEND, Some(thread), "hi");
    assert_eq!(
        h.handle(person).await,
        Handled::Chat,
        "the limit counts replies to bots only"
    );
    h.chats_done().await;
    assert_eq!(h.sends().len(), 4);
    h.close().await;
}

#[tokio::test]
async fn bot_reply_limit_caps_top_level_channel_messages() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), Some(2));
    let mut handled = Vec::new();
    for n in 91..=93 {
        let id = format!("13000000000000000{n}");
        handled.push(h.handle(bot_event(&id, PEER_BOT, None)).await);
    }
    assert_eq!(
        handled,
        vec![
            Handled::Chat,
            Handled::Chat,
            Handled::Dropped(DropReason::BotLimit),
        ],
        "each top-level message has its own thread key, so they count per channel"
    );
    h.chats_done().await;
    assert_eq!(
        h.sends().len(),
        2,
        "two chat replies, the third bot message is dropped"
    );
    h.close().await;
}

#[tokio::test]
async fn bot_replies_have_no_cap_without_the_setting() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    let thread = "1300000000000000070";
    for n in 71..=90 {
        let id = format!("13000000000000000{n}");
        assert_eq!(
            h.handle(bot_event(&id, PEER_BOT, Some(thread))).await,
            Handled::Chat
        );
    }
    h.chats_done().await;
    assert_eq!(h.sends().len(), 20);
    h.close().await;
}

#[tokio::test]
async fn duplicate_message_id_processed_once() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let mid = "1300000000000000001";
    queued(h.handle(channel_msg(mid, "hello")).await);
    assert_eq!(
        h.handle(channel_msg(mid, "hello")).await,
        Handled::Duplicate
    );
    h.drain().await;
    assert_eq!(
        h.handle(channel_msg(mid, "hello")).await,
        Handled::Duplicate
    );
    assert!(!h.step().await);

    assert_eq!(h.count(), 1);
    assert_eq!(h.prompts().len(), 1);
    assert_eq!(h.threads_created(), 1);
    assert_eq!(h.sends().len(), 1);
    h.close().await;
}

#[tokio::test]
async fn omp_exit1_marks_needs_check_and_notifies() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_with(dir.path(), "hang", exit1_wrapper(dir.path()));
    let mid = "1300000000000000040";
    let id = queued(h.handle(channel_msg(mid, "hello")).await);
    assert!(h.step().await);

    let row = h.row(id);
    assert_eq!(row.state, State::NeedsCheck);
    assert_eq!(row.error.as_deref(), Some(E_CHILD_LOST));
    assert!(row.notice_sent);
    assert_eq!(h.prompts().len(), 1);
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), notice_text(N_INTERRUPTED).to_owned())]
    );
    h.close().await;
}

#[tokio::test]
async fn queued_behind_interrupted_is_held_under_one_notice() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_with(dir.path(), "hang", exit1_wrapper(dir.path()));
    let thread = "1300000000000000050";
    let first = queued(h.handle(channel_msg(thread, "first")).await);
    let second = queued(
        h.handle(event("1300000000000000051", OWNER, Some(thread), "second"))
            .await,
    );
    h.drain().await;

    assert_eq!(h.row(first).state, State::NeedsCheck);
    let held = h.row(second);
    assert_eq!(held.state, State::NeedsCheck);
    assert!(held.notice_sent);
    assert_eq!(h.prompts().len(), 1, "the held message never reaches omp");
    assert_eq!(
        h.sends(),
        vec![(thread.to_owned(), notice_text(N_INTERRUPTED).to_owned())]
    );
    h.close().await;
}

#[tokio::test]
async fn hold_behind_announced_interruption_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let thread = "1300000000000000055";
    let other = "1300000000000000056";
    let (first, other_id, second) = {
        let h = Harness::open_with(dir.path(), "hang", exit1_wrapper(dir.path()));
        let first = queued(h.handle(channel_msg(thread, "first")).await);
        let other_id = queued(h.handle(channel_msg(other, "other")).await);
        let second = queued(
            h.handle(event("1300000000000000057", OWNER, Some(thread), "second"))
                .await,
        );
        assert!(h.step().await);
        assert_eq!(h.row(first).state, State::NeedsCheck);
        assert_eq!(
            h.sends(),
            vec![(thread.to_owned(), notice_text(N_INTERRUPTED).to_owned())]
        );
        // The process stops while the other conversation's turn is in flight.
        h.store
            .lock()
            .unwrap()
            .mark(other_id, State::Processing, None)
            .unwrap();
        h.close().await;
        (first, other_id, second)
    };

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    h.drain().await;

    let prompted: Vec<Value> = h.prompts().iter().map(|p| p["message"].clone()).collect();
    assert_eq!(prompted, ["first"], "the held message never reaches omp");
    let held = h.row(second);
    assert_eq!(held.state, State::NeedsCheck);
    assert_eq!(held.error.as_deref(), Some(E_DISPATCH_HELD));
    assert!(held.notice_sent);
    assert!(h.row(first).notice_sent);
    assert_eq!(h.row(other_id).state, State::NeedsCheck);
    assert_eq!(
        h.sends(),
        vec![(other.to_owned(), notice_text(N_INTERRUPTED).to_owned())],
        "no second notice for the already announced thread"
    );
    h.close().await;
}

#[tokio::test]
async fn restart_with_processing_row_sends_notice_and_no_prompt() {
    let dir = tempfile::tempdir().unwrap();
    let mid = "1300000000000000060";
    let id = {
        let h = Harness::open(dir.path(), "ok");
        let id = queued(h.handle(channel_msg(mid, "hello")).await);
        h.store
            .lock()
            .unwrap()
            .mark(id, State::Processing, None)
            .unwrap();
        id
    };

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), notice_text(N_INTERRUPTED).to_owned())]
    );
    assert!(!h.step().await);
    let row = h.row(id);
    assert_eq!(row.state, State::NeedsCheck);
    assert!(row.notice_sent);
    drop(h);

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(!h.step().await);
    assert!(
        h.sends().is_empty(),
        "the notice is posted exactly once across restarts"
    );
    assert!(h.prompts().is_empty());
    assert_eq!(h.starts(), 0);
}

#[tokio::test]
async fn long_reply_split_into_chunks_all_under_2000() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let text = "word ".repeat(900);
    let id = queued(h.handle(channel_msg("1300000000000000070", &text)).await);
    h.drain().await;

    let reply = format!("echo: {text}");
    let chunks: Vec<String> = h.sends().into_iter().map(|(_, text)| text).collect();
    assert!(chunks.len() >= 3);
    assert!(chunks.iter().all(|c| c.chars().count() <= LIMIT));
    assert_eq!(chunks.concat(), reply);
    assert_eq!(h.row(id).state, State::Done);
    h.close().await;
}

#[test]
fn reply_longer_than_ten_messages_is_cut_with_note() {
    let text = "x".repeat(LIMIT * (MAX_REPLY_MESSAGES + 3));
    let chunks = reply_messages(&text, LIMIT);
    assert_eq!(chunks.len(), MAX_REPLY_MESSAGES);
    assert!(chunks.iter().all(|c| c.chars().count() <= LIMIT));
    assert!(
        chunks
            .last()
            .unwrap()
            .ends_with(notice_text(N_REPLY_TRUNCATED))
    );
    assert_eq!(reply_messages("short", LIMIT), ["short"]);
}

#[tokio::test]
async fn slash_message_not_forwarded() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let handled = h
        .handle(channel_msg("1300000000000000080", "/model x"))
        .await;
    assert_eq!(handled, Handled::Notice(N_UNSUPPORTED_INPUT));
    assert!(!h.step().await);
    assert!(h.prompts().is_empty());
    assert_eq!(h.count(), 0);
    assert_eq!(
        h.sends(),
        vec![(
            CHANNEL.to_owned(),
            notice_text(N_UNSUPPORTED_INPUT).to_owned()
        )]
    );

    let empty = h.handle(channel_msg("1300000000000000081", "  ")).await;
    assert_eq!(empty, Handled::Notice(N_UNSUPPORTED_INPUT));
    assert_eq!(h.count(), 0);
    assert_eq!(h.threads_created(), 0);
}

#[tokio::test]
async fn open_session_failure_no_prompt_one_notice() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "open_session_fail");
    let mid = "1300000000000000090";
    let id = queued(h.handle(channel_msg(mid, "hello")).await);
    assert!(h.step().await);

    let row = h.row(id);
    assert_eq!(row.state, State::Failed);
    assert_eq!(row.error.as_deref(), Some(E_OPEN_SESSION));
    assert!(h.prompts().is_empty());
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), notice_text(N_FAILED).to_owned())]
    );
    h.close().await;
}

#[tokio::test]
async fn send_failure_keeps_reply_pending_then_delivers_once() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let mid = "1300000000000000100";
    let id = queued(h.handle(channel_msg(mid, "hello")).await);
    h.adapter.fail_next_send();
    assert!(h.step().await);

    let row = h.row(id);
    assert_eq!(row.state, State::ReplyPending);
    assert_eq!(row.reply.as_deref(), Some("echo: hello"));

    assert_eq!(bounded(h.dispatcher.resend_pending()).await.unwrap(), 1);
    assert_eq!(h.row(id).state, State::Done);
    assert_eq!(bounded(h.dispatcher.resend_pending()).await.unwrap(), 0);
    assert!(!h.step().await);

    let sends = h.sends();
    assert_eq!(sends.len(), 2, "one failed attempt, one delivery");
    assert!(
        sends
            .iter()
            .all(|(channel, text)| channel == mid && text == "echo: hello")
    );
    assert_eq!(h.prompts().len(), 1);
    h.close().await;
}

#[tokio::test]
async fn reply_pending_is_resent_once_after_adapter_reconnect() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let inbound = h.adapter.start().await.unwrap();
    let runner = tokio::spawn(Arc::clone(&h.dispatcher).run(inbound));
    let mid = "1300000000000000115";
    h.adapter.fail_next_send();
    h.adapter.inject(channel_msg(mid, "hello")).await;
    bounded(h.adapter.wait_for_send(1)).await;
    // The reply is stored before the (failed) send, so the row is reply_pending now.
    assert_eq!(h.row(1).state, State::ReplyPending);

    h.adapter.reconnect();
    bounded(h.adapter.wait_for_send(2)).await;
    // A later message in the same thread is served after the resend finished, so the
    // first row is settled and the same omp child (no restart) is still in use.
    h.adapter
        .inject(event("1300000000000000116", OWNER, Some(mid), "second"))
        .await;
    bounded(h.adapter.wait_for_send(3)).await;

    assert_eq!(h.row(1).state, State::Done);
    assert_eq!(
        h.sends(),
        vec![
            (mid.to_owned(), "echo: hello".to_owned()),
            (mid.to_owned(), "echo: hello".to_owned()),
            (mid.to_owned(), "echo: second".to_owned()),
        ],
        "one failed attempt, exactly one delivery after the reconnect"
    );
    let hello_prompts = h
        .prompts()
        .iter()
        .filter(|p| p["message"] == "hello")
        .count();
    assert_eq!(hello_prompts, 1);
    assert_eq!(h.starts(), 1);
    runner.abort();
    let _ = runner.await;
    h.close().await;
}

#[tokio::test]
async fn reply_pending_is_resent_once_on_startup() {
    let dir = tempfile::tempdir().unwrap();
    let mid = "1300000000000000110";
    let id = {
        let h = Harness::open(dir.path(), "ok");
        let id = queued(h.handle(channel_msg(mid, "hello")).await);
        h.adapter.fail_next_send();
        assert!(h.step().await);
        assert_eq!(h.row(id).state, State::ReplyPending);
        h.close().await;
        id
    };

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends(), vec![(mid.to_owned(), "echo: hello".to_owned())]);
    assert_eq!(h.row(id).state, State::Done);
    drop(h);

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(h.sends().is_empty());
    assert_eq!(h.prompts().len(), 1);
}

#[tokio::test]
async fn injection_payload_reaches_omp_byte_for_byte_and_never_runs() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let payload = "$(touch /tmp/moraebot-pwned-7f3a); touch /tmp/moraebot-pwned-7f3a-b";
    let mid = "1300000000000000120";
    queued(h.handle(channel_msg(mid, payload)).await);
    h.drain().await;

    let prompts = h.prompts();
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0]["message"], payload);
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), format!("echo: {payload}"))]
    );
    assert!(!Path::new("/tmp/moraebot-pwned-7f3a").exists());
    assert!(!Path::new("/tmp/moraebot-pwned-7f3a-b").exists());
    h.close().await;
}

#[tokio::test]
async fn token_sentinel_absent_from_logs_db_and_omp_io() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let guard = moraebot::logging::init(&h.config.home, moraebot::logging::CORE_LOG_PREFIX)
        .expect("logging");
    queued(h.handle(channel_msg("1300000000000000130", "hello")).await);
    h.drain().await;
    assert_eq!(h.sends().len(), 1);
    let home = h.config.home.clone();
    let fake_log = dir.path().join("fake-omp.log");
    h.close().await;
    drop(guard);

    let mut checked = Vec::new();
    for sub in [home.clone(), home.join("logs")] {
        for entry in std::fs::read_dir(&sub).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if path.is_file() && (name.starts_with("inbox.sqlite") || sub.ends_with("logs")) {
                checked.push(path);
            }
        }
    }
    checked.push(fake_log);
    assert!(
        checked
            .iter()
            .any(|p| p.to_string_lossy().contains("moraebot.log"))
    );
    assert!(checked.iter().any(|p| p.ends_with("inbox.sqlite")));
    for path in checked {
        let bytes = std::fs::read(&path).unwrap();
        let found = bytes
            .windows(TOKEN_SENTINEL.len())
            .any(|w| w == TOKEN_SENTINEL.as_bytes());
        assert!(!found, "token sentinel leaked into {}", path.display());
    }
}

// ---- slice 3: inbound and outbound files ----

fn attachment(name: &str, reference: &str, size: u64) -> InboundAttachment {
    InboundAttachment {
        name: name.into(),
        size,
        mime: Some("text/plain".into()),
        reference: AttachmentRef(reference.into()),
    }
}

fn with_files(mut ev: InboundEvent, attachments: Vec<InboundAttachment>) -> InboundEvent {
    ev.attachments = attachments;
    ev
}

fn fetches(h: &Harness) -> Vec<PathBuf> {
    h.adapter
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            Call::FetchAttachment { dest, .. } => Some(dest),
            _ => None,
        })
        .collect()
}

fn file_sends(h: &Harness) -> Vec<(String, u64)> {
    h.adapter
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            Call::SendFile { name, bytes, .. } => Some((name, bytes)),
            _ => None,
        })
        .collect()
}

fn notices(h: &Harness, code: &str) -> usize {
    h.sends()
        .iter()
        .filter(|(_, text)| text == notice_text(code))
        .count()
}

fn out_files(h: &Harness, id: i64) -> Vec<OutFile> {
    serde_json::from_str(h.row(id).out_files.as_deref().expect("out_files stored")).unwrap()
}

/// The paths listed in a prompt's attached-files block.
fn listed_paths(message: &str) -> Vec<PathBuf> {
    message
        .lines()
        .filter_map(|l| l.strip_prefix("- "))
        .filter_map(|l| l.split_once(" (name: ").map(|(p, _)| PathBuf::from(p)))
        .collect()
}

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

fn handled_files_rejected(handled: Handled, notice: &str) {
    assert_eq!(
        handled,
        Handled::FilesRejected(match notice {
            N_FILE_NOT_ALLOWED => N_FILE_NOT_ALLOWED,
            _ => N_FILE_NOT_RECEIVED,
        })
    );
}

/// A file in the harness work folder; returns its canonical path.
fn work_file(h: &Harness, name: &str, bytes: &[u8]) -> PathBuf {
    let path = h.config.work_dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    path.canonicalize().unwrap()
}

/// Leaves a `reply_pending` row with its text delivered and one outbound file, as a
/// process that stopped during the file upload would (`attempting` when `in_flight`).
async fn seed_reply_with_file(h: &Harness, mid: &str, file: &Path, in_flight: bool) -> i64 {
    let id = queued(h.handle(channel_msg(mid, "hello")).await);
    let entry = OutFile {
        path: file.to_owned(),
        name: file.file_name().unwrap().to_string_lossy().into_owned(),
        sent: false,
        skipped: false,
        attempting: false,
        attempts: 0,
    };
    let json = serde_json::to_string(&vec![entry]).unwrap();
    let store = h.store.lock().unwrap();
    store.mark(id, State::Processing, None).unwrap();
    store.save_reply(id, "echo: hello", Some(&json)).unwrap();
    store.mark_text_sent(id).unwrap();
    if in_flight {
        store.mark_file_attempting(id, 0).unwrap();
    }
    id
}

#[tokio::test]
async fn attachment_downloaded_after_store_and_listed_in_prompt() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let mid = "1300000000000000200";
    let inbox = h.config.work_dir.join(files::INBOX_DIR);
    let slug_dir = inbox.join(format!("fake-th-{mid}"));
    let row_dir = slug_dir.join("1");
    assert!(!inbox.exists());
    h.adapter
        .set_attachment_bytes("ref-1", b"hello file".to_vec());
    let gate = h.adapter.hold_next_fetch();
    let ev = with_files(
        channel_msg(mid, "look at this"),
        vec![attachment("notes.txt", "ref-1", 10)],
    );
    let d = Arc::clone(&h.dispatcher);
    let join = tokio::spawn(async move { d.handle_event(ev).await });
    bounded(
        h.adapter
            .wait_for_call(|c| matches!(c, Call::FetchAttachment { .. })),
    )
    .await;
    // The fetch runs only once the row is stored, and the row waits for its files.
    assert_eq!(h.count(), 1);
    let waiting = h.row(1);
    assert_eq!(waiting.state, State::Queued);
    assert!(!waiting.files_ready);
    gate.notify_one();
    let id = queued(bounded(join).await.unwrap().unwrap());
    h.drain().await;

    for level in [&inbox, &slug_dir, &row_dir] {
        assert_eq!(mode(level), 0o700, "inbox level mode");
    }
    let prompts = h.prompts();
    assert_eq!(prompts.len(), 1);
    let message = prompts[0]["message"].as_str().unwrap().to_owned();
    assert!(message.starts_with("look at this\n\n[Attached files, saved on disk]\n"));
    let listed = listed_paths(&message);
    assert_eq!(listed, fetches(&h));
    assert_eq!(listed.len(), 1);
    assert!(listed[0].is_absolute());
    assert_eq!(listed[0].parent().unwrap(), row_dir);
    assert_eq!(std::fs::read(&listed[0]).unwrap(), b"hello file");
    assert_eq!(mode(&listed[0]), 0o600);
    assert_eq!(h.row(id).state, State::Done);
    h.close().await;
}

#[tokio::test]
async fn relative_work_dir_lists_absolute_paths() {
    std::fs::create_dir_all("target").unwrap();
    let tmp = tempfile::Builder::new()
        .prefix("s3-rel-")
        .tempdir_in("target")
        .unwrap();
    let cwd = std::env::current_dir().unwrap();
    let root = tmp.path().strip_prefix(&cwd).unwrap_or(tmp.path());
    assert!(root.is_relative());
    let h = Harness::open(root, "ok");
    assert!(h.config.work_dir.is_relative());
    h.adapter.set_attachment_bytes("ref-1", b"abc".to_vec());
    let mid = "1300000000000000201";
    queued(
        h.handle(with_files(
            channel_msg(mid, "hi"),
            vec![attachment("a.txt", "ref-1", 3)],
        ))
        .await,
    );
    h.drain().await;

    let message = h.prompts()[0]["message"].as_str().unwrap().to_owned();
    let listed = listed_paths(&message);
    assert_eq!(listed.len(), 1);
    assert!(listed[0].is_absolute());
    let expected_dir = cwd
        .join(root)
        .join("work")
        .join(files::INBOX_DIR)
        .join(format!("fake-th-{mid}"))
        .join("1");
    assert_eq!(listed[0].parent().unwrap(), expected_dir);
    assert!(listed[0].is_file());
    h.close().await;
}

#[tokio::test]
async fn inbox_dir_failure_fails_row_without_fetch() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    std::fs::write(h.config.work_dir.join(files::INBOX_DIR), b"not a folder").unwrap();
    let mid = "1300000000000000202";
    let handled = h
        .handle(with_files(
            channel_msg(mid, "hi"),
            vec![attachment("a.txt", "ref-1", 3)],
        ))
        .await;
    handled_files_rejected(handled, N_FILE_NOT_RECEIVED);
    assert!(!h.step().await);
    let row = h.row(1);
    assert_eq!(row.state, State::Failed);
    assert_eq!(row.error.as_deref(), Some(files::E_FILE_WRITE));
    assert!(row.notice_sent);
    assert!(fetches(&h).is_empty());
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), notice_text(N_FILE_NOT_RECEIVED).to_owned())]
    );
    assert!(h.prompts().is_empty());
    h.close().await;
}

#[tokio::test]
async fn attachment_only_message_runs() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    h.adapter.set_attachment_bytes("ref-1", b"img".to_vec());
    let mid = "1300000000000000203";
    let id = queued(
        h.handle(with_files(
            channel_msg(mid, ""),
            vec![attachment("photo.png", "ref-1", 3)],
        ))
        .await,
    );
    h.drain().await;
    let message = h.prompts()[0]["message"].as_str().unwrap().to_owned();
    assert!(message.starts_with("[Attached files, saved on disk]\n"));
    assert_eq!(listed_paths(&message).len(), 1);
    assert_eq!(h.row(id).state, State::Done);
    assert_eq!(notices(&h, N_UNSUPPORTED_INPUT), 0);
    h.close().await;
}

#[tokio::test]
async fn non_owner_attachment_never_fetched() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let handled = h
        .handle(with_files(
            event("1300000000000000204", STRANGER, None, "hi"),
            vec![attachment("a.txt", "ref-1", 3)],
        ))
        .await;
    assert_eq!(handled, Handled::Dropped(DropReason::NotOwner));
    assert_eq!(h.count(), 0);
    assert!(h.adapter.calls().is_empty());
    assert!(!h.config.work_dir.join(files::INBOX_DIR).exists());
}

#[tokio::test]
async fn too_many_files_rejected_without_store() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let many: Vec<InboundAttachment> = (0..=files::MAX_FILES)
        .map(|i| attachment(&format!("f{i}.txt"), &format!("ref-{i}"), 1))
        .collect();
    assert_eq!(many.len(), 11);
    let handled = h
        .handle(with_files(channel_msg("1300000000000000205", "hi"), many))
        .await;
    assert_eq!(handled, Handled::FilesRejected(N_FILE_NOT_RECEIVED));
    assert_eq!(h.count(), 0);
    assert!(fetches(&h).is_empty());
    assert_eq!(h.threads_created(), 0);
    assert_eq!(
        h.sends(),
        vec![(
            CHANNEL.to_owned(),
            notice_text(N_FILE_NOT_RECEIVED).to_owned()
        )]
    );
}

#[tokio::test]
async fn large_declared_size_is_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    h.adapter.set_attachment_bytes("ref-1", b"small".to_vec());
    let mid = "1300000000000000206";
    let id = queued(
        h.handle(with_files(
            channel_msg(mid, "big"),
            vec![attachment("big.bin", "ref-1", 5 * 1024 * 1024 * 1024)],
        ))
        .await,
    );
    h.drain().await;
    assert_eq!(fetches(&h).len(), 1);
    assert_eq!(h.prompts().len(), 1);
    assert_eq!(h.row(id).state, State::Done);
    assert_eq!(h.sends().len(), 1, "only the reply, no notice");
    h.close().await;
}

async fn fetch_fails_with(code: &'static str, notice: &'static str) {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    h.adapter.fail_next_fetch(code);
    let mid = "1300000000000000207";
    let handled = h
        .handle(with_files(
            channel_msg(mid, "hi"),
            vec![attachment("a.txt", "ref-1", 3)],
        ))
        .await;
    handled_files_rejected(handled, notice);
    assert!(!h.step().await);
    let row = h.row(1);
    assert_eq!(row.state, State::Failed);
    assert_eq!(row.error.as_deref(), Some(code));
    assert!(row.notice_sent);
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), notice_text(notice).to_owned())]
    );
    assert!(h.prompts().is_empty());
    h.close().await;
}

#[tokio::test]
async fn fetch_failure_fails_row_with_notice() {
    fetch_fails_with(files::E_FILE_FETCH, N_FILE_NOT_RECEIVED).await;
}

#[tokio::test]
async fn disk_full_fails_row_with_notice() {
    fetch_fails_with(files::E_FILE_WRITE, N_FILE_NOT_RECEIVED).await;
}

#[tokio::test]
async fn denied_download_posts_permission_notice() {
    fetch_fails_with(files::E_FILE_DENIED, N_FILE_NOT_ALLOWED).await;
}

#[tokio::test(start_paused = true)]
async fn fetch_timeout_fails_row_with_notice() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let _gate = h.adapter.hold_next_fetch();
    let mid = "1300000000000000208";
    let ev = with_files(
        channel_msg(mid, "hi"),
        vec![attachment("a.txt", "ref-1", 3)],
    );
    let start = tokio::time::Instant::now();
    let handled = tokio::time::timeout(files::STALL_TIMEOUT + BOUND, h.dispatcher.handle_event(ev))
        .await
        .expect("the stall watchdog ends the fetch")
        .unwrap();
    let elapsed = start.elapsed();
    assert!(elapsed >= files::STALL_TIMEOUT, "elapsed {elapsed:?}");
    assert!(
        elapsed < files::STALL_TIMEOUT + BOUND,
        "elapsed {elapsed:?}"
    );
    assert_eq!(handled, Handled::FilesRejected(N_FILE_NOT_RECEIVED));
    let row = h.row(1);
    assert_eq!(row.state, State::Failed);
    assert_eq!(row.error.as_deref(), Some(files::E_FILE_FETCH));
    assert_eq!(notices(&h, N_FILE_NOT_RECEIVED), 1);

    let slash = h.handle(channel_msg("1300000000000000209", "/x")).await;
    assert_eq!(slash, Handled::Notice(N_UNSUPPORTED_INPUT));
}

#[tokio::test(start_paused = true)]
async fn slow_progressing_download_succeeds_past_stall_window() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let mut feed = h.adapter.stream_next_fetch();
    let ev = with_files(
        channel_msg("1300000000000000210", "hi"),
        vec![attachment("slow.bin", "ref-1", 16)],
    );
    let start = tokio::time::Instant::now();
    let d = Arc::clone(&h.dispatcher);
    let join = tokio::spawn(async move { d.handle_event(ev).await });
    bounded(
        h.adapter
            .wait_for_call(|c| matches!(c, Call::FetchAttachment { .. })),
    )
    .await;
    for total in [4u64, 8, 12, 16] {
        tokio::time::advance(Duration::from_secs(100)).await;
        feed.chunks.send(vec![b'x'; 4]).await.unwrap();
        assert_eq!(bounded(feed.written.recv()).await, Some(total));
    }
    drop(feed.chunks);
    let id = queued(bounded(join).await.unwrap().unwrap());
    assert!(start.elapsed() >= Duration::from_secs(400));
    let row = h.row(id);
    assert_eq!(row.state, State::Queued);
    assert!(row.files_ready);
    let saved = fetches(&h);
    assert_eq!(std::fs::read(&saved[0]).unwrap().len(), 16);
    assert_eq!(notices(&h, N_FILE_NOT_RECEIVED), 0);
}

#[tokio::test]
async fn download_ending_after_hold_keeps_hold_and_one_notice() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_with(dir.path(), "hang", exit1_wrapper(dir.path()));
    let thread = "1300000000000000211";
    let first = queued(h.handle(channel_msg(thread, "first")).await);
    h.adapter.fail_next_fetch(files::E_FILE_FETCH);
    let gate = h.adapter.hold_next_fetch();
    let b = with_files(
        event("1300000000000000212", OWNER, Some(thread), "second"),
        vec![attachment("a.txt", "ref-1", 3)],
    );
    let d = Arc::clone(&h.dispatcher);
    let join = tokio::spawn(async move { d.handle_event(b).await });
    bounded(
        h.adapter
            .wait_for_call(|c| matches!(c, Call::FetchAttachment { .. })),
    )
    .await;
    assert!(h.step().await);
    assert_eq!(h.row(first).state, State::NeedsCheck);
    gate.notify_one();
    bounded(join).await.unwrap().unwrap();

    let held = h.row(2);
    assert_eq!(held.state, State::NeedsCheck);
    assert_eq!(held.error.as_deref(), Some(E_DISPATCH_HELD));
    assert_eq!(
        h.sends(),
        vec![(thread.to_owned(), notice_text(N_INTERRUPTED).to_owned())]
    );
    assert_eq!(notices(&h, N_FILE_NOT_RECEIVED), 0);
    h.close().await;
}

#[tokio::test]
async fn slash_with_attachment_still_refused() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let handled = h
        .handle(with_files(
            channel_msg("1300000000000000213", "/model x"),
            vec![attachment("a.txt", "ref-1", 3)],
        ))
        .await;
    assert_eq!(handled, Handled::Notice(N_UNSUPPORTED_INPUT));
    assert_eq!(h.count(), 0);
    assert!(fetches(&h).is_empty());
}

#[tokio::test]
async fn outbound_file_sent_after_text() {
    let dir = tempfile::tempdir().unwrap();
    let report = dir.path().join("work").join("report.txt");
    let h = Harness::open_sending(dir.path(), std::slice::from_ref(&report));
    work_file(&h, "report.txt", b"twelve bytes");
    let mid = "1300000000000000220";
    let id = queued(h.handle(channel_msg(mid, "make a report")).await);
    h.drain().await;

    let order: Vec<&str> = h
        .adapter
        .calls()
        .iter()
        .filter_map(|c| match c {
            Call::Send { .. } => Some("send"),
            Call::SendFile { .. } => Some("file"),
            _ => None,
        })
        .collect();
    assert_eq!(order, ["send", "file"]);
    assert_eq!(file_sends(&h), vec![("report.txt".to_owned(), 12)]);
    assert_eq!(h.row(id).state, State::Done);
    let entries = out_files(&h, id);
    assert!(entries[0].sent && !entries[0].skipped && !entries[0].attempting);
    h.close().await;
}

#[tokio::test]
async fn outbound_transient_error_notifies_once_and_resends_file() {
    let dir = tempfile::tempdir().unwrap();
    let report = dir.path().join("work").join("report.txt");
    let h = Harness::open_sending(dir.path(), std::slice::from_ref(&report));
    work_file(&h, "report.txt", b"data");
    h.adapter.fail_next_send_file(E_ADAPTER_UPLOAD);
    let id = queued(h.handle(channel_msg("1300000000000000221", "go")).await);
    h.drain().await;
    assert_eq!(h.row(id).state, State::ReplyPending);
    assert_eq!(notices(&h, N_FILE_NOT_SENT), 1);
    assert!(!out_files(&h, id)[0].attempting);

    bounded(h.dispatcher.resend_pending()).await.unwrap();
    assert_eq!(file_sends(&h).len(), 2);
    assert_eq!(h.sends().len(), 2, "one text send, one notice");
    assert_eq!(notices(&h, N_FILE_NOT_SENT), 1);
    assert_eq!(h.row(id).state, State::Done);
    assert!(out_files(&h, id)[0].sent);
    h.close().await;
}

#[tokio::test]
async fn outbound_permission_error_skips_file_with_one_notice() {
    let dir = tempfile::tempdir().unwrap();
    let report = dir.path().join("work").join("report.txt");
    let h = Harness::open_sending(dir.path(), std::slice::from_ref(&report));
    work_file(&h, "report.txt", b"data");
    h.adapter.fail_next_send_file(E_ADAPTER_UPLOAD_DENIED);
    let id = queued(h.handle(channel_msg("1300000000000000222", "go")).await);
    h.drain().await;
    assert_eq!(h.row(id).state, State::Done);
    assert!(out_files(&h, id)[0].skipped);
    assert_eq!(notices(&h, N_FILE_NOT_SENT), 1);
    assert_eq!(h.sends().len(), 2, "one text send, one notice");
    assert_eq!(bounded(h.dispatcher.resend_pending()).await.unwrap(), 0);
    assert_eq!(file_sends(&h).len(), 1);
    h.close().await;
}

#[tokio::test]
async fn outbound_file_deleted_before_send_is_skipped_with_notice() {
    let dir = tempfile::tempdir().unwrap();
    let report = dir.path().join("work").join("report.txt");
    let h = Harness::open_sending(dir.path(), std::slice::from_ref(&report));
    let path = work_file(&h, "report.txt", b"data");
    h.adapter.fail_next_send_file(E_ADAPTER_UPLOAD);
    let id = queued(h.handle(channel_msg("1300000000000000223", "go")).await);
    h.drain().await;
    assert_eq!(h.row(id).state, State::ReplyPending);
    std::fs::remove_file(&path).unwrap();
    bounded(h.dispatcher.resend_pending()).await.unwrap();
    assert_eq!(file_sends(&h).len(), 1, "no upload of the deleted file");
    assert!(out_files(&h, id)[0].skipped);
    assert_eq!(notices(&h, N_FILE_NOT_SENT), 1);
    assert_eq!(h.row(id).state, State::Done);
    h.close().await;
}

#[tokio::test]
async fn outbound_unreadable_file_is_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let h = Harness::open_sending(dir.path(), &[work.join("one.txt"), work.join("two.txt")]);
    let one = work_file(&h, "one.txt", b"first");
    work_file(&h, "two.txt", b"second");
    h.adapter.fail_next_send_file(E_ADAPTER_UPLOAD);
    let id = queued(h.handle(channel_msg("1300000000000000224", "go")).await);
    h.drain().await;
    assert_eq!(h.row(id).state, State::ReplyPending);
    assert_eq!(notices(&h, N_FILE_NOT_SENT), 1);

    std::fs::set_permissions(&one, std::fs::Permissions::from_mode(0o000)).unwrap();
    h.adapter.reconnect();
    bounded(h.dispatcher.resend_pending()).await.unwrap();
    std::fs::set_permissions(&one, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        file_sends(&h),
        vec![("one.txt".to_owned(), 5), ("two.txt".to_owned(), 6)],
        "the unreadable file is not uploaded again"
    );
    let entries = out_files(&h, id);
    assert!(entries[0].skipped && entries[1].sent);
    assert_eq!(notices(&h, N_FILE_NOT_SENT), 1);
    assert_eq!(h.sends().len(), 2, "one text send, one notice");
    assert_eq!(h.row(id).state, State::Done);
    h.close().await;
}

#[tokio::test]
async fn startup_announces_incomplete_download() {
    let dir = tempfile::tempdir().unwrap();
    let mid = "1300000000000000225";
    let id = {
        let h = Harness::open(dir.path(), "ok");
        let mut store = h.store.lock().unwrap();
        let admitted = store
            .admit_with_files(
                "fake",
                mid,
                &format!("fake:th:{mid}"),
                CHANNEL,
                OWNER,
                "hi",
                Some(r#"[{"name":"a.txt","size":3,"mime":null}]"#),
            )
            .unwrap();
        store.remember_thread("fake", mid).unwrap();
        drop(store);
        match admitted {
            moraebot::store::Admitted::New(id) => id,
            other => panic!("expected New, got {other:?}"),
        }
    };

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(!h.step().await);
    let row = h.row(id);
    assert_eq!(row.state, State::Failed);
    assert_eq!(row.error.as_deref(), Some(files::E_FILE_INCOMPLETE));
    assert!(row.notice_sent);
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), notice_text(N_FILE_NOT_RECEIVED).to_owned())]
    );
    assert!(h.prompts().is_empty());
    drop(h);

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(h.sends().is_empty(), "the notice is posted once");
}

#[tokio::test]
async fn file_in_flight_at_crash_is_skipped_once_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mid = "1300000000000000226";
    let id = {
        let h = Harness::open(dir.path(), "ok");
        let file = work_file(&h, "report.txt", b"data");
        let id = seed_reply_with_file(&h, mid, &file, true).await;
        h.close().await;
        id
    };

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(file_sends(&h).is_empty());
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), notice_text(N_FILE_NOT_SENT).to_owned())]
    );
    assert_eq!(h.row(id).state, State::Done);
    let entry = &out_files(&h, id)[0];
    assert!(entry.skipped && !entry.attempting);
    h.close().await;

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(h.adapter.calls().is_empty());
    h.close().await;
}

#[tokio::test(start_paused = true)]
async fn upload_timeout_skips_file_with_one_notice() {
    let dir = tempfile::tempdir().unwrap();
    let mid = "1300000000000000227";
    let id = {
        let h = Harness::open(dir.path(), "ok");
        let file = work_file(&h, "report.txt", b"data");
        let id = seed_reply_with_file(&h, mid, &file, false).await;
        h.close().await;
        id
    };

    let h = Harness::open(dir.path(), "ok");
    let _gate = h.adapter.hold_next_send_file();
    let start = tokio::time::Instant::now();
    tokio::time::timeout(files::UPLOAD_TIMEOUT + BOUND, h.dispatcher.startup())
        .await
        .expect("the upload timeout ends the call")
        .unwrap();
    let elapsed = start.elapsed();
    assert!(elapsed >= files::UPLOAD_TIMEOUT, "elapsed {elapsed:?}");
    assert!(
        elapsed < files::UPLOAD_TIMEOUT + BOUND,
        "elapsed {elapsed:?}"
    );
    assert_eq!(file_sends(&h).len(), 1);
    assert_eq!(h.row(id).state, State::Done);
    let entry = &out_files(&h, id)[0];
    assert!(entry.skipped && !entry.attempting);
    assert_eq!(entry.attempts, 1);
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), notice_text(N_FILE_NOT_SENT).to_owned())],
        "one notice, no text send"
    );
    assert_eq!(bounded(h.dispatcher.resend_pending()).await.unwrap(), 0);
    assert_eq!(file_sends(&h).len(), 1);
    h.close().await;

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(h.adapter.calls().is_empty());
    h.close().await;
}

#[tokio::test]
async fn transient_upload_failure_is_skipped_after_max_attempts() {
    let dir = tempfile::tempdir().unwrap();
    let mid = "1300000000000000228";
    let id = {
        let h = Harness::open(dir.path(), "ok");
        let file = work_file(&h, "report.txt", b"data");
        let id = seed_reply_with_file(&h, mid, &file, false).await;
        h.close().await;
        id
    };

    let h = Harness::open(dir.path(), "ok");
    h.adapter.fail_next_send_file(E_ADAPTER_UPLOAD);
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(file_sends(&h).len(), 1);
    assert_eq!(notices(&h, N_FILE_NOT_SENT), 1);
    assert_eq!(h.row(id).state, State::ReplyPending);
    for attempts in [2, 3] {
        h.adapter.fail_next_send_file(E_ADAPTER_UPLOAD);
        assert_eq!(bounded(h.dispatcher.resend_pending()).await.unwrap(), 0);
        assert_eq!(file_sends(&h).len(), attempts);
    }
    bounded(h.dispatcher.resend_pending()).await.unwrap();
    assert_eq!(file_sends(&h).len(), 3, "no fourth attempt");
    let entry = &out_files(&h, id)[0];
    assert!(entry.skipped && !entry.sent);
    assert_eq!(entry.attempts, files::MAX_UPLOAD_ATTEMPTS);
    assert_eq!(h.row(id).state, State::Done);
    assert_eq!(
        h.sends(),
        vec![(mid.to_owned(), notice_text(N_FILE_NOT_SENT).to_owned())],
        "one notice in total, no text send"
    );
    h.close().await;

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(h.adapter.calls().is_empty());
    h.close().await;
}

#[tokio::test]
async fn next_message_waits_while_file_downloads() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let inbound = h.adapter.start().await.unwrap();
    let runner = tokio::spawn(Arc::clone(&h.dispatcher).run(inbound));
    let first = "1300000000000000230";
    let second = "1300000000000000231";
    let gate = h.adapter.hold_next_fetch();
    h.adapter
        .inject(with_files(
            channel_msg(first, "with file"),
            vec![attachment("a.txt", "ref-1", 3)],
        ))
        .await;
    bounded(
        h.adapter
            .wait_for_call(|c| matches!(c, Call::FetchAttachment { .. })),
    )
    .await;
    h.adapter.inject(channel_msg(second, "plain")).await;
    // `run` awaits one event at a time, so the second message is not admitted yet.
    assert_eq!(h.count(), 1);
    let second_thread = |c: &Call| matches!(c, Call::CreateThread { from_message_id, .. } if from_message_id == second);
    assert!(!h.adapter.calls().iter().any(second_thread));

    gate.notify_one();
    bounded(h.adapter.wait_for_call(second_thread)).await;
    bounded(h.adapter.wait_for_send(2)).await;
    let targets: Vec<String> = h.sends().into_iter().map(|(channel, _)| channel).collect();
    assert_eq!(targets, [first, second], "FIFO order unchanged");
    runner.abort();
    let _ = runner.await;
    h.close().await;
}

// --- One lead for every conversation, lane rows and steer (todo 7) ---

const THREAD_A: &str = "111";
const THREAD_B: &str = "222";
const DM_CHANNEL: &str = "D333";
const LANE_KEY: &str = "discord:th:222";

fn in_thread(message_id: &str, thread: &str, text: &str) -> InboundEvent {
    event(message_id, OWNER, Some(thread), text)
}

fn dm(message_id: &str, channel: &str, text: &str) -> InboundEvent {
    let mut ev = event(message_id, OWNER, None, text);
    ev.channel_id = channel.into();
    ev.is_dm = true;
    ev
}

fn steered(handled: Handled) -> i64 {
    match handled {
        Handled::Steered(id) => id,
        other => panic!("expected Steered, got {other:?}"),
    }
}

impl Harness {
    /// fake-omp in mode `steer`: the first prompt starts a run and waits for a steer,
    /// answered per `case` (`merged`, `separate`, `hold`).
    fn open_steer(root: &Path, case: &str) -> Self {
        Self::build(
            root,
            "steer",
            PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")),
            false,
            vec![("FAKE_OMP_STEER_CASE", case.to_owned())],
        )
    }

    /// Runs one worker step on its own task: it returns once the turn's group is settled.
    fn spawn_step(&self) -> JoinHandle<bool> {
        let dispatcher = Arc::clone(&self.dispatcher);
        tokio::spawn(async move { dispatcher.process_next().await.expect("worker step") })
    }

    /// Waits until the lead reported the start of a turn for conversation `key`.
    async fn wait_turn(&self, key: &str) {
        let mut turn = self.dispatcher.running_turn();
        bounded(turn.wait_for(|running| running.as_deref() == Some(key)))
            .await
            .map(|_| ())
            .expect("omp manager alive");
    }

    /// Handles an event on its own task (for an event whose fetch is held).
    fn spawn_handle(&self, ev: InboundEvent) -> JoinHandle<Handled> {
        let dispatcher = Arc::clone(&self.dispatcher);
        tokio::spawn(async move { dispatcher.handle_event(ev).await.expect("handle event") })
    }

    async fn wait_for_fetch(&self) {
        bounded(
            self.adapter
                .wait_for_call(|c| matches!(c, Call::FetchAttachment { .. })),
        )
        .await;
    }

    /// The raw prompt frames, tag line included.
    fn raw_prompts(&self) -> Vec<Value> {
        self.log_lines()
            .iter()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|f| f["type"] == "prompt")
            .collect()
    }
}

/// The prompt frame on a fake-omp log line, its `message` turned into the body: the frame
/// must start with a moraebot tag line, which is dropped.
fn prompt_frame(line: &str) -> Option<Value> {
    let mut frame = serde_json::from_str::<Value>(line)
        .ok()
        .filter(|f| f["type"] == "prompt")?;
    let message = frame["message"].as_str().expect("prompt message");
    assert!(
        lead::parse_tag(message).is_some(),
        "prompt starts with a moraebot tag"
    );
    let body = message.split_once('\n').map_or("", |(_, body)| body);
    frame["message"] = Value::String(body.to_owned());
    Some(frame)
}

/// Streams the lines fake-omp appends to `FAKE_OMP_LOG`. The log is a FIFO that the tap
/// holds open for reading and writing, so a test waits for a line instead of polling.
struct LogTap {
    path: PathBuf,
    lines: mpsc::UnboundedReceiver<String>,
}

impl LogTap {
    /// Makes `path` a FIFO; create it before the first fake-omp child starts.
    fn create(path: &Path) -> Self {
        let made = std::process::Command::new("mkfifo")
            .arg(path)
            .status()
            .expect("run mkfifo");
        assert!(made.success(), "mkfifo failed");
        let fifo = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open fifo");
        let (tx, lines) = mpsc::unbounded_channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(fifo).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            path: path.to_owned(),
            lines,
        }
    }

    async fn next_line(&mut self) -> String {
        bounded(self.lines.recv()).await.expect("log tap open")
    }

    /// Waits for the next prompt frame (see [`prompt_frame`]).
    async fn next_prompt(&mut self) -> Value {
        loop {
            if let Some(frame) = prompt_frame(&self.next_line().await) {
                return frame;
            }
        }
    }

    /// The prompt frames not read yet. Call it once no fake-omp child is left: the end
    /// marker written here reaches the tap after every line written before it.
    async fn rest_prompts(&mut self) -> Vec<Value> {
        const END: &str = "TAP_END";
        std::fs::OpenOptions::new()
            .write(true)
            .open(&self.path)
            .and_then(|mut fifo| fifo.write_all(format!("{END}\n").as_bytes()))
            .expect("write the end marker");
        let mut prompts = Vec::new();
        loop {
            let line = self.next_line().await;
            if line == END {
                return prompts;
            }
            prompts.extend(prompt_frame(&line));
        }
    }
}

fn behavior(frame: &Value) -> &str {
    frame["streamingBehavior"].as_str().unwrap_or_default()
}

#[tokio::test]
async fn prompt_carries_conversation_envelope() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let id = queued(
        h.handle(dm("1300000000000000900", DM_CHANNEL, "hello"))
            .await,
    );
    h.drain().await;

    let frames = h.raw_prompts();
    assert_eq!(frames.len(), 1);
    let message = frames[0]["message"].as_str().unwrap();
    assert!(message.starts_with("[moraebot conversation="));
    let tag = lead::parse_tag(message).expect("tag line");
    assert_eq!(tag.conversation, h.row(id).conversation_key);
    assert_eq!(tag.message, Some(id));
    assert_eq!(tag.lane, None);
    assert_eq!(
        h.sends(),
        [(DM_CHANNEL.to_owned(), "echo: hello".to_owned())]
    );
    h.close().await;
}

#[tokio::test]
async fn replies_go_to_each_message_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    queued(
        h.handle(in_thread("1300000000000000901", THREAD_B, "in thread"))
            .await,
    );
    queued(
        h.handle(dm("1300000000000000902", DM_CHANNEL, "in dm"))
            .await,
    );
    h.drain().await;

    assert_eq!(
        h.sends(),
        [
            (THREAD_B.to_owned(), "echo: in thread".to_owned()),
            (DM_CHANNEL.to_owned(), "echo: in dm".to_owned()),
        ]
    );
    assert_eq!(h.starts(), 1, "one lead child serves both conversations");
    h.close().await;
}

#[tokio::test]
async fn lane_row_runs_without_owner_check_and_replies_to_lane_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let lane = match h
        .store
        .lock()
        .unwrap()
        .start_lane("toolu_1", LANE_KEY, "t", None, None, MAX_RUNNING_LANES)
        .unwrap()
    {
        LaneStart::New(id) => id,
        other => panic!("expected a new lane, got {other:?}"),
    };
    let input = lead::lane_result(LANE_KEY, lane, &LaneResult::Text("lane done"));
    assert!(
        h.store
            .lock()
            .unwrap()
            .finish_lane(lane, 1, &input, LaneState::Idle, None)
            .unwrap()
    );
    h.drain().await;

    let row = h.row(1);
    assert_eq!(row.origin, "lane");
    assert_eq!(row.state, State::Done);
    assert_eq!(
        h.sends(),
        [(THREAD_B.to_owned(), "echo: lane done".to_owned())]
    );
    let frames = h.raw_prompts();
    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0]["message"],
        input.as_str(),
        "the stored input is sent as is"
    );

    // An owner-origin row with a foreign author is still refused.
    let foreign = match h
        .store
        .lock()
        .unwrap()
        .admit(
            "fake",
            "1300000000000000903",
            LANE_KEY,
            CHANNEL,
            STRANGER,
            "x",
        )
        .unwrap()
    {
        Admitted::New(id) => id,
        other => panic!("expected New, got {other:?}"),
    };
    h.drain().await;
    let row = h.row(foreign);
    assert_eq!(row.state, State::Failed);
    assert_eq!(row.error.as_deref(), Some(E_ACCESS_NOT_OWNER));
    assert_eq!(
        h.raw_prompts().len(),
        1,
        "the foreign row never reaches omp"
    );
    h.close().await;
}

#[tokio::test]
async fn same_conversation_message_is_steered_into_running_turn() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "merged");
    let a = queued(
        h.handle(in_thread("1300000000000000910", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;
    let b = steered(
        h.handle(in_thread("1300000000000000911", THREAD_A, "B"))
            .await,
    );
    assert!(bounded(step).await.unwrap());

    assert_eq!(h.sends(), [(THREAD_A.to_owned(), "echo: A + B".to_owned())]);
    assert_eq!(h.row(a).state, State::Done);
    let row_b = h.row(b);
    assert_eq!(row_b.state, State::Done);
    assert_eq!(row_b.reply, None);
    assert_eq!(row_b.steered_into, Some(a));
    let prompts = h.prompts();
    assert_eq!(prompts.len(), 2, "no second prompt for B");
    assert_eq!(behavior(&prompts[0]), "followUp");
    assert_eq!(behavior(&prompts[1]), "steer");
    assert_eq!(prompts[1]["message"], "B");
    assert!(!h.step().await, "nothing left to run");
    h.close().await;
}

#[tokio::test]
async fn stalled_lead_turn_gets_one_notice_and_keeps_running() {
    let dir = tempfile::tempdir().unwrap();
    // `steer`: the lead starts the turn, then sends nothing until a steer arrives.
    let h = Harness::open_steer(dir.path(), "merged");
    let a = queued(
        h.handle(in_thread("1300000000000000960", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;

    let t0 = Instant::now();
    assert!(
        !h.dispatcher.post_stall_notice(t0).await,
        "the first tick starts the clock"
    );
    assert!(
        !h.dispatcher
            .post_stall_notice(t0 + DEFAULT_STALL_AFTER - WATCH_TICK)
            .await
    );
    assert!(
        h.dispatcher
            .post_stall_notice(t0 + DEFAULT_STALL_AFTER)
            .await
    );
    assert!(
        !h.dispatcher
            .post_stall_notice(t0 + 3 * DEFAULT_STALL_AFTER)
            .await,
        "one notice per stall"
    );
    let notice = (THREAD_A.to_owned(), stalled_text(20));
    assert_eq!(h.sends(), std::slice::from_ref(&notice));
    assert!(!step.is_finished(), "the stalled turn is still running");

    // Nothing was killed: the lead answers once the steer arrives and the turn completes.
    steered(
        h.handle(in_thread("1300000000000000961", THREAD_A, "B"))
            .await,
    );
    assert!(bounded(step).await.unwrap());
    assert_eq!(
        h.sends(),
        [notice, (THREAD_A.to_owned(), "echo: A + B".to_owned())]
    );
    assert_eq!(h.row(a).state, State::Done);
    assert_eq!(h.starts(), 1, "the lead child was never restarted");
    assert!(
        !h.dispatcher
            .post_stall_notice(t0 + 5 * DEFAULT_STALL_AFTER)
            .await,
        "no turn, no notice"
    );
    h.close().await;
}

#[tokio::test]
async fn separate_steer_answers_are_posted_once_each_in_one_reply() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "separate");
    let a = queued(
        h.handle(in_thread("1300000000000000920", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;
    let b = steered(
        h.handle(in_thread("1300000000000000921", THREAD_A, "B"))
            .await,
    );
    assert!(bounded(step).await.unwrap());

    assert_eq!(
        h.sends(),
        [(THREAD_A.to_owned(), "echo: A\n\necho: B".to_owned())]
    );
    assert_eq!(h.row(a).state, State::Done);
    assert_eq!(h.row(b).state, State::Done);
    h.close().await;
}

#[tokio::test]
async fn other_conversation_message_waits_for_next_turn() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "merged");
    let a = queued(
        h.handle(in_thread("1300000000000000930", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;
    let c = queued(
        h.handle(in_thread("1300000000000000931", THREAD_B, "C"))
            .await,
    );
    // The steer releases A's turn; without it the turn would never end.
    steered(
        h.handle(in_thread("1300000000000000932", THREAD_A, "B"))
            .await,
    );
    assert!(bounded(step).await.unwrap());
    h.drain().await;
    bounded(h.adapter.wait_for_send(2)).await;

    let sends = h.sends();
    assert_eq!(sends.len(), 2);
    assert_eq!(sends[0].0, THREAD_A);
    assert!(sends[0].1.contains("echo: A + B"));
    assert_eq!(sends[1], (THREAD_B.to_owned(), "echo: C".to_owned()));
    assert_eq!(h.row(c).state, State::Done);
    let prompts = h.prompts();
    let steer = prompts
        .iter()
        .position(|f| behavior(f) == "steer")
        .expect("steer frame");
    let c_frame = prompts
        .iter()
        .position(|f| f["message"] == "C")
        .expect("C's prompt frame");
    assert!(c_frame > steer, "C runs as a later turn");
    assert_eq!(behavior(&prompts[c_frame]), "followUp");
    h.close().await;
}

#[tokio::test]
async fn older_queued_message_of_conversation_blocks_steer() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "merged");
    let a = queued(
        h.handle(in_thread("1300000000000000940", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;

    h.adapter.set_attachment_bytes("ref-b", b"bbb".to_vec());
    let gate = h.adapter.hold_next_fetch();
    let b_task = h.spawn_handle(with_files(
        in_thread("1300000000000000941", THREAD_A, "B"),
        vec![attachment("b.txt", "ref-b", 3)],
    ));
    h.wait_for_fetch().await;
    let waiting = h.row(2);
    assert_eq!(waiting.state, State::Queued);
    assert!(!waiting.files_ready);
    let c = queued(
        h.handle(in_thread("1300000000000000942", THREAD_A, "C"))
            .await,
    );
    gate.notify_one();
    let b = steered(bounded(b_task).await.unwrap());
    assert_eq!(b, 2);
    assert!(bounded(step).await.unwrap());
    h.drain().await;
    bounded(h.adapter.wait_for_send(2)).await;

    let sends = h.sends();
    assert_eq!(sends.len(), 2);
    assert_eq!(sends[0].0, THREAD_A);
    assert!(
        sends[0]
            .1
            .starts_with("echo: A + B\n\n[Attached files, saved on disk]\n"),
        "A's and B's texts, A first"
    );
    assert_eq!(sends[1], (THREAD_A.to_owned(), "echo: C".to_owned()));
    assert_eq!(h.row(c).state, State::Done);

    let prompts = h.prompts();
    let steer = prompts
        .iter()
        .position(|f| behavior(f) == "steer")
        .expect("steer frame");
    let body = prompts[steer]["message"].as_str().unwrap();
    assert!(body.starts_with("B\n\n[Attached files, saved on disk]\n"));
    let saved: Vec<SavedFile> =
        serde_json::from_str(h.row(b).attachments.as_deref().expect("saved files")).unwrap();
    assert_eq!(saved.len(), 1);
    assert!(body.contains(saved[0].path.to_string_lossy().as_ref()));
    let c_frame = prompts
        .iter()
        .position(|f| f["message"] == "C")
        .expect("C's prompt frame");
    assert!(c_frame > steer, "C runs after the steer");
    assert_eq!(behavior(&prompts[c_frame]), "followUp");
    h.close().await;
}

#[tokio::test]
async fn group_needs_check_marks_every_member() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "hold");
    let a = queued(
        h.handle(in_thread("1300000000000000950", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;
    let b = steered(
        h.handle(in_thread("1300000000000000951", THREAD_A, "B"))
            .await,
    );

    let gate = h.adapter.hold_next_fetch();
    let d_task = h.spawn_handle(with_files(
        in_thread("1300000000000000952", THREAD_A, "D"),
        vec![attachment("d.txt", "ref-d", 3)],
    ));
    h.wait_for_fetch().await;
    let d = 3;
    assert_eq!(h.row(d).text, "D");
    assert_eq!(h.row(d).state, State::Queued);

    // `hold` never ends the turn: only a shutdown that does not wait on it ends it.
    bounded(h.dispatcher.shutdown()).await;
    assert!(bounded(step).await.unwrap());
    bounded(h.adapter.wait_for_send(1)).await;

    for id in [a, b] {
        let row = h.row(id);
        assert_eq!(row.state, State::NeedsCheck);
        assert_eq!(row.error.as_deref(), Some(E_CHILD_LOST));
    }
    let row_d = h.row(d);
    assert_eq!(row_d.state, State::NeedsCheck);
    assert_eq!(row_d.error.as_deref(), Some(E_DISPATCH_HELD));
    assert_eq!(notices(&h, N_INTERRUPTED), 1);
    assert_eq!(
        h.sends(),
        [(THREAD_A.to_owned(), notice_text(N_INTERRUPTED).to_owned())]
    );

    gate.notify_one();
    bounded(d_task).await.unwrap();
    assert_eq!(h.row(d).state, State::NeedsCheck);
    // `try_steer` only queues B's frame; the shutdown may kill the child before it is
    // written, so B's steer may or may not be in the log. D must never reach omp.
    let bodies: Vec<String> = h
        .prompts()
        .iter()
        .map(|p| p["message"].as_str().expect("prompt body").to_owned())
        .collect();
    assert_eq!(bodies.first().map(String::as_str), Some("A"));
    assert!(
        bodies.iter().all(|b| b == "A" || b == "B") && bodies.len() <= 2,
        "only A and B's steer: {bodies:?}"
    );
    h.close().await;
}

#[tokio::test]
async fn second_same_conversation_message_waits_for_next_turn() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "hold");
    let mut tap = LogTap::create(&dir.path().join("fake-omp.log"));
    let a = queued(
        h.handle(in_thread("1300000000000000960", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;
    assert_eq!(tap.next_prompt().await["message"], "A");
    let b = steered(
        h.handle(in_thread("1300000000000000961", THREAD_A, "B"))
            .await,
    );
    // `try_steer` only queues B's frame for the lead's worker thread: wait until fake-omp
    // read it, or the shutdown below may kill the child before the frame is written.
    let b_frame = tap.next_prompt().await;
    assert_eq!(b_frame["message"], "B");
    assert_eq!(behavior(&b_frame), "steer");
    let c = queued(
        h.handle(in_thread("1300000000000000962", THREAD_A, "C"))
            .await,
    );

    bounded(h.dispatcher.shutdown()).await;
    assert!(bounded(step).await.unwrap());
    bounded(h.adapter.wait_for_send(1)).await;

    for id in [a, b] {
        let row = h.row(id);
        assert_eq!(row.state, State::NeedsCheck);
        assert_eq!(row.error.as_deref(), Some(E_CHILD_LOST));
    }
    let row_c = h.row(c);
    assert_eq!(row_c.state, State::NeedsCheck);
    assert_eq!(row_c.error.as_deref(), Some(E_DISPATCH_HELD));
    assert_eq!(
        h.sends(),
        [(THREAD_A.to_owned(), notice_text(N_INTERRUPTED).to_owned())]
    );
    // The shutdown reaped the child, so the tap holds every frame it read.
    let rest = tap.rest_prompts().await;
    assert!(rest.is_empty(), "A and B's steer only: {rest:?}");
    h.close().await;
}

#[tokio::test]
async fn startup_posts_lane_interrupted_once() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let lane = {
        let mut store = h.store.lock().unwrap();
        let lane = match store
            .start_lane("toolu_1", LANE_KEY, "t", None, None, MAX_RUNNING_LANES)
            .unwrap()
        {
            LaneStart::New(id) => id,
            other => panic!("expected a new lane, got {other:?}"),
        };
        store.recover_on_start().unwrap();
        lane
    };
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(
        h.sends(),
        [(
            THREAD_B.to_owned(),
            notice_text(N_LANE_INTERRUPTED).to_owned()
        )]
    );
    assert!(
        h.store
            .lock()
            .unwrap()
            .lane(lane)
            .unwrap()
            .unwrap()
            .notice_sent
    );

    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(notices(&h, N_LANE_INTERRUPTED), 1, "posted once");
    assert_eq!(h.raw_prompts().len(), 0, "nothing is re-prompted");
    h.close().await;
}

/// Runs a gateway on a fresh home under `root` (with `lead_override` as
/// `<home>/hints/lead.md`), serves one DM and returns the appended system prompt the lead read
/// at start.
async fn lead_system_prompt(root: &Path, lead_override: Option<&str>) -> String {
    let config = test_config(root, PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")));
    if let Some(text) = lead_override {
        let path = hints::override_path(&config.home, lead::GUIDANCE_NAME);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, text).unwrap();
    }
    let log = root.join("fake-omp.log");
    let adapter = Arc::new(FakeAdapter::new());
    let as_dyn: Arc<dyn MessengerAdapter> = adapter.clone();
    let vars = [
        ("FAKE_OMP_MODE", "ok".to_owned()),
        ("FAKE_OMP_LOG", log.to_string_lossy().into_owned()),
    ];
    let gateway = Gateway::open(&config, as_dyn)
        .expect("open gateway")
        .with_omp(|omp| omp.with_extra_env(vars));
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(gateway.run(async {
        let _ = stopped.await;
    }));
    adapter
        .inject(dm("1300000000000000970", DM_CHANNEL, "hello"))
        .await;
    bounded(adapter.wait_for_send(1)).await;
    let _ = stop.send(());
    bounded(task).await.unwrap().expect("clean stop");

    let log = std::fs::read_to_string(&log).expect("fake-omp log");
    let starts: Vec<&str> = log
        .lines()
        .filter(|l| l.starts_with("START pid="))
        .collect();
    assert_eq!(starts.len(), 1, "one lead child");
    appended(starts[0])
}

#[tokio::test]
async fn gateway_appends_lead_guidance_then_messenger_hints() {
    let dir = tempfile::tempdir().unwrap();
    let fake = FakeAdapter::new();
    let hints_text = fake.behavior_hints();

    let first_line = lead::DEFAULT_GUIDANCE.lines().next().unwrap();
    let built_in = lead_system_prompt(&dir.path().join("built-in"), None).await;
    assert!(built_in.starts_with(first_line));
    assert!(built_in.ends_with(&format!("\n\n{hints_text}")));

    let custom = lead_system_prompt(&dir.path().join("custom"), Some("custom lead\n")).await;
    assert_eq!(custom, format!("custom lead\n\n{hints_text}"));

    let empty = lead_system_prompt(&dir.path().join("empty"), Some("")).await;
    assert_eq!(empty, hints_text);
}

/// The whole P1a path through `Gateway::open`: an owner "always remember" is answered by
/// moraebot without starting the lead, lands in `<home>/memory/core/owner.md`, and the
/// next lead turn carries it in the core block of the appended system prompt.
#[tokio::test]
async fn gateway_answers_memory_commands_and_appends_core_memory_to_the_lead() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path(), PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")));
    let log = dir.path().join("fake-omp.log");
    let adapter = Arc::new(FakeAdapter::new());
    let as_dyn: Arc<dyn MessengerAdapter> = adapter.clone();
    let vars = [
        ("FAKE_OMP_MODE", "ok".to_owned()),
        ("FAKE_OMP_LOG", log.to_string_lossy().into_owned()),
    ];
    let gateway = Gateway::open(&config, as_dyn)
        .expect("open gateway")
        .with_omp(|omp| omp.with_extra_env(vars));
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(gateway.run(async {
        let _ = stopped.await;
    }));

    adapter
        .inject(dm(
            "1300000000000000980",
            DM_CHANNEL,
            "항상 기억해: 답은 한국어로",
        ))
        .await;
    bounded(adapter.wait_for_send(1)).await;
    assert!(
        std::fs::read_to_string(&log).unwrap_or_default().is_empty(),
        "a memory command starts no lead"
    );
    adapter
        .inject(dm("1300000000000000981", DM_CHANNEL, "hello"))
        .await;
    bounded(adapter.wait_for_send(2)).await;
    let _ = stop.send(());
    bounded(task).await.unwrap().expect("clean stop");

    let sends: Vec<(String, String)> = adapter
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            Call::Send { channel, text } => Some((channel, text)),
            _ => None,
        })
        .collect();
    assert!(sends[0].1.starts_with("Remembered m-"), "{sends:?}");
    let owner_md = config.home.join("memory/core/owner.md");
    let saved = std::fs::read_to_string(&owner_md).expect("core/owner.md");
    assert!(saved.contains("답은 한국어로"), "{saved}");

    let log = std::fs::read_to_string(&log).expect("fake-omp log");
    let append = appended(
        log.lines()
            .find(|l| l.starts_with("START pid="))
            .expect("one lead child"),
    );
    let block = append
        .split_once(omp::CORE_MEMORY_OPEN)
        .map(|(_, rest)| rest)
        .expect("core memory block");
    assert!(block.contains("답은 한국어로 (m-"), "{block}");
    assert!(
        block.trim_end().ends_with(omp::CORE_MEMORY_CLOSE),
        "{block}"
    );
}

fn onboarding_row(h: &Harness) -> Option<OnboardingRow> {
    h.store.lock().unwrap().onboarding().unwrap()
}

fn greeting() -> (String, String) {
    (
        OWNER_DM.to_owned(),
        notice_text(N_ONBOARDING_HELLO).to_owned(),
    )
}

fn channel_greeting() -> (String, String) {
    (
        CHANNEL.to_owned(),
        notice_text(N_ONBOARDING_HELLO).to_owned(),
    )
}

/// How many times the owner's DM was opened.
fn open_dms(h: &Harness) -> usize {
    h.adapter
        .calls()
        .iter()
        .filter(|call| matches!(call, Call::OpenDm { user_id } if user_id == OWNER))
        .count()
}

/// The `message` of every logged prompt frame, tag line included.
fn raw_prompt_messages(h: &Harness) -> Vec<String> {
    h.log_lines()
        .iter()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|f| f["type"] == "prompt")
        .map(|f| f["message"].as_str().expect("prompt message").to_owned())
        .collect()
}

#[tokio::test]
async fn onboarding_request_greets_once_in_the_owner_dm() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    onboarding::request(&h.config.home, GreetTarget::Dm).unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert_eq!(h.sends(), vec![greeting()]);
    assert_eq!(open_dms(&h), 1);
    let calls = h.adapter.calls();
    let opened = calls
        .iter()
        .position(|call| matches!(call, Call::OpenDm { .. }))
        .expect("DM opened");
    let sent = calls
        .iter()
        .position(|call| matches!(call, Call::Send { .. }))
        .expect("greeting sent");
    assert!(opened < sent, "the DM is opened before the greeting");
    assert!(
        std::fs::symlink_metadata(onboarding::request_file(&h.config.home)).is_err(),
        "request file removed"
    );
    assert_eq!(
        onboarding_row(&h),
        Some(OnboardingRow {
            state: OnboardingState::Active,
            after_row: 0
        })
    );

    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends().len(), 1);
    assert_eq!(open_dms(&h), 1);
    h.close().await;
}

#[tokio::test]
async fn no_request_means_no_greeting() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();

    assert!(h.sends().is_empty());
    assert_eq!(onboarding_row(&h), None);
    h.close().await;
}

#[tokio::test]
async fn failed_greeting_is_not_retried() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    onboarding::request(&h.config.home, GreetTarget::Channel).unwrap();
    h.adapter.fail_next_send();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert_eq!(h.sends(), vec![channel_greeting()]);
    assert!(std::fs::symlink_metadata(onboarding::request_file(&h.config.home)).is_err());
    assert_eq!(
        onboarding_row(&h).map(|row| row.state),
        Some(OnboardingState::Active)
    );

    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends().len(), 1);
    h.close().await;
}

#[tokio::test]
async fn channel_request_greets_in_the_channel() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    onboarding::request(&h.config.home, GreetTarget::Channel).unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert_eq!(h.sends(), vec![channel_greeting()]);
    assert_eq!(open_dms(&h), 0);
    h.close().await;
}

#[tokio::test]
async fn failed_dm_greeting_falls_back_to_the_channel() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    onboarding::request(&h.config.home, GreetTarget::Dm).unwrap();
    h.adapter.fail_next_send();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert_eq!(h.sends(), vec![greeting(), channel_greeting()]);
    assert_eq!(
        onboarding_row(&h).map(|row| row.state),
        Some(OnboardingState::Active)
    );

    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends().len(), 2);
    h.close().await;
}

#[tokio::test]
async fn unopened_dm_greets_in_the_channel() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    onboarding::request(&h.config.home, GreetTarget::Dm).unwrap();
    h.adapter.fail_next_open_dm();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert_eq!(h.sends(), vec![channel_greeting()]);
    assert_eq!(open_dms(&h), 1);
    h.close().await;
}

#[tokio::test]
async fn greeting_failing_in_both_places_is_not_retried() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    onboarding::request(&h.config.home, GreetTarget::Dm).unwrap();
    h.adapter.fail_next_open_dm();
    h.adapter.fail_next_send();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert_eq!(h.sends(), vec![channel_greeting()]);
    assert_eq!(open_dms(&h), 1);
    assert!(std::fs::symlink_metadata(onboarding::request_file(&h.config.home)).is_err());
    assert_eq!(
        onboarding_row(&h).map(|row| row.state),
        Some(OnboardingState::Active)
    );

    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends().len(), 1);
    assert_eq!(open_dms(&h), 1);
    h.close().await;
}

#[tokio::test]
async fn old_empty_request_greets_in_the_dm() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    std::fs::write(onboarding::request_file(&h.config.home), b"").unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert_eq!(h.sends(), vec![greeting()]);
    h.close().await;
}

#[tokio::test]
async fn owner_dm_reply_is_onboarding_turn_1() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    onboarding::request(&h.config.home, GreetTarget::Dm).unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends(), vec![greeting()]);

    let mut ev = channel_msg("1300000000000000201", "hi");
    ev.is_dm = true;
    ev.channel_id = "444".into();
    assert_eq!(queued(h.handle(ev).await), 1);
    h.drain().await;

    let prompts = h.prompts();
    let sends = h.sends();
    let key = h.row(1).conversation_key;
    h.close().await;
    assert_eq!(
        prompts[0]["message"],
        format!("{}\nhi", onboarding::notice_line(1))
    );
    assert_eq!(
        sends.last().map(|(channel, _)| channel.as_str()),
        Some("444")
    );
    assert_eq!(key, "fake:dm:444");
}

#[tokio::test]
async fn new_request_restarts_onboarding() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let id = queued(h.handle(channel_msg("1300000000000000081", "hello")).await);
    assert_eq!(id, 1);
    h.drain().await;
    assert_eq!(h.sends().len(), 1);
    {
        let store = h.store.lock().unwrap();
        store.start_onboarding().unwrap();
        assert!(store.finish_onboarding(OnboardingState::Done).unwrap());
    }

    onboarding::request(&h.config.home, GreetTarget::Dm).unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert_eq!(
        onboarding_row(&h),
        Some(OnboardingRow {
            state: OnboardingState::Active,
            after_row: 1
        })
    );
    let sends = h.sends();
    assert_eq!(sends.len(), 2);
    assert_eq!(sends.last(), Some(&greeting()));
    h.close().await;
}

#[tokio::test]
async fn unremovable_request_sends_no_greeting() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    std::fs::create_dir(onboarding::request_file(&h.config.home)).unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert!(h.sends().is_empty());
    assert_eq!(
        onboarding_row(&h).map(|row| row.state),
        Some(OnboardingState::Active)
    );
    h.close().await;
}

#[tokio::test]
async fn onboarding_marks_owner_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    onboarding::request(&h.config.home, GreetTarget::Dm).unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends(), vec![greeting()]);
    assert_eq!(
        onboarding_row(&h),
        Some(OnboardingRow {
            state: OnboardingState::Active,
            after_row: 0
        })
    );

    assert_eq!(
        queued(h.handle(channel_msg("1300000000000000091", "hi")).await),
        1
    );
    h.drain().await;
    assert_eq!(
        queued(h.handle(channel_msg("1300000000000000092", "more")).await),
        2
    );
    h.drain().await;

    let prompts = h.prompts();
    assert_eq!(prompts.len(), 2);
    assert_eq!(
        prompts[0]["message"],
        format!("{}\nhi", onboarding::notice_line(1))
    );
    assert_eq!(
        prompts[1]["message"],
        format!("{}\nmore", onboarding::notice_line(2))
    );
    let raw = raw_prompt_messages(&h);
    assert_eq!(raw.len(), 2);
    assert_eq!(lead::parse_tag(&raw[0]).expect("tag").message, Some(1));
    assert_eq!(lead::parse_tag(&raw[1]).expect("tag").message, Some(2));
    h.close().await;
}

#[tokio::test]
async fn onboarding_mark_stops_after_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    onboarding::request(&h.config.home, GreetTarget::Dm).unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();

    for n in 0..11 {
        let id = format!("13000000000000001{n:02}");
        queued(h.handle(channel_msg(&id, "m")).await);
        h.drain().await;
    }

    let prompts = h.prompts();
    assert_eq!(prompts.len(), 11);
    for (n, frame) in prompts.iter().take(10).enumerate() {
        assert_eq!(
            frame["message"],
            format!("{}\nm", onboarding::notice_line(n as i64 + 1))
        );
    }
    assert_eq!(prompts[10]["message"], "m");
    let raw = raw_prompt_messages(&h);
    assert_eq!(raw.len(), 11);
    assert_eq!(lead::parse_tag(&raw[10]).expect("tag").message, Some(11));
    h.close().await;
}

#[tokio::test]
async fn finished_onboarding_marks_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    {
        let store = h.store.lock().unwrap();
        store.start_onboarding().unwrap();
        assert!(store.finish_onboarding(OnboardingState::Done).unwrap());
    }

    queued(h.handle(channel_msg("1300000000000000093", "hi")).await);
    h.drain().await;

    let prompts = h.prompts();
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0]["message"], "hi");
    assert_eq!(
        onboarding_row(&h).map(|row| row.state),
        Some(OnboardingState::Done)
    );
    let raw = raw_prompt_messages(&h);
    assert_eq!(raw.len(), 1);
    assert_eq!(lead::parse_tag(&raw[0]).expect("tag").message, Some(1));
    h.close().await;
}

const NEWS_KEY: &str = "fake:th:444";

/// Lane news items of job 1: (lane id, conversation key, text).
fn news(items: &[(i64, &str, &str)]) -> Vec<LaneNews> {
    items
        .iter()
        .map(|&(lane_id, key, text)| LaneNews {
            lane_id,
            seq: 1,
            conversation_key: key.to_owned(),
            text: text.to_owned(),
        })
        .collect()
}

/// Starts a running lane of conversation `key` in the store and returns its id.
fn running_lane(h: &Harness, call: &str, key: &str) -> i64 {
    match h
        .store
        .lock()
        .unwrap()
        .start_lane(
            call,
            key,
            "t1\nmore",
            Some("file ."),
            None,
            MAX_RUNNING_LANES,
        )
        .unwrap()
    {
        LaneStart::New(id) => id,
        other => panic!("expected New, got {other:?}"),
    }
}

/// The lane news rows in the store: (message id, text, state).
fn news_rows(h: &Harness) -> Vec<(String, String, State)> {
    (1..=h.count())
        .map(|id| h.row(id))
        .filter(|row| row.origin == "lane" && row.message_id.contains(":news:"))
        .map(|row| (row.message_id, row.text, row.state))
        .collect()
}

#[tokio::test]
async fn lane_news_waits_for_a_quiet_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let lane = running_lane(&h, "call-1", NEWS_KEY);
    let t0 = Instant::now();
    queued(h.handle(event("4441", OWNER, Some("444"), "hi")).await);
    let n = news(&[(lane, NEWS_KEY, "qa step 1")]);
    let mut posted = Vec::new();
    for at in [
        t0,
        t0 + NEWS_AFTER - Duration::from_secs(1),
        t0 + NEWS_AFTER,
        t0 + NEWS_AFTER + WATCH_TICK,
    ] {
        posted.push(h.dispatcher.post_news(n.clone(), at).await);
    }
    let rows = news_rows(&h);
    let sends = h.sends();
    h.close().await;

    assert_eq!(posted, [0, 0, 1, 0]);
    assert_eq!(
        rows,
        [(
            n[0].message_id(),
            lead::lane_news(NEWS_KEY, lane, "qa step 1"),
            State::Queued
        )]
    );
    assert!(sends.is_empty(), "nothing is posted in the chat: {sends:?}");
}

#[tokio::test]
async fn lane_news_counts_quiet_time_before_the_report() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let lane = running_lane(&h, "call-1", NEWS_KEY);
    let t0 = Instant::now();
    queued(h.handle(event("4441", OWNER, Some("444"), "hi")).await);
    let n = news(&[(lane, NEWS_KEY, "qa step 1")]);
    let a = h.dispatcher.post_news(Vec::new(), t0).await;
    let b = h.dispatcher.post_news(n, t0 + NEWS_AFTER).await;
    h.close().await;

    assert_eq!([a, b], [0, 1]);
}

#[tokio::test]
async fn lane_news_clock_restarts_on_owner_message() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let lane = running_lane(&h, "call-1", NEWS_KEY);
    let t0 = Instant::now();
    queued(h.handle(event("4441", OWNER, Some("444"), "hi")).await);
    let n = news(&[(lane, NEWS_KEY, "qa step 1")]);
    let a = h.dispatcher.post_news(n.clone(), t0).await;
    queued(h.handle(event("4442", OWNER, Some("444"), "more")).await);
    let b = h.dispatcher.post_news(n.clone(), t0 + NEWS_AFTER).await;
    let c = h.dispatcher.post_news(n, t0 + 2 * NEWS_AFTER).await;
    h.close().await;

    assert_eq!([a, b, c], [0, 0, 1]);
}

#[tokio::test]
async fn lane_news_clock_restarts_on_a_reply() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let t0 = Instant::now();
    let lane = running_lane(&h, "call-1", NEWS_KEY);
    let input = lead::lane_result(NEWS_KEY, lane, &LaneResult::Text("r"));
    let finished = h
        .store
        .lock()
        .unwrap()
        .finish_lane(lane, 1, &input, LaneState::Idle, None)
        .unwrap();
    let stepped = h.step().await;
    let n = news(&[(lane, NEWS_KEY, "qa step 1")]);
    let a = h.dispatcher.post_news(n.clone(), t0).await;
    let b = h.dispatcher.post_news(n, t0 + NEWS_AFTER).await;
    let sends = h.sends();
    h.close().await;

    assert!(finished);
    assert!(stepped);
    assert_eq!([a, b], [0, 1]);
    assert_eq!(sends.len(), 1, "only the reply; no raw news: {sends:?}");
    assert!(sends[0].1.starts_with("echo: "));
}

#[tokio::test]
async fn lane_news_reaches_the_lead_as_one_row_per_note() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let lane = running_lane(&h, "call-1", NEWS_KEY);
    let other = running_lane(&h, "call-2", "fake:th:555");
    let t0 = Instant::now();
    let both = news(&[(lane, NEWS_KEY, "a1"), (other, "fake:th:555", "b1")]);
    let a = h.dispatcher.post_news(both.clone(), t0).await;
    // The same note again, once the conversations are quiet: its row exists already.
    let b = h.dispatcher.post_news(Vec::new(), t0 + WATCH_TICK).await;
    let c = h
        .dispatcher
        .post_news(both[..1].to_vec(), t0 + WATCH_TICK + NEWS_AFTER)
        .await;
    let rows = news_rows(&h);
    h.drain().await;
    let messages = raw_prompt_messages(&h);
    let sends = h.sends();
    h.close().await;

    assert_eq!([a, b, c], [2, 0, 1]);
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        messages,
        [
            lead::lane_news(NEWS_KEY, lane, "a1"),
            lead::lane_news("fake:th:555", other, "b1"),
        ]
    );
    assert_eq!(sends.len(), 2, "one lead reply per note: {sends:?}");
    assert!(sends.iter().all(|(_, text)| text.starts_with("echo: ")));
}

#[tokio::test]
async fn lane_news_is_steered_into_a_running_turn_of_its_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "merged");
    let a = queued(
        h.handle(in_thread("1300000000000000980", THREAD_A, "A"))
            .await,
    );
    let key = h.row(a).conversation_key;
    let lane = running_lane(&h, "call-1", &key);
    let step = h.spawn_step();
    h.wait_turn(&key).await;
    let t0 = Instant::now();
    let n = news(&[(lane, &key, "qa step 1")]);
    let first = h.dispatcher.post_news(Vec::new(), t0).await;
    let sent = h.dispatcher.post_news(n.clone(), t0 + NEWS_AFTER).await;
    // In flight: the same note is not offered again before it settles.
    let again = h.dispatcher.post_news(n.clone(), t0 + 2 * NEWS_AFTER).await;
    steered(
        h.handle(in_thread("1300000000000000981", THREAD_A, "B"))
            .await,
    );
    assert!(bounded(step).await.unwrap());
    // The turn ended and the lead took the note: the next step marks it handed on.
    let settled = h.dispatcher.post_news(n, t0 + 3 * NEWS_AFTER).await;
    let rows = news_rows(&h);
    let messages = raw_prompt_messages(&h);
    h.close().await;

    assert_eq!([first, sent, again, settled], [0, 0, 0, 1]);
    assert!(
        rows.is_empty(),
        "no news row: the turn took the note: {rows:?}"
    );
    assert_eq!(
        messages,
        [
            lead::owner_input(&key, a, "A"),
            format!(
                "{}\n{}",
                omp::NEWS_NOTE_TAG,
                lead::lane_news_note(lane, "qa step 1")
            ),
            lead::owner_input(&key, a + 1, "B"),
        ]
    );
}

#[tokio::test]
async fn lane_news_omp_refuses_waits_as_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::build(
        dir.path(),
        "steer",
        PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")),
        false,
        vec![
            ("FAKE_OMP_STEER_CASE", "merged".to_owned()),
            ("FAKE_OMP_REJECT_TRIGGER", "qa-refuse".to_owned()),
        ],
    );
    let a = queued(
        h.handle(in_thread("1300000000000000984", THREAD_A, "A"))
            .await,
    );
    let key = h.row(a).conversation_key;
    let lane = running_lane(&h, "call-1", &key);
    let step = h.spawn_step();
    h.wait_turn(&key).await;
    let t0 = Instant::now();
    let n = news(&[(lane, &key, "qa-refuse step 1")]);
    let quiet = h.dispatcher.post_news(Vec::new(), t0).await;
    let sent = h.dispatcher.post_news(n.clone(), t0 + NEWS_AFTER).await;
    let rows_during = news_rows(&h);
    steered(
        h.handle(in_thread("1300000000000000985", THREAD_A, "B"))
            .await,
    );
    assert!(bounded(step).await.unwrap());
    let settled = h
        .dispatcher
        .post_news(Vec::new(), t0 + NEWS_AFTER + WATCH_TICK)
        .await;
    let rows = news_rows(&h);
    h.close().await;

    assert_eq!([quiet, sent, settled], [0, 0, 1]);
    assert!(rows_during.is_empty(), "{rows_during:?}");
    assert_eq!(
        rows,
        [(
            n[0].message_id(),
            lead::lane_news(&key, lane, "qa-refuse step 1"),
            State::Queued
        )]
    );
}

#[tokio::test]
async fn lane_news_in_a_cut_turn_waits_as_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "merged");
    let a = queued(
        h.handle(in_thread("1300000000000000986", THREAD_A, "A"))
            .await,
    );
    let key = h.row(a).conversation_key;
    let lane = running_lane(&h, "call-1", &key);
    let step = h.spawn_step();
    h.wait_turn(&key).await;
    let t0 = Instant::now();
    let quiet = h.dispatcher.post_news(Vec::new(), t0).await;
    let sent = h
        .dispatcher
        .post_news(news(&[(lane, &key, "qa step 1")]), t0 + NEWS_AFTER)
        .await;
    // The turn ends without an answer to the held note: a shutdown kills the lead.
    bounded(h.dispatcher.shutdown()).await;
    assert!(bounded(step).await.unwrap());
    let settled = h
        .dispatcher
        .post_news(Vec::new(), t0 + NEWS_AFTER + WATCH_TICK)
        .await;
    let rows = news_rows(&h);

    assert_eq!([quiet, sent, settled], [0, 0, 1]);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].1, lead::lane_news(&key, lane, "qa step 1"));
    assert_eq!(rows[0].2, State::Queued);
}

#[tokio::test]
async fn lane_news_waits_as_a_row_while_the_lead_works_elsewhere() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "merged");
    let a = queued(
        h.handle(in_thread("1300000000000000982", THREAD_A, "A"))
            .await,
    );
    let key = h.row(a).conversation_key;
    let lane = running_lane(&h, "call-1", NEWS_KEY);
    let step = h.spawn_step();
    h.wait_turn(&key).await;
    let handed = h
        .dispatcher
        .post_news(news(&[(lane, NEWS_KEY, "qa step 1")]), Instant::now())
        .await;
    let rows = news_rows(&h);
    steered(
        h.handle(in_thread("1300000000000000983", THREAD_A, "B"))
            .await,
    );
    assert!(bounded(step).await.unwrap());
    let during = raw_prompt_messages(&h).len();
    h.drain().await;
    let messages = raw_prompt_messages(&h);
    h.close().await;

    assert_eq!(handed, 1);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].2, State::Queued);
    assert_eq!(
        during, 2,
        "the note never joined the other conversation's turn"
    );
    assert_eq!(
        messages.last(),
        Some(&lead::lane_news(NEWS_KEY, lane, "qa step 1"))
    );
}

#[tokio::test]
async fn stale_lane_news_is_dropped_at_its_turn() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let lane = running_lane(&h, "call-1", NEWS_KEY);
    let handed = h
        .dispatcher
        .post_news(news(&[(lane, NEWS_KEY, "qa step 1")]), Instant::now())
        .await;
    let input = lead::lane_result(NEWS_KEY, lane, &LaneResult::Text("r"));
    assert!(
        h.store
            .lock()
            .unwrap()
            .finish_lane(lane, 1, &input, LaneState::Idle, None)
            .unwrap()
    );
    let dropped = h.step().await;
    let after_drop = raw_prompt_messages(&h);
    h.drain().await;
    let messages = raw_prompt_messages(&h);
    let rows = news_rows(&h);
    let news_row = h.row(1);
    h.close().await;

    assert_eq!(handed, 1);
    assert!(dropped);
    assert!(after_drop.is_empty(), "{after_drop:?}");
    assert_eq!(messages, [input]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].2, State::Failed);
    assert_eq!(
        news_row.error.as_deref(),
        Some(moraebot::dispatch::E_LANE_NEWS_STALE)
    );
}

#[tokio::test]
async fn lane_news_is_posted_raw_while_the_lead_cannot_start() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_with(dir.path(), "ok", dir.path().join("no-such-omp"));
    let lane = running_lane(&h, "call-1", NEWS_KEY);
    queued(h.handle(event("4441", OWNER, Some("444"), "hi")).await);
    let failed = h.step().await;
    let t0 = Instant::now();
    let n = news(&[(lane, NEWS_KEY, "qa step 1")]);
    let a = h.dispatcher.post_news(n.clone(), t0).await;
    h.adapter.fail_next_send();
    let b = h.dispatcher.post_news(n.clone(), t0 + NEWS_AFTER).await;
    let c = h
        .dispatcher
        .post_news(n, t0 + NEWS_AFTER + WATCH_TICK)
        .await;
    let line = ("444".to_owned(), news_line(lane, "qa step 1"));
    let raw = h.sends().into_iter().filter(|s| *s == line).count();
    let rows = news_rows(&h);
    h.close().await;

    assert!(failed);
    assert_eq!([a, b, c], [0, 0, 1]);
    assert_eq!(raw, 2, "the refused send is tried again");
    assert!(rows.is_empty(), "{rows:?}");
}

#[tokio::test]
async fn lane_news_skips_a_key_without_a_target() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let t0 = Instant::now();
    let n = news(&[(1, "fake:zz:1", "x")]);
    let a = h.dispatcher.post_news(n.clone(), t0).await;
    let b = h.dispatcher.post_news(n, t0 + NEWS_AFTER).await;
    let sends = h.sends();
    h.close().await;

    assert_eq!([a, b], [0, 0]);
    assert!(sends.is_empty());
}

#[tokio::test]
async fn lane_news_hands_a_running_lane_report_on_once() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::build(
        dir.path(),
        "tools",
        PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")),
        false,
        vec![
            (
                "FAKE_OMP_TOOL_CALLS",
                json!({"tool": "lane_start", "arguments": {"task": "run-lane-tools t1", "done": "file ."}})
                    .to_string(),
            ),
            ("FAKE_OMP_LANE_MODE", "tools".into()),
            (
                "FAKE_OMP_LANE_TOOL_CALLS",
                json!({"tool": LANE_REPORT_TOOL, "arguments": {"text": "qa step 1"}}).to_string(),
            ),
            ("FAKE_OMP_LANE_HOLD_AFTER_TOOLS", "1".into()),
        ],
    );
    let t0 = Instant::now();
    queued(
        h.handle(event("4441", OWNER, Some("444"), "run-tools go"))
            .await,
    );
    let stepped = h.step().await;
    let reported = bounded(h.dispatcher.lanes().lane_reports().wait_for(|n| *n >= 1))
        .await
        .is_ok();
    let a = h
        .dispatcher
        .post_news(h.dispatcher.lanes().news(), t0)
        .await;
    let b = h
        .dispatcher
        .post_news(h.dispatcher.lanes().news(), t0 + NEWS_AFTER)
        .await;
    let left = h.dispatcher.lanes().news();
    let rows = news_rows(&h);
    let sends = h.sends();
    bounded(h.dispatcher.shutdown()).await;

    assert!(stepped);
    assert!(reported);
    assert_eq!([a, b], [0, 1]);
    assert!(left.is_empty());
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].1, lead::lane_news(NEWS_KEY, 1, "qa step 1"));
    let raw = ("444".to_owned(), news_line(1, "qa step 1"));
    assert!(!sends.contains(&raw), "{sends:?}");
}

#[tokio::test]
async fn lane_news_forgets_quiet_conversations() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let lane = running_lane(&h, "call-1", NEWS_KEY);
    let t0 = Instant::now();
    queued(h.handle(event("4441", OWNER, Some("444"), "hi")).await);
    queued(h.handle(event("4442", OWNER, Some("555"), "hi")).await);
    let d = &h.dispatcher;
    let a = d.talk_len();
    let p1 = d.post_news(Vec::new(), t0).await;
    let b = d.talk_len();
    let p2 = d
        .post_news(news(&[(lane, NEWS_KEY, "qa step 1")]), t0 + NEWS_AFTER)
        .await;
    let c = d.talk_len();
    let p3 = d.post_news(Vec::new(), t0 + NEWS_AFTER + WATCH_TICK).await;
    let dd = d.talk_len();
    let p4 = d
        .post_news(Vec::new(), t0 + 2 * NEWS_AFTER + WATCH_TICK)
        .await;
    let e = d.talk_len();
    let rows = news_rows(&h);
    h.close().await;

    assert_eq!([a, b, c, dd, e], [2, 2, 1, 1, 0]);
    assert_eq!([p1, p2, p3, p4], [0, 1, 0, 0]);
    assert_eq!(rows.len(), 1);
}

// --- Busy lead turns and status questions (#68) ---

/// Stores an owner row of conversation `key` that stays `queued` (no steer is tried).
fn queue_owner_row(h: &Harness, message_id: &str, key: &str, text: &str) -> i64 {
    match h
        .store
        .lock()
        .unwrap()
        .admit("fake", message_id, key, CHANNEL, OWNER, text)
        .unwrap()
    {
        Admitted::New(id) => id,
        other => panic!("expected New, got {other:?}"),
    }
}

#[tokio::test]
async fn busy_lead_gets_one_note_while_an_owner_row_waits() {
    let dir = tempfile::tempdir().unwrap();
    // `note`: the first moraebot note ends the run.
    let h = Harness::open_steer(dir.path(), "note");
    let a = queued(
        h.handle(in_thread("1300000000000000990", THREAD_A, "A"))
            .await,
    );
    let key = h.row(a).conversation_key;
    let step = h.spawn_step();
    h.wait_turn(&key).await;
    let t0 = Instant::now();
    let empty = h.dispatcher.post_busy_note(t0 + DEFAULT_HANDOFF_AFTER);
    let later = queue_owner_row(&h, "1300000000000000991", &key, "later");
    let early = h.dispatcher.post_busy_note(t0);
    let sent = h.dispatcher.post_busy_note(t0 + DEFAULT_HANDOFF_AFTER);
    let again = h.dispatcher.post_busy_note(t0 + 3 * DEFAULT_HANDOFF_AFTER);
    assert!(bounded(step).await.unwrap());
    let reply = h.sends();
    h.drain().await;
    let messages = raw_prompt_messages(&h);
    let later_row = h.row(later);
    h.close().await;

    assert!(!empty, "nothing waits: no note");
    assert!(!early, "the turn is younger than the threshold");
    assert!(sent);
    assert!(!again, "one note per turn");
    assert_eq!(messages.len(), 3, "{messages:?}");
    assert!(
        messages[1].starts_with(&format!("{}\n", omp::BUSY_NOTE_TAG)),
        "{}",
        messages[1]
    );
    assert_eq!(messages[2], lead::owner_input(&key, later, "later"));
    assert_eq!(reply.len(), 1);
    assert_eq!(
        later_row.state,
        State::Done,
        "the waiting row got its own turn"
    );
}

#[tokio::test]
async fn busy_lead_with_a_steered_owner_row_gets_one_note() {
    let dir = tempfile::tempdir().unwrap();
    // `hold`: the steer is taken and the turn never ends by itself.
    let h = Harness::open_steer(dir.path(), "hold");
    let mut tap = LogTap::create(&dir.path().join("fake-omp.log"));
    let a = queued(
        h.handle(in_thread("1300000000000000992", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;
    assert_eq!(tap.next_prompt().await["message"], "A");
    steered(
        h.handle(in_thread("1300000000000000993", THREAD_A, "B"))
            .await,
    );
    assert_eq!(tap.next_prompt().await["message"], "B");
    let sent = h
        .dispatcher
        .post_busy_note(Instant::now() + DEFAULT_HANDOFF_AFTER);
    let note = loop {
        let line = tap.next_line().await;
        if let Ok(frame) = serde_json::from_str::<Value>(&line)
            && frame["type"] == "prompt"
        {
            break frame;
        }
    };
    bounded(h.dispatcher.shutdown()).await;
    assert!(bounded(step).await.unwrap());

    assert!(sent);
    assert_eq!(behavior(&note), "steer");
    assert!(
        note["message"]
            .as_str()
            .unwrap()
            .starts_with(omp::BUSY_NOTE_TAG)
    );
}

#[tokio::test]
async fn busy_note_is_off_when_the_turn_limit_is_not_longer() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path(), PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")));
    config.omp.turn_timeout_minutes = Some(DEFAULT_HANDOFF_AFTER.as_secs() / 60);
    let h = Harness::with_config(
        dir.path(),
        config,
        "steer",
        false,
        vec![("FAKE_OMP_STEER_CASE", "note".to_owned())],
    );
    let a = queued(
        h.handle(in_thread("1300000000000000997", THREAD_A, "A"))
            .await,
    );
    let key = h.row(a).conversation_key;
    let step = h.spawn_step();
    h.wait_turn(&key).await;
    queue_owner_row(&h, "1300000000000000998", &key, "later");
    let sent = h
        .dispatcher
        .post_busy_note(Instant::now() + 2 * DEFAULT_HANDOFF_AFTER);
    bounded(h.dispatcher.shutdown()).await;
    assert!(bounded(step).await.unwrap());

    assert!(!sent, "the turn limit ends the turn first");
}

#[tokio::test]
async fn status_question_is_answered_while_a_turn_runs_and_never_reaches_the_lead() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_steer(dir.path(), "merged");
    let a = queued(
        h.handle(in_thread("1300000000000000994", THREAD_A, "A"))
            .await,
    );
    let key = h.row(a).conversation_key;
    let step = h.spawn_step();
    h.wait_turn(&key).await;
    let asked = h
        .handle(in_thread("1300000000000000995", THREAD_A, "뭐 하고 있어?"))
        .await;
    let status_sends = h.sends();
    // The owner's steer slot is still free.
    steered(
        h.handle(in_thread("1300000000000000996", THREAD_A, "B"))
            .await,
    );
    assert!(bounded(step).await.unwrap());
    let messages = raw_prompt_messages(&h);
    let id = match asked {
        Handled::Status(id) => id,
        other => panic!("expected Status, got {other:?}"),
    };
    let status_row = h.row(id);
    h.close().await;

    assert_eq!(status_row.state, State::Done);
    assert_eq!(status_sends.len(), 1);
    assert_eq!(status_sends[0].0, THREAD_A);
    assert!(
        status_sends[0]
            .1
            .starts_with("이 대화 일을 1분째 하고 있어요"),
        "{}",
        status_sends[0].1
    );
    assert_eq!(
        messages,
        [
            lead::owner_input(&key, a, "A"),
            lead::owner_input(&key, a + 2, "B"),
        ]
    );
}

#[tokio::test]
async fn status_questions_go_to_the_lead_when_direct_status_is_off() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = test_config(dir.path(), PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")));
    config.lead.direct_status = Some(false);
    let h = Harness::with_config(dir.path(), config, "ok", false, Vec::new());
    let id = queued(h.handle(event("4444", OWNER, Some("444"), "status")).await);
    h.drain().await;
    let messages = raw_prompt_messages(&h);
    let sends = h.sends();
    let row = h.row(id);
    h.close().await;

    assert_eq!(messages, [lead::owner_input(NEWS_KEY, id, "status")]);
    assert_eq!(sends, [("444".to_owned(), "echo: status".to_owned())]);
    assert_eq!(row.state, State::Done);
}

#[tokio::test]
async fn queued_status_question_is_answered_by_the_worker() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let task = format!("fix {}/src\nmore", h.config.work_dir.display());
    let lane = match h
        .store
        .lock()
        .unwrap()
        .start_lane(
            "call-1",
            NEWS_KEY,
            &task,
            Some("file ."),
            None,
            MAX_RUNNING_LANES,
        )
        .unwrap()
    {
        LaneStart::New(id) => id,
        other => panic!("expected New, got {other:?}"),
    };
    let id = queue_owner_row(&h, "4443", NEWS_KEY, "status");
    let stepped = h.step().await;
    let sends = h.sends();
    let starts = h.starts();
    let row = h.row(id);
    h.close().await;

    assert!(stepped);
    assert_eq!(row.state, State::Done);
    assert_eq!(starts, 0, "the lead never started");
    assert_eq!(
        sends,
        [(
            "444".to_owned(),
            format!(
                "Not working on anything right now.\nBackground lanes of this conversation: 1\n- lane {lane}: fix ./src\nPrompt cache: no usage measured yet.\nRecall: on (mid-task on), 0 calls in the last hour, last delivered pick never."
            )
        )]
    );
}

// --- Owner memory commands (#39 P1a) ---

impl Harness {
    /// Attaches a memory store under the test home (fixed day) and returns its handle.
    fn attach_memory(self) -> (Self, MemoryHandle) {
        let memory = self.memory_handle();
        let h = self.adjust_dispatcher(|d| d.with_memory(memory.clone()));
        (h, memory)
    }

    fn memory_handle(&self) -> MemoryHandle {
        let today = Day::from_ymd(2026, 11, 20).expect("day");
        let store = MemoryStore::open(
            &self.config.home,
            Redactor::default(),
            Arc::new(move || today),
        )
        .expect("memory store");
        MemoryHandle::spawn(store).expect("memory worker")
    }

    /// Rebuilds the harness with `adjust` applied to its dispatcher.
    fn adjust_dispatcher(self, adjust: impl FnOnce(Dispatcher) -> Dispatcher) -> Self {
        let Self {
            root,
            config,
            store,
            adapter,
            dispatcher,
            chat,
        } = self;
        let dispatcher = match Arc::try_unwrap(dispatcher) {
            Ok(dispatcher) => adjust(dispatcher),
            Err(_) => panic!("the harness holds the only dispatcher"),
        };
        Self {
            root,
            config,
            store,
            adapter,
            dispatcher: Arc::new(dispatcher),
            chat,
        }
    }
}

fn memory_row(handled: Handled) -> i64 {
    match handled {
        Handled::Memory(id) => id,
        other => panic!("expected Memory, got {other:?}"),
    }
}

#[tokio::test]
async fn memory_command_mid_turn_is_answered_and_never_steered() {
    let dir = tempfile::tempdir().unwrap();
    let (h, memory) = Harness::open_steer(dir.path(), "merged").attach_memory();
    let a = queued(
        h.handle(in_thread("1300000000000009001", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;

    // The running turn of this conversation would take a steer from the owner here.
    let m = memory_row(
        h.handle(in_thread(
            "1300000000000009002",
            THREAD_A,
            "기억해: 배포는 금요일에 안 해",
        ))
        .await,
    );
    let row_m = h.row(m);
    assert_eq!(row_m.state, State::Done);
    assert_eq!(row_m.steered_into, None);
    let reply = row_m.reply.clone().expect("memory reply");
    assert!(reply.starts_with("Remembered m-"), "{reply}");
    let saved = memory
        .entries(&Bucket::Thread(row_m.conversation_key.clone()))
        .unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].text, "배포는 금요일에 안 해");
    assert!(
        memory.entries(&Bucket::Owner).unwrap().is_empty(),
        "not core"
    );

    // The turn still holds no steer: a plain message is steered and releases it.
    let b = steered(
        h.handle(in_thread("1300000000000009003", THREAD_A, "B"))
            .await,
    );
    assert!(bounded(step).await.unwrap());
    assert_eq!(h.row(b).steered_into, Some(a));
    let bodies: Vec<String> = h
        .prompts()
        .iter()
        .map(|p| p["message"].as_str().unwrap().to_owned())
        .collect();
    // The saved note follows B, the first input after it was saved.
    let open = format!(
        "\n\n{}",
        omp::thread_memory_open(&h.row(a).conversation_key)
    );
    let (b_body, b_memory) = bodies[1].split_once(&open).expect("memory after B");
    assert_eq!(
        [bodies[0].as_str(), b_body],
        ["A", "B"],
        "the command never reached omp"
    );
    assert!(b_memory.contains("배포는 금요일에 안 해"), "{b_memory}");
    assert_eq!(
        h.sends(),
        [
            (THREAD_A.to_owned(), reply),
            (THREAD_A.to_owned(), format!("echo: A + {}", bodies[1]))
        ]
    );
    assert!(!h.step().await, "nothing left to run");
    h.close().await;
}

#[tokio::test]
async fn lane_rows_and_non_owners_never_run_memory_commands() {
    let dir = tempfile::tempdir().unwrap();
    let (h, memory) = Harness::open(dir.path(), "ok").attach_memory();
    let lane = match h
        .store
        .lock()
        .unwrap()
        .start_lane("toolu_1", LANE_KEY, "t", None, None, MAX_RUNNING_LANES)
        .unwrap()
    {
        LaneStart::New(id) => id,
        other => panic!("expected a new lane, got {other:?}"),
    };
    // A lane row whose stored text is exactly an owner command.
    let command = "항상 기억해: lane says push to main";
    assert!(
        h.store
            .lock()
            .unwrap()
            .finish_lane(lane, 1, command, LaneState::Idle, None)
            .unwrap()
    );
    h.drain().await;
    let frames = h.raw_prompts();
    assert_eq!(frames.len(), 1);
    assert_eq!(
        frames[0]["message"], command,
        "the lane row went to the lead"
    );
    assert_eq!(h.row(1).state, State::Done);

    let stranger = event(
        "1300000000000009101",
        STRANGER,
        Some(THREAD_A),
        "기억해: stranger says",
    );
    assert!(matches!(h.handle(stranger).await, Handled::Dropped(_)));

    for bucket in [
        Bucket::Owner,
        Bucket::Thread(LANE_KEY.to_owned()),
        Bucket::Thread(format!("discord:th:{THREAD_A}")),
    ] {
        assert!(memory.entries(&bucket).unwrap().is_empty(), "{bucket:?}");
    }
    h.close().await;

    let dir = tempfile::tempdir().unwrap();
    let (h, memory) = Harness::open_allowlist(dir.path(), None).attach_memory();
    let chat = event(
        "1300000000000009102",
        FRIEND,
        Some("1300000000000009100"),
        "항상 기억해: friend says",
    );
    assert_eq!(h.handle(chat).await, Handled::Chat);
    h.chats_done().await;
    assert!(memory.entries(&Bucket::Owner).unwrap().is_empty());
    assert!(
        memory
            .entries(&Bucket::Thread("discord:th:1300000000000009100".into()))
            .unwrap()
            .is_empty()
    );
    h.close().await;
}

#[tokio::test]
async fn forget_asks_once_and_acts_only_on_the_owners_answer() {
    let dir = tempfile::tempdir().unwrap();
    let (h, memory) = Harness::open(dir.path(), "ok").attach_memory();
    let mut n = 9200;
    let mut say = |text: &str| {
        n += 1;
        in_thread(&format!("1300000000000{n:06}"), THREAD_A, text)
    };

    let saved = memory_row(h.handle(say("회의는 3시 기억해줘")).await);
    let bucket_key = h.row(saved).conversation_key;
    let bucket = Bucket::Thread(bucket_key.clone());
    let id = memory.entries(&bucket).unwrap()[0].id.clone();
    let reply = |row: i64| h.row(row).reply.expect("reply");

    // Asked, then something else: the question is dropped and the later yes goes to the lead.
    let asked = memory_row(h.handle(say(&format!("{id} 지워"))).await);
    assert!(reply(asked).starts_with("Forget this?"), "{}", reply(asked));
    assert_eq!(
        memory.entries(&bucket).unwrap().len(),
        1,
        "asking forgets nothing"
    );
    queued(h.handle(say("오늘 뭐해")).await);
    queued(h.handle(say("응")).await);
    h.drain().await;
    assert_eq!(memory.entries(&bucket).unwrap().len(), 1);

    // Asked, then no.
    memory_row(h.handle(say(&format!("forget {id}"))).await);
    let kept = memory_row(h.handle(say("아니")).await);
    assert_eq!(reply(kept), format!("Kept {id}."));
    assert_eq!(memory.entries(&bucket).unwrap().len(), 1);

    // Asked, then yes.
    memory_row(h.handle(say(&format!("{id}를 지워 줘"))).await);
    let forgot = memory_row(h.handle(say("응")).await);
    assert!(
        reply(forgot).starts_with(&format!("Forgot {id}.")),
        "{}",
        reply(forgot)
    );
    assert!(memory.entries(&bucket).unwrap().is_empty());
    assert!(memory.show(&id).unwrap().expect("archived").archived);

    let bodies: Vec<String> = h
        .prompts()
        .iter()
        .map(|p| p["message"].as_str().unwrap().to_owned())
        .collect();
    // The saved note follows the first plain message only: it did not change before the second.
    let open = format!("\n\n{}", omp::thread_memory_open(&bucket_key));
    let (first, first_memory) = bodies[0].split_once(&open).expect("memory after it");
    assert_eq!(
        [first, bodies[1].as_str()],
        ["오늘 뭐해", "응"],
        "only the plain messages reached omp"
    );
    assert!(first_memory.contains("회의는 3시"), "{first_memory}");
    h.close().await;
}

#[tokio::test]
async fn memory_commands_never_use_an_onboarding_turn() {
    let dir = tempfile::tempdir().unwrap();
    let (h, memory) = Harness::open(dir.path(), "ok").attach_memory();
    onboarding::request(&h.config.home, GreetTarget::Dm).unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();
    let mut n = 9300;
    let mut say = |text: &str| {
        n += 1;
        in_thread(&format!("1300000000000{n:06}"), THREAD_A, text)
    };

    queued(h.handle(say("hi")).await);
    h.drain().await;
    let saved = memory_row(h.handle(say("회의는 3시 기억해줘")).await);
    memory_row(h.handle(say("뭐 기억해?")).await);
    let bucket = Bucket::Thread(h.row(saved).conversation_key);
    let id = memory.entries(&bucket).unwrap()[0].id.clone();
    memory_row(h.handle(say(&format!("{id} 지워"))).await);
    memory_row(h.handle(say("응")).await);
    assert!(memory.entries(&bucket).unwrap().is_empty());
    queued(h.handle(say("more")).await);
    h.drain().await;

    let messages: Vec<Value> = h.prompts().iter().map(|p| p["message"].clone()).collect();
    assert_eq!(
        messages,
        [
            format!("{}\nhi", onboarding::notice_line(1)),
            format!("{}\nmore", onboarding::notice_line(2)),
        ],
        "memory commands and the forget answer are not onboarding turns"
    );
    h.close().await;
}

// --- Plain-language memory requests, corrections and the omp version notice (#39 P2) ---

#[tokio::test]
async fn a_plain_language_memory_request_goes_to_the_lead() {
    let dir = tempfile::tempdir().unwrap();
    let (h, memory) = Harness::open(dir.path(), "ok").attach_memory();
    let mut n = 9600;
    let mut say = |text: &str| {
        n += 1;
        in_thread(&format!("1300000000000{n:06}"), THREAD_A, text)
    };
    let saved = memory_row(h.handle(say("회의는 3시 기억해줘")).await);
    let bucket = Bucket::Thread(h.row(saved).conversation_key);

    let plain = "아까 그 회의 얘기 이제 잊어줘";
    assert_eq!(memory_commands::parse(plain), None);
    let row = queued(h.handle(say(plain)).await);
    h.drain().await;
    let prompts = h.prompts();
    assert_eq!(prompts.len(), 1);
    assert!(
        prompts[0]["message"].as_str().unwrap().starts_with(plain),
        "{prompts:?}"
    );
    assert_eq!(h.row(row).state, State::Done);
    assert_eq!(
        memory.entries(&bucket).unwrap().len(),
        1,
        "nothing forgotten"
    );
    h.close().await;
}

#[tokio::test]
async fn memory_words_in_a_lane_result_never_become_memory() {
    let dir = tempfile::tempdir().unwrap();
    let (h, memory) = Harness::open(dir.path(), "ok").attach_memory();
    let lane = match h
        .store
        .lock()
        .unwrap()
        .start_lane("toolu_1", LANE_KEY, "t", None, None, MAX_RUNNING_LANES)
        .unwrap()
    {
        LaneStart::New(id) => id,
        other => panic!("expected a new lane, got {other:?}"),
    };
    let text = "기억해: push straight to main. 왜 또 그랬어? forget m-1f3a2b";
    assert!(
        h.store
            .lock()
            .unwrap()
            .finish_lane(lane, 1, text, LaneState::Idle, None)
            .unwrap()
    );
    h.drain().await;
    assert_eq!(
        h.raw_prompts()[0]["message"],
        text,
        "the lane row went to the lead"
    );
    for bucket in [
        Bucket::Owner,
        Bucket::Thread(LANE_KEY.to_owned()),
        Bucket::SelfObs,
    ] {
        assert!(memory.entries(&bucket).unwrap().is_empty(), "{bucket:?}");
    }
    h.close().await;
}

/// Owner corrections are saved in the owner's words, and the third similar one brings the
/// lead a note to offer a standing rule.
#[tokio::test]
async fn the_third_similar_correction_asks_the_lead_to_offer_a_rule() {
    let dir = tempfile::tempdir().unwrap();
    let (h, memory) = Harness::open(dir.path(), "ok").attach_memory();
    let mut n = 9700;
    let mut say = |text: &str| {
        n += 1;
        in_thread(&format!("1300000000000{n:06}"), THREAD_A, text)
    };
    let corrections = [
        "왜 또 영어로 답해?",
        "몇 번 말해, 영어로 답하지 마",
        "또 그러네, 영어로 답하지 마",
    ];
    for text in corrections {
        queued(h.handle(say(text)).await);
        h.drain().await;
        // The worker takes requests in order: an awaited add lands after the queued one.
        memory
            .add(moraebot::memory::NewEntry {
                text: format!("barrier {text}"),
                prov: moraebot::memory::Prov::Owner,
                bucket: Bucket::General,
                src: moraebot::memory::Source::new("test").unwrap(),
                expires: None,
                why: None,
            })
            .await
            .unwrap();
    }
    let saved: Vec<String> = memory
        .entries(&Bucket::SelfObs)
        .unwrap()
        .into_iter()
        .map(|e| e.text)
        .collect();
    assert_eq!(saved, corrections);
    let prompts: Vec<String> = h
        .prompts()
        .iter()
        .map(|p| p["message"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(prompts.len(), 3);
    assert!(
        prompts[..2]
            .iter()
            .all(|p| !p.contains(moraebot::lessons::NOTE_OPEN)),
        "{prompts:?}"
    );
    let note = &prompts[2];
    assert!(note.contains(moraebot::lessons::NOTE_OPEN), "{note}");
    assert!(note.contains("3 times"), "{note}");
    assert!(note.contains("기억해: <rule>"), "{note}");
    h.close().await;
}

/// An owner correction steered into a running turn is captured too, and the note rides on
/// the steered text.
#[tokio::test]
async fn a_steered_correction_is_captured_with_its_note() {
    let dir = tempfile::tempdir().unwrap();
    let (h, memory) = Harness::open_steer(dir.path(), "merged").attach_memory();
    for (row, text) in [
        (901, "왜 또 영어로 답해?"),
        (902, "몇 번 말해, 영어로 답하지 마"),
    ] {
        memory
            .add(moraebot::memory::NewEntry {
                text: text.into(),
                prov: moraebot::memory::Prov::Owner,
                bucket: Bucket::SelfObs,
                src: moraebot::memory::Source::row(row),
                expires: None,
                why: None,
            })
            .await
            .unwrap();
    }
    let a = queued(
        h.handle(in_thread("1300000000000000990", THREAD_A, "A"))
            .await,
    );
    let step = h.spawn_step();
    h.wait_turn(&h.row(a).conversation_key).await;
    steered(
        h.handle(in_thread(
            "1300000000000000991",
            THREAD_A,
            "또 그러네, 영어로 답하지 마",
        ))
        .await,
    );
    assert!(bounded(step).await.unwrap());
    let prompts = h.prompts();
    assert_eq!(behavior(&prompts[1]), "steer");
    let steer = prompts[1]["message"].as_str().unwrap();
    assert!(steer.contains(moraebot::lessons::NOTE_OPEN), "{steer}");
    assert!(steer.contains("3 times"), "{steer}");
    memory
        .add(moraebot::memory::NewEntry {
            text: "barrier".into(),
            prov: moraebot::memory::Prov::Owner,
            bucket: Bucket::General,
            src: moraebot::memory::Source::new("test").unwrap(),
            expires: None,
            why: None,
        })
        .await
        .unwrap();
    let saved: Vec<String> = memory
        .entries(&Bucket::SelfObs)
        .unwrap()
        .into_iter()
        .map(|e| e.text)
        .collect();
    assert_eq!(saved.last().unwrap(), "또 그러네, 영어로 답하지 마");
    h.close().await;
}

#[tokio::test]
async fn startup_tells_the_owner_once_about_an_unchecked_omp_version() {
    let unchecked = OverlayCheck {
        version: "18.23.0".to_owned(),
        warning: Some("W_OMP_VERSION: omp 18.23.0 is not 18.22.2".to_owned()),
    };
    let notice = notice_text(N_OMP_VERSION).to_owned();

    // In the owner's DM, once per version.
    let dir = tempfile::tempdir().unwrap();
    let h =
        Harness::open(dir.path(), "ok").adjust_dispatcher(|d| d.with_omp_check(unchecked.clone()));
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends(), vec![(OWNER_DM.to_owned(), notice.clone())]);
    assert!(
        h.store
            .lock()
            .unwrap()
            .memory_logged(W_OMP_VERSION, "18.23.0")
            .unwrap()
    );
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends().len(), 1, "not again at the next start");
    assert_eq!(open_dms(&h), 1);
    h.close().await;

    // In the configured channel when the DM fails.
    let dir = tempfile::tempdir().unwrap();
    let h =
        Harness::open(dir.path(), "ok").adjust_dispatcher(|d| d.with_omp_check(unchecked.clone()));
    h.adapter.fail_next_open_dm();
    bounded(h.dispatcher.startup()).await.unwrap();
    assert_eq!(h.sends(), vec![(CHANNEL.to_owned(), notice.clone())]);
    h.close().await;

    // Nothing for the checked version.
    let dir = tempfile::tempdir().unwrap();
    let checked = OverlayCheck {
        version: "18.22.2".to_owned(),
        warning: None,
    };
    let h = Harness::open(dir.path(), "ok").adjust_dispatcher(|d| d.with_omp_check(checked));
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(h.sends().is_empty());
    assert_eq!(open_dms(&h), 0);
    h.close().await;
}

// --- Core saves of a turn that did not reply (#42 review) ---

impl Harness {
    /// The lead (fake-omp mode `tools`) has `memory_write` on a memory store under the test
    /// home (fixed day) and a 5 s turn limit; it runs `script` as host tool calls, then ends
    /// its turn as FAKE_OMP_TOOLS_END=`end` says.
    fn open_lead_memory(root: &Path, script: String, end: &str) -> Self {
        let config = test_config(root, PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")));
        let today = Day::from_ymd(2026, 11, 20).expect("day");
        let store = MemoryStore::open(&config.home, Redactor::default(), Arc::new(move || today))
            .expect("memory store");
        let memory = MemoryHandle::spawn(store).expect("memory worker");
        Self::with_lead(
            root,
            config,
            "tools",
            false,
            vec![
                ("FAKE_OMP_TOOL_CALLS", script),
                ("FAKE_OMP_TOOLS_END", end.to_owned()),
            ],
            move |omp| {
                omp.with_turn_timeout(Duration::from_secs(5))
                    .with_memory_clock(Arc::new(move || today))
                    .with_memory(memory)
            },
        )
    }
}

/// An owner turn whose lead saves a standing rule to core and then ends as `end` says:
/// the messages posted in the conversation.
async fn core_save_then(end: &str) -> Vec<(String, String)> {
    let dir = tempfile::tempdir().unwrap();
    let (h, _) = core_save_turn(dir.path(), end, |_| {}).await;
    let sends = h.sends();
    h.close().await;
    sends
}

/// The turn of [`core_save_then`] on a harness under `root`, after `before` set it up: the
/// harness, still open, and the turn's row.
async fn core_save_turn(root: &Path, end: &str, before: impl FnOnce(&Harness)) -> (Harness, i64) {
    let rule = "릴리스는 금요일에 하자";
    let write = json!({
        "tool": moraebot::memory_tools::MEMORY_WRITE_TOOL,
        "arguments": {"text": rule, "bucket_hint": "core", "why": "qa"}
    });
    let h = Harness::open_lead_memory(root, write.to_string(), end);
    before(&h);
    let id = queued(
        h.handle(event(
            "4441",
            OWNER,
            Some("444"),
            &format!("run-tools. {rule}"),
        ))
        .await,
    );
    assert!(h.step().await);
    (h, id)
}

/// No message in `sends` names a memory save.
fn assert_no_memory_note(sends: &[(String, String)]) {
    assert!(
        sends.iter().all(|(_, text)| !text.contains("[memory]")),
        "{sends:?}"
    );
}

#[tokio::test]
async fn a_core_save_in_a_failed_turn_posts_only_the_notice() {
    let sends = core_save_then("error").await;
    assert_no_memory_note(&sends);
    assert_eq!(
        sends,
        [("444".to_owned(), notice_text(N_FAILED).to_owned())]
    );
}

#[tokio::test]
async fn a_core_save_in_an_interrupted_turn_posts_only_the_notice() {
    let sends = core_save_then("hang").await;
    assert_no_memory_note(&sends);
    assert_eq!(
        sends,
        [("444".to_owned(), notice_text(N_INTERRUPTED).to_owned())]
    );
}

#[tokio::test]
async fn a_core_save_in_a_replied_turn_leaves_the_reply_alone() {
    let sends = core_save_then("").await;
    assert_no_memory_note(&sends);
    assert_eq!(sends.len(), 1, "one message: {sends:?}");
    let reply = &sends[0].1;
    assert!(reply.starts_with("echo: "), "{reply:?}");
    assert!(!reply.ends_with('\n'), "nothing is appended: {reply:?}");
}

#[tokio::test]
async fn a_core_save_of_a_failed_turn_is_not_posted_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (h, id) = core_save_turn(dir.path(), "error", |h| h.adapter.fail_next_send()).await;
    assert_eq!(h.row(id).state, State::Failed);
    assert_eq!(h.row(id).reply, None, "nothing is stored to post later");
    assert_eq!(bounded(h.dispatcher.resend_pending()).await.unwrap(), 0);
    h.close().await;

    let h = Harness::open(dir.path(), "ok");
    bounded(h.dispatcher.startup()).await.unwrap();
    assert!(h.sends().is_empty(), "{:?}", h.sends());
}

/// A [`FakeAdapter`] as a bridge adapter looks to the dispatcher: an optional home target
/// from the hello, and every `ack_inbound` recorded.
struct BridgeLikeAdapter {
    inner: Arc<FakeAdapter>,
    home: Option<String>,
    acks: Mutex<Vec<String>>,
    ack_count: watch::Sender<usize>,
}

impl BridgeLikeAdapter {
    fn new(inner: Arc<FakeAdapter>, home: Option<&str>) -> Self {
        Self {
            inner,
            home: home.map(str::to_owned),
            acks: Mutex::default(),
            ack_count: watch::channel(0).0,
        }
    }

    fn acks(&self) -> Vec<String> {
        self.acks.lock().unwrap().clone()
    }

    async fn wait_for_acks(&self, n: usize) {
        let mut rx = self.ack_count.subscribe();
        rx.wait_for(|count| *count >= n).await.expect("ack counter");
    }
}

#[async_trait]
impl MessengerAdapter for BridgeLikeAdapter {
    fn platform(&self) -> &str {
        self.inner.platform()
    }

    fn message_limit(&self) -> usize {
        self.inner.message_limit()
    }

    fn behavior_hints(&self) -> &str {
        self.inner.behavior_hints()
    }

    fn home_target(&self) -> Option<String> {
        self.home.clone()
    }

    async fn start(&self) -> Result<mpsc::Receiver<InboundEvent>, AdapterError> {
        self.inner.start().await
    }

    async fn send(&self, channel: &str, text: &str) -> Result<SendResult, AdapterError> {
        self.inner.send(channel, text).await
    }

    async fn create_thread(
        &self,
        channel_id: &str,
        from_message_id: &str,
        name: &str,
    ) -> Result<String, AdapterError> {
        self.inner
            .create_thread(channel_id, from_message_id, name)
            .await
    }

    async fn start_typing(&self, channel: &str) -> Result<TypingHandle, AdapterError> {
        self.inner.start_typing(channel).await
    }

    async fn open_dm(&self, user_id: &str) -> Result<String, AdapterError> {
        self.inner.open_dm(user_id).await
    }

    fn reconnects(&self) -> Option<watch::Receiver<u64>> {
        self.inner.reconnects()
    }

    async fn ack_inbound(&self, message_id: &str) {
        self.acks.lock().unwrap().push(message_id.to_owned());
        self.ack_count.send_modify(|count| *count += 1);
    }
}

/// A harness whose dispatcher talks to a [`BridgeLikeAdapter`] with `home`.
fn open_bridge_like(root: &Path, home: Option<&str>) -> (Harness, Arc<BridgeLikeAdapter>) {
    let config = test_config(root, PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")));
    bridge_like_harness(root, config, home)
}

fn bridge_like_harness(
    root: &Path,
    config: Config,
    home: Option<&str>,
) -> (Harness, Arc<BridgeLikeAdapter>) {
    let mut bridge = None;
    let h = Harness::assemble(
        root,
        config,
        "ok",
        Vec::new(),
        |omp| omp,
        |fake| {
            let adapter = Arc::new(BridgeLikeAdapter::new(fake, home));
            bridge = Some(Arc::clone(&adapter));
            adapter
        },
    );
    (h, bridge.expect("adapter built"))
}

/// An owner DM in channel `444`, inside `thread_id` when given.
fn owner_dm(message_id: &str, thread_id: Option<&str>, text: &str) -> InboundEvent {
    let mut ev = event(message_id, OWNER, thread_id, text);
    ev.is_dm = true;
    ev.channel_id = "444".into();
    ev
}

#[tokio::test]
async fn a_dm_in_a_thread_is_keyed_and_answered_in_that_thread() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let id = queued(
        h.handle(owner_dm("444:101", Some("444:100"), "in the thread"))
            .await,
    );
    let plain = queued(h.handle(owner_dm("444:102", None, "plain dm")).await);
    h.drain().await;

    assert_eq!(h.row(id).conversation_key, "fake:th:444:100");
    assert_eq!(h.row(plain).conversation_key, "fake:dm:444");
    assert_eq!(h.row(id).state, State::Done, "{:?}", h.row(id).error);
    assert_eq!(
        h.sends(),
        vec![
            ("444:100".to_owned(), "echo: in the thread".to_owned()),
            ("444".to_owned(), "echo: plain dm".to_owned()),
        ]
    );
    assert_eq!(h.threads_created(), 0);
    h.close().await;
}

#[tokio::test]
async fn a_root_dm_runs_without_opening_a_thread() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let id = queued(h.handle(owner_dm("444:200", Some("444:200"), "root")).await);
    // A restart before the turn: startup opens missing threads of thread starters.
    bounded(h.dispatcher.startup()).await.unwrap();
    h.drain().await;

    assert_eq!(h.row(id).conversation_key, "fake:th:444:200");
    assert_eq!(h.row(id).state, State::Done);
    assert_eq!(
        h.sends(),
        vec![("444:200".to_owned(), "echo: root".to_owned())]
    );
    assert_eq!(h.threads_created(), 0);
    h.close().await;
}

#[tokio::test]
async fn the_greeting_fallback_goes_to_the_adapter_home() {
    let dir = tempfile::tempdir().unwrap();
    let (h, _bridge) = open_bridge_like(dir.path(), Some("home-999"));
    onboarding::request(&h.config.home, GreetTarget::Channel).unwrap();
    bounded(h.dispatcher.startup()).await.unwrap();

    assert_eq!(
        h.sends(),
        vec![(
            "home-999".to_owned(),
            notice_text(N_ONBOARDING_HELLO).to_owned()
        )]
    );
    h.close().await;
}

#[tokio::test]
async fn every_handled_event_is_acked_and_a_store_error_is_not() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path(), PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")));
    // The store holds its file exclusively, so the failure is planted before it opens:
    // storing a message with the text "lost" fails.
    let store_file = config.home.join("inbox.sqlite");
    drop(Store::open(&store_file).unwrap());
    rusqlite::Connection::open(&store_file)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER lost_fails BEFORE INSERT ON inbox WHEN NEW.text = 'lost'
             BEGIN SELECT RAISE(ABORT, 'injected store failure'); END;",
        )
        .unwrap();
    let (h, bridge) = bridge_like_harness(dir.path(), config, None);
    let inbound = h.adapter.start().await.unwrap();
    let runner = tokio::spawn(Arc::clone(&h.dispatcher).run(inbound));

    // Queued, then answered.
    h.adapter
        .inject(channel_msg("1300000000000000401", "stored"))
        .await;
    bounded(bridge.wait_for_acks(1)).await;
    bounded(h.adapter.wait_for_send(1)).await;
    // Dropped, notice, duplicate.
    h.adapter
        .inject(event("1300000000000000402", STRANGER, None, "hi"))
        .await;
    h.adapter
        .inject(channel_msg("1300000000000000403", "/help"))
        .await;
    h.adapter
        .inject(channel_msg("1300000000000000401", "stored"))
        .await;
    bounded(bridge.wait_for_acks(4)).await;

    // Storing the next owner message fails; a dropped message after it shows the loop
    // went on without acking it.
    h.adapter
        .inject(channel_msg("1300000000000000404", "lost"))
        .await;
    h.adapter
        .inject(event("1300000000000000405", STRANGER, None, "after"))
        .await;
    bounded(bridge.wait_for_acks(5)).await;

    assert_eq!(
        bridge.acks(),
        [
            "1300000000000000401",
            "1300000000000000402",
            "1300000000000000403",
            "1300000000000000401",
            "1300000000000000405",
        ]
    );
    runner.abort();
    let _ = runner.await;
    h.close().await;
}

// --- New session command (#74) ---

const SESSION_THREAD: &str = "555";
const SESSION_KEY: &str = "fake:th:555";

fn session_msg(message_id: &str, text: &str) -> InboundEvent {
    event(message_id, OWNER, Some(SESSION_THREAD), text)
}

/// Every `SESSION_ENTRIES <pid> <n>` line fake-omp logged at `open_session`.
fn session_entries(h: &Harness) -> Vec<(u32, i64)> {
    h.log_lines()
        .iter()
        .filter_map(|l| {
            let (pid, n) = l.strip_prefix("SESSION_ENTRIES ")?.split_once(' ')?;
            Some((pid.parse().ok()?, n.parse().ok()?))
        })
        .collect()
}

/// The `session-backup-*` folders beside the lead's session folder.
fn session_backups(h: &Harness) -> Vec<PathBuf> {
    let Ok(dir) = std::fs::read_dir(h.config.home.join("lead")) else {
        return Vec::new();
    };
    let mut found: Vec<PathBuf> = dir
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("session-backup-"))
        })
        .collect();
    found.sort();
    found
}

/// Writes a history file into the lead's session folder, as an omp that kept a session would.
fn plant_history(h: &Harness) {
    let dir = omp::lead_session_dir(&h.config);
    std::fs::write(dir.join("history.jsonl"), "old turns").expect("history file");
}

fn start_running_lane(h: &Harness) -> i64 {
    match h
        .store
        .lock()
        .unwrap()
        .start_lane(
            "call-74",
            SESSION_KEY,
            "long work",
            None,
            None,
            MAX_RUNNING_LANES,
        )
        .unwrap()
    {
        LaneStart::New(id) => id,
        other => panic!("expected New, got {other:?}"),
    }
}

#[tokio::test]
async fn new_session_moves_the_session_aside_and_the_next_turn_starts_empty() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let before = queued(h.handle(session_msg("7401", "before")).await);
    h.drain().await;
    plant_history(&h);
    let command = queued(h.handle(session_msg("7402", "새 세션")).await);
    let row_kept = {
        // The worker has not run the command yet: nothing changed.
        let store = h.store.lock().unwrap();
        store.session_id(lead::LEAD_SESSION).unwrap().is_some()
    };
    let after = queued(h.handle(session_msg("7403", "after")).await);
    h.drain().await;
    let backups = session_backups(&h);
    let entries = session_entries(&h);
    let starts = h.start_argvs();
    let messages = raw_prompt_messages(&h);
    let sends = h.sends();
    let command_row = h.row(command);
    let session_dir = omp::lead_session_dir(&h.config);
    let session_id = h
        .store
        .lock()
        .unwrap()
        .session_id(lead::LEAD_SESSION)
        .unwrap();
    h.close().await;

    assert!(row_kept);
    assert_eq!(backups.len(), 1, "{backups:?}");
    assert_eq!(
        std::fs::read_to_string(backups[0].join("history.jsonl")).unwrap(),
        "old turns"
    );
    assert!(!session_dir.join("history.jsonl").exists());
    assert_eq!(starts.len(), 2, "a new lead child started");
    assert_eq!(
        entries,
        [(starts[0].0, 0), (starts[1].0, 0)],
        "the new child opened an empty session folder"
    );
    assert_eq!(
        messages,
        [
            lead::owner_input(SESSION_KEY, before, "before"),
            lead::owner_input(SESSION_KEY, after, "after"),
        ],
        "the command never reaches the lead"
    );
    assert_eq!(command_row.state, State::Done);
    assert_eq!(sends.len(), 3);
    assert_eq!(
        sends[0],
        (SESSION_THREAD.to_owned(), "echo: before".to_owned())
    );
    let name = backups[0]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    assert!(
        sends[1].1.starts_with("새 세션을 열었어요") && sends[1].1.contains(&name),
        "{}",
        sends[1].1
    );
    assert_eq!(
        sends[2],
        (SESSION_THREAD.to_owned(), "echo: after".to_owned())
    );
    assert!(session_id.is_some(), "the new child stored its session id");
}

#[tokio::test]
async fn new_session_with_running_lanes_asks_first_and_changes_nothing_until_confirmed() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    queued(h.handle(session_msg("7411", "before")).await);
    h.drain().await;
    plant_history(&h);
    start_running_lane(&h);
    queued(h.handle(session_msg("7412", "new session")).await);
    h.drain().await;
    let asked = h.sends();
    let backups_after_ask = session_backups(&h);
    let id_after_ask = h
        .store
        .lock()
        .unwrap()
        .session_id(lead::LEAD_SESSION)
        .unwrap();
    let history_kept = omp::lead_session_dir(&h.config)
        .join("history.jsonl")
        .exists();

    queued(h.handle(session_msg("7413", "new session confirm")).await);
    h.drain().await;
    queued(h.handle(session_msg("7414", "after")).await);
    h.drain().await;
    let sends = h.sends();
    let backups = session_backups(&h);
    let starts = h.starts();
    h.close().await;

    assert_eq!(
        asked.last().map(|(_, text)| text.as_str()),
        Some(new_session::confirm_text(Lang::English, 1).as_str())
    );
    assert!(backups_after_ask.is_empty());
    assert!(history_kept);
    assert!(id_after_ask.is_some());
    assert_eq!(backups.len(), 1);
    assert!(backups[0].join("history.jsonl").exists());
    assert!(
        sends[2].1.starts_with("Opened a new session."),
        "{}",
        sends[2].1
    );
    assert_eq!(sends[3].1, "echo: after");
    assert_eq!(starts, 2);
}

#[tokio::test]
async fn new_session_keeps_the_gateway_serving() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let inbound = h.adapter.start().await.unwrap();
    let runner = tokio::spawn(Arc::clone(&h.dispatcher).run(inbound));
    h.adapter
        .inject(channel_msg("1300000000000007421", "one"))
        .await;
    bounded(h.adapter.wait_for_send(1)).await;
    h.adapter
        .inject(channel_msg("1300000000000007422", "/new"))
        .await;
    bounded(h.adapter.wait_for_send(2)).await;
    h.adapter
        .inject(channel_msg("1300000000000007423", "two"))
        .await;
    bounded(h.adapter.wait_for_send(3)).await;
    let serving = !runner.is_finished();
    let texts: Vec<String> = h.sends().into_iter().map(|(_, text)| text).collect();
    let pids: Vec<u32> = h.start_argvs().into_iter().map(|(pid, _)| pid).collect();
    runner.abort();
    let _ = runner.await;
    h.close().await;

    assert!(serving, "the dispatcher kept running");
    assert_eq!(texts[0], "echo: one");
    assert!(
        texts[1].starts_with("Opened a new session."),
        "{}",
        texts[1]
    );
    assert_eq!(texts[2], "echo: two");
    assert_eq!(pids.len(), 2);
    assert_ne!(pids[0], pids[1]);
    assert!(!alive(pids[0]), "the old lead child was reaped");
}

#[tokio::test]
async fn new_session_waits_for_the_running_turn_and_rows_behind_it_reach_the_new_lead() {
    let dir = tempfile::tempdir().unwrap();
    let gate = dir.path().join("reply-gate");
    let h = Harness::build(
        dir.path(),
        "ok",
        PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")),
        false,
        vec![("FAKE_OMP_REPLY_GATE", gate.to_string_lossy().into_owned())],
    );
    let a = queued(h.handle(in_thread("7431", THREAD_A, "A")).await);
    let key = h.row(a).conversation_key;
    let step = h.spawn_step();
    h.wait_turn(&key).await;
    let command = h.handle(in_thread("7432", THREAD_A, "새 세션")).await;
    let later = h.handle(in_thread("7433", THREAD_A, "later")).await;
    std::fs::write(&gate, "").unwrap();
    assert!(bounded(step).await.unwrap());
    h.drain().await;
    let messages = raw_prompt_messages(&h);
    let starts = h.start_argvs();
    let entries = session_entries(&h);
    let sends = h.sends();
    let backups = session_backups(&h);
    h.close().await;

    queued(command);
    let later = queued(later);
    assert_eq!(
        messages,
        [
            lead::owner_input(&key, a, "A"),
            lead::owner_input(&key, later, "later"),
        ]
    );
    assert_eq!(starts.len(), 2);
    assert_eq!(entries, [(starts[0].0, 0), (starts[1].0, 0)]);
    assert_eq!(backups.len(), 1);
    let texts: Vec<&str> = sends.iter().map(|(_, text)| text.as_str()).collect();
    assert_eq!(texts.len(), 3, "{texts:?}");
    assert_eq!(texts[0], "echo: A");
    assert!(texts[1].starts_with("새 세션을 열었어요"), "{}", texts[1]);
    assert_eq!(texts[2], "echo: later");
}

#[tokio::test]
async fn a_longer_message_about_a_new_session_goes_to_the_lead() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let id = queued(h.handle(session_msg("7441", "새 세션 열어줄래?")).await);
    h.drain().await;
    let messages = raw_prompt_messages(&h);
    let backups = session_backups(&h);
    h.close().await;

    assert_eq!(
        messages,
        [lead::owner_input(SESSION_KEY, id, "새 세션 열어줄래?")]
    );
    assert!(backups.is_empty());
}

#[tokio::test]
async fn a_chat_sender_cannot_start_a_new_session() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open_allowlist(dir.path(), None);
    let handled = h
        .handle(event("7451", FRIEND, Some(SESSION_THREAD), "새 세션"))
        .await;
    h.chats_done().await;
    let backups = session_backups(&h);
    let lead_id = h
        .store
        .lock()
        .unwrap()
        .session_id(lead::LEAD_SESSION)
        .unwrap();
    h.close().await;

    assert_eq!(handled, Handled::Chat);
    assert!(backups.is_empty());
    assert_eq!(lead_id, None, "the lead never started");
}

#[tokio::test]
async fn a_new_session_command_with_a_file_is_refused_and_never_reaches_the_lead() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    let mut handled = Vec::new();
    for (mid, text) in [("7461", "/new"), ("7462", "새 세션")] {
        let ev = with_files(
            session_msg(mid, text),
            vec![attachment("a.txt", "ref-a", 3)],
        );
        handled.push(h.handle(ev).await);
    }
    h.drain().await;
    let stored = h.count();
    let fetched = fetches(&h);
    let starts = h.starts();
    let notices = notices(&h, N_NEW_SESSION_FILES);
    h.close().await;

    assert_eq!(
        handled,
        [
            Handled::Notice(N_NEW_SESSION_FILES),
            Handled::Notice(N_NEW_SESSION_FILES),
        ]
    );
    assert_eq!(stored, 0, "nothing stored");
    assert!(fetched.is_empty(), "no file fetched");
    assert_eq!(starts, 0, "the lead never started");
    assert_eq!(notices, 2);
}

#[tokio::test]
async fn another_owner_message_closes_the_new_session_question() {
    let dir = tempfile::tempdir().unwrap();
    let h = Harness::open(dir.path(), "ok");
    start_running_lane(&h);
    queued(h.handle(session_msg("7471", "new session")).await);
    h.drain().await;
    queued(h.handle(session_msg("7472", "something else")).await);
    h.drain().await;
    queued(h.handle(session_msg("7473", "new session confirm")).await);
    h.drain().await;
    let texts: Vec<String> = h.sends().into_iter().map(|(_, text)| text).collect();
    let backups = session_backups(&h);
    h.close().await;

    let question = new_session::confirm_text(Lang::English, 1);
    assert_eq!(
        texts,
        [
            question.clone(),
            "echo: something else".to_owned(),
            question
        ]
    );
    assert!(backups.is_empty(), "a stale confirmation changed nothing");
}
