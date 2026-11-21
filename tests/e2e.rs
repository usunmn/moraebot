//! Offline end-to-end tests: the library-level gateway with `FakeAdapter` and `fake-omp`,
//! plus the shipped binary for the startup refusals that need no network.

use std::fs::OpenOptions;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use moraebot::adapter::{InboundEvent, MessengerAdapter};
use moraebot::config::{AccessMode, Config, DiscordConfig, E_TOKEN_MODE, OmpConfig, Platform};
use moraebot::dispatch::{E_DISPATCH_HELD, N_INTERRUPTED, notice_text};
use moraebot::gateway::{Gateway, GatewayError, STORE_FILE};
use moraebot::lead;
use moraebot::store::{E_ALREADY_RUNNING, State, Store};
use moraebot::testing::{Call, FakeAdapter};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const OWNER: &str = "111";
const CHANNEL: &str = "333";
const MESSAGE_ID: &str = "1300000000000000001";
const TOKEN_SENTINEL: &str = "TOKEN_SENTINEL_9f3a";
/// Shutdown must finish within the 25 s the run command promises.
const SHUTDOWN_BOUND: Duration = Duration::from_secs(25);
const BOUND: Duration = Duration::from_secs(30);

async fn bounded<T>(bound: Duration, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(bound, future)
        .await
        .expect("finished within bound")
}

/// Streams the lines fake-omp appends to `FAKE_OMP_LOG`. The log is a FIFO that the tap
/// holds open for reading and writing, so a test waits for a line instead of polling.
struct LogTap {
    lines: mpsc::UnboundedReceiver<String>,
    seen: Vec<String>,
}

impl LogTap {
    fn create(path: &Path) -> Self {
        let made = Command::new("mkfifo")
            .arg(path)
            .status()
            .expect("run mkfifo");
        assert!(made.success(), "mkfifo failed");
        let fifo = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open fifo");
        let (tx, lines) = mpsc::unbounded_channel();
        std::thread::spawn(move || {
            for line in BufReader::new(fifo).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            lines,
            seen: Vec::new(),
        }
    }

    async fn wait_for(&mut self, matches: impl Fn(&str) -> bool) {
        bounded(BOUND, async {
            if self.seen.iter().any(|line| matches(line)) {
                return;
            }
            loop {
                let line = self.lines.recv().await.expect("log tap open");
                let found = matches(&line);
                self.seen.push(line);
                if found {
                    return;
                }
            }
        })
        .await;
    }

    async fn wait_for_start(&mut self) {
        self.wait_for(|line| line.starts_with("START pid=")).await;
    }

    async fn wait_for_prompt(&mut self) {
        self.wait_for(|line| {
            serde_json::from_str::<Value>(line).is_ok_and(|frame| frame["type"] == "prompt")
        })
        .await;
    }

    fn pids(&self) -> Vec<u32> {
        self.seen
            .iter()
            .filter_map(|line| line.strip_prefix("START pid="))
            .map(|rest| {
                rest.split_whitespace()
                    .next()
                    .expect("pid")
                    .parse()
                    .expect("pid")
            })
            .collect()
    }

    fn prompts(&self) -> usize {
        self.seen
            .iter()
            .filter(|line| {
                serde_json::from_str::<Value>(line).is_ok_and(|frame| frame["type"] == "prompt")
            })
            .count()
    }
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(Stdio::null())
        .status()
        .expect("run kill")
        .success()
}

fn terminate(pid: u32) {
    let _ = Command::new("kill")
        .args(["-9", &pid.to_string()])
        .stderr(Stdio::null())
        .status();
}

struct Running {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), GatewayError>>,
}

impl Running {
    async fn stop(self) -> Result<(), GatewayError> {
        let _ = self.stop.send(());
        bounded(SHUTDOWN_BOUND, self.task)
            .await
            .expect("gateway task joined")
    }
}

struct Rig {
    _dir: tempfile::TempDir,
    root: PathBuf,
    config: Config,
    adapter: Arc<FakeAdapter>,
}

impl Rig {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().to_owned();
        let home = root.join("home");
        let work_dir = root.join("work");
        std::fs::create_dir_all(home.join("secrets")).expect("home");
        std::fs::create_dir_all(&work_dir).expect("work dir");
        let token_file = home.join("secrets").join("discord.token");
        std::fs::write(&token_file, TOKEN_SENTINEL).expect("token file");
        std::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o600))
            .expect("token mode");
        let config = Config {
            messenger: Platform::Discord,
            work_dir,
            omp_path: PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")),
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
        };
        Self {
            _dir: dir,
            root,
            config,
            adapter: Arc::new(FakeAdapter::new()),
        }
    }

    fn log(&self) -> PathBuf {
        self.root.join("fake-omp.log")
    }

    fn gateway(&self, mode: &str) -> Result<Gateway, GatewayError> {
        self.gateway_with(self.adapter.clone(), mode)
    }

    fn gateway_with(&self, adapter: Arc<FakeAdapter>, mode: &str) -> Result<Gateway, GatewayError> {
        let adapter: Arc<dyn MessengerAdapter> = adapter;
        let vars = [
            ("FAKE_OMP_MODE", mode.to_owned()),
            ("FAKE_OMP_LOG", self.log().to_string_lossy().into_owned()),
        ];
        Ok(Gateway::open(&self.config, adapter)?.with_omp(|omp| omp.with_extra_env(vars)))
    }

    fn start(&self, mode: &str) -> Running {
        self.start_with(self.adapter.clone(), mode)
    }

    /// Starts a gateway on a fresh adapter (a restarted process gets a new connection).
    fn start_with(&self, adapter: Arc<FakeAdapter>, mode: &str) -> Running {
        let gateway = self.gateway_with(adapter, mode).expect("open gateway");
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(gateway.run(async {
            let _ = stopped.await;
        }));
        Running { stop, task }
    }

    /// The inbox rows after the gateway released the database.
    fn first_row_state(&self) -> State {
        let store = Store::open(&self.config.home.join(STORE_FILE)).expect("reopen store");
        store.get(1).expect("read row").expect("row exists").state
    }
}

/// The body of a logged prompt frame: its `message` must start with a moraebot tag line,
/// which is dropped.
fn prompt_body(frame: &Value) -> String {
    let message = frame["message"].as_str().expect("prompt message");
    assert!(
        lead::parse_tag(message).is_some(),
        "prompt starts with a moraebot tag"
    );
    message
        .split_once('\n')
        .map_or("", |(_, body)| body)
        .to_owned()
}

fn owner_message(message_id: &str, text: &str) -> InboundEvent {
    InboundEvent {
        message_id: message_id.into(),
        author_id: OWNER.into(),
        author_is_bot: false,
        channel_id: CHANNEL.into(),
        thread_id: None,
        is_dm: false,
        text: text.into(),
        attachments: Vec::new(),
    }
}

#[tokio::test]
async fn round_trip_replies_in_a_new_thread_and_stops_clean() {
    let rig = Rig::new();
    let mut tap = LogTap::create(&rig.log());
    let running = rig.start("ok");

    rig.adapter.inject(owner_message(MESSAGE_ID, "hello")).await;
    bounded(BOUND, rig.adapter.wait_for_send(1)).await;
    tap.wait_for_start().await;

    let calls = rig.adapter.calls();
    assert!(calls.contains(&Call::CreateThread {
        channel_id: CHANNEL.into(),
        from_message_id: MESSAGE_ID.into(),
        name: "hello".into(),
    }));
    assert!(calls.contains(&Call::Send {
        channel: MESSAGE_ID.into(),
        text: "echo: hello".into(),
    }));

    running.stop().await.expect("clean stop");
    assert_eq!(rig.first_row_state(), State::Done);
    for pid in tap.pids() {
        assert!(!process_exists(pid), "fake-omp {pid} is still alive");
    }
}

#[tokio::test]
async fn shutdown_during_a_hanging_turn_leaves_needs_check_and_no_child() {
    let rig = Rig::new();
    let mut tap = LogTap::create(&rig.log());
    let running = rig.start("hang");

    rig.adapter.inject(owner_message(MESSAGE_ID, "hello")).await;
    tap.wait_for_prompt().await;

    let _ = running.stop.send(());
    let mut task = running.task;
    let finished = tokio::time::timeout(SHUTDOWN_BOUND, &mut task).await;
    if finished.is_err() {
        for pid in tap.pids() {
            terminate(pid);
        }
        panic!("the gateway did not stop within the bound");
    }
    finished
        .unwrap()
        .expect("gateway task joined")
        .expect("clean stop");
    assert_eq!(rig.first_row_state(), State::NeedsCheck);
    assert_eq!(
        tap.prompts(),
        1,
        "the interrupted message must not be re-prompted"
    );
    let pids = tap.pids();
    assert_eq!(pids.len(), 1);
    for pid in pids {
        assert!(!process_exists(pid), "fake-omp {pid} is still alive");
    }
}

#[tokio::test]
async fn graceful_stop_then_restart_holds_queued_rows_behind_the_interrupted_one() {
    let rig = Rig::new();
    let mut tap = LogTap::create(&rig.log());
    let running = rig.start("hang");

    rig.adapter.inject(owner_message(MESSAGE_ID, "first")).await;
    let mut second = owner_message("1300000000000000002", "second");
    second.thread_id = Some(MESSAGE_ID.into());
    rig.adapter.inject(second).await;
    // Events are handled in order, so this notice proves "second" was stored.
    rig.adapter
        .inject(owner_message("1300000000000000003", "/marker"))
        .await;
    bounded(BOUND, rig.adapter.wait_for_send(1)).await;
    tap.wait_for_prompt().await;
    running.stop().await.expect("clean stop");

    let adapter = Arc::new(FakeAdapter::new());
    let running = rig.start_with(adapter.clone(), "ok");
    // A message in another conversation, admitted after the held row, is served after it.
    adapter
        .inject(owner_message("1300000000000000005", "third"))
        .await;
    bounded(BOUND, adapter.wait_for_send(2)).await;
    tap.wait_for(|line| {
        serde_json::from_str::<Value>(line).is_ok_and(|frame| {
            frame["type"] == "prompt" && prompt_body(&frame).starts_with("third")
        })
    })
    .await;
    running.stop().await.expect("clean stop");

    let messages: Vec<String> = tap
        .seen
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|frame| frame["type"] == "prompt")
        .map(|frame| prompt_body(&frame))
        .collect();
    // The restarted moraebot resumes the lead's session, which may hold an earlier memory
    // update, so its first input says the system prompt holds again.
    assert_eq!(
        messages,
        [
            "first".to_owned(),
            format!("third\n\n{}", moraebot::pin::revert_notice())
        ],
        "the held message never reaches omp"
    );
    let sends: Vec<(String, String)> = adapter
        .calls()
        .into_iter()
        .filter_map(|call| match call {
            Call::Send { channel, text } => Some((channel, text)),
            _ => None,
        })
        .collect();
    assert_eq!(
        sends,
        vec![
            (MESSAGE_ID.to_owned(), notice_text(N_INTERRUPTED).to_owned()),
            (
                "1300000000000000005".to_owned(),
                format!("echo: third\n\n{}", moraebot::pin::revert_notice()),
            ),
        ],
        "exactly one interrupted notice for the conversation"
    );
    let store = Store::open(&rig.config.home.join(STORE_FILE)).expect("reopen store");
    let held = store.get(2).expect("read row").expect("row exists");
    assert_eq!(held.state, State::NeedsCheck);
    assert_eq!(held.error.as_deref(), Some(E_DISPATCH_HELD));
    assert!(held.notice_sent);
    for pid in tap.pids() {
        assert!(!process_exists(pid), "fake-omp {pid} is still alive");
    }
}

#[tokio::test]
async fn second_gateway_on_the_same_home_is_refused() {
    let rig = Rig::new();
    let running = rig.start("ok");

    let refused = rig.gateway("ok").err().expect("second open must fail");
    assert_eq!(refused.code(), E_ALREADY_RUNNING);

    running.stop().await.expect("clean stop");
    rig.gateway("ok")
        .expect("the home is free again after a clean stop");
}

#[test]
fn run_refuses_a_token_file_that_others_can_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    let home = dir.path().join("home");
    let work_dir = dir.path().join("work");
    std::fs::create_dir_all(home.join("secrets")).expect("home");
    let token_file = home.join("secrets").join("discord.token");
    std::fs::write(&token_file, TOKEN_SENTINEL).expect("token file");
    std::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o644))
        .expect("token mode");
    let config_path = dir.path().join("config.toml");
    std::fs::write(
        &config_path,
        format!(
            "owner_id = {OWNER}\ntoken_file = {token_file:?}\nwork_dir = {work_dir:?}\n\
             omp_path = {omp:?}\nhome = {home:?}\n\n[discord]\nchannel_id = {CHANNEL}\n",
            omp = env!("CARGO_BIN_EXE_fake-omp"),
        ),
    )
    .expect("config file");

    let output = Command::new(env!("CARGO_BIN_EXE_moraebot"))
        .args(["run", "--config"])
        .arg(&config_path)
        .env("MORAEBOT_HOME", &home)
        .env("HOME", dir.path())
        .output()
        .expect("run moraebot");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(E_TOKEN_MODE), "stderr: {stderr}");
    assert!(!stderr.contains(TOKEN_SENTINEL), "token leaked to stderr");
}
