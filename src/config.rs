//! Configuration loading and validation.
//!
//! The config file never contains the bot token: it only points at a token file,
//! and the token content is never read into [`Config`].

use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::slack_bridge::config::check_slack_id;

pub const E_MESSENGER: &str = "E_MESSENGER";
pub const E_MESSENGER_MISSING: &str = "E_MESSENGER_MISSING";
pub const E_OWNER_MISSING: &str = "E_OWNER_MISSING";
pub const E_CHANNEL_MISSING: &str = "E_CHANNEL_MISSING";
pub const E_ACCESS_MODE: &str = "E_ACCESS_MODE";
pub const E_TOKEN_MISSING: &str = "E_TOKEN_MISSING";
pub const E_TOKEN_NOT_FILE: &str = "E_TOKEN_NOT_FILE";
pub const E_TOKEN_MODE: &str = "E_TOKEN_MODE";
pub const E_OMP_MISSING: &str = "E_OMP_MISSING";
pub const E_GIT_MISSING: &str = "E_GIT_MISSING";
pub const E_WORK_DIR: &str = "E_WORK_DIR";
pub const E_HOME_UNKNOWN: &str = "E_HOME_UNKNOWN";
pub const E_HOME_LAYOUT: &str = "E_HOME_LAYOUT";
pub const E_CONFIG_READ: &str = "E_CONFIG_READ";
pub const E_CONFIG_PARSE: &str = "E_CONFIG_PARSE";
pub const E_CONFIG_VALUE: &str = "E_CONFIG_VALUE";
/// The config still names Slack directly; Slack now runs in `moraebot-slack-bridge`.
pub const E_SLACK_MOVED: &str = "E_SLACK_MOVED";
pub const E_IO: &str = "E_IO";
/// Advisory only: someone besides the owner is listed and may talk to moraebot.
pub const W_ALLOWLIST: &str = "W_ALLOWLIST";
/// Advisory only: bots are listed and `max_bot_replies` is unset, so bot chats have no cap.
pub const W_BOT_REPLIES: &str = "W_BOT_REPLIES";
/// Advisory only: config.toml holds a table moraebot no longer reads (`[memory.jev]`); it is
/// ignored.
pub const W_CONFIG_IGNORED: &str = "W_CONFIG_IGNORED";
/// The removed jev table, still accepted in old files.
const JEV_TABLE: &str = "[memory.jev]";

/// The window that `max_bot_replies` counts replies in, per thread.
pub const BOT_REPLY_WINDOW_MINUTES: u64 = 10;

const DEFAULT_WORK_DIR: &str = "~/moraebot-work";

/// An error with a stable machine-readable code. Tests assert the code, never the prose.
#[derive(Debug, Clone)]
pub struct ConfigError {
    pub code: &'static str,
    pub message: String,
}

impl ConfigError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn io(code: &'static str, context: &str, err: &io::Error) -> Self {
        Self::new(code, format!("{context}: {err}"))
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ConfigError {}

impl From<io::Error> for ConfigError {
    fn from(err: io::Error) -> Self {
        Self::new(E_IO, err.to_string())
    }
}

/// Process environment inputs, passed explicitly so tests never touch real variables.
#[derive(Debug, Clone, Default)]
pub struct Env {
    /// `$HOME`.
    pub home_dir: Option<PathBuf>,
    /// `$MORAEBOT_HOME`, overrides the moraebot home folder.
    pub moraebot_home: Option<PathBuf>,
    /// `$PATH`.
    pub path: Option<OsString>,
}

impl Env {
    pub fn from_process() -> Self {
        let nonempty = |key: &str| std::env::var_os(key).filter(|v| !v.is_empty());
        Self {
            home_dir: nonempty("HOME").map(PathBuf::from),
            moraebot_home: nonempty("MORAEBOT_HOME").map(PathBuf::from),
            path: std::env::var_os("PATH"),
        }
    }

    /// `$MORAEBOT_HOME` if set, else `$HOME/.moraebot`.
    pub fn default_home(&self) -> Result<PathBuf, ConfigError> {
        if let Some(home) = &self.moraebot_home {
            return Ok(home.clone());
        }
        self.home_dir
            .as_ref()
            .map(|h| h.join(".moraebot"))
            .ok_or_else(|| {
                ConfigError::new(E_HOME_UNKNOWN, "HOME is not set; set MORAEBOT_HOME or HOME")
            })
    }

    pub fn default_config_path(&self) -> Result<PathBuf, ConfigError> {
        self.default_home().map(|home| home.join("config.toml"))
    }

    /// Expands a leading `~` using `$HOME`.
    pub fn expand_tilde(&self, path: &Path) -> Result<PathBuf, ConfigError> {
        let Ok(rest) = path.strip_prefix("~") else {
            return Ok(path.to_path_buf());
        };
        let home = self.home_dir.as_ref().ok_or_else(|| {
            ConfigError::new(E_HOME_UNKNOWN, "HOME is not set, cannot expand '~'")
        })?;
        Ok(home.join(rest))
    }
}

/// `owner_only` (the default) or `allowlist`, which turns on the `allow` lists in the
/// messenger sections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    OwnerOnly,
    Allowlist,
}

impl AccessMode {
    pub fn as_str(self) -> &'static str {
        match self {
            AccessMode::OwnerOnly => "owner_only",
            AccessMode::Allowlist => "allowlist",
        }
    }
}

/// Whether a listed sender is a person or a bot account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderKind {
    User,
    Bot,
}

impl SenderKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SenderKind::User => "user",
            SenderKind::Bot => "bot",
        }
    }
}

/// What a listed sender may do. Only `chat` exists in this version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Chat,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Chat => "chat",
        }
    }
}

/// One `[[<messenger>.allow]]` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllowEntry<Id> {
    pub id: Id,
    pub kind: SenderKind,
    pub role: Role,
}

/// The allowlist settings of one messenger section. Empty unless
/// `access_mode = "allowlist"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Access<Id> {
    pub allow: Vec<AllowEntry<Id>>,
    /// Replies to bot messages allowed per thread in any [`BOT_REPLY_WINDOW_MINUTES`]
    /// window. Absent means no limit.
    pub max_bot_replies: Option<u32>,
}

impl<Id> Default for Access<Id> {
    fn default() -> Self {
        Self {
            allow: Vec::new(),
            max_bot_replies: None,
        }
    }
}

/// The messenger the gateway talks to. Only one is active at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Discord,
    /// A separate bridge process on a unix socket (see `docs/bridge.md`). The bridge
    /// names the real platform in its hello.
    Bridge,
}

impl Platform {
    pub fn as_str(self) -> &'static str {
        match self {
            Platform::Discord => "discord",
            Platform::Bridge => "bridge",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiscordConfig {
    pub owner_id: u64,
    pub channel_id: u64,
    pub token_file: PathBuf,
    pub access: Access<u64>,
}

/// `[bridge]`: where the bridge listens and who may talk to moraebot. Ids are opaque
/// text; the messenger behind the bridge defines their format.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub socket: PathBuf,
    pub owner_id: String,
    pub access: Access<String>,
}

/// How long a running turn may show no progress before the stall notice, when
/// `omp.stall_after_minutes` is left out.
pub const DEFAULT_STALL_AFTER: Duration = Duration::from_secs(20 * 60);

#[derive(Debug, Clone, Default)]
pub struct OmpConfig {
    /// Absent means no turn time limit.
    pub turn_timeout_minutes: Option<u64>,
    /// Absent means no cost limit.
    pub max_cost_usd: Option<f64>,
    /// Absent means [`DEFAULT_STALL_AFTER`].
    pub stall_after_minutes: Option<u64>,
}

impl OmpConfig {
    /// How long a running lead or lane turn may show no progress before moraebot sends one
    /// stall notice. It never stops the turn.
    pub fn stall_after(&self) -> Duration {
        self.stall_after_minutes
            .map_or(DEFAULT_STALL_AFTER, |minutes| {
                Duration::from_secs(minutes.saturating_mul(60))
            })
    }
}

/// How long a lead turn may run while an owner message of its conversation waits before the
/// busy note, when `lead.handoff_after_minutes` is left out.
pub const DEFAULT_HANDOFF_AFTER: Duration = Duration::from_secs(5 * 60);

/// How long the lead keeps its pinned system prompt after its last turn ended, when
/// `lead.cache_minutes` is left out.
pub const DEFAULT_CACHE_WINDOW: Duration = Duration::from_secs(60 * 60);

/// `[lead]`: how moraebot nudges a lead turn that keeps the owner waiting, and how long the
/// lead keeps its pinned system prompt.
#[derive(Debug, Clone, Default)]
pub struct LeadConfig {
    /// Absent means [`DEFAULT_HANDOFF_AFTER`]; `0` turns the busy note off.
    pub handoff_after_minutes: Option<u64>,
    /// Absent means [`DEFAULT_CACHE_WINDOW`]; `0` restarts the lead on any memory change.
    pub cache_minutes: Option<u64>,
    /// Absent means `true`: moraebot answers owner status questions itself.
    pub direct_status: Option<bool>,
}

impl LeadConfig {
    /// How long a lead turn may run while an owner message of its conversation waits before
    /// moraebot sends the busy note; `None` when it is turned off. It never stops the turn.
    pub fn handoff_after(&self) -> Option<Duration> {
        match self.handoff_after_minutes {
            None => Some(DEFAULT_HANDOFF_AFTER),
            Some(0) => None,
            Some(minutes) => Some(Duration::from_secs(minutes.saturating_mul(60))),
        }
    }

    /// How long the lead may sit idle and still keep the system prompt it started with: a
    /// memory or persona change then reaches it as an update block in its next input, and the
    /// prompt is set again only once the lead was idle this long (the provider's prompt cache
    /// is cold by then). `None` (`cache_minutes = 0`) restarts the lead on any change.
    pub fn cache_window(&self) -> Option<Duration> {
        match self.cache_minutes {
            None => Some(DEFAULT_CACHE_WINDOW),
            Some(0) => None,
            Some(minutes) => Some(Duration::from_secs(minutes.saturating_mul(60))),
        }
    }

    /// Whether moraebot answers an owner status question ("status", "뭐 하고 있어", ...)
    /// itself; when `false` the message goes to the lead like any other.
    pub fn direct_status(&self) -> bool {
        self.direct_status.unwrap_or(true)
    }
}

/// A secret value held in memory. `Debug` never shows it, and it is never serialized.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[secret]")
    }
}

/// The `[memory]` settings.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MemoryConfig {
    /// Tables in the file moraebot no longer reads (`[memory.jev]`), reported as
    /// [`W_CONFIG_IGNORED`].
    pub ignored: Vec<String>,
    pub filter: FilterConfig,
    /// `[memory.self]`.
    pub self_file: SelfConfig,
    /// `[memory.recall]`.
    pub recall: RecallConfig,
}

impl MemoryConfig {
    /// The recall advisor's mode: `memory.recall.mode` when set, else `on` (the Kibitzer
    /// default: an install runs the advisor on `memory.recall.model`, `@smol` unless set).
    pub fn recall_mode(&self) -> RecallMode {
        self.recall.mode.unwrap_or(RecallMode::On)
    }

    /// What the advisor does while a lead turn runs: `memory.recall.mid_task` when set, else
    /// the advisor's own mode (`on` with `mode = "on"`, `shadow` with `mode = "shadow"`);
    /// always `off` when the advisor itself is off. `on` loads only with `mode = "on"`.
    pub fn recall_mid_task(&self) -> MidTask {
        match (self.recall_mode(), self.recall.mid_task) {
            (RecallMode::Off, _) => MidTask::Off,
            (_, Some(mid)) => mid,
            (RecallMode::On, None) => MidTask::On,
            (RecallMode::Shadow, None) => MidTask::Shadow,
        }
    }

    /// `memory.recall.model`, or `None` when it is empty (omp's default model then).
    pub fn recall_model(&self) -> Option<&str> {
        let model = self.recall.model.trim();
        (!model.is_empty()).then_some(model)
    }

    /// Whether owner corrections are captured: `[memory.self] enabled`.
    pub fn self_file_on(&self) -> bool {
        self.self_file.enabled
    }
}

/// What the recall advisor does (#59).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecallMode {
    /// No advisor runs.
    Off,
    /// The advisor runs after each owner input and its picks are only logged, never given to
    /// the lead.
    Shadow,
    /// The advisor runs before each owner turn, which waits for it at most `wait_ms`; its picks
    /// go into the lead's input as a guarded memory block (late ones with the conversation's
    /// next owner message). The default.
    On,
}

impl RecallMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::On => "on",
        }
    }

    fn parse(text: &str) -> Result<Self, ConfigError> {
        match text {
            "off" => Ok(Self::Off),
            "shadow" => Ok(Self::Shadow),
            "on" => Ok(Self::On),
            other => Err(ConfigError::new(
                E_CONFIG_VALUE,
                format!("memory.recall.mode '{other}' must be \"off\", \"shadow\" or \"on\""),
            )),
        }
    }
}

/// What the recall advisor does while a lead turn runs (#59, PR 3b): it wakes on the lead's
/// tool events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MidTask {
    /// No wake on tool events.
    Off,
    /// The advisor wakes on tool events and its picks are only logged (`phase=mid`).
    Shadow,
    /// Its picks are steered into the running turn as a guarded memory note. Only with
    /// `memory.recall.mode = "on"`.
    On,
}

impl MidTask {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::On => "on",
        }
    }

    fn parse(text: &str) -> Result<Self, ConfigError> {
        match text {
            "off" => Ok(Self::Off),
            "shadow" => Ok(Self::Shadow),
            "on" => Ok(Self::On),
            other => Err(ConfigError::new(
                E_CONFIG_VALUE,
                format!("memory.recall.mid_task '{other}' must be \"off\", \"shadow\" or \"on\""),
            )),
        }
    }
}

/// The `[memory.recall]` settings: the recall advisor (#59).
#[derive(Debug, Clone, PartialEq)]
pub struct RecallConfig {
    /// `None`: `on` ([`MemoryConfig::recall_mode`]).
    pub mode: Option<RecallMode>,
    /// The omp model of the advisor (`--model`); an omp model role such as `@smol` (the
    /// default) or a model name; empty is omp's default model.
    pub model: String,
    /// The most entries one advisor answer may pick.
    pub max_items: usize,
    /// The most entries offered to the advisor in one call.
    pub max_candidates: usize,
    /// Advisor calls allowed in any hour; a wake over it is skipped and logged.
    pub max_calls_per_hour: u32,
    /// The largest advisor message, in bytes; candidates that do not fit are left out.
    pub max_input_bytes: usize,
    /// How long one advisor call may take.
    pub timeout_seconds: u64,
    /// With `mode = "on"`: how long an owner turn waits for the advisor before it starts
    /// without the picks (they then go with the conversation's next owner message). At most
    /// [`MAX_RECALL_WAIT_MS`]; 0 never waits.
    pub wait_ms: u64,
    /// `None`: the advisor's mode ([`MemoryConfig::recall_mid_task`]).
    pub mid_task: Option<MidTask>,
    /// The most mid-task notes delivered to one conversation within `mid_cooldown_minutes`;
    /// a wake on a tool event while that many were delivered is not started. Wakes that
    /// deliver nothing never count.
    pub mid_max_delivered: u32,
    /// The window of `mid_max_delivered`, in minutes.
    pub mid_cooldown_minutes: u64,
}

/// The default omp model of the recall advisor: omp's small fast model role.
pub const DEFAULT_RECALL_MODEL: &str = "@smol";

/// `[memory.recall]` keys moraebot no longer reads; each is ignored with a
/// [`W_CONFIG_IGNORED`] naming it.
const OLD_RECALL_KEYS: [&str; 2] = [
    "memory.recall.mid_max_wakes_per_turn",
    "memory.recall.mid_min_interval_seconds",
];

/// The longest `memory.recall.wait_ms`: an owner turn never waits longer for recalled notes.
pub const MAX_RECALL_WAIT_MS: u64 = 10_000;

impl Default for RecallConfig {
    fn default() -> Self {
        Self {
            mode: None,
            model: DEFAULT_RECALL_MODEL.to_owned(),
            max_items: 3,
            max_candidates: 32,
            max_calls_per_hour: 30,
            max_input_bytes: 16 * 1024,
            timeout_seconds: 60,
            wait_ms: 2500,
            mid_task: None,
            mid_max_delivered: 2,
            mid_cooldown_minutes: 10,
        }
    }
}

/// The `[memory.self]` settings: the bot's self file (#59).
#[derive(Debug, Clone, PartialEq)]
pub struct SelfConfig {
    /// Whether owner corrections are saved in `self/observations.md` ([`crate::lessons`]).
    pub enabled: bool,
    /// Phrases that make an owner sentence a correction of the bot. Each matches as words in
    /// order, the last one as a word start so Korean endings still match (`하지 말라고 했`
    /// matches `하지 말라고 했잖아`); `…` or `...` allows any words in between. Case is ignored.
    pub correction_patterns: Vec<String>,
}

impl Default for SelfConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            correction_patterns: [
                "몇 번 말해",
                "몇 번을 말해",
                "몇 번째 말하",
                "왜 또",
                "또 그러",
                "그러지 마",
                "하지 말라고 했",
                "I told you",
                "why did you … again",
                "stop doing",
                "don't do that again",
            ]
            .iter()
            .map(|s| (*s).to_owned())
            .collect(),
        }
    }
}

/// The `[memory.filter]` lists for lane and subagent excerpts. Each configured list replaces
/// its built-in default.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterConfig {
    pub approval: Vec<String>,
    pub credentials: Vec<String>,
    pub commands: Vec<String>,
}

impl Default for FilterConfig {
    fn default() -> Self {
        let list = |items: &[&str]| items.iter().map(|s| (*s).to_owned()).collect();
        Self {
            approval: list(&[
                "오너",
                "승인",
                "허락",
                "허가",
                "권한",
                "owner",
                "approve",
                "approved",
                "approval",
                "permission",
                "permitted",
                "allowed",
                "authorize",
                "authorized",
                "consent",
            ]),
            credentials: list(&[
                "토큰",
                "비밀번호",
                "패스워드",
                "자격 증명",
                "자격증명",
                "token",
                "tokens",
                "password",
                "passwd",
                "credential",
                "credentials",
                "secret",
                "secrets",
                "api key",
                "apikey",
                "api_key",
                "private key",
            ]),
            commands: list(&[
                "$ ", "./", "| sh", "| bash", "&&", "rm -rf", "sudo", "curl", "wget", "sh", "bash",
                "zsh", "chmod", "eval",
            ]),
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub messenger: Platform,
    pub discord: Option<DiscordConfig>,
    pub bridge: Option<BridgeConfig>,
    pub work_dir: PathBuf,
    pub omp_path: PathBuf,
    pub home: PathBuf,
    pub access_mode: AccessMode,
    pub omp: OmpConfig,
    pub lead: LeadConfig,
    pub memory: MemoryConfig,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("messenger", &self.messenger)
            .field("discord", &self.discord)
            .field("bridge", &self.bridge)
            .field("work_dir", &self.work_dir)
            .field("omp_path", &self.omp_path)
            .field("home", &self.home)
            .field("access_mode", &self.access_mode)
            .field("omp", &self.omp)
            .field("lead", &self.lead)
            .field("memory", &self.memory)
            .finish()
    }
}

/// On-disk shape of `config.toml`. The top-level `owner_id` and `token_file` are the
/// slice-1 Discord layout: they are read but never written.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    #[serde(skip_serializing_if = "Option::is_none")]
    messenger: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    work_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    omp_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    home: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    discord: Option<DiscordSection>,
    /// Read only to report [`E_SLACK_MOVED`]; never written.
    #[serde(skip_serializing)]
    slack: Option<toml::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bridge: Option<BridgeSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    omp: Option<OmpSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lead: Option<LeadSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory: Option<MemorySection>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiscordSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bot_replies: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    allow: Option<Vec<AllowSection<u64>>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BridgeSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    socket: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bot_replies: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    allow: Option<Vec<AllowSection<String>>>,
}

/// The pre-bridge `[slack]` section, read only by [`Config::load_legacy_slack`].
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacySlackSection {
    owner_id: Option<String>,
    channel_id: Option<String>,
    bot_token_file: Option<PathBuf>,
    app_token_file: Option<PathBuf>,
    max_bot_replies: Option<u32>,
    allow: Option<Vec<AllowSection<String>>>,
}

/// What a legacy `[slack]` section held that now belongs to `slack-bridge.toml`. Token
/// paths are expanded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacySlack {
    /// The old top-level `messenger` value (`None` when it was not set); the move makes
    /// it `"bridge"`.
    pub messenger: Option<String>,
    /// The socket path written into `[bridge]`, for `slack-bridge.toml` too.
    pub socket: PathBuf,
    pub channel_id: String,
    pub bot_token_file: PathBuf,
    pub app_token_file: PathBuf,
}

/// On-disk `[[<messenger>.allow]]` entry. `kind` and `role` stay text so a bad value
/// gets an `E_CONFIG_VALUE` that names the entry.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AllowSection<Id> {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
}

impl<Id: Clone> AllowSection<Id> {
    fn from_entry(entry: &AllowEntry<Id>) -> Self {
        Self {
            id: Some(entry.id.clone()),
            kind: Some(entry.kind.as_str().to_string()),
            role: Some(entry.role.as_str().to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OmpSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    turn_timeout_minutes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_cost_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stall_after_minutes: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LeadSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    handoff_after_minutes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_minutes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    direct_status: Option<bool>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MemorySection {
    /// The removed `[memory.jev]` table (any keys): read only to warn with
    /// [`W_CONFIG_IGNORED`], never written.
    #[serde(skip_serializing)]
    jev: Option<toml::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<FilterSection>,
    #[serde(rename = "self", skip_serializing_if = "Option::is_none")]
    self_file: Option<SelfSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recall: Option<RecallSection>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RecallSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_items: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_calls_per_hour: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_input_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mid_task: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mid_max_delivered: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mid_cooldown_minutes: Option<u64>,
    /// No longer read ([`OLD_RECALL_KEYS`]); never written.
    #[serde(default, skip_serializing)]
    mid_max_wakes_per_turn: Option<toml::Value>,
    /// No longer read ([`OLD_RECALL_KEYS`]); never written.
    #[serde(default, skip_serializing)]
    mid_min_interval_seconds: Option<toml::Value>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SelfSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    correction_patterns: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FilterSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    approval: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credentials: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    commands: Option<Vec<String>>,
}

/// `Some(value)` when it differs from the default, so `to_toml` writes only changed values.
fn changed<T: PartialEq + Clone>(value: &T, default: &T) -> Option<T> {
    (value != default).then(|| value.clone())
}

/// `true` when every field of a section is absent.
fn is_empty_section<T: Serialize>(section: &T) -> bool {
    toml::Value::try_from(section)
        .ok()
        .and_then(|v| v.as_table().map(toml::Table::is_empty))
        .unwrap_or(false)
}

impl MemoryConfig {
    fn from_section(section: Option<MemorySection>) -> Result<Self, ConfigError> {
        let section = section.unwrap_or_default();
        macro_rules! take {
            ($target:expr, $source:expr, [$($field:ident),*]) => {
                $(if let Some(value) = $source.$field { $target.$field = value; })*
            };
        }
        let mut filter = FilterConfig::default();
        if let Some(lists) = section.filter {
            take!(filter, lists, [approval, credentials, commands]);
        }
        let mut self_file = SelfConfig::default();
        if let Some(section) = section.self_file {
            take!(self_file, section, [enabled, correction_patterns]);
        }
        let mut recall = RecallConfig::default();
        let mut ignored: Vec<String> = section
            .jev
            .map(|_| JEV_TABLE.to_owned())
            .into_iter()
            .collect();
        if let Some(section) = section.recall {
            recall.mode = section.mode.as_deref().map(RecallMode::parse).transpose()?;
            recall.mid_task = section
                .mid_task
                .as_deref()
                .map(MidTask::parse)
                .transpose()?;
            for (old, key) in [
                section.mid_max_wakes_per_turn.is_some(),
                section.mid_min_interval_seconds.is_some(),
            ]
            .into_iter()
            .zip(OLD_RECALL_KEYS)
            {
                if old {
                    ignored.push(key.to_owned());
                }
            }
            take!(
                recall,
                section,
                [
                    model,
                    max_items,
                    max_candidates,
                    max_calls_per_hour,
                    max_input_bytes,
                    timeout_seconds,
                    wait_ms,
                    mid_max_delivered,
                    mid_cooldown_minutes
                ]
            );
        }
        let memory = Self {
            ignored,
            filter,
            self_file,
            recall,
        };
        memory.check_values()?;
        Ok(memory)
    }

    fn check_values(&self) -> Result<(), ConfigError> {
        let bad = |message: String| Err(ConfigError::new(E_CONFIG_VALUE, message));
        let f = &self.filter;
        for (name, list) in [
            ("approval", &f.approval),
            ("credentials", &f.credentials),
            ("commands", &f.commands),
        ] {
            if list.iter().any(|pattern| pattern.trim().is_empty()) {
                return bad(format!(
                    "memory.filter.{name} must not hold an empty pattern"
                ));
            }
        }
        if self
            .self_file
            .correction_patterns
            .iter()
            .any(|pattern| pattern.trim().is_empty())
        {
            return bad("memory.self.correction_patterns must not hold an empty pattern".into());
        }
        let r = &self.recall;
        for (name, value) in [
            ("max_items", r.max_items as u64),
            ("max_candidates", r.max_candidates as u64),
            ("max_calls_per_hour", u64::from(r.max_calls_per_hour)),
            ("max_input_bytes", r.max_input_bytes as u64),
            ("timeout_seconds", r.timeout_seconds),
            ("mid_max_delivered", u64::from(r.mid_max_delivered)),
            ("mid_cooldown_minutes", r.mid_cooldown_minutes),
        ] {
            if value == 0 {
                return bad(format!("memory.recall.{name} must be greater than zero"));
            }
        }
        if r.max_items > r.max_candidates {
            return bad("memory.recall.max_items must not exceed max_candidates".into());
        }
        if r.wait_ms > MAX_RECALL_WAIT_MS {
            return bad(format!(
                "memory.recall.wait_ms must not exceed {MAX_RECALL_WAIT_MS}"
            ));
        }
        if r.mid_task == Some(MidTask::On) && self.recall_mode() != RecallMode::On {
            return bad(
                "memory.recall.mid_task = \"on\" needs memory.recall.mode = \"on\"".to_owned(),
            );
        }
        Ok(())
    }

    /// The on-disk section holding only the values that differ from their defaults.
    fn to_section(&self) -> Option<MemorySection> {
        let df = FilterConfig::default();
        let filter = FilterSection {
            approval: changed(&self.filter.approval, &df.approval),
            credentials: changed(&self.filter.credentials, &df.credentials),
            commands: changed(&self.filter.commands, &df.commands),
        };
        let ds = SelfConfig::default();
        let self_file = SelfSection {
            enabled: changed(&self.self_file.enabled, &ds.enabled),
            correction_patterns: changed(
                &self.self_file.correction_patterns,
                &ds.correction_patterns,
            ),
        };
        let (r, dr) = (&self.recall, RecallConfig::default());
        let recall = RecallSection {
            mode: r.mode.map(|mode| mode.as_str().to_owned()),
            model: changed(&r.model, &dr.model),
            max_items: changed(&r.max_items, &dr.max_items),
            max_candidates: changed(&r.max_candidates, &dr.max_candidates),
            max_calls_per_hour: changed(&r.max_calls_per_hour, &dr.max_calls_per_hour),
            max_input_bytes: changed(&r.max_input_bytes, &dr.max_input_bytes),
            timeout_seconds: changed(&r.timeout_seconds, &dr.timeout_seconds),
            wait_ms: changed(&r.wait_ms, &dr.wait_ms),
            mid_task: r.mid_task.map(|mid| mid.as_str().to_owned()),
            mid_max_delivered: changed(&r.mid_max_delivered, &dr.mid_max_delivered),
            mid_cooldown_minutes: changed(&r.mid_cooldown_minutes, &dr.mid_cooldown_minutes),
            mid_max_wakes_per_turn: None,
            mid_min_interval_seconds: None,
        };
        let section = MemorySection {
            jev: None,
            filter: (!is_empty_section(&filter)).then_some(filter),
            self_file: (!is_empty_section(&self_file)).then_some(self_file),
            recall: (!is_empty_section(&recall)).then_some(recall),
        };
        (!is_empty_section(&section)).then_some(section)
    }

    /// The `check-config` line for the recall advisor. Advisory: an empty model only means
    /// the advisor runs on omp's default model.
    fn recall_check(&self) -> Check {
        let mode = self.recall_mode();
        let mid = format!(", mid-task {}", self.recall_mid_task().as_str());
        let result = match (mode, self.recall_model()) {
            (RecallMode::Off, _) => Ok("off".to_owned()),
            (RecallMode::Shadow, Some(model)) => Ok(format!("shadow, model {model}{mid}")),
            (RecallMode::Shadow, None) => Err(ConfigError::new(
                E_CONFIG_VALUE,
                "shadow without memory.recall.model: the advisor runs on omp's default model; \
                 set a small fast model there",
            )),
            (RecallMode::On, Some(model)) => Ok(format!(
                "on, model {model}, owner turns wait up to {} ms{mid}",
                self.recall.wait_ms
            )),
            (RecallMode::On, None) => Err(ConfigError::new(
                E_CONFIG_VALUE,
                "on without memory.recall.model: the advisor runs on omp's default model before \
                 every owner turn; set a small fast model there",
            )),
        };
        Check {
            name: "memory.recall",
            result,
            advisory: true,
        }
    }

    /// The `check-config` line for tables moraebot no longer reads (`[memory.jev]`): an
    /// advisory [`W_CONFIG_IGNORED`] naming them. `None` when there is none.
    fn ignored_check(&self) -> Option<Check> {
        (!self.ignored.is_empty()).then(|| Check {
            name: "memory",
            result: Err(ConfigError::new(
                W_CONFIG_IGNORED,
                format!(
                    "{} is no longer used and is ignored; remove it",
                    self.ignored.join(", ")
                ),
            )),
            advisory: true,
        })
    }
}

/// The `E_CONFIG_PARSE` error for `text`. toml's message quotes the offending line, so when
/// the file holds an `api_key` key (never valid) or the error names it, a fixed message is
/// returned instead, so the key is
/// never printed by `check-config` or the gateway's start error.
fn parse_error(text: &str, err: &toml::de::Error) -> ConfigError {
    let names_key = |key: &str| {
        key.rsplit('.')
            .next()
            .is_some_and(|last| last.trim().trim_matches(['"', '\'']) == "api_key")
    };
    let key_line = text
        .lines()
        .filter_map(|line| line.split_once('='))
        .any(|(key, _)| names_key(key));
    if key_line || err.message().contains("`api_key`") {
        return ConfigError::new(
            E_CONFIG_PARSE,
            "config.toml holds an `api_key` key, which is not allowed (the line is not shown, so \
             the key is not printed): remove it",
        );
    }
    ConfigError::new(E_CONFIG_PARSE, err.to_string())
}

/// One line of `check-config` output.
#[derive(Debug)]
pub struct Check {
    pub name: &'static str,
    /// `Ok(detail)` or the failure.
    pub result: Result<String, ConfigError>,
    /// A failing advisory check is reported as `WARN` and never fails validation.
    pub advisory: bool,
}

impl Config {
    pub fn load(path: &Path, env: &Env) -> Result<Config, ConfigError> {
        let text = fs::read_to_string(path).map_err(|e| {
            ConfigError::io(
                E_CONFIG_READ,
                &format!("cannot read {}", path.display()),
                &e,
            )
        })?;
        Self::from_toml(&text, env)
    }

    /// Parses config text and resolves defaults. Filesystem checks live in [`Config::validate`].
    pub fn from_toml(text: &str, env: &Env) -> Result<Config, ConfigError> {
        let file: ConfigFile = toml::from_str(text).map_err(|e| parse_error(text, &e))?;

        let access_mode = match file.access_mode.as_deref() {
            None | Some("owner_only") => AccessMode::OwnerOnly,
            Some("allowlist") => AccessMode::Allowlist,
            Some(other) => {
                return Err(ConfigError::new(
                    E_ACCESS_MODE,
                    format!(
                        "access_mode '{other}' is not available; use 'owner_only' or 'allowlist'"
                    ),
                ));
            }
        };
        if file.slack.is_some() {
            return Err(ConfigError::new(
                E_SLACK_MOVED,
                "Slack now runs in the separate moraebot-slack-bridge process; run `moraebot \
                 setup slack` once to move the [slack] section to [bridge] and slack-bridge.toml",
            ));
        }
        if file.messenger.as_deref() == Some("slack") {
            return Err(ConfigError::new(
                E_SLACK_MOVED,
                "messenger = \"slack\" is gone: Slack now runs in the separate \
                 moraebot-slack-bridge process; set messenger = \"bridge\" with a [bridge] \
                 section by hand, or set messenger = \"discord\" and run `moraebot setup slack`, \
                 which writes both",
            ));
        }
        let messenger = match file.messenger.as_deref() {
            None | Some("discord") => Platform::Discord,
            Some("bridge") => Platform::Bridge,
            Some(other) => {
                return Err(ConfigError::new(
                    E_MESSENGER,
                    format!("messenger '{other}' is not available; use 'discord' or 'bridge'"),
                ));
            }
        };
        let discord = discord_config(
            file.discord,
            file.owner_id,
            file.token_file,
            access_mode,
            env,
        )?;
        let bridge = bridge_config(file.bridge, access_mode, env)?;
        let missing = match messenger {
            Platform::Discord => discord.is_none().then_some("[discord]"),
            Platform::Bridge => bridge.is_none().then_some("[bridge]"),
        };
        if let Some(section) = missing {
            return Err(ConfigError::new(
                E_MESSENGER_MISSING,
                format!(
                    "messenger is '{}' but the {section} section is missing",
                    messenger.as_str()
                ),
            ));
        }
        let omp_path = file
            .omp_path
            .ok_or_else(|| ConfigError::new(E_OMP_MISSING, "omp_path is required"))?;

        let omp = file.omp.unwrap_or(OmpSection {
            turn_timeout_minutes: None,
            max_cost_usd: None,
            stall_after_minutes: None,
        });
        if omp.turn_timeout_minutes == Some(0) {
            return Err(ConfigError::new(
                E_CONFIG_VALUE,
                "omp.turn_timeout_minutes must be greater than zero (omit it for no limit)",
            ));
        }
        if omp.stall_after_minutes == Some(0) {
            return Err(ConfigError::new(
                E_CONFIG_VALUE,
                "omp.stall_after_minutes must be greater than zero (omit it for 20 minutes)",
            ));
        }
        if let Some(cost) = omp.max_cost_usd
            && !(cost.is_finite() && cost > 0.0)
        {
            return Err(ConfigError::new(
                E_CONFIG_VALUE,
                "omp.max_cost_usd must be greater than zero (omit it for no limit)",
            ));
        }

        let memory = MemoryConfig::from_section(file.memory)?;

        let home = match file.home {
            Some(home) => env.expand_tilde(&home)?,
            None => env.default_home()?,
        };
        let work_dir =
            env.expand_tilde(&file.work_dir.unwrap_or_else(|| DEFAULT_WORK_DIR.into()))?;

        Ok(Config {
            messenger,
            discord,
            bridge,
            work_dir,
            omp_path: env.expand_tilde(&omp_path)?,
            home,
            access_mode,
            omp: OmpConfig {
                turn_timeout_minutes: omp.turn_timeout_minutes,
                max_cost_usd: omp.max_cost_usd,
                stall_after_minutes: omp.stall_after_minutes,
            },
            lead: file
                .lead
                .map_or_else(LeadConfig::default, |lead| LeadConfig {
                    handoff_after_minutes: lead.handoff_after_minutes,
                    cache_minutes: lead.cache_minutes,
                    direct_status: lead.direct_status,
                }),
            memory,
        })
    }

    /// Reads a `config.toml` that still holds the pre-bridge `[slack]` section, for the
    /// one-time move in `moraebot setup slack`. Returns `None` without such a section.
    /// Otherwise the returned config has `messenger = "bridge"` and a `[bridge]` section at
    /// the path `socket` returns (asked only when there is a section to move) with the old
    /// owner, reply limit and allow entries, and the Slack-only values come back as
    /// [`LegacySlack`]. Nothing is written.
    pub fn load_legacy_slack(
        path: &Path,
        env: &Env,
        socket: impl FnOnce() -> Result<PathBuf, ConfigError>,
    ) -> Result<Option<(Config, LegacySlack)>, ConfigError> {
        let text = fs::read_to_string(path).map_err(|e| {
            ConfigError::io(
                E_CONFIG_READ,
                &format!("cannot read {}", path.display()),
                &e,
            )
        })?;
        let mut table: toml::Table = toml::from_str(&text).map_err(|e| parse_error(&text, &e))?;
        let Some(slack) = table.remove("slack") else {
            return Ok(None);
        };
        if table.contains_key("bridge") {
            return Err(ConfigError::new(
                E_CONFIG_VALUE,
                "config.toml holds both [slack] and [bridge]; remove one of them, then run \
                 `moraebot setup slack` again",
            ));
        }
        let section: LegacySlackSection = slack
            .try_into()
            .map_err(|e| ConfigError::new(E_CONFIG_PARSE, format!("[slack]: {e}")))?;
        let owner_id = section
            .owner_id
            .ok_or_else(|| ConfigError::new(E_OWNER_MISSING, "slack.owner_id is required"))?;
        let channel_id = section
            .channel_id
            .ok_or_else(|| ConfigError::new(E_CHANNEL_MISSING, "slack.channel_id is required"))?;
        let bot_token_file = section
            .bot_token_file
            .ok_or_else(|| ConfigError::new(E_TOKEN_MISSING, "slack.bot_token_file is required"))?;
        let app_token_file = section
            .app_token_file
            .ok_or_else(|| ConfigError::new(E_TOKEN_MISSING, "slack.app_token_file is required"))?;
        check_slack_id("slack.owner_id", &owner_id, &['U', 'W'])?;
        check_slack_id("slack.channel_id", &channel_id, &['C', 'G'])?;

        let socket = socket()?;
        let bridge = BridgeSection {
            socket: Some(socket.clone()),
            owner_id: Some(owner_id),
            max_bot_replies: section.max_bot_replies,
            allow: section.allow,
        };
        let bridge = toml::Value::try_from(bridge)
            .map_err(|e| ConfigError::new(E_CONFIG_VALUE, e.to_string()))?;
        table.insert("bridge".into(), bridge);
        let messenger = table
            .insert("messenger".into(), Platform::Bridge.as_str().into())
            .and_then(|old| old.as_str().map(str::to_owned));
        let moved =
            toml::to_string(&table).map_err(|e| ConfigError::new(E_CONFIG_VALUE, e.to_string()))?;
        let config = Self::from_toml(&moved, env)?;
        // The bridge section only checks that ids are text; the old section knew they
        // are Slack ids. A bot is matched by its user id or its `B...` bot id.
        if let Some(bridge) = &config.bridge {
            for (index, entry) in bridge.access.allow.iter().enumerate() {
                let prefixes: &[char] = match entry.kind {
                    SenderKind::User => &['U', 'W'],
                    SenderKind::Bot => &['U', 'W', 'B'],
                };
                check_slack_id(&format!("slack.allow[{index}].id"), &entry.id, prefixes)?;
            }
        }
        let legacy = LegacySlack {
            messenger,
            socket,
            channel_id,
            bot_token_file: env.expand_tilde(&bot_token_file)?,
            app_token_file: env.expand_tilde(&app_token_file)?,
        };
        Ok(Some((config, legacy)))
    }

    /// Serializes to `config.toml` text, including the resolved `home`.
    pub fn to_toml(&self) -> Result<String, ConfigError> {
        let has_omp_limits = self.omp.turn_timeout_minutes.is_some()
            || self.omp.max_cost_usd.is_some()
            || self.omp.stall_after_minutes.is_some();
        let file = ConfigFile {
            messenger: Some(self.messenger.as_str().to_string()),
            owner_id: None,
            token_file: None,
            work_dir: Some(self.work_dir.clone()),
            omp_path: Some(self.omp_path.clone()),
            home: Some(self.home.clone()),
            access_mode: Some(self.access_mode.as_str().to_string()),
            discord: self.discord.as_ref().map(|d| DiscordSection {
                owner_id: Some(d.owner_id),
                channel_id: Some(d.channel_id),
                token_file: Some(d.token_file.clone()),
                max_bot_replies: d.access.max_bot_replies,
                allow: allow_sections(&d.access),
            }),
            slack: None,
            bridge: self.bridge.as_ref().map(|b| BridgeSection {
                socket: Some(b.socket.clone()),
                owner_id: Some(b.owner_id.clone()),
                max_bot_replies: b.access.max_bot_replies,
                allow: allow_sections(&b.access),
            }),
            omp: has_omp_limits.then_some(OmpSection {
                turn_timeout_minutes: self.omp.turn_timeout_minutes,
                max_cost_usd: self.omp.max_cost_usd,
                stall_after_minutes: self.omp.stall_after_minutes,
            }),
            lead: (self.lead.handoff_after_minutes.is_some()
                || self.lead.cache_minutes.is_some()
                || self.lead.direct_status.is_some())
            .then_some(LeadSection {
                handoff_after_minutes: self.lead.handoff_after_minutes,
                cache_minutes: self.lead.cache_minutes,
                direct_status: self.lead.direct_status,
            }),
            memory: self.memory.to_section(),
        };
        toml::to_string(&file).map_err(|e| ConfigError::new(E_CONFIG_VALUE, e.to_string()))
    }

    /// The memory store folder, `<home>/memory`.
    pub fn memory_dir(&self) -> PathBuf {
        crate::memory::memory_dir(&self.home)
    }

    /// The active messenger's owner id as text.
    pub fn owner_id(&self) -> String {
        match self.messenger {
            Platform::Discord => self.discord.as_ref().map(|d| d.owner_id.to_string()),
            Platform::Bridge => self.bridge.as_ref().map(|b| b.owner_id.clone()),
        }
        .unwrap_or_default()
    }

    /// The active messenger's channel id as text. Empty for the bridge, whose channel
    /// comes from the `home` of its hello.
    pub fn channel_id(&self) -> String {
        match self.messenger {
            Platform::Discord => self.discord.as_ref().map(|d| d.channel_id.to_string()),
            Platform::Bridge => None,
        }
        .unwrap_or_default()
    }

    /// Runs every check, creating the work folder and the home layout when missing.
    /// Token files are checked for every configured messenger; the inactive one is advisory.
    pub fn checks(&self) -> Vec<Check> {
        let active = self.messenger;
        let mut checks = vec![Check {
            name: "owner_id",
            result: self.check_owner(),
            advisory: false,
        }];
        let channel_name = match active {
            Platform::Discord => Some("discord.channel_id"),
            Platform::Bridge => None,
        };
        if let Some(name) = channel_name {
            checks.push(Check {
                name,
                result: self.check_channel(),
                advisory: false,
            });
        }
        if self.access_mode == AccessMode::Allowlist {
            match active {
                Platform::Discord => {
                    if let Some(discord) = &self.discord {
                        checks.extend(access_checks(
                            "discord.allow",
                            "discord.max_bot_replies",
                            &discord.access,
                        ));
                    }
                }
                Platform::Bridge => {
                    if let Some(bridge) = &self.bridge {
                        checks.extend(access_checks(
                            "bridge.allow",
                            "bridge.max_bot_replies",
                            &bridge.access,
                        ));
                    }
                }
            }
        }
        if let Some(discord) = &self.discord {
            checks.push(Check {
                name: "discord token_file",
                result: check_token_file(&discord.token_file),
                advisory: active != Platform::Discord,
            });
        }
        if let Some(bridge) = &self.bridge {
            checks.push(Check {
                name: "bridge socket",
                result: crate::bridge::socket::check_path(&bridge.socket)
                    .map(|()| bridge.socket.display().to_string()),
                advisory: active != Platform::Bridge,
            });
        }
        checks.extend(self.memory.ignored_check());
        checks.push(self.memory.recall_check());
        checks.extend([
            Check {
                name: "work_dir",
                result: self.check_work_dir(),
                advisory: false,
            },
            Check {
                name: "omp_path",
                result: self.check_omp(),
                advisory: false,
            },
            Check {
                name: "git",
                result: check_git(Path::new("git")),
                advisory: false,
            },
            Check {
                name: "home layout",
                result: self.ensure_home_layout(),
                advisory: false,
            },
            Check {
                name: "hints",
                result: self.check_hints(),
                advisory: false,
            },
        ]);
        checks
    }

    /// Returns the first failing check of the active messenger and the shared settings.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.checks()
            .into_iter()
            .filter(|check| !check.advisory)
            .try_for_each(|check| check.result.map(drop))
    }

    /// Creates the home folder with `conversations/`, `logs/` and `secrets/`, all mode 0700.
    pub fn ensure_home_layout(&self) -> Result<String, ConfigError> {
        for dir in [
            self.home.clone(),
            self.home.join("conversations"),
            self.home.join("logs"),
            self.home.join("secrets"),
        ] {
            ensure_private_dir(&dir).map_err(|e| {
                ConfigError::io(
                    E_HOME_LAYOUT,
                    &format!("cannot prepare {}", dir.display()),
                    &e,
                )
            })?;
        }
        Ok(self.home.display().to_string())
    }

    fn check_owner(&self) -> Result<String, ConfigError> {
        if self.owner_id().trim_matches('0').is_empty() {
            return Err(ConfigError::new(
                E_OWNER_MISSING,
                "owner_id must be a non-zero user id",
            ));
        }
        Ok("set".to_string())
    }

    fn check_channel(&self) -> Result<String, ConfigError> {
        let channel = match self.messenger {
            Platform::Discord => self.discord.as_ref().map(|d| d.channel_id.to_string()),
            Platform::Bridge => None,
        }
        .unwrap_or_default();
        if channel.trim_matches('0').is_empty() {
            return Err(ConfigError::new(
                E_CHANNEL_MISSING,
                format!(
                    "{}.channel_id must be a non-zero channel id",
                    self.messenger.as_str()
                ),
            ));
        }
        Ok("set".to_string())
    }

    fn check_work_dir(&self) -> Result<String, ConfigError> {
        let path = &self.work_dir;
        if path.is_dir() {
            return Ok(format!("{} (exists)", path.display()));
        }
        if path.exists() {
            return Err(ConfigError::new(
                E_WORK_DIR,
                format!("{} exists but is not a folder", path.display()),
            ));
        }
        fs::create_dir_all(path).map_err(|e| {
            ConfigError::io(E_WORK_DIR, &format!("cannot create {}", path.display()), &e)
        })?;
        Ok(format!("{} (created)", path.display()))
    }

    /// Validates only the active messenger's override file; the built-in defaults live in
    /// the adapters.
    fn check_hints(&self) -> Result<String, ConfigError> {
        if self.messenger == Platform::Bridge {
            // The override file is named after the platform the bridge reports in its
            // hello, which is unknown until it connects.
            return Ok("checked when the bridge connects".to_owned());
        }
        let platform = self.messenger.as_str();
        let path = crate::hints::override_path(&self.home, platform);
        if !path.exists() {
            return Ok("default when absent".to_owned());
        }
        crate::hints::load(&self.home, platform, "")?;
        Ok(format!("override {}", path.display()))
    }

    fn check_omp(&self) -> Result<String, ConfigError> {
        let path = &self.omp_path;
        let meta = fs::metadata(path).map_err(|e| {
            ConfigError::io(
                E_OMP_MISSING,
                &format!("cannot find omp at {}", path.display()),
                &e,
            )
        })?;
        if !meta.is_file() || meta.permissions().mode() & 0o111 == 0 {
            return Err(ConfigError::new(
                E_OMP_MISSING,
                format!("{} is not an executable file", path.display()),
            ));
        }
        Ok(path.display().to_string())
    }
}

/// Runs `<git> --version`: the memory store is a git repository, so moraebot needs git.
fn check_git(git: &Path) -> Result<String, ConfigError> {
    let output = std::process::Command::new(git)
        .arg("--version")
        .output()
        .map_err(|e| {
            ConfigError::io(
                E_GIT_MISSING,
                &format!(
                    "cannot run {}; install git (the memory store needs it)",
                    git.display()
                ),
                &e,
            )
        })?;
    if !output.status.success() {
        return Err(ConfigError::new(
            E_GIT_MISSING,
            format!("{} --version failed; check the git install", git.display()),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// A token file must exist, be a regular file and be private to the owner (no group or
/// other bits).
pub(crate) fn check_token_file(path: &Path) -> Result<String, ConfigError> {
    let meta = fs::metadata(path).map_err(|e| {
        ConfigError::io(
            E_TOKEN_MISSING,
            &format!("cannot read token file {}", path.display()),
            &e,
        )
    })?;
    if !meta.is_file() {
        return Err(ConfigError::new(
            E_TOKEN_NOT_FILE,
            format!("token file {} is not a regular file", path.display()),
        ));
    }
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(ConfigError::new(
            E_TOKEN_MODE,
            format!(
                "token file {} has mode {mode:03o}; only the owner may access it, run: chmod 600 {}",
                path.display(),
                path.display()
            ),
        ));
    }
    Ok(format!("{} (mode {mode:03o})", path.display()))
}

/// One `WARN` line per listed sender, so the owner sees who else can talk to moraebot,
/// and one line for the bot reply limit (a `WARN` when bots are listed without one).
fn access_checks<Id: fmt::Display>(
    allow_name: &'static str,
    limit_name: &'static str,
    access: &Access<Id>,
) -> Vec<Check> {
    let mut checks: Vec<Check> = access
        .allow
        .iter()
        .map(|entry| Check {
            name: allow_name,
            result: Err(ConfigError::new(
                W_ALLOWLIST,
                format!(
                    "{} (kind {}, role {}) may talk to moraebot",
                    entry.id,
                    entry.kind.as_str(),
                    entry.role.as_str()
                ),
            )),
            advisory: true,
        })
        .collect();
    if checks.is_empty() {
        checks.push(Check {
            name: allow_name,
            result: Ok("empty, only the owner can talk to moraebot".to_string()),
            advisory: true,
        });
    }
    let lists_bots = access
        .allow
        .iter()
        .any(|entry| entry.kind == SenderKind::Bot);
    checks.push(Check {
        name: limit_name,
        result: match access.max_bot_replies {
            Some(limit) => Ok(format!(
                "{limit} replies to bots per thread per {BOT_REPLY_WINDOW_MINUTES} minutes"
            )),
            None if lists_bots => Err(ConfigError::new(
                W_BOT_REPLIES,
                format!(
                    "not set, so listed bots get chat replies with no limit and each reply is a \
                     paid model turn; set {limit_name} to cap them"
                ),
            )),
            None => Ok("no limit".to_string()),
        },
        advisory: true,
    });
    checks
}

fn allow_sections<Id: Clone>(access: &Access<Id>) -> Option<Vec<AllowSection<Id>>> {
    (!access.allow.is_empty()).then(|| access.allow.iter().map(AllowSection::from_entry).collect())
}

/// Validates one section's `allow` list and `max_bot_replies`. Both need
/// `access_mode = "allowlist"`, so an `owner_only` config never looks wider than it is.
fn access_config<Id: PartialEq + fmt::Display>(
    section: &str,
    mode: AccessMode,
    owner_id: &Id,
    allow: Option<Vec<AllowSection<Id>>>,
    max_bot_replies: Option<u32>,
    check_id: impl Fn(&str, &Id, SenderKind) -> Result<(), ConfigError>,
) -> Result<Access<Id>, ConfigError> {
    if mode == AccessMode::OwnerOnly && (allow.is_some() || max_bot_replies.is_some()) {
        return Err(ConfigError::new(
            E_CONFIG_VALUE,
            format!(
                "{section}.allow and {section}.max_bot_replies need access_mode = \"allowlist\""
            ),
        ));
    }
    if max_bot_replies == Some(0) {
        return Err(ConfigError::new(
            E_CONFIG_VALUE,
            format!("{section}.max_bot_replies must be greater than zero (omit it for no limit)"),
        ));
    }
    let mut entries: Vec<AllowEntry<Id>> = Vec::new();
    for (index, raw) in allow.unwrap_or_default().into_iter().enumerate() {
        let name = format!("{section}.allow[{index}]");
        let id = raw
            .id
            .ok_or_else(|| ConfigError::new(E_CONFIG_VALUE, format!("{name}.id is required")))?;
        let kind = match raw.kind.as_deref() {
            Some("user") => SenderKind::User,
            Some("bot") => SenderKind::Bot,
            Some(other) => {
                return Err(ConfigError::new(
                    E_CONFIG_VALUE,
                    format!("{name}.kind '{other}' is not valid; use 'user' or 'bot'"),
                ));
            }
            None => {
                return Err(ConfigError::new(
                    E_CONFIG_VALUE,
                    format!("{name}.kind is required: 'user' or 'bot'"),
                ));
            }
        };
        check_id(&format!("{name}.id"), &id, kind)?;
        if &id == owner_id {
            return Err(ConfigError::new(
                E_CONFIG_VALUE,
                format!("{name}.id {id} is the owner; the owner is never listed in allow"),
            ));
        }
        if entries.iter().any(|entry| entry.id == id) {
            return Err(ConfigError::new(
                E_CONFIG_VALUE,
                format!("{name}.id {id} is listed more than once"),
            ));
        }
        let role = match raw.role.as_deref() {
            None | Some("chat") => Role::Chat,
            Some("work") => {
                return Err(ConfigError::new(
                    E_CONFIG_VALUE,
                    format!(
                        "{name}.role 'work' does not exist yet; listed senders can only chat, and only the owner gives work"
                    ),
                ));
            }
            Some(other) => {
                return Err(ConfigError::new(
                    E_CONFIG_VALUE,
                    format!("{name}.role '{other}' is not available; only 'chat' exists"),
                ));
            }
        };
        entries.push(AllowEntry { id, kind, role });
    }
    Ok(Access {
        allow: entries,
        max_bot_replies,
    })
}

/// Merges the `[discord]` section with the slice-1 top-level keys. A key given in both
/// places is ambiguous. `None` means no Discord settings at all.
fn discord_config(
    section: Option<DiscordSection>,
    legacy_owner: Option<u64>,
    legacy_token: Option<PathBuf>,
    mode: AccessMode,
    env: &Env,
) -> Result<Option<DiscordConfig>, ConfigError> {
    if section.is_none() && legacy_owner.is_none() && legacy_token.is_none() {
        return Ok(None);
    }
    let section = section.unwrap_or_default();
    if legacy_owner.is_some() && section.owner_id.is_some() {
        return Err(ConfigError::new(
            E_CONFIG_VALUE,
            "owner_id is set both at the top level and in [discord]; keep only the [discord] one",
        ));
    }
    if legacy_token.is_some() && section.token_file.is_some() {
        return Err(ConfigError::new(
            E_CONFIG_VALUE,
            "token_file is set both at the top level and in [discord]; keep only the [discord] one",
        ));
    }
    let owner_id = legacy_owner
        .or(section.owner_id)
        .ok_or_else(|| ConfigError::new(E_OWNER_MISSING, "owner_id is required"))?;
    let channel_id = section
        .channel_id
        .ok_or_else(|| ConfigError::new(E_CHANNEL_MISSING, "discord.channel_id is required"))?;
    let token_file = legacy_token
        .or(section.token_file)
        .ok_or_else(|| ConfigError::new(E_TOKEN_MISSING, "token_file is required"))?;
    let access = access_config(
        "discord",
        mode,
        &owner_id,
        section.allow,
        section.max_bot_replies,
        |name, id, _kind| {
            if *id == 0 {
                return Err(ConfigError::new(
                    E_CONFIG_VALUE,
                    format!("{name} must be a non-zero user id"),
                ));
            }
            Ok(())
        },
    )?;
    Ok(Some(DiscordConfig {
        owner_id,
        channel_id,
        token_file: env.expand_tilde(&token_file)?,
        access,
    }))
}

/// The core does not know the id format of the messenger behind the bridge, so an id
/// only has to be non-empty text without whitespace.
fn bridge_config(
    section: Option<BridgeSection>,
    mode: AccessMode,
    env: &Env,
) -> Result<Option<BridgeConfig>, ConfigError> {
    let Some(section) = section else {
        return Ok(None);
    };
    let socket = section
        .socket
        .ok_or_else(|| ConfigError::new(E_CONFIG_VALUE, "bridge.socket is required"))?;
    let owner_id = section
        .owner_id
        .ok_or_else(|| ConfigError::new(E_OWNER_MISSING, "bridge.owner_id is required"))?;
    check_bridge_id("bridge.owner_id", &owner_id)?;
    let access = access_config(
        "bridge",
        mode,
        &owner_id,
        section.allow,
        section.max_bot_replies,
        |name, id, _kind| check_bridge_id(name, id),
    )?;
    Ok(Some(BridgeConfig {
        socket: env.expand_tilde(&socket)?,
        owner_id,
        access,
    }))
}

fn check_bridge_id(name: &str, id: &str) -> Result<(), ConfigError> {
    if id.is_empty() || id.chars().any(char::is_whitespace) {
        return Err(ConfigError::new(
            E_CONFIG_VALUE,
            format!("{name} must be a non-empty id without spaces"),
        ));
    }
    Ok(())
}

fn ensure_private_dir(path: &Path) -> io::Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

/// Prints each check as `OK`/`FAIL`. Returns true when every check passed.
pub fn report_checks(checks: &[Check], out: &mut impl io::Write) -> io::Result<bool> {
    let mut all_ok = true;
    for check in checks {
        match &check.result {
            Ok(detail) => writeln!(out, "OK   {}: {detail}", check.name)?,
            Err(err) if check.advisory => writeln!(out, "WARN {}: {err}", check.name)?,
            Err(err) => {
                all_ok = false;
                writeln!(out, "FAIL {}: {err}", check.name)?;
            }
        }
    }
    Ok(all_ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn quoted(path: &Path) -> String {
        toml::Value::from(path.to_str().expect("utf-8 path")).to_string()
    }

    struct Fixture {
        dir: TempDir,
        token: PathBuf,
        omp: PathBuf,
        env: Env,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            let token = dir.path().join("token");
            fs::write(&token, "TOKEN_SENTINEL_9f3a").expect("write token");
            fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).expect("chmod token");
            let omp = dir.path().join("omp");
            fs::write(&omp, "#!/bin/sh\n").expect("write omp");
            fs::set_permissions(&omp, fs::Permissions::from_mode(0o755)).expect("chmod omp");
            let env = Env {
                home_dir: Some(dir.path().to_path_buf()),
                moraebot_home: None,
                path: None,
            };
            Self {
                dir,
                token,
                omp,
                env,
            }
        }

        fn valid_toml(&self) -> String {
            format!(
                "owner_id = 111\ntoken_file = {}\nomp_path = {}\n[discord]\nchannel_id = 222\n",
                quoted(&self.token),
                quoted(&self.omp)
            )
        }

        fn load(&self, text: &str) -> Result<Config, ConfigError> {
            Config::from_toml(text, &self.env)
        }
    }

    fn code<T: fmt::Debug>(result: Result<T, ConfigError>) -> &'static str {
        result.expect_err("expected an error").code
    }

    #[test]
    fn memory_config_defaults() {
        let fx = Fixture::new();
        let cfg = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(cfg.memory, MemoryConfig::default());
        assert!(cfg.memory.filter.commands.contains(&"sh".to_owned()));
        assert!(
            cfg.memory.self_file_on(),
            "corrections are captured by default"
        );
        assert!(cfg.checks().iter().all(|c| c.name != "memory"));
    }

    /// An install upgraded from a version with jev keeps loading: `[memory.jev]` (any keys)
    /// is ignored with one advisory `W_CONFIG_IGNORED` naming it, and never written again.
    #[test]
    fn an_old_jev_table_is_ignored_with_a_warning() {
        let fx = Fixture::new();
        let text = format!(
            "{}[memory.jev]\nenabled = true\napi_key_file = \"/nowhere\"\nbase_url = \"http://x\"\n\
             api_key = \"KEY_SENTINEL_51c2\"\n[memory.jev.questions]\nspan = \"Pick one.\"\n",
            fx.valid_toml()
        );
        let cfg = fx.load(&text).expect("loads");
        assert_eq!(cfg.memory.ignored, ["[memory.jev]"]);
        assert!(cfg.memory.self_file_on());
        assert_eq!(cfg.memory.recall_mode(), RecallMode::On);
        let checks = cfg.checks();
        let warn = checks
            .iter()
            .find(|c| c.name == "memory")
            .expect("ignored check");
        assert!(warn.advisory);
        let err = warn.result.as_ref().expect_err("warns");
        assert_eq!(err.code, W_CONFIG_IGNORED);
        assert!(err.message.contains("[memory.jev]"), "{}", err.message);
        let shown = format!("{cfg:?}");
        let written = cfg.to_toml().expect("serializes");
        for out in [&shown, &written] {
            assert!(!out.contains("KEY_SENTINEL_51c2"), "{out}");
        }
        assert!(!written.contains("jev"), "{written}");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn memory_config_survives_a_to_toml_round_trip() {
        let fx = Fixture::new();
        let unchanged = fx.load(&fx.valid_toml()).expect("loads");
        assert!(
            !unchanged.to_toml().expect("serializes").contains("memory"),
            "defaults are not written"
        );
        let text = format!(
            "{}[memory.filter]\ncommands = []\napproval = [\"ok\"]\n",
            fx.valid_toml()
        );
        let cfg = fx.load(&text).expect("loads");
        let written = cfg.to_toml().expect("serializes");
        assert!(!written.contains("timeout_seconds"), "defaults stay out");
        let again = fx.load(&written).expect("reloads");
        assert_eq!(again.memory, cfg.memory);
        assert!(again.memory.filter.commands.is_empty());
        assert_eq!(again.memory.filter.approval, vec!["ok".to_owned()]);
        assert_eq!(
            again.memory.filter.credentials,
            FilterConfig::default().credentials
        );
    }

    #[test]
    fn memory_self_section_round_trips_and_refuses_an_empty_pattern() {
        let fx = Fixture::new();
        let unchanged = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(unchanged.memory.self_file, SelfConfig::default());
        assert!(unchanged.memory.self_file.enabled);
        let text = format!(
            "{}[memory.self]\nenabled = false\ncorrection_patterns = [\"다시 해\"]\n",
            fx.valid_toml()
        );
        let cfg = fx.load(&text).expect("loads");
        assert!(!cfg.memory.self_file.enabled);
        assert_eq!(cfg.memory.self_file.correction_patterns, ["다시 해"]);
        let written = cfg.to_toml().expect("serializes");
        assert!(written.contains("[memory.self]"), "{written}");
        assert_eq!(fx.load(&written).expect("reloads").memory, cfg.memory);
        let empty = format!(
            "{}[memory.self]\ncorrection_patterns = [\" \"]\n",
            fx.valid_toml()
        );
        assert_eq!(fx.load(&empty).expect_err("refused").code, E_CONFIG_VALUE);
    }

    #[test]
    fn memory_recall_is_on_by_default_on_the_smol_model() {
        let fx = Fixture::new();
        let mode = |extra: &str| {
            let cfg = fx
                .load(&format!("{}{extra}", fx.valid_toml()))
                .expect("loads");
            cfg.memory.recall_mode()
        };
        let plain = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(plain.memory.recall, RecallConfig::default());
        assert_eq!(plain.memory.recall_mode(), RecallMode::On);
        assert_eq!(plain.memory.recall_mid_task(), MidTask::On);
        assert_eq!(plain.memory.recall_model(), Some("@smol"));
        let model = "[memory.recall]\nmodel = \"qa-small\"\n";
        for (extra, want) in [
            (String::new(), RecallMode::On),
            (model.to_owned(), RecallMode::On),
            (format!("{model}mode = \"off\"\n"), RecallMode::Off),
            (
                "[memory.recall]\nmode = \"shadow\"\n".to_owned(),
                RecallMode::Shadow,
            ),
            (format!("{model}mode = \"on\"\n"), RecallMode::On),
        ] {
            assert_eq!(mode(&extra), want, "{extra}");
        }
        let empty = fx
            .load(&format!(
                "{}[memory.recall]\nmodel = \"\"\n",
                fx.valid_toml()
            ))
            .expect("loads");
        assert_eq!(empty.memory.recall_model(), None, "omp's default model");
        let model_only = fx
            .load(&format!("{}{model}", fx.valid_toml()))
            .expect("loads");
        let written = model_only.to_toml().unwrap();
        assert!(
            !written.lines().any(|line| line.starts_with("mode =")),
            "the default stays implicit: {written}"
        );

        let text = format!(
            "{}[memory.recall]\nmode = \"off\"\nmodel = \"claude-haiku-5-5\"\nmax_calls_per_hour = 5\n",
            fx.valid_toml()
        );
        let cfg = fx.load(&text).expect("loads");
        assert_eq!(cfg.memory.recall_mode(), RecallMode::Off);
        assert_eq!(cfg.memory.recall_mid_task(), MidTask::Off);
        assert_eq!(cfg.memory.recall_model(), Some("claude-haiku-5-5"));
        assert_eq!(cfg.memory.recall.max_calls_per_hour, 5);
        assert_eq!(cfg.memory.recall.wait_ms, 2500);
        let written = cfg.to_toml().expect("serializes");
        assert!(written.contains("[memory.recall]"), "{written}");
        assert!(
            !written.contains("wait_ms"),
            "defaults stay implicit: {written}"
        );
        assert_eq!(fx.load(&written).expect("reloads").memory, cfg.memory);

        let with = |body: &str| format!("{}[memory.recall]\n{body}\n", fx.valid_toml());
        let on = fx
            .load(&with("mode = \"on\"\nwait_ms = 0"))
            .expect("on with no wait loads");
        assert_eq!(on.memory.recall_mode(), RecallMode::On);
        assert_eq!(on.memory.recall.wait_ms, 0);
        let written = on.to_toml().expect("serializes");
        assert!(written.contains("mode = \"on\""), "{written}");
        assert_eq!(fx.load(&written).expect("reloads").memory, on.memory);
        let longest = format!("wait_ms = {MAX_RECALL_WAIT_MS}");
        assert!(fx.load(&with(&longest)).is_ok());
        let too_long = format!("wait_ms = {}", MAX_RECALL_WAIT_MS + 1);
        for bad in [
            too_long.as_str(),
            "mode = \"live\"",
            "max_items = 0",
            "timeout_seconds = 0",
            "max_items = 40",
        ] {
            assert_eq!(code(fx.load(&with(bad))), E_CONFIG_VALUE, "{bad}");
        }
    }

    #[test]
    fn check_config_names_the_advisor_mode_and_warns_without_a_model() {
        let fx = Fixture::new();
        let check = |text: &str| {
            let cfg = fx.load(text).expect("loads");
            let check = cfg
                .checks()
                .into_iter()
                .find(|c| c.name == "memory.recall")
                .expect("recall check");
            assert!(check.advisory);
            check.result.map_err(|e| e.code)
        };
        let plain = fx.valid_toml();
        assert_eq!(
            check(&plain),
            Ok("on, model @smol, owner turns wait up to 2500 ms, mid-task on".to_owned())
        );
        let off = format!("{plain}[memory.recall]\nmode = \"off\"\n");
        assert_eq!(check(&off), Ok("off".to_owned()), "turned off on purpose");
        let shadow = format!("{plain}[memory.recall]\nmode = \"shadow\"\nmodel = \"qa-small\"\n");
        assert_eq!(
            check(&shadow),
            Ok("shadow, model qa-small, mid-task shadow".to_owned())
        );
        for mode in ["shadow", "on"] {
            let empty = format!("{plain}[memory.recall]\nmode = \"{mode}\"\nmodel = \"\"\n");
            assert_eq!(
                check(&empty),
                Err(E_CONFIG_VALUE),
                "{mode} on omp's default model"
            );
        }
        let mid_off = format!("{plain}[memory.recall]\nmid_task = \"off\"\n");
        assert_eq!(
            check(&mid_off),
            Ok("on, model @smol, owner turns wait up to 2500 ms, mid-task off".to_owned())
        );
    }

    #[test]
    fn mid_task_recall_follows_the_mode_unless_set_and_is_on_only_with_mode_on() {
        let fx = Fixture::new();
        let with = |body: &str| format!("{}[memory.recall]\n{body}\n", fx.valid_toml());
        let mid = |body: &str| {
            fx.load(&with(body))
                .expect("loads")
                .memory
                .recall_mid_task()
        };
        assert_eq!(
            mid("mode = \"off\""),
            MidTask::Off,
            "no advisor, no mid wakes"
        );
        assert_eq!(mid("mode = \"off\"\nmid_task = \"shadow\""), MidTask::Off);
        assert_eq!(mid("mode = \"shadow\""), MidTask::Shadow);
        assert_eq!(mid("mode = \"on\""), MidTask::On);
        assert_eq!(mid("mid_task = \"on\""), MidTask::On);
        assert_eq!(mid("mode = \"on\"\nmid_task = \"shadow\""), MidTask::Shadow);
        assert_eq!(mid("mode = \"shadow\"\nmid_task = \"off\""), MidTask::Off);
        assert_eq!(mid("mid_task = \"off\""), MidTask::Off);
        let plain = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(plain.memory.recall.mid_max_delivered, 2);
        assert_eq!(plain.memory.recall.mid_cooldown_minutes, 10);

        let set = fx
            .load(&with(
                "mode = \"on\"\nmid_task = \"on\"\nmid_max_delivered = 4\nmid_cooldown_minutes = 5",
            ))
            .expect("loads");
        let written = set.to_toml().expect("serializes");
        assert!(written.contains("mid_task = \"on\""), "{written}");
        assert!(written.contains("mid_max_delivered = 4"), "{written}");
        assert_eq!(fx.load(&written).expect("reloads").memory, set.memory);
        let default = fx.load(&with("mode = \"shadow\"")).expect("loads");
        let written = default.to_toml().expect("serializes");
        assert!(
            !written.contains("mid_"),
            "defaults stay implicit: {written}"
        );

        for bad in [
            "mode = \"shadow\"\nmid_task = \"on\"",
            "mode = \"off\"\nmid_task = \"on\"",
            "mode = \"on\"\nmid_task = \"live\"",
            "mid_max_delivered = 0",
            "mid_cooldown_minutes = 0",
        ] {
            assert_eq!(code(fx.load(&with(bad))), E_CONFIG_VALUE, "{bad}");
        }
    }

    /// The per-turn cap and the interval of earlier versions are gone: their keys still load,
    /// with one advisory `W_CONFIG_IGNORED` naming them, and are never written again.
    #[test]
    fn the_old_mid_wake_keys_are_ignored_with_a_warning() {
        let fx = Fixture::new();
        let text = format!(
            "{}[memory.recall]\nmid_max_wakes_per_turn = 4\nmid_min_interval_seconds = 0\n",
            fx.valid_toml()
        );
        let cfg = fx.load(&text).expect("loads");
        assert_eq!(cfg.memory.ignored, OLD_RECALL_KEYS);
        let checks = cfg.checks();
        let warn = checks
            .iter()
            .find(|c| c.name == "memory")
            .expect("ignored check");
        let err = warn.result.as_ref().expect_err("warns");
        assert_eq!(err.code, W_CONFIG_IGNORED);
        assert!(
            err.message.contains("mid_max_wakes_per_turn"),
            "{}",
            err.message
        );
        let written = cfg.to_toml().expect("serializes");
        assert!(!written.contains("mid_m"), "{written}");
    }

    /// moraedang on #46: toml's parse error quotes the offending line, so a key written into
    /// `config.toml` must never reach the error text.
    #[test]
    fn memory_config_never_echoes_a_key_written_into_the_file() {
        let fx = Fixture::new();
        let sentinel = "KEY_SENTINEL_e41f";
        for text in [
            format!("{}[memory]\napi_key = \"{sentinel}\"\n", fx.valid_toml()),
            format!(
                "{}[memory.filter]\napi_key = \"{sentinel}\n",
                fx.valid_toml()
            ),
        ] {
            let err = fx.load(&text).expect_err("refused");
            assert_eq!(err.code, E_CONFIG_PARSE);
            assert!(!err.to_string().contains(sentinel), "{err}");
            assert!(!format!("{err:?}").contains(sentinel), "{err:?}");
        }
    }

    #[test]
    fn memory_config_refuses_an_empty_filter_pattern() {
        let fx = Fixture::new();
        let empty = format!("{}[memory.filter]\ncommands = [\"\"]\n", fx.valid_toml());
        assert_eq!(code(fx.load(&empty)), E_CONFIG_VALUE);
    }

    #[test]
    fn memory_config_refuses_unknown_keys() {
        let fx = Fixture::new();
        for section in ["[memory]\nnope = 1\n", "[memory.filter]\nnope = []\n"] {
            let text = format!("{}{section}", fx.valid_toml());
            assert_eq!(code(fx.load(&text)), E_CONFIG_PARSE, "{section}");
        }
    }

    #[test]
    fn missing_owner_id_is_an_error() {
        let fx = Fixture::new();
        let text = fx.valid_toml().replace("owner_id = 111\n", "");
        assert_eq!(code(fx.load(&text)), E_OWNER_MISSING);
    }

    #[test]
    fn channel_id_follows_the_active_messenger() {
        let fx = Fixture::new();
        let mut cfg = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(cfg.channel_id(), "222");
        cfg.messenger = Platform::Bridge;
        assert_eq!(
            cfg.channel_id(),
            "",
            "the bridge reports its channel in the hello"
        );
    }

    #[test]
    fn owner_id_zero_fails_validation() {
        let fx = Fixture::new();
        let text = fx.valid_toml().replace("owner_id = 111", "owner_id = 0");
        let cfg = fx.load(&text).expect("loads");
        assert_eq!(code(cfg.validate()), E_OWNER_MISSING);
    }

    #[test]
    fn missing_channel_id_is_an_error() {
        let fx = Fixture::new();
        let text = fx.valid_toml().replace("[discord]\nchannel_id = 222\n", "");
        assert_eq!(code(fx.load(&text)), E_CHANNEL_MISSING);
    }

    #[test]
    fn channel_id_zero_fails_validation() {
        let fx = Fixture::new();
        let text = fx
            .valid_toml()
            .replace("channel_id = 222", "channel_id = 0");
        let cfg = fx.load(&text).expect("loads");
        assert_eq!(code(cfg.validate()), E_CHANNEL_MISSING);
    }

    #[test]
    fn token_file_mode_0644_is_rejected() {
        let fx = Fixture::new();
        fs::set_permissions(&fx.token, fs::Permissions::from_mode(0o644)).expect("chmod");
        let cfg = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(code(cfg.validate()), E_TOKEN_MODE);
    }

    #[test]
    fn missing_token_file_is_rejected() {
        let fx = Fixture::new();
        fs::remove_file(&fx.token).expect("remove");
        let cfg = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(code(cfg.validate()), E_TOKEN_MISSING);
    }

    #[test]
    fn token_path_that_is_a_directory_is_rejected() {
        let fx = Fixture::new();
        let text = fx
            .valid_toml()
            .replace(&quoted(&fx.token), &quoted(fx.dir.path()));
        let cfg = fx.load(&text).expect("loads");
        assert_eq!(code(cfg.validate()), E_TOKEN_NOT_FILE);
    }

    #[test]
    fn unknown_key_is_rejected() {
        let fx = Fixture::new();
        let text = format!("{}surprise = 1\n", fx.valid_toml());
        assert_eq!(code(fx.load(&text)), E_CONFIG_PARSE);
    }

    #[test]
    fn unknown_nested_key_is_rejected() {
        let fx = Fixture::new();
        let text = format!("{}surprise = 1\n", fx.valid_toml());
        assert_eq!(code(fx.load(&text)), E_CONFIG_PARSE);
        let nested = format!("{}[omp]\nsurprise = 1\n", fx.valid_toml());
        assert_eq!(code(fx.load(&nested)), E_CONFIG_PARSE);
    }

    #[test]
    fn access_mode_open_is_rejected() {
        let fx = Fixture::new();
        let text = format!("access_mode = \"open\"\n{}", fx.valid_toml());
        assert_eq!(code(fx.load(&text)), E_ACCESS_MODE);
    }

    #[test]
    fn owner_only_access_mode_is_accepted() {
        let fx = Fixture::new();
        let text = format!("access_mode = \"owner_only\"\n{}", fx.valid_toml());
        assert_eq!(
            fx.load(&text).expect("loads").access_mode,
            AccessMode::OwnerOnly
        );
    }

    fn allowlist_toml(fx: &Fixture, extra: &str) -> String {
        format!("access_mode = \"allowlist\"\n{}{extra}", fx.valid_toml())
    }

    const DISCORD_ALLOW: &str = "max_bot_replies = 30\n\
         [[discord.allow]]\nid = 333\nkind = \"user\"\nrole = \"chat\"\n\
         [[discord.allow]]\nid = 444\nkind = \"bot\"\n";

    #[test]
    fn owner_only_configs_load_with_an_empty_allowlist() {
        let fx = Fixture::new();
        let discord = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(discord.access_mode, AccessMode::OwnerOnly);
        assert_eq!(discord.discord.expect("discord").access, Access::default());
        let bridge = fx.load(&bridge_toml(&fx, "/run/b.sock")).expect("loads");
        assert_eq!(bridge.bridge.expect("bridge").access, Access::default());
    }

    #[test]
    fn discord_allowlist_is_parsed() {
        let fx = Fixture::new();
        let cfg = fx.load(&allowlist_toml(&fx, DISCORD_ALLOW)).expect("loads");
        assert_eq!(cfg.access_mode, AccessMode::Allowlist);
        let access = cfg.discord.expect("discord").access;
        assert_eq!(
            access.allow,
            vec![
                AllowEntry {
                    id: 333,
                    kind: SenderKind::User,
                    role: Role::Chat
                },
                AllowEntry {
                    id: 444,
                    kind: SenderKind::Bot,
                    role: Role::Chat
                },
            ]
        );
        assert_eq!(access.max_bot_replies, Some(30));
    }

    #[test]
    fn max_bot_replies_absent_means_no_limit() {
        let fx = Fixture::new();
        let text = allowlist_toml(&fx, &DISCORD_ALLOW.replace("max_bot_replies = 30\n", ""));
        let cfg = fx.load(&text).expect("loads");
        assert_eq!(cfg.discord.expect("discord").access.max_bot_replies, None);
        let zero = allowlist_toml(&fx, &DISCORD_ALLOW.replace("= 30", "= 0"));
        assert_eq!(code(fx.load(&zero)), E_CONFIG_VALUE);
    }

    #[test]
    fn invalid_allow_entries_are_config_value_errors() {
        let fx = Fixture::new();
        let cases = [
            (
                "unknown kind",
                DISCORD_ALLOW.replace("\"user\"", "\"admin\""),
            ),
            ("work role", DISCORD_ALLOW.replace("\"chat\"", "\"work\"")),
            (
                "unknown role",
                DISCORD_ALLOW.replace("\"chat\"", "\"owner\""),
            ),
            (
                "missing kind",
                DISCORD_ALLOW.replace("kind = \"user\"\n", ""),
            ),
            ("missing id", DISCORD_ALLOW.replace("id = 333\n", "")),
            ("zero id", DISCORD_ALLOW.replace("id = 333", "id = 0")),
            (
                "duplicate id",
                DISCORD_ALLOW.replace("id = 444", "id = 333"),
            ),
            (
                "owner listed",
                DISCORD_ALLOW.replace("id = 444", "id = 111"),
            ),
        ];
        for (case, extra) in cases {
            assert_eq!(
                code(fx.load(&allowlist_toml(&fx, &extra))),
                E_CONFIG_VALUE,
                "{case}"
            );
        }
        let message = |role: &str| {
            let extra = DISCORD_ALLOW.replace("\"chat\"", role);
            fx.load(&allowlist_toml(&fx, &extra))
                .expect_err("rejected")
                .message
        };
        assert_ne!(message("\"work\""), message("\"owner\""));
        let unknown_key = allowlist_toml(&fx, &format!("{DISCORD_ALLOW}surprise = 1\n"));
        assert_eq!(code(fx.load(&unknown_key)), E_CONFIG_PARSE);
    }

    #[test]
    fn allowlist_keys_need_allowlist_mode() {
        let fx = Fixture::new();
        let allow = format!("{}{DISCORD_ALLOW}", fx.valid_toml());
        assert_eq!(code(fx.load(&allow)), E_CONFIG_VALUE);
        let limit = format!("{}max_bot_replies = 5\n", fx.valid_toml());
        assert_eq!(code(fx.load(&limit)), E_CONFIG_VALUE);
    }

    #[test]
    fn to_toml_keeps_the_allowlist() {
        let fx = Fixture::new();
        let cfg = fx.load(&allowlist_toml(&fx, DISCORD_ALLOW)).expect("loads");
        let again = fx
            .load(&cfg.to_toml().expect("serializes"))
            .expect("reloads");
        assert_eq!(again.access_mode, AccessMode::Allowlist);
        assert_eq!(
            again.discord.expect("discord").access,
            cfg.discord.expect("discord").access
        );
    }

    #[test]
    fn check_config_warns_when_bots_are_listed_without_a_reply_limit() {
        let fx = Fixture::new();
        let text = allowlist_toml(&fx, &DISCORD_ALLOW.replace("max_bot_replies = 30\n", ""));
        let cfg = fx.load(&text).expect("loads");
        cfg.validate().expect("warnings never fail validation");
        let mut out = Vec::new();
        assert!(report_checks(&cfg.checks(), &mut out).expect("report"));
        let report = String::from_utf8(out).expect("utf-8");
        assert!(
            report.lines().any(
                |l| l.starts_with("WARN discord.max_bot_replies:") && l.contains(W_BOT_REPLIES)
            ),
            "{report}"
        );

        let users_only = DISCORD_ALLOW
            .replace("max_bot_replies = 30\n", "")
            .replace("[[discord.allow]]\nid = 444\nkind = \"bot\"\n", "");
        let cfg = fx.load(&allowlist_toml(&fx, &users_only)).expect("loads");
        let mut out = Vec::new();
        report_checks(&cfg.checks(), &mut out).expect("report");
        let report = String::from_utf8(out).expect("utf-8");
        assert!(
            report.contains("OK   discord.max_bot_replies: no limit"),
            "{report}"
        );
    }

    #[test]
    fn check_config_warns_with_each_allowed_id_and_role() {
        let fx = Fixture::new();
        let cfg = fx.load(&allowlist_toml(&fx, DISCORD_ALLOW)).expect("loads");
        cfg.validate().expect("warnings never fail validation");
        let mut out = Vec::new();
        assert!(report_checks(&cfg.checks(), &mut out).expect("report"));
        let report = String::from_utf8(out).expect("utf-8");
        let warns: Vec<_> = report
            .lines()
            .filter(|line| line.starts_with("WARN discord.allow:"))
            .collect();
        assert_eq!(warns.len(), 2, "{report}");
        assert!(warns[0].contains(W_ALLOWLIST) && warns[0].contains("333"));
        assert!(warns[0].contains("user") && warns[0].contains("chat"));
        assert!(warns[1].contains("444") && warns[1].contains("bot"));
        assert!(report.contains("OK   discord.max_bot_replies: 30 "));

        let owner_only = fx.load(&fx.valid_toml()).expect("loads");
        let names: Vec<_> = owner_only.checks().iter().map(|c| c.name).collect();
        assert!(!names.contains(&"discord.allow"));
    }

    #[test]
    fn omp_path_is_required() {
        let fx = Fixture::new();
        let text = fx
            .valid_toml()
            .replace(&format!("omp_path = {}\n", quoted(&fx.omp)), "");
        assert_eq!(code(fx.load(&text)), E_OMP_MISSING);
    }

    #[test]
    fn omp_path_must_exist_and_be_executable() {
        let fx = Fixture::new();
        fs::set_permissions(&fx.omp, fs::Permissions::from_mode(0o644)).expect("chmod");
        let cfg = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(code(cfg.validate()), E_OMP_MISSING);
        fs::remove_file(&fx.omp).expect("remove");
        assert_eq!(code(cfg.validate()), E_OMP_MISSING);
    }

    #[test]
    fn git_check_runs_git_version() {
        let fx = Fixture::new();
        assert!(
            check_git(Path::new("git"))
                .expect("git")
                .starts_with("git version")
        );
        let missing = fx.dir.path().join("no-git");
        assert_eq!(code(check_git(&missing)), E_GIT_MISSING);
        assert_eq!(code(check_git(Path::new("false"))), E_GIT_MISSING);
    }

    #[test]
    fn valid_config_creates_work_dir_and_private_home_layout() {
        let fx = Fixture::new();
        let home = fx.dir.path().join("home");
        let work = fx.dir.path().join("work");
        let env = Env {
            moraebot_home: Some(home.clone()),
            ..fx.env.clone()
        };
        let text = format!("work_dir = {}\n{}", quoted(&work), fx.valid_toml());
        let cfg = Config::from_toml(&text, &env).expect("loads");
        cfg.validate().expect("valid");
        assert!(work.is_dir());
        for dir in [
            &home,
            &home.join("conversations"),
            &home.join("logs"),
            &home.join("secrets"),
        ] {
            let mode = fs::metadata(dir).expect("dir exists").permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{}", dir.display());
        }
    }

    #[test]
    fn defaults_come_from_the_environment() {
        let fx = Fixture::new();
        let cfg = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(cfg.work_dir, fx.dir.path().join("moraebot-work"));
        assert_eq!(cfg.home, fx.dir.path().join(".moraebot"));
        assert_eq!(cfg.omp.turn_timeout_minutes, None);
        assert_eq!(cfg.omp.max_cost_usd, None);
        assert_eq!(cfg.omp.stall_after_minutes, None);
        assert_eq!(cfg.omp.stall_after(), Duration::from_secs(20 * 60));
        assert_eq!(cfg.lead.handoff_after(), Some(Duration::from_secs(5 * 60)));
    }

    #[test]
    fn handoff_after_is_parsed_and_zero_turns_it_off() {
        let fx = Fixture::new();
        let seven = format!("{}[lead]\nhandoff_after_minutes = 7\n", fx.valid_toml());
        let cfg = fx.load(&seven).expect("loads");
        assert_eq!(cfg.lead.handoff_after(), Some(Duration::from_secs(7 * 60)));
        let again = fx
            .load(&cfg.to_toml().expect("serializes"))
            .expect("reloads");
        assert_eq!(again.lead.handoff_after_minutes, Some(7));
        let off = format!("{}[lead]\nhandoff_after_minutes = 0\n", fx.valid_toml());
        assert_eq!(fx.load(&off).expect("loads").lead.handoff_after(), None);
        let unknown = format!("{}[lead]\nhandoff_after = 3\n", fx.valid_toml());
        assert!(fx.load(&unknown).is_err());
    }

    #[test]
    fn cache_minutes_is_parsed_and_zero_restarts_on_any_change() {
        let fx = Fixture::new();
        let cfg = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(cfg.lead.cache_window(), Some(Duration::from_secs(60 * 60)));
        let ten = format!("{}[lead]\ncache_minutes = 10\n", fx.valid_toml());
        let cfg = fx.load(&ten).expect("loads");
        assert_eq!(cfg.lead.cache_window(), Some(Duration::from_secs(10 * 60)));
        let again = fx
            .load(&cfg.to_toml().expect("serializes"))
            .expect("reloads");
        assert_eq!(again.lead.cache_minutes, Some(10));
        let off = format!("{}[lead]\ncache_minutes = 0\n", fx.valid_toml());
        assert_eq!(fx.load(&off).expect("loads").lead.cache_window(), None);
        let negative = format!("{}[lead]\ncache_minutes = -1\n", fx.valid_toml());
        assert!(fx.load(&negative).is_err());
    }

    #[test]
    fn direct_status_defaults_on_and_can_be_turned_off() {
        let fx = Fixture::new();
        assert!(
            fx.load(&fx.valid_toml())
                .expect("loads")
                .lead
                .direct_status()
        );
        let off = format!("{}[lead]\ndirect_status = false\n", fx.valid_toml());
        let cfg = fx.load(&off).expect("loads");
        assert!(!cfg.lead.direct_status());
        assert_eq!(cfg.lead.handoff_after(), Some(DEFAULT_HANDOFF_AFTER));
        let again = fx
            .load(&cfg.to_toml().expect("serializes"))
            .expect("reloads");
        assert_eq!(again.lead.direct_status, Some(false));
        let on = format!("{}[lead]\ndirect_status = true\n", fx.valid_toml());
        assert!(fx.load(&on).expect("loads").lead.direct_status());
    }

    #[test]
    fn config_home_wins_over_env_and_env_is_the_fallback() {
        let fx = Fixture::new();
        let env_home = fx.dir.path().join("from-env");
        let file_home = fx.dir.path().join("from-file");
        let env = Env {
            moraebot_home: Some(env_home.clone()),
            ..fx.env.clone()
        };
        let with_home = format!("home = {}\n{}", quoted(&file_home), fx.valid_toml());
        assert_eq!(
            Config::from_toml(&with_home, &env).expect("loads").home,
            file_home
        );
        assert_eq!(
            Config::from_toml(&fx.valid_toml(), &env)
                .expect("loads")
                .home,
            env_home
        );
    }

    #[test]
    fn optional_limits_are_parsed() {
        let fx = Fixture::new();
        let text = format!(
            "{}[omp]\nturn_timeout_minutes = 30\nmax_cost_usd = 2.5\nstall_after_minutes = 7\n",
            fx.valid_toml()
        );
        let cfg = fx.load(&text).expect("loads");
        assert_eq!(cfg.omp.turn_timeout_minutes, Some(30));
        assert_eq!(cfg.omp.max_cost_usd, Some(2.5));
        assert_eq!(cfg.omp.stall_after_minutes, Some(7));
        assert_eq!(cfg.omp.stall_after(), Duration::from_secs(7 * 60));
    }

    #[test]
    fn zero_limits_are_rejected() {
        let fx = Fixture::new();
        let timeout = format!("{}[omp]\nturn_timeout_minutes = 0\n", fx.valid_toml());
        assert_eq!(code(fx.load(&timeout)), E_CONFIG_VALUE);
        let cost = format!("{}[omp]\nmax_cost_usd = 0.0\n", fx.valid_toml());
        assert_eq!(code(fx.load(&cost)), E_CONFIG_VALUE);
        let stall = format!("{}[omp]\nstall_after_minutes = 0\n", fx.valid_toml());
        assert_eq!(code(fx.load(&stall)), E_CONFIG_VALUE);
    }

    #[test]
    fn debug_shows_token_path_but_never_token_content() {
        let fx = Fixture::new();
        let cfg = fx.load(&fx.valid_toml()).expect("loads");
        let shown = format!("{cfg:?}");
        assert!(shown.contains(fx.token.to_str().expect("utf-8 path")));
        assert!(!shown.contains("TOKEN_SENTINEL_9f3a"));
    }

    #[test]
    fn to_toml_round_trips() {
        let fx = Fixture::new();
        let text = format!(
            "{}[omp]\nturn_timeout_minutes = 5\nstall_after_minutes = 9\n",
            fx.valid_toml()
        );
        let cfg = fx.load(&text).expect("loads");
        let again = fx
            .load(&cfg.to_toml().expect("serializes"))
            .expect("reloads");
        assert_eq!(again.owner_id(), cfg.owner_id());
        assert_eq!(again.home, cfg.home);
        assert_eq!(again.work_dir, cfg.work_dir);
        assert_eq!(again.omp_path, cfg.omp_path);
        assert_eq!(again.omp.turn_timeout_minutes, Some(5));
        assert_eq!(again.omp.stall_after_minutes, Some(9));
    }

    #[test]
    fn legacy_discord_file_loads() {
        let fx = Fixture::new();
        let cfg = fx.load(&fx.valid_toml()).expect("loads");
        assert_eq!(cfg.messenger, Platform::Discord);
        let discord = cfg.discord.expect("discord section");
        assert_eq!(discord.owner_id, 111);
        assert_eq!(discord.channel_id, 222);
        assert_eq!(discord.token_file, fx.token);
        assert!(cfg.bridge.is_none());
    }

    #[test]
    fn legacy_and_section_owner_conflict_is_config_value() {
        let fx = Fixture::new();
        let owner = fx
            .valid_toml()
            .replace("[discord]\n", "[discord]\nowner_id = 333\n");
        assert_eq!(code(fx.load(&owner)), E_CONFIG_VALUE);
        let token = fx.valid_toml().replace(
            "[discord]\n",
            &format!("[discord]\ntoken_file = {}\n", quoted(&fx.token)),
        );
        assert_eq!(code(fx.load(&token)), E_CONFIG_VALUE);
    }

    #[test]
    fn a_slack_section_or_messenger_is_slack_moved() {
        let fx = Fixture::new();
        let section = "[slack]\nowner_id = \"U111\"\nchannel_id = \"C222\"\n";
        let cases = [
            format!("{}{section}", fx.valid_toml()),
            format!("messenger = \"slack\"\n{}", fx.valid_toml()),
            format!("messenger = \"slack\"\n{}{section}", fx.valid_toml()),
            format!(
                "messenger = \"slack\"\nomp_path = {}\n{section}",
                quoted(&fx.omp)
            ),
        ];
        for text in cases {
            assert_eq!(code(fx.load(&text)), E_SLACK_MOVED, "{text}");
        }
    }

    const LEGACY_SLACK: &str = "[slack]\nowner_id = \"U111\"\nchannel_id = \"C222\"\n\
         bot_token_file = \"~/secrets/slack-bot.token\"\n\
         app_token_file = \"~/secrets/slack-app.token\"\n";

    fn legacy(fx: &Fixture, text: &str) -> Result<Option<(Config, LegacySlack)>, ConfigError> {
        let path = fx.dir.path().join("config.toml");
        fs::write(&path, text).expect("write config");
        Config::load_legacy_slack(&path, &fx.env, || {
            Ok(PathBuf::from("/h/u/run/slack-bridge.sock"))
        })
    }

    #[test]
    fn legacy_slack_section_moves_to_bridge() {
        let fx = Fixture::new();
        let text = format!(
            "messenger = \"slack\"\naccess_mode = \"allowlist\"\nomp_path = {}\n{LEGACY_SLACK}\
             max_bot_replies = 4\n[[slack.allow]]\nid = \"B0BOT\"\nkind = \"bot\"\n",
            quoted(&fx.omp)
        );
        let (config, slack) = legacy(&fx, &text).expect("reads").expect("legacy section");
        assert_eq!(config.messenger, Platform::Bridge);
        let bridge = config.bridge.as_ref().expect("bridge");
        assert_eq!(bridge.socket, Path::new("/h/u/run/slack-bridge.sock"));
        assert_eq!(bridge.owner_id, "U111");
        assert_eq!(bridge.access.max_bot_replies, Some(4));
        assert_eq!(bridge.access.allow.len(), 1);
        assert_eq!(bridge.access.allow[0].id, "B0BOT");
        assert_eq!(bridge.access.allow[0].kind, SenderKind::Bot);
        assert_eq!(
            slack,
            LegacySlack {
                messenger: Some("slack".into()),
                socket: PathBuf::from("/h/u/run/slack-bridge.sock"),
                channel_id: "C222".into(),
                bot_token_file: fx.dir.path().join("secrets/slack-bot.token"),
                app_token_file: fx.dir.path().join("secrets/slack-app.token"),
            }
        );
        let written = Config::from_toml(&config.to_toml().expect("toml"), &fx.env);
        assert!(
            written.is_ok(),
            "the moved config loads with the normal loader"
        );
        let discord = text.replace("messenger = \"slack\"", "messenger = \"discord\"");
        let (config, slack) = legacy(&fx, &discord)
            .expect("reads")
            .expect("legacy section");
        assert_eq!(config.messenger, Platform::Bridge);
        assert_eq!(slack.messenger.as_deref(), Some("discord"));
    }

    #[test]
    fn no_legacy_slack_section_reads_as_none() {
        let fx = Fixture::new();
        let path = fx.dir.path().join("config.toml");
        let slack_messenger = format!("messenger = \"slack\"\n{}", fx.valid_toml());
        for text in [fx.valid_toml(), slack_messenger] {
            fs::write(&path, &text).expect("write config");
            // The socket is only asked for when there is a section to move.
            let unused = || Err(ConfigError::new(E_CONFIG_VALUE, "socket asked for"));
            let read = Config::load_legacy_slack(&path, &fx.env, unused).expect("reads");
            assert!(read.is_none(), "{text}");
        }
    }

    #[test]
    fn bad_legacy_slack_sections_are_refused() {
        let fx = Fixture::new();
        let omp = format!("omp_path = {}\n", quoted(&fx.omp));
        let cases = [
            (
                format!(
                    "{omp}{LEGACY_SLACK}[bridge]\nsocket = \"/h/u/s.sock\"\nowner_id = \"U1\"\n"
                ),
                E_CONFIG_VALUE,
            ),
            (
                format!("{omp}{}", LEGACY_SLACK.replace("U111", "111")),
                E_CONFIG_VALUE,
            ),
            (
                format!("{omp}{}", LEGACY_SLACK.replace("C222", "X222")),
                E_CONFIG_VALUE,
            ),
            (
                format!(
                    "{omp}{}",
                    LEGACY_SLACK.replace("channel_id = \"C222\"\n", "")
                ),
                E_CHANNEL_MISSING,
            ),
            (format!("{omp}{LEGACY_SLACK}extra = 1\n"), E_CONFIG_PARSE),
            (
                format!(
                    "access_mode = \"allowlist\"\n{omp}{LEGACY_SLACK}\
                     [[slack.allow]]\nid = \"B0BOT\"\nkind = \"user\"\n"
                ),
                E_CONFIG_VALUE,
            ),
        ];
        for (text, expected) in cases {
            assert_eq!(code(legacy(&fx, &text)), expected, "{text}");
        }
    }

    #[test]
    fn unknown_messenger_rejected() {
        let fx = Fixture::new();
        let text = format!("messenger = \"telegram\"\n{}", fx.valid_toml());
        assert_eq!(code(fx.load(&text)), E_MESSENGER);
    }

    #[test]
    fn to_toml_writes_new_layout_and_round_trips() {
        let fx = Fixture::new();
        let cfg = fx.load(&fx.valid_toml()).expect("loads");
        let text = cfg.to_toml().expect("serializes");
        let top_level = text.split('[').next().expect("text before first table");
        assert!(top_level.contains("messenger = \"discord\""));
        assert!(!top_level.contains("owner_id"));
        assert!(!top_level.contains("token_file"));
        let again = fx.load(&text).expect("reloads");
        let (a, b) = (
            again.discord.expect("discord"),
            cfg.discord.expect("discord"),
        );
        assert_eq!(
            (a.owner_id, a.channel_id, a.token_file),
            (b.owner_id, b.channel_id, b.token_file)
        );

        // Nothing Slack-specific is ever written: no [slack] table, no token file key
        // besides Discord's.
        let both = format!(
            "{}[bridge]\nsocket = \"/run/b.sock\"\nowner_id = \"U111\"\n",
            fx.valid_toml()
        );
        let text = fx
            .load(&both)
            .expect("loads")
            .to_toml()
            .expect("serializes");
        let table: toml::Table = toml::from_str(&text).expect("valid TOML");
        assert!(!table.contains_key("slack"), "{text}");
        assert!(!text.contains("bot_token_file") && !text.contains("app_token_file"));
        let bridge = table["bridge"].as_table().expect("bridge table");
        assert!(bridge.keys().all(|key| !key.contains("token")), "{text}");
    }

    #[test]
    fn checks_cover_both_messengers() {
        let fx = Fixture::new();
        let socket = fx.dir.path().join("b.sock");
        let both = format!(
            "{}[bridge]\nsocket = {}\nowner_id = \"U111\"\n",
            fx.valid_toml(),
            quoted(&socket)
        );
        let cfg = fx.load(&both).expect("loads");
        let names: Vec<_> = cfg.checks().iter().map(|c| c.name).collect();
        for name in ["discord token_file", "bridge socket"] {
            assert!(names.contains(&name), "{name}");
        }
        // With the bridge active, a bad Discord token file is advisory only.
        fs::set_permissions(&fx.token, fs::Permissions::from_mode(0o644)).expect("chmod");
        let bridge_active = format!("messenger = \"bridge\"\n{both}");
        let cfg = fx.load(&bridge_active).expect("loads");
        cfg.validate().expect("the active bridge is fine");
        let mut out = Vec::new();
        assert!(report_checks(&cfg.checks(), &mut out).expect("report"));
        let report = String::from_utf8(out).expect("utf-8");
        assert!(report.contains("WARN discord token_file"), "{report}");
        assert!(report.contains(E_TOKEN_MODE));

        // Discord active: the same bad file now fails validation.
        let cfg = fx.load(&both).expect("loads");
        assert_eq!(code(cfg.validate()), E_TOKEN_MODE);
    }

    fn bridge_toml(fx: &Fixture, socket: &str) -> String {
        format!(
            "messenger = \"bridge\"\nomp_path = {}\n[bridge]\nsocket = {}\nowner_id = \"U111\"\n",
            quoted(&fx.omp),
            toml::Value::from(socket)
        )
    }

    fn check<'a>(checks: &'a [Check], name: &str) -> &'a Check {
        checks
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no check named {name}"))
    }

    #[test]
    fn bridge_section_loads_and_round_trips() {
        let fx = Fixture::new();
        let text = format!(
            "access_mode = \"allowlist\"\n{}max_bot_replies = 3\n\
             [[bridge.allow]]\nid = \"U333\"\nkind = \"user\"\n\
             [[bridge.allow]]\nid = \"B444\"\nkind = \"bot\"\n",
            bridge_toml(&fx, "~/run/slack-bridge.sock")
        );
        let cfg = fx.load(&text).expect("loads");
        assert_eq!(cfg.messenger, Platform::Bridge);
        assert_eq!(cfg.messenger.as_str(), "bridge");
        let bridge = cfg.bridge.clone().expect("bridge section");
        assert_eq!(bridge.socket, fx.dir.path().join("run/slack-bridge.sock"));
        assert_eq!(bridge.owner_id, "U111");
        assert_eq!(bridge.access.max_bot_replies, Some(3));
        let ids: Vec<_> = bridge.access.allow.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["U333", "B444"]);
        assert_eq!(cfg.owner_id(), "U111");
        assert_eq!(cfg.channel_id(), "", "the channel comes from the hello");

        let text = cfg.to_toml().expect("serializes");
        assert!(text.contains("[bridge]"));
        let again = fx.load(&text).expect("reloads");
        assert_eq!(again.messenger, Platform::Bridge);
        let again = again.bridge.expect("bridge section");
        assert_eq!(again.socket, bridge.socket);
        assert_eq!(again.owner_id, bridge.owner_id);
        assert_eq!(again.access, bridge.access);
    }

    #[test]
    fn bridge_active_without_section_is_messenger_missing() {
        let fx = Fixture::new();
        let text = format!("messenger = \"bridge\"\n{}", fx.valid_toml());
        assert_eq!(code(fx.load(&text)), E_MESSENGER_MISSING);
    }

    #[test]
    fn bridge_section_values_are_checked() {
        let fx = Fixture::new();
        let no_socket = format!(
            "messenger = \"bridge\"\nomp_path = {}\n[bridge]\nowner_id = \"U111\"\n",
            quoted(&fx.omp)
        );
        assert_eq!(code(fx.load(&no_socket)), E_CONFIG_VALUE);
        let base = bridge_toml(&fx, "/run/b.sock");
        let no_owner = base.replace("owner_id = \"U111\"\n", "");
        assert_eq!(code(fx.load(&no_owner)), E_OWNER_MISSING);
        for owner in ["", "U1 11"] {
            let text = base.replace("\"U111\"", &format!("\"{owner}\""));
            assert_eq!(code(fx.load(&text)), E_CONFIG_VALUE, "{owner:?}");
        }
        let allowlist = format!("access_mode = \"allowlist\"\n{base}");
        for entry in [
            "[[bridge.allow]]\nid = \"\"\nkind = \"user\"\n",
            "[[bridge.allow]]\nid = \"U111\"\nkind = \"user\"\n",
            "[[bridge.allow]]\nid = \"U333\"\n",
        ] {
            assert_eq!(
                code(fx.load(&format!("{allowlist}{entry}"))),
                E_CONFIG_VALUE,
                "{entry}"
            );
        }
        let owner_only = format!("{base}[[bridge.allow]]\nid = \"U333\"\nkind = \"user\"\n");
        assert_eq!(code(fx.load(&owner_only)), E_CONFIG_VALUE);
    }

    #[test]
    fn bridge_checks_the_socket_and_skips_the_channel() {
        let fx = Fixture::new();
        let socket = fx.dir.path().join("b.sock");
        let cfg = fx
            .load(&bridge_toml(&fx, socket.to_str().expect("utf-8")))
            .expect("loads");
        let checks = cfg.checks();
        assert!(check(&checks, "bridge socket").result.is_ok());
        assert!(!check(&checks, "bridge socket").advisory);
        assert!(checks.iter().all(|c| !c.name.ends_with("channel_id")));
        assert!(check(&checks, "hints").result.is_ok());
        cfg.validate().expect("a valid bridge config passes");

        // The one socket-path checker (`bridge::socket::check_path`) reports
        // `E_BRIDGE_SOCKET` for a relative path and for one too long for `sockaddr_un`.
        let relative = fx.load(&bridge_toml(&fx, "run/b.sock")).expect("loads");
        assert_eq!(
            code(check(&relative.checks(), "bridge socket").result.clone()),
            crate::bridge::E_BRIDGE_SOCKET
        );
        assert_eq!(code(relative.validate()), crate::bridge::E_BRIDGE_SOCKET);
        let long = format!("/{}/b.sock", "d".repeat(120));
        let long = fx.load(&bridge_toml(&fx, &long)).expect("loads");
        assert_eq!(
            code(check(&long.checks(), "bridge socket").result.clone()),
            crate::bridge::E_BRIDGE_SOCKET
        );
        assert_eq!(code(long.validate()), crate::bridge::E_BRIDGE_SOCKET);

        // A bridge section next to an active Discord is advisory only.
        let discord = format!(
            "{}[bridge]\nsocket = \"run/b.sock\"\nowner_id = \"U111\"\n",
            fx.valid_toml()
        );
        let cfg = fx.load(&discord).expect("loads");
        assert!(check(&cfg.checks(), "bridge socket").advisory);
        cfg.validate()
            .expect("the inactive bridge does not fail validation");
    }

    #[test]
    fn bridge_allowlist_is_reported() {
        let fx = Fixture::new();
        let socket = fx.dir.path().join("b.sock");
        let text = format!(
            "access_mode = \"allowlist\"\n{}[[bridge.allow]]\nid = \"B444\"\nkind = \"bot\"\n",
            bridge_toml(&fx, socket.to_str().expect("utf-8"))
        );
        let checks = fx.load(&text).expect("loads").checks();
        assert_eq!(
            code(check(&checks, "bridge.allow").result.clone()),
            W_ALLOWLIST
        );
        assert_eq!(
            code(check(&checks, "bridge.max_bot_replies").result.clone()),
            W_BOT_REPLIES
        );
    }

    #[test]
    fn tilde_needs_home() {
        let env = Env::default();
        assert_eq!(code(env.expand_tilde(Path::new("~/x"))), E_HOME_UNKNOWN);
        assert_eq!(
            env.expand_tilde(Path::new("/abs")).expect("ok"),
            PathBuf::from("/abs")
        );
    }
}
