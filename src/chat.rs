//! Chat role: messages from a listed `chat` sender are answered by their own omp child
//! that cannot act on this machine. It never reaches the lead, its session or its tools.
//!
//! One child per chat turn, closed (and reaped) when the turn ends, so no chat child is
//! left idle. Each conversation has its own omp session under
//! `<home>/chat/<conversation>/session`, never the lead's; turns of one conversation run
//! one at a time, different conversations in parallel, at most [`MAX_CHAT_CHILDREN`]
//! children at once (later turns wait for a slot). A redelivered chat message (same
//! message id among the last [`RECENT_CHAT_MESSAGES`]) is not run again. The child
//! starts with every omp switch that removes a way to act ([`chat_args`]): no built-in
//! tools, no extensions, skills, rules or context files, no LSP or PTY, in its own empty
//! folder `<home>/chat/cwd/<turn>` (made fresh for the turn and removed after it),
//! with the overlay `<home>/chat/omp-chat.yml` ([`CHAT_OVERLAY`]) that turns off every
//! config discovery source (so no MCP server, plugin, hook or skill), memory, lesson
//! capture, the advisor and omp's Auto-QA (its `report_issue` tool comes even with
//! `--no-tools`), and with no host tools (no `set_host_tools`). Before the
//! prompt the child's own tool list is read (`get_state`): any tool refuses the turn
//! ([`E_CHAT_TOOLS`]).
//!
//! Attachments from a chat sender are delivered with the prompt only: images as prompt
//! images, small text files inline, anything else by name. A file over its kind's limit
//! ([`MAX_IMAGE_BYTES`], [`MAX_INLINE_TEXT_BYTES`]) by its declared size is not fetched
//! and is named only; a download that streams past the limit anyway is stopped and
//! deleted, and the sender gets the file notice. The rest are fetched into
//! `<home>/chat/<conversation>/files/` and removed after the turn; nothing goes into the
//! owner's work folder.
//!
//! `shutdown_all` stops new chat turns and kills every chat child, like lanes.

use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use omp_rpc::{GetStateCommand, ImageContent, LitImage, PromptCommand};
use serde_json::Map;

use crate::config::Config;
use crate::omp::{
    self, E_OMP_SPAWN, GroupKill, OpenedChild, Outcome, child_command, key_slug, lock, private_dir,
    stderr_log_path,
};
use crate::store::Store;

/// The chat child listed tools of its own; the turn was not run.
pub const E_CHAT_TOOLS: &str = "E_CHAT_TOOLS";
/// The gateway is stopping; no chat turn starts.
pub const E_CHAT_STOPPING: &str = "E_CHAT_STOPPING";

/// An image attachment larger than this is named, not sent.
pub const MAX_IMAGE_BYTES: usize = 5 * 1024 * 1024;
/// A text attachment larger than this is named, not inlined.
pub const MAX_INLINE_TEXT_BYTES: usize = 64 * 1024;
/// Chat children running at once; later turns wait for a slot.
pub const MAX_CHAT_CHILDREN: usize = 2;
/// Chat message ids remembered to drop a redelivered message.
pub const RECENT_CHAT_MESSAGES: usize = 1024;

/// The settings overlay of every chat child (`--config`). It beats the user's global
/// omp config for this process only. `disabledProviders` lists every omp config
/// discovery source, so no MCP server, plugin, hook, skill or command is loaded: with
/// `--no-tools` alone omp lists a user-level MCP server's tools as top-level tools. The
/// list replaces the user's own `disabledProviders` for this child (README, access).
pub const CHAT_OVERLAY: &str = "\
# Written by moraebot for chat children. Edits are overwritten.
disabledProviders:
  - native
  - omp-plugins
  - claude
  - agent-plugins
  - codex
  - agents
  - claude-plugins
  - gemini
  - opencode
  - cursor
  - windsurf
  - cline
  - github
  - vscode
  - agents-md
  - claude-md
  - custom-context
  - mcp-json
  - ssh-json
  - ssh-config
mcp:
  enableProjectConfig: false
memory:
  backend: off
autolearn:
  enabled: false
advisor:
  enabled: false
dev:
  autoqa: false
";

/// Appended to the chat child's system prompt.
pub const CHAT_GUIDANCE: &str = "\
You are moraebot's chat assistant, talking with a guest of the owner (not the owner).
You have no tools and no access to the owner's machine, files, lanes or other conversations. Do not offer to run, read, write or send anything; answer from the conversation alone.
Attached files appear in the message: images as images, small text files inline, others by name only.
Other bots may be allowed to message you. Do not keep an exchange with another bot going for its own sake.";

/// `<home>/chat`.
pub fn chat_root(config: &Config) -> PathBuf {
    config.home.join("chat")
}

/// `<home>/chat/cwd`: holds one empty working folder per running chat turn.
pub fn chat_cwd(config: &Config) -> PathBuf {
    chat_root(config).join("cwd")
}

/// `<home>/chat/cwd/<turn>`: the working folder of one chat turn, made empty for it.
pub fn chat_turn_cwd(config: &Config, turn: u64) -> PathBuf {
    chat_cwd(config).join(turn.to_string())
}

/// `<home>/chat/omp-chat.yml`.
pub fn chat_overlay_path(config: &Config) -> PathBuf {
    chat_root(config).join("omp-chat.yml")
}

/// `<home>/chat/<conversation>/session`.
pub fn chat_session_dir(config: &Config, key: &str) -> PathBuf {
    chat_root(config).join(key_slug(key)).join("session")
}

/// `<home>/chat/<conversation>/files/<message>`: a chat message's attachments while its
/// turn runs.
pub fn chat_files_dir(config: &Config, key: &str, message_id: &str) -> PathBuf {
    chat_root(config)
        .join(key_slug(key))
        .join("files")
        .join(key_slug(message_id))
}

/// The store name of a conversation's chat session id.
pub fn chat_session_name(key: &str) -> String {
    format!("chat:{key}")
}

/// Exact omp argv (without the program) for a chat child. Unlike the lead (`omp_args`)
/// there is no `--approval-mode yolo`, and every switch that adds a way to act is off.
pub fn chat_args(config: &Config, key: &str, cwd: &Path) -> Vec<OsString> {
    locked_args(
        config,
        &chat_session_dir(config, key),
        cwd,
        &chat_overlay_path(config),
        CHAT_GUIDANCE,
        None,
    )
}

/// Exact omp argv (without the program) for a child that cannot act on this machine: the
/// chat child, the memory summarizer and the recall advisor. `overlay` must hold
/// [`CHAT_OVERLAY`]; `guidance` is appended to the system prompt; `model` becomes `--model=`
/// (`None`: omp's default model, as for every chat child).
pub fn locked_args(
    config: &Config,
    session_dir: &Path,
    cwd: &Path,
    overlay: &Path,
    guidance: &str,
    model: Option<&str>,
) -> Vec<OsString> {
    let mut args: Vec<OsString> = ["--mode", "rpc", "--no-ui", "--session-dir"]
        .into_iter()
        .map(OsString::from)
        .collect();
    args.push(session_dir.into());
    args.push("--cwd".into());
    args.push(cwd.into());
    args.push("--config".into());
    args.push(overlay.into());
    for flag in [
        "--no-tools",
        "--no-extensions",
        "--no-skills",
        "--no-rules",
        "--no-context-files",
        "--no-lsp",
        "--no-pty",
        "--no-title",
    ] {
        args.push(flag.into());
    }
    args.push(format!("--append-system-prompt={guidance}").into());
    if let Some(model) = model {
        args.push(format!("--model={model}").into());
    }
    if let Some(usd) = config.omp.max_cost_usd {
        args.push("--max-cost".into());
        args.push(usd.to_string().into());
    }
    args
}

/// One received attachment of a chat message.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatFile {
    pub name: String,
    pub mime: Option<String>,
    pub size: u64,
    /// `None`: over [`size_limit`], so never fetched or read; it is named only.
    pub bytes: Option<Vec<u8>>,
}

/// The largest attachment of this mime type that is read into the prompt.
pub fn size_limit(mime: Option<&str>) -> u64 {
    let limit = if mime.is_some_and(|m| m.starts_with("image/")) {
        MAX_IMAGE_BYTES
    } else {
        MAX_INLINE_TEXT_BYTES
    };
    limit as u64
}

/// The prompt of a chat turn: the text with a block naming every file (small text files
/// inline), and the image files as prompt images.
pub fn chat_prompt(text: &str, files: &[ChatFile]) -> (String, Vec<ImageContent>) {
    if files.is_empty() {
        return (text.to_owned(), Vec::new());
    }
    let mut message = text.to_owned();
    message.push_str("\n\n[Attached files]");
    let mut images = Vec::new();
    for file in files {
        let mime = file.mime.as_deref().unwrap_or("application/octet-stream");
        let head = format!("\n- {} ({mime}, {} bytes)", file.name, file.size);
        message.push_str(&head);
        let Some(bytes) = file.bytes.as_deref() else {
            message.push_str(": too large to show");
            continue;
        };
        if mime.starts_with("image/") {
            if bytes.len() <= MAX_IMAGE_BYTES {
                message.push_str(": attached as an image");
                images.push(ImageContent {
                    r#type: Some(LitImage),
                    data: Some(base64::engine::general_purpose::STANDARD.encode(bytes)),
                    mime_type: Some(mime.to_owned()),
                    detail: None,
                    url: None,
                    provider_file: None,
                    extra: Map::new(),
                });
            } else {
                message.push_str(": too large to show");
            }
            continue;
        }
        match std::str::from_utf8(bytes) {
            Ok(body) if bytes.len() <= MAX_INLINE_TEXT_BYTES => {
                message.push_str(":\n```\n");
                message.push_str(body);
                if !body.ends_with('\n') {
                    message.push('\n');
                }
                message.push_str("```");
            }
            Ok(_) => message.push_str(": too large to show"),
            Err(_) => message.push_str(": not text, not shown"),
        }
    }
    (message, images)
}

/// Running chat children by turn number; `stopping` refuses new ones.
#[derive(Default)]
struct ChatSet {
    stopping: bool,
    next: u64,
    children: HashMap<u64, Option<GroupKill>>,
}

pub struct ChatManager {
    config: Config,
    store: Arc<Mutex<Store>>,
    pub(crate) extra_env: Vec<(OsString, OsString)>,
    pub(crate) turn_timeout: Duration,
    set: Arc<Mutex<ChatSet>>,
    /// One lock per conversation: its turns run one at a time. An entry lives only while
    /// a turn of its conversation runs or waits.
    conversations: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// [`MAX_CHAT_CHILDREN`] permits; closed by [`Self::shutdown_all`].
    slots: tokio::sync::Semaphore,
    /// The last [`RECENT_CHAT_MESSAGES`] chat message ids, oldest first.
    recent: Mutex<(VecDeque<String>, HashSet<String>)>,
}

impl ChatManager {
    pub(crate) fn new(config: Config, store: Arc<Mutex<Store>>, turn_timeout: Duration) -> Self {
        // Turn numbers restart with the process: drop working folders a crash left behind.
        remove_files(&chat_cwd(&config));
        Self {
            config,
            store,
            extra_env: Vec::new(),
            turn_timeout,
            set: Arc::default(),
            conversations: Mutex::default(),
            slots: tokio::sync::Semaphore::new(MAX_CHAT_CHILDREN),
            recent: Mutex::default(),
        }
    }

    /// False when `message_id` was seen among the last [`RECENT_CHAT_MESSAGES`] chat
    /// messages (a redelivery); records it otherwise.
    pub fn first_delivery(&self, message_id: &str) -> bool {
        let mut recent = lock(&self.recent);
        let (order, seen) = &mut *recent;
        if !seen.insert(message_id.to_owned()) {
            return false;
        }
        order.push_back(message_id.to_owned());
        if order.len() > RECENT_CHAT_MESSAGES
            && let Some(oldest) = order.pop_front()
        {
            seen.remove(&oldest);
        }
        true
    }

    /// Forgets `message_id` again, so a redelivery of a message that was never run (its
    /// thread could not be opened) is run then.
    pub fn forget_delivery(&self, message_id: &str) {
        let mut recent = lock(&self.recent);
        let (order, seen) = &mut *recent;
        if seen.remove(message_id) {
            order.retain(|id| id != message_id);
        }
    }

    /// Conversations with a chat turn running or waiting.
    pub fn open_conversations(&self) -> usize {
        lock(&self.conversations).len()
    }

    /// Runs one chat turn for conversation `key` in a fresh tool-less child and closes
    /// it. Turns of the same conversation wait for each other, and every turn waits for
    /// one of the [`MAX_CHAT_CHILDREN`] slots.
    pub async fn reply(&self, key: &str, text: &str, files: &[ChatFile]) -> Outcome {
        let conversation = Arc::clone(lock(&self.conversations).entry(key.to_owned()).or_default());
        let turn = conversation.lock().await;
        let outcome = match self.slots.acquire().await {
            Ok(_slot) => self.run(key, text, files).await,
            Err(_) => failed(E_CHAT_STOPPING, "the gateway is stopping"),
        };
        drop(turn);
        drop(conversation);
        // Clones are taken under this map lock, so an entry held only by the map has no
        // turn running or waiting.
        lock(&self.conversations).retain(|_, lock| Arc::strong_count(lock) > 1);
        outcome
    }

    async fn run(&self, key: &str, text: &str, files: &[ChatFile]) -> Outcome {
        let turn = {
            let mut set = lock(&self.set);
            if set.stopping {
                return failed(E_CHAT_STOPPING, "the gateway is stopping");
            }
            set.next += 1;
            let turn = set.next;
            set.children.insert(turn, None);
            turn
        };
        let cwd = chat_turn_cwd(&self.config, turn);
        let outcome = match self.prepare(turn, key, &cwd) {
            Ok((command, previous_session)) => {
                let (message, images) = chat_prompt(text, files);
                self.spawn_turn(turn, key, command, previous_session, message, images)
                    .await
            }
            Err(outcome) => outcome,
        };
        // The worker reaped its child (or none was spawned): never signal that pid again.
        if let Some(Some(mut group)) = lock(&self.set).children.remove(&turn) {
            group.disarm();
        }
        if let Ok(entries) = std::fs::read_dir(&cwd)
            && entries.count() > 0
        {
            tracing::warn!(
                cwd = %cwd.display(),
                "chat child wrote into its working folder; removed with the turn"
            );
        }
        remove_files(&cwd);
        outcome
    }

    /// The child command and the conversation's previous session id. The turn's working
    /// folder `cwd` is made empty and the overlay is written again before every start,
    /// through a per-turn temp file renamed over it, so a child starting meanwhile never
    /// reads a partial overlay.
    fn prepare(
        &self,
        turn: u64,
        key: &str,
        cwd: &Path,
    ) -> Result<(std::process::Command, Option<String>), Outcome> {
        let spawn_failed = |e: std::io::Error| failed(E_OMP_SPAWN, e);
        remove_files(cwd);
        private_dir(cwd).map_err(spawn_failed)?;
        let overlay = chat_overlay_path(&self.config);
        let temp = overlay.with_extension(format!("yml.{turn}"));
        std::fs::write(&temp, CHAT_OVERLAY)
            .and_then(|()| std::fs::rename(&temp, &overlay))
            .map_err(|e| {
                let _ = std::fs::remove_file(&temp);
                spawn_failed(e)
            })?;
        private_dir(&chat_session_dir(&self.config, key)).map_err(spawn_failed)?;
        let previous = lock(&self.store)
            .session_id(&chat_session_name(key))
            .map_err(|e| failed(e.code(), e))?;
        let command = child_command(
            &self.config,
            chat_args(&self.config, key, cwd),
            &stderr_log_path(&self.config, &chat_session_name(key)),
            &self.extra_env,
        )
        .map_err(spawn_failed)?;
        Ok((command, previous))
    }

    async fn spawn_turn(
        &self,
        turn: u64,
        key: &str,
        command: std::process::Command,
        previous_session: Option<String>,
        message: String,
        images: Vec<ImageContent>,
    ) -> Outcome {
        let name = chat_session_name(key);
        let session_dir = chat_session_dir(&self.config, key)
            .to_string_lossy()
            .into_owned();
        let set = Arc::clone(&self.set);
        let timeout = self.turn_timeout;
        let worker = tokio::task::spawn_blocking(move || {
            let opened = omp::open_child(
                &name,
                command,
                Vec::new(),
                session_dir,
                previous_session,
                move |group| {
                    let mut set = lock(&set);
                    if set.stopping {
                        return false;
                    }
                    set.children.insert(turn, Some(group));
                    true
                },
                |_| {},
            );
            let OpenedChild {
                client, session_id, ..
            } = match opened {
                Ok(opened) => opened,
                Err(outcome) => return (outcome, None),
            };
            let outcome = match client.call(&GetStateCommand {}) {
                Ok(state) if state.dump_tools.is_empty() => {
                    let prompt = PromptCommand {
                        message,
                        images: (!images.is_empty()).then_some(images),
                        streaming_behavior: None,
                    };
                    omp::map_result(client.prompt_and_wait(&prompt, timeout))
                }
                Ok(state) => {
                    let names: Vec<String> =
                        state.dump_tools.into_iter().map(|tool| tool.name).collect();
                    tracing::error!(
                        code = E_CHAT_TOOLS,
                        tools = ?names,
                        "chat child lists tools; turn refused"
                    );
                    failed(
                        E_CHAT_TOOLS,
                        format!("the chat child lists tools: {names:?}"),
                    )
                }
                Err(e) => failed(E_OMP_SPAWN, e),
            };
            omp::close(&name, client);
            (outcome, Some(session_id))
        });
        let (outcome, session_id) = match worker.await {
            Ok(done) => done,
            Err(e) => (failed(E_OMP_SPAWN, e), None),
        };
        if let Some(session_id) = session_id
            && let Err(e) = lock(&self.store).set_session_id(&chat_session_name(key), &session_id)
        {
            tracing::warn!(code = e.code(), "could not store a chat session id");
        }
        outcome
    }

    /// Refuses new chat turns and kills every running chat child; each turn's worker
    /// then sees its child gone and returns.
    pub fn shutdown_all(&self) {
        self.slots.close();
        let mut set = lock(&self.set);
        set.stopping = true;
        for group in set.children.values().flatten() {
            group.kill();
        }
    }

    /// Where a chat message's attachments are saved while its turn runs.
    pub fn files_dir(&self, key: &str, message_id: &str) -> PathBuf {
        chat_files_dir(&self.config, key, message_id)
    }

    /// Chat children alive now (spawned and not yet reaped).
    pub fn running(&self) -> usize {
        lock(&self.set).children.values().flatten().count()
    }
}

fn failed(code: &'static str, detail: impl ToString) -> Outcome {
    Outcome::Failed {
        code,
        detail: detail.to_string(),
    }
}

/// One attachment of a chat message: its name, mime type, declared size, and where it
/// was saved (`None`: over its [`size_limit`], not fetched).
pub type SavedFile = (String, Option<String>, u64, Option<PathBuf>);

/// Reads the saved attachments of a chat message, then removes their folder. A file over
/// its [`size_limit`] on disk is not read.
pub fn take_files(dir: &Path, saved: Vec<SavedFile>) -> Vec<ChatFile> {
    let files = saved
        .into_iter()
        .filter_map(|(name, mime, size, path)| {
            let Some(path) = path else {
                return Some(ChatFile {
                    name,
                    mime,
                    size,
                    bytes: None,
                });
            };
            let read = std::fs::metadata(&path).and_then(|meta| {
                if meta.len() > size_limit(mime.as_deref()) {
                    Ok((meta.len(), None))
                } else {
                    std::fs::read(&path).map(|bytes| (bytes.len() as u64, Some(bytes)))
                }
            });
            match read {
                Ok((size, bytes)) => Some(ChatFile {
                    name,
                    mime,
                    size,
                    bytes,
                }),
                Err(e) => {
                    tracing::warn!(error = %e, "chat attachment not readable");
                    None
                }
            }
        })
        .collect();
    remove_files(dir);
    files
}

/// Removes a chat message's attachment folder; best effort.
pub fn remove_files(dir: &Path) {
    if let Err(e) = std::fs::remove_dir_all(dir)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(error = %e, "chat attachment folder not removed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, mime: Option<&str>, bytes: &[u8]) -> ChatFile {
        ChatFile {
            name: name.into(),
            mime: mime.map(str::to_owned),
            size: bytes.len() as u64,
            bytes: Some(bytes.to_vec()),
        }
    }

    #[test]
    fn prompt_without_files_is_the_text() {
        let (message, images) = chat_prompt("hi", &[]);
        assert_eq!(message, "hi");
        assert!(images.is_empty());
    }

    #[test]
    fn images_attach_text_inlines_others_are_named() {
        let files = [
            file("a.png", Some("image/png"), b"png"),
            file("b.txt", Some("text/plain"), b"hello"),
            file("c.bin", None, &[0xff, 0xfe]),
        ];
        let (message, images) = chat_prompt("look", &files);
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].data.as_deref(), Some("cG5n"));
        assert_eq!(images[0].mime_type.as_deref(), Some("image/png"));
        assert!(message.starts_with("look\n\n[Attached files]"));
        assert!(message.contains("- a.png (image/png, 3 bytes): attached as an image"));
        assert!(message.contains("- b.txt (text/plain, 5 bytes):\n```\nhello\n```"));
        assert!(message.contains("- c.bin (application/octet-stream, 2 bytes): not text"));
    }

    #[test]
    fn oversized_files_are_named_only() {
        let big_text = vec![b'a'; MAX_INLINE_TEXT_BYTES + 1];
        let big_image = vec![0u8; MAX_IMAGE_BYTES + 1];
        let (message, images) = chat_prompt(
            "x",
            &[
                file("t.txt", Some("text/plain"), &big_text),
                file("i.png", Some("image/png"), &big_image),
            ],
        );
        assert!(images.is_empty());
        assert_eq!(message.matches("too large to show").count(), 2);
    }

    #[test]
    fn unread_files_are_named_with_their_size() {
        let skipped = ChatFile {
            name: "big.txt".into(),
            mime: Some("text/plain".into()),
            size: 9_000_000,
            bytes: None,
        };
        let (message, images) = chat_prompt("x", &[skipped]);
        assert!(images.is_empty());
        assert!(message.contains("- big.txt (text/plain, 9000000 bytes): too large to show"));
    }

    #[test]
    fn take_files_does_not_read_a_file_over_its_limit_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let files_dir = dir.path().join("files");
        std::fs::create_dir(&files_dir).unwrap();
        let small = files_dir.join("small.txt");
        let big = files_dir.join("big.txt");
        std::fs::write(&small, b"hi").unwrap();
        std::fs::write(&big, vec![b'a'; MAX_INLINE_TEXT_BYTES + 1]).unwrap();
        let text = Some("text/plain".to_owned());
        let files = take_files(
            &files_dir,
            vec![
                ("small.txt".into(), text.clone(), 2, Some(small)),
                // Declared small, larger on disk: the disk size wins.
                ("big.txt".into(), text.clone(), 3, Some(big)),
                ("skipped.txt".into(), text, 9_000_000, None),
            ],
        );
        assert_eq!(files[0].bytes.as_deref(), Some(&b"hi"[..]));
        assert_eq!(files[1].bytes, None);
        assert_eq!(files[1].size, MAX_INLINE_TEXT_BYTES as u64 + 1);
        assert_eq!(files[2].bytes, None);
        assert_eq!(files[2].size, 9_000_000);
        assert!(!files_dir.exists(), "the folder is removed");
    }
}
