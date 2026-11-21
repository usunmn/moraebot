//! A locked one-shot omp caller: the recall advisor ([`crate::recall`]) uses it.
//!
//! It is omp locked down exactly like a chat child ([`chat::locked_args`],
//! [`chat::CHAT_OVERLAY`]): no tools, extensions, skills, rules, context files, LSP or PTY, no
//! config discovery source, memory, lesson capture or advisor, an empty working folder, and a
//! `get_state` check that refuses the call when the child lists any tool. Each call runs in a
//! fresh child with a fresh session folder under the caller's root (the empty working folder
//! is one shared `cwd` there, so calls share a cached prompt prefix), and the folder is removed
//! afterwards, so omp's session file does not keep the text on disk. The child gets the same
//! environment as every omp child (no messenger token) and runs on the owner's own model
//! provider.

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use omp_rpc::{GetStateCommand, PromptCommand};

use crate::chat::{CHAT_OVERLAY, locked_args};
use crate::config::Config;
use crate::omp::{self, GroupKill, OpenedChild, Outcome, child_command, lock, private_dir};

/// A locked omp caller (see the module doc).
pub struct Summarizer {
    config: Config,
    /// Holds one folder per call (`<pid>-<n>`) and the overlay.
    root: PathBuf,
    log_name: String,
    extra_env: Vec<(OsString, OsString)>,
    timeout: Duration,
    next: AtomicU64,
}

impl Summarizer {
    /// A locked caller with its call folders under `root`, its stderr in
    /// the log named `log_name` and `timeout` per call (the recall advisor is one). Folders a
    /// crash left behind are removed.
    pub fn locked(config: &Config, root: PathBuf, log_name: &str, timeout: Duration) -> Self {
        remove_dir(&root);
        Self {
            config: config.clone(),
            root,
            log_name: log_name.to_owned(),
            extra_env: Vec::new(),
            timeout,
            next: AtomicU64::new(0),
        }
    }

    /// Adds variables to the child environment (tests script the fake omp with them).
    pub fn with_extra_env<K, V>(mut self, vars: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<OsString>,
        V: Into<OsString>,
    {
        self.extra_env
            .extend(vars.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// One locked call with the fixed system prompt `guidance` and `message` on `model`
    /// (`None`: omp's default model), in a fresh child and session folder (removed afterwards).
    /// The recall advisor uses it with its own guidance. `Err` when the child could not run.
    pub async fn ask(
        &self,
        guidance: &'static str,
        message: String,
        model: Option<&str>,
    ) -> Result<String, String> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let run = self.root.join(format!("{}-{n}", std::process::id()));
        let result = self.run(&run, guidance, message, model).await;
        remove_dir(&run);
        result
    }

    async fn run(
        &self,
        run: &std::path::Path,
        guidance: &'static str,
        message: String,
        model: Option<&str>,
    ) -> Result<String, String> {
        // One shared empty cwd: omp names the cwd at the start of the first user turn, so a
        // per-call cwd would keep calls from sharing a cached prompt prefix.
        let (session, cwd) = (run.join("session"), self.root.join("cwd"));
        let overlay = self.root.join("omp-locked.yml");
        let prepared = private_dir(&session)
            .and_then(|()| private_dir(&cwd))
            .and_then(|()| {
                let temp = run.join("omp-locked.yml");
                std::fs::write(&temp, CHAT_OVERLAY)?;
                std::fs::rename(&temp, &overlay)
            })
            .and_then(|()| {
                child_command(
                    &self.config,
                    locked_args(&self.config, &session, &cwd, &overlay, guidance, model),
                    &omp::stderr_log_path(&self.config, &self.log_name),
                    &self.extra_env,
                )
            });
        let command = prepared.map_err(|e| format!("{}: {e}", omp::E_OMP_SPAWN))?;
        let session_dir = session.to_string_lossy().into_owned();
        let timeout = self.timeout;
        let group: Arc<Mutex<Option<GroupKill>>> = Arc::default();
        let guard = KillOnDrop(Arc::clone(&group));
        let log_name = self.log_name.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let opened = omp::open_child(
                &log_name,
                command,
                Vec::new(),
                session_dir,
                None,
                move |kill| {
                    *lock(&group) = Some(kill);
                    true
                },
                |_| {},
            );
            let OpenedChild { client, .. } = opened?;
            let outcome = match client.call(&GetStateCommand {}) {
                Ok(state) if state.dump_tools.is_empty() => {
                    let prompt = PromptCommand {
                        message,
                        images: None,
                        streaming_behavior: None,
                    };
                    omp::map_result(client.prompt_and_wait(&prompt, timeout))
                }
                Ok(state) => {
                    let names: Vec<String> =
                        state.dump_tools.into_iter().map(|tool| tool.name).collect();
                    Outcome::Failed {
                        code: crate::chat::E_CHAT_TOOLS,
                        detail: format!("the locked child lists tools: {names:?}"),
                    }
                }
                Err(e) => Outcome::Failed {
                    code: omp::E_OMP_SPAWN,
                    detail: e.to_string(),
                },
            };
            omp::close(&log_name, client);
            Ok(outcome)
        });
        let outcome = worker.await.map_err(|e| e.to_string())?;
        // The worker reaped its child: never signal that pid again.
        guard.disarm();
        match outcome {
            // An empty answer ran: it is checked (and refused) like any answer that does not
            // fit the notes, not taken for an outage.
            Ok(Outcome::Reply(reply)) => Ok(reply),
            Ok(Outcome::NoAgent) => Ok(String::new()),
            Ok(Outcome::Failed { code, detail } | Outcome::NeedsCheck { code, detail })
            | Err(Outcome::Failed { code, detail } | Outcome::NeedsCheck { code, detail }) => {
                Err(format!("{code}: {detail}"))
            }
            Err(Outcome::Reply(_) | Outcome::NoAgent) => Err("no child".to_owned()),
        }
    }
}

/// Kills the summarizer child's process group when the call is dropped before its worker
/// reaped the child.
struct KillOnDrop(Arc<Mutex<Option<GroupKill>>>);

impl KillOnDrop {
    fn disarm(self) {
        if let Some(mut group) = lock(&self.0).take() {
            group.disarm();
        }
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        // A still armed group kills itself when dropped.
        lock(&self.0).take();
    }
}

fn remove_dir(dir: &std::path::Path) {
    if let Err(e) = std::fs::remove_dir_all(dir)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(dir = %dir.display(), error = %e, "summarizer folder not removed");
    }
}
