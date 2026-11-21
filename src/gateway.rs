//! Library entry of the gateway: wires the store, the omp manager, a messenger adapter
//! and the dispatcher, and shuts them down in order.
//!
//! The adapter is injected, so tests run the whole gateway with `FakeAdapter` while
//! `moraebot run` passes the configured messenger's adapter.
//!
//! Shutdown order: stop taking events from the adapter, let the event being handled
//! finish its store write, drop the in-flight turn, stop the lanes, kill and reap the
//! lead and every lane child, then mark the interrupted rows and lanes `needs_check` so
//! nothing is re-prompted, and wait until the memory writer and the inbox released their
//! locks: a detached thread (an omp host tool call) may still hold the store for a moment
//! after its task was stopped.

use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinError;

use crate::adapter::{AdapterError, MessengerAdapter};
use crate::config::{Config, ConfigError, CurateMode, RecallMode};
use crate::curate::Curator;
use crate::dispatch::Dispatcher;
use crate::hints;
use crate::lead;
use crate::memory::{self, MemoryError, MemoryExit, MemoryHandle, MemoryStore, Redactor};
use crate::omp::{OmpManager, lock};
use crate::omp_overlay::{self, OverlayCheck};
use crate::onboarding;
use crate::recall::Advisor;
use crate::store::{Store, StoreError};

/// File name of the inbox database inside the home folder.
pub const STORE_FILE: &str = "inbox.sqlite";
/// The dispatcher task, or one of its background tasks, panicked, ended or was cancelled.
pub const E_GATEWAY_TASK: &str = "E_GATEWAY_TASK";
/// The process could not listen for termination signals.
pub const E_GATEWAY_SIGNAL: &str = "E_GATEWAY_SIGNAL";
/// The gateway did not finish shutting down within its time bound.
pub const E_SHUTDOWN_TIMEOUT: &str = "E_SHUTDOWN_TIMEOUT";

/// Capacity of the queue between the adapter and the dispatcher.
const EVENT_QUEUE: usize = 64;
/// How long shutdown waits for the memory worker and the inbox to release their locks.
const RELEASE_LIMIT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("{0}")]
    Store(#[from] StoreError),
    #[error("{0}")]
    Adapter(#[from] AdapterError),
    #[error("{0}")]
    Config(#[from] ConfigError),
    #[error("{0}")]
    Memory(#[from] MemoryError),
    #[error("{E_GATEWAY_TASK}: dispatcher task failed: {0}")]
    Task(JoinError),
    #[error("{E_GATEWAY_TASK}: the {0} task stopped")]
    TaskEnded(&'static str),
    #[error("{E_GATEWAY_SIGNAL}: cannot listen for signals: {0}")]
    Signal(std::io::Error),
    #[error("{E_SHUTDOWN_TIMEOUT}: the gateway did not stop in time")]
    ShutdownTimeout,
}

impl GatewayError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Store(err) => err.code(),
            Self::Adapter(err) => err.code,
            Self::Config(err) => err.code,
            Self::Memory(err) => err.code,
            Self::Task(_) | Self::TaskEnded(_) => E_GATEWAY_TASK,
            Self::Signal(_) => E_GATEWAY_SIGNAL,
            Self::ShutdownTimeout => E_SHUTDOWN_TIMEOUT,
        }
    }
}

pub struct Gateway {
    config: Config,
    adapter: Arc<dyn MessengerAdapter>,
    store: Arc<Mutex<Store>>,
    memory: MemoryHandle,
    memory_exit: MemoryExit,
    omp: OmpManager,
    /// What the start check found about omp (`None` in tests that skip the check).
    omp_check: Option<OverlayCheck>,
    /// The memory curator, run by the dispatcher; `None` with `memory.curate.mode = off`.
    curator: Option<Curator>,
}

impl Gateway {
    /// Opens the inbox and builds the omp manager with the lead guidance followed by the
    /// adapter's behavior hints (each replaced by its override file in the home,
    /// `E_HINTS_FILE` when unusable). A second gateway on the same home fails here with
    /// `E_ALREADY_RUNNING`. It also opens the memory store under `<home>/memory` (refusing an
    /// old 0.2.0 layout with `E_MEMORY_OLD_LAYOUT`) and starts its writer thread, which
    /// serves the owner's memory commands, and writes the omp overlay the lead and lanes run
    /// with (`E_OMP_OVERLAY` when it cannot); nothing else is started yet. The memory store
    /// and the recall advisor share one redactor, and so does the memory curator, built unless
    /// `memory.curate.mode = off`. Memory log and recall ledger rows older than
    /// 30 days are pruned here.
    pub fn open(config: &Config, adapter: Arc<dyn MessengerAdapter>) -> Result<Self, GatewayError> {
        omp_overlay::write_overlay(&config.home)?;
        let guidance = hints::load(&config.home, lead::GUIDANCE_NAME, lead::DEFAULT_GUIDANCE)?;
        let hints = hints::load(&config.home, adapter.platform(), adapter.behavior_hints())?;
        let mut inbox = Store::open(&config.home.join(STORE_FILE))?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        inbox.prune_memory(now)?;
        let store = Arc::new(Mutex::new(inbox));
        let redactor = Redactor::from_config(config);
        let (memory, memory_exit) = MemoryHandle::spawn_with_exit(MemoryStore::open(
            &config.home,
            redactor.clone(),
            memory::system_clock(),
        )?)?;
        // Leftover curator log rows settle here, whatever the curate mode.
        crate::curate::settle_pending(&store, memory.root());
        let advisor = (config.memory.recall_mode() != RecallMode::Off)
            .then(|| Arc::new(Advisor::new(config, Arc::clone(&store), redactor.clone())));
        let mut omp = OmpManager::new(config.clone(), Arc::clone(&store))
            .with_hints(lead::system_prompt(&guidance, &hints))
            .with_memory(memory.clone());
        if let Some(advisor) = advisor {
            omp = omp.with_recall(advisor);
        }
        let curator = (config.memory.curate.mode != CurateMode::Off)
            .then(|| Curator::new(config, Arc::clone(&store), memory.clone(), redactor.clone()));
        let tools =
            onboarding::host_tools(Arc::clone(&store), config.home.clone(), omp.turn_context());
        let omp = omp.with_host_tools(tools);
        Ok(Self {
            config: config.clone(),
            adapter,
            store,
            memory,
            memory_exit,
            omp,
            omp_check: None,
            curator,
        })
    }

    /// Hands over what the start check found about omp: a version warning goes to the owner
    /// once per version (`N_OMP_VERSION`).
    pub fn with_omp_check(mut self, check: &OverlayCheck) -> Self {
        self.omp_check = Some(check.clone());
        self
    }

    /// Adjusts the omp manager (tests add child environment variables or a turn limit).
    pub fn with_omp(mut self, adjust: impl FnOnce(OmpManager) -> OmpManager) -> Self {
        self.omp = adjust(self.omp);
        self
    }

    /// `Some(conversation key)` while a lead turn that accepts a steer runs. Take it
    /// before [`Self::run`] consumes the gateway.
    pub fn running_turn(&self) -> watch::Receiver<Option<String>> {
        self.omp.running_turn()
    }

    /// Serves until `shutdown` resolves (or the adapter stops), then shuts down cleanly; on a
    /// clean stop it first waits, `RELEASE_LIMIT` for both together, until the memory lock and
    /// the inbox are free.
    pub async fn run(self, shutdown: impl Future<Output = ()>) -> Result<(), GatewayError> {
        let Self {
            config,
            adapter,
            store,
            memory,
            memory_exit,
            omp,
            omp_check,
            curator,
        } = self;
        let mut inbound = adapter.start().await?;
        let mut dispatcher =
            Dispatcher::new(&config, adapter, Arc::clone(&store), omp).with_memory(memory);
        if let Some(check) = omp_check {
            dispatcher = dispatcher.with_omp_check(check);
        }
        if let Some(curator) = curator {
            dispatcher = dispatcher.with_curator(curator);
        }
        let dispatcher = Arc::new(dispatcher);
        let (events, queue) = mpsc::channel(EVENT_QUEUE);
        let mut dispatch = tokio::spawn(dispatcher.run(queue));
        tracing::info!("gateway started");

        tokio::pin!(shutdown);
        let ended_early = loop {
            tokio::select! {
                finished = &mut dispatch => break Some(finished),
                () = &mut shutdown => break None,
                event = inbound.recv() => match event {
                    Some(event) => {
                        if events.send(event).await.is_err() {
                            break None;
                        }
                    }
                    None => break None,
                },
            }
        };

        tracing::info!("gateway stopping");
        // Closing the queue ends the dispatcher: it finishes the event it is handling,
        // drops the in-flight turn and stops the lead and lane children.
        drop(events);
        drop(inbound);
        let finished = match ended_early {
            Some(finished) => finished,
            None => dispatch.await,
        };
        // A turn that was cut off is still `processing` and a stopped lane `running`:
        // make them `needs_check`.
        lock(&store).recover_on_start()?;
        // The dispatcher held the last memory handles, and the gateway's is the last store
        // handle it can drop itself; wait for the worker and any detached thread still holding
        // the store, so the home is free when `run` returns.
        let store_closed = lock(&store).closed();
        drop(store);
        let (memory_released, store_released) = tokio::task::spawn_blocking(move || {
            let deadline = std::time::Instant::now() + RELEASE_LIMIT;
            let memory = memory_exit.wait(RELEASE_LIMIT);
            let store =
                store_closed.wait(deadline.saturating_duration_since(std::time::Instant::now()));
            (memory, store)
        })
        .await
        .unwrap_or((false, false));
        if !memory_released {
            tracing::warn!("memory worker still running at shutdown");
        }
        if !store_released {
            tracing::warn!("inbox still open at shutdown");
        }
        finished.map_err(GatewayError::Task)??;
        tracing::info!("gateway stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AccessMode, DiscordConfig, OmpConfig, Platform};
    use crate::memory::{Bucket, NewEntry, Prov, Source};
    use crate::store::{RecallPhase, RecallRow, RecallState};
    use crate::testing::{Call, FakeAdapter};
    use std::time::Duration;

    const CHAN: &str = "discord:th:600";

    fn config(dir: &std::path::Path) -> Config {
        let home = dir.join("home");
        std::fs::create_dir_all(&home).expect("home");
        let mut memory = crate::config::MemoryConfig::default();
        memory.curate.mode = CurateMode::On;
        // Checked configs need at least a minute; zero makes the fresh lead idle enough.
        memory.curate.idle_minutes = 0;
        Config {
            messenger: Platform::Discord,
            work_dir: dir.to_path_buf(),
            omp_path: dir.join("omp"),
            home: home.clone(),
            access_mode: AccessMode::OwnerOnly,
            discord: Some(DiscordConfig {
                owner_id: 111,
                channel_id: 222,
                token_file: home.join("token"),
                access: Default::default(),
            }),
            bridge: None,
            omp: OmpConfig::default(),
            lead: Default::default(),
            memory,
        }
    }

    #[tokio::test]
    async fn a_due_curator_runs_once_and_tells_the_owner_about_core() {
        let dir = tempfile::tempdir().expect("tmp");
        let fake = Arc::new(FakeAdapter::new());
        let mut gateway = Gateway::open(&config(dir.path()), fake.clone()).expect("open");
        let id = gateway
            .memory
            .add(NewEntry {
                text: "리뷰는 한국어로 해 주세요".into(),
                prov: Prov::Owner,
                bucket: Bucket::Thread(CHAN.into()),
                src: Source::row(3),
                expires: None,
                why: None,
            })
            .await
            .expect("add")
            .id;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs() as i64;
        let delivered: Vec<RecallRow> = (1..=3)
            .map(|generation| RecallRow {
                conversation: CHAN.into(),
                entry_id: id.clone(),
                entry_hash: "h".into(),
                state: RecallState::Delivered,
                lead_generation: generation,
                lead_epoch: 0,
                row_id: None,
                at: now - 1000,
                phase: RecallPhase::Owner,
            })
            .collect();
        lock(&gateway.store)
            .record_recall(&delivered)
            .expect("ledger");
        let plan = format!(r#"{{"promote":["{id}"]}}"#);
        gateway.curator = gateway.curator.map(|c| c.with_answers(vec![Ok(plan)]));
        let store = Arc::clone(&gateway.store);
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn(gateway.run(async {
            stopped.await.ok();
        }));

        let notice = |call: &Call| matches!(call, Call::Send { text, .. } if text.contains(&id));
        tokio::time::timeout(Duration::from_secs(20), fake.wait_for_call(notice))
            .await
            .expect("core notice");
        let sent: Vec<String> = fake
            .calls()
            .into_iter()
            .filter_map(|call| match call {
                Call::Send { text, .. } if text.contains(&id) => Some(text),
                _ => None,
            })
            .collect();
        assert_eq!(
            sent,
            vec![format!(
                "기억 정리로 핵심 기억이 바뀌었어요: 핵심으로 올림 {id}. 되돌리려면 `{id} 지워`"
            )]
        );
        assert!(fake.calls().contains(&Call::OpenDm {
            user_id: "111".into()
        }));
        let log = lock(&store).recent_curate_log(10).expect("log");
        assert_eq!(log.len(), 1, "{log:?}");
        assert_eq!(
            (log[0].op.as_str(), log[0].entry_id.as_str()),
            ("promote", id.as_str())
        );
        drop(store);
        stop.send(()).expect("stop");
        run.await.expect("join").expect("clean stop");
    }

    #[tokio::test]
    async fn leftover_curator_log_rows_settle_at_start_even_with_curation_off() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut config = config(dir.path());
        config.memory.curate.mode = CurateMode::Off;
        {
            let mut store = Store::open(&config.home.join(STORE_FILE)).expect("store");
            store
                .log_curate_pending(&[crate::store::CurateLogRow {
                    run_id: 1,
                    at: 1,
                    op: "save".into(),
                    entry_id: "r7.s0".into(),
                    other_id: None,
                    bucket: format!("thread:{CHAN}"),
                    detail: None,
                    shadow: false,
                }])
                .expect("pending");
        }
        let gateway = Gateway::open(&config, Arc::new(FakeAdapter::new())).expect("open");
        assert!(gateway.curator.is_none());
        let store = lock(&gateway.store);
        assert!(store.pending_curate_log().expect("pending").is_empty());
        assert!(store.recent_curate_log(5).expect("log").is_empty());
    }
}
