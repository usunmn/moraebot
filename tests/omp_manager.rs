use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moraebot::access::Sender;
use moraebot::config::{AccessMode, Config, DiscordConfig, OmpConfig, Platform, Role};
use moraebot::files;
use moraebot::lead;
use moraebot::omp::{self, GroupReport, OmpManager, Outcome};
use moraebot::onboarding;
use moraebot::persona;
use moraebot::store::{Admitted, OnboardingRow, OnboardingState, Store};
use moraebot::testing::appended;
use serde_json::Value;

const KEY_A: &str = "discord:th:1300000000000000001";
const KEY_B: &str = "discord:th:1300000000000000002";
const BOUND: Duration = Duration::from_secs(30);

struct Harness {
    dir: tempfile::TempDir,
    config: Config,
    store: Arc<Mutex<Store>>,
}

impl Harness {
    fn new() -> Self {
        Self::with_omp(OmpConfig::default())
    }

    fn with_omp(omp: OmpConfig) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let home = dir.path().join("home");
        let work_dir = dir.path().join("work");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&work_dir).expect("work dir");
        let config = Config {
            messenger: Platform::Discord,
            work_dir,
            omp_path: PathBuf::from(env!("CARGO_BIN_EXE_fake-omp")),
            home: home.clone(),
            access_mode: AccessMode::OwnerOnly,
            discord: Some(DiscordConfig {
                owner_id: 111,
                channel_id: 222,
                token_file: home.join("secrets").join("discord.token"),
                access: Default::default(),
            }),
            bridge: None,
            omp,
            lead: Default::default(),
            memory: moraebot::config::MemoryConfig::default(),
        };
        let mut store = Store::open(&home.join("inbox.sqlite")).expect("store");
        for (n, key) in [KEY_A, KEY_B].into_iter().enumerate() {
            store
                .admit("discord", &format!("m{n}"), key, "222", "111", "seed")
                .expect("admit seed row");
        }
        Self {
            dir,
            config,
            store: Arc::new(Mutex::new(store)),
        }
    }

    fn log(&self) -> PathBuf {
        self.dir.path().join("fake-omp.log")
    }

    fn manager(&self, mode: &str) -> OmpManager {
        OmpManager::new(self.config.clone(), Arc::clone(&self.store)).with_extra_env([
            ("FAKE_OMP_MODE", mode.to_owned()),
            ("FAKE_OMP_LOG", self.log().to_string_lossy().into_owned()),
        ])
    }

    fn log_lines(&self) -> Vec<String> {
        std::fs::read_to_string(self.log())
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn start_pids(&self) -> Vec<u32> {
        self.log_lines()
            .iter()
            .filter_map(|l| l.strip_prefix("START pid="))
            .map(|rest| rest.split_whitespace().next().unwrap().parse().unwrap())
            .collect()
    }

    fn frames(&self, kind: &str) -> Vec<Value> {
        self.log_lines()
            .iter()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|f| f["type"] == kind)
            .collect()
    }

    fn set_session(&self, session_id: &str) {
        self.store
            .lock()
            .unwrap()
            .set_session_id(lead::LEAD_SESSION, session_id)
            .unwrap();
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(BOUND, future)
        .await
        .expect("operation finished within bound")
}

fn alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .expect("run kill -0")
        .success()
}

fn expected_argv(config: &Config) -> Vec<String> {
    let session = omp::lead_session_dir(config).to_string_lossy().into_owned();
    let work = config.work_dir.to_string_lossy().into_owned();
    let overlay = moraebot::omp_overlay::overlay_path(&config.home)
        .to_string_lossy()
        .into_owned();
    [
        "--mode",
        "rpc",
        "--no-ui",
        "--session-dir",
        &session,
        "--cwd",
        &work,
        "--config",
        &overlay,
        "--approval-mode",
        "yolo",
    ]
    .map(str::to_owned)
    .to_vec()
}

fn start_argv(line: &str) -> Vec<String> {
    let json = line.split_once("argv=").expect("argv in START line").1;
    serde_json::from_str(json).expect("argv json")
}

#[tokio::test]
async fn argv_is_exact_and_env_drops_only_discord_names() {
    let h = Harness::new();
    let manager = h.manager("ok").with_extra_env([
        ("MORAEBOT_DISCORD_PROBE", "x"),
        ("PROVIDER_PROBE_API_KEY", "y"),
    ]);
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    assert_eq!(outcome, Outcome::Reply("echo: hi".into()));

    let lines = h.log_lines();
    let start = lines
        .iter()
        .find(|l| l.starts_with("START pid="))
        .expect("START line");
    assert_eq!(start_argv(start), expected_argv(&h.config));
    let env = lines
        .iter()
        .find(|l| l.starts_with("ENV "))
        .expect("ENV line");
    let names: Vec<&str> = env["ENV ".len()..].split_whitespace().collect();
    assert!(
        names
            .iter()
            .all(|n| !n.to_ascii_uppercase().contains("DISCORD")),
        "{names:?}"
    );
    assert!(names.contains(&"PROVIDER_PROBE_API_KEY"));
    for kept in ["PATH", "HOME"] {
        if std::env::var_os(kept).is_some() {
            assert!(names.contains(&kept), "{kept} must be kept");
        }
    }

    let stderr_log = omp::stderr_log_path(&h.config, lead::LEAD_SESSION);
    let mode = std::fs::metadata(&stderr_log)
        .expect("stderr log exists")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
    assert_eq!(
        h.store
            .lock()
            .unwrap()
            .session_id(lead::LEAD_SESSION)
            .unwrap()
            .as_deref(),
        Some("fake-session-1")
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn max_cost_adds_flag_only_when_set() {
    let h = Harness::with_omp(OmpConfig {
        turn_timeout_minutes: None,
        max_cost_usd: Some(2.5),
        stall_after_minutes: None,
    });
    let manager = h.manager("ok");
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    let start = h
        .log_lines()
        .into_iter()
        .find(|l| l.starts_with("START pid="))
        .unwrap();
    let mut expected = expected_argv(&h.config);
    expected.extend(["--max-cost".to_owned(), "2.5".to_owned()]);
    assert_eq!(start_argv(&start), expected);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn flag_present_when_hints_set() {
    let h = Harness::with_omp(OmpConfig {
        turn_timeout_minutes: None,
        max_cost_usd: Some(2.5),
        stall_after_minutes: None,
    });
    let manager = h.manager("ok").with_hints("probe hints".into());
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    let start = h
        .log_lines()
        .into_iter()
        .find(|l| l.starts_with("START pid="))
        .unwrap();
    let mut expected = expected_argv(&h.config);
    let flag = format!(
        "--append-system-prompt={}",
        omp::lead_append_path(&h.config).display()
    );
    expected.extend([flag, "--max-cost".to_owned(), "2.5".to_owned()]);
    assert_eq!(start_argv(&start), expected);
    assert_eq!(appended(&start), "probe hints");
    manager.shutdown_all().await;
}

/// The lead's hints, persona and core memory reach omp as a private file: argv names the file
/// and holds none of the text, the file is the owner's only (`0600`, folder `0700`), and omp
/// read exactly the appended prompt from it at start.
#[tokio::test]
async fn lead_prompt_is_passed_as_a_private_file() {
    let h = Harness::new();
    save_sora(&h);
    let mut store = qa_memory_store(&h);
    store
        .add(qa_note(
            "Answer in Korean always",
            moraebot::memory::Bucket::Owner,
        ))
        .expect("core rule");
    drop(store);
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let manager = h
        .manager("ok")
        .with_hints("probe hints".into())
        .with_memory_clock(Arc::new(move || today));
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    let start = h
        .log_lines()
        .into_iter()
        .find(|l| l.starts_with("START pid="))
        .unwrap();
    bounded(manager.shutdown_all()).await;

    let path = omp::lead_append_path(&h.config);
    assert!(path.is_absolute());
    let argv = start_argv(&start);
    let flags: Vec<&String> = argv
        .iter()
        .filter(|a| a.starts_with("--append-system-prompt"))
        .collect();
    assert_eq!(
        flags,
        [&format!("--append-system-prompt={}", path.display())]
    );
    for secret in [
        "probe hints",
        "Name: Sora",
        "Answer in Korean",
        omp::CORE_MEMORY_OPEN,
    ] {
        assert!(
            !start.split_once(" argv=").unwrap().1.contains(secret),
            "{secret} in argv"
        );
    }
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&path), 0o600);
    assert_eq!(mode(path.parent().unwrap()), 0o700);
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.starts_with("probe hints"), "{text}");
    assert!(text.contains("Name: Sora"), "{text}");
    let core = text
        .split_once(omp::CORE_MEMORY_OPEN)
        .expect("core memory block")
        .1;
    assert!(core.contains("Answer in Korean always"), "{core}");
    assert_eq!(appended(&start), text, "omp read the file at start");
}

#[tokio::test]
async fn no_flag_when_hints_empty() {
    let h = Harness::new();
    let manager = h.manager("ok").with_hints(String::new());
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    let start = h
        .log_lines()
        .into_iter()
        .find(|l| l.starts_with("START pid="))
        .unwrap();
    assert!(
        start_argv(&start)
            .iter()
            .all(|a| !a.starts_with("--append-system-prompt"))
    );
    manager.shutdown_all().await;
}

/// Per `START` line in log order, the appended system prompt that child read at start (empty
/// when it had none).
fn appends(h: &Harness) -> Vec<String> {
    h.log_lines()
        .iter()
        .filter(|l| l.starts_with("START pid="))
        .map(|l| appended(l))
        .collect()
}

fn save_persona(h: &Harness, name: &str, tone: &str, owner: &str) {
    let text = persona::render(name, tone, owner).unwrap();
    persona::save(&h.config.home, &text).unwrap();
}

#[tokio::test]
async fn lead_appends_persona_after_hints() {
    let h = Harness::new();
    save_persona(
        &h,
        "Sora",
        "short and warm",
        "Owner 111 likes short replies",
    );
    let manager = h.manager("ok").with_hints("H".into());
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    let loaded = persona::load(&h.config.home).unwrap();
    let expected = persona::append("H", loaded.as_deref());
    let appends = appends(&h);
    assert_eq!(appends, vec![expected]);
    assert!(appends[0].contains("Name: Sora"));
    bounded(manager.shutdown_all()).await;
}

fn save_sora(h: &Harness) {
    save_persona(h, "Sora", "short and warm", "Owner 111 likes short replies");
}

#[tokio::test]
async fn persona_change_restarts_the_lead_before_the_next_turn() {
    let h = Harness::new();
    save_sora(&h);
    // `lead.cache_minutes = 0`: any change restarts the lead before the next turn.
    let m = h.manager("ok").with_cache_window(None);
    let r1 = bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    save_persona(&h, "Kai", "", "");
    let r2 = bounded(m.prompt(KEY_B, 2, Sender::Owner, "again"))
        .await
        .main
        .outcome;
    let pids = h.start_pids();
    let first_alive = pids.first().map(|p| alive(*p));
    let leases = h.frames("get_session_lease");
    let rest: Vec<Vec<String>> = h
        .log_lines()
        .iter()
        .filter(|l| l.starts_with("START pid="))
        .map(|l| {
            start_argv(l)
                .into_iter()
                .filter(|a| !a.starts_with("--append-system-prompt="))
                .collect()
        })
        .collect();
    let appends = appends(&h);
    bounded(m.shutdown_all()).await;

    assert_eq!(r1, Outcome::Reply("echo: hi".into()));
    assert_eq!(r2, Outcome::Reply("echo: again".into()));
    assert_eq!(pids.len(), 2);
    assert_eq!(first_alive, Some(false));
    assert_eq!(leases.len(), 1);
    assert_eq!(
        leases[0]["sessionIds"],
        serde_json::json!(["fake-session-1"])
    );
    assert_eq!(rest.len(), 2);
    for argv in &rest {
        assert_eq!(argv, &expected_argv(&h.config));
    }
    assert_eq!(appends.len(), 2);
    let first = &appends[0];
    let second = &appends[1];
    assert!(first.contains("Name: Sora") && !first.contains("Name: Kai"));
    assert!(second.contains("Name: Kai") && !second.contains("Name: Sora"));
}

#[tokio::test]
async fn unchanged_persona_keeps_the_lead() {
    let h = Harness::new();
    save_sora(&h);
    let m = h.manager("ok");
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    save_sora(&h);
    bounded(m.prompt(KEY_B, 2, Sender::Owner, "again")).await;
    bounded(m.prompt(KEY_A, 3, Sender::Owner, "more")).await;
    let pids = h.start_pids();
    bounded(m.shutdown_all()).await;

    assert_eq!(pids.len(), 1);
}

#[tokio::test]
async fn removed_persona_restarts_without_it() {
    let h = Harness::new();
    save_sora(&h);
    let first = persona::append("H", persona::load(&h.config.home).unwrap().as_deref());
    let m = h
        .manager("ok")
        .with_hints("H".into())
        .with_cache_window(None);
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    std::fs::remove_file(persona::path(&h.config.home)).unwrap();
    bounded(m.prompt(KEY_B, 2, Sender::Owner, "again")).await;
    let appends = appends(&h);
    bounded(m.shutdown_all()).await;

    assert_eq!(appends, vec![first, "H".to_owned()]);
}

#[tokio::test]
async fn refused_persona_change_restarts_without_it() {
    let h = Harness::new();
    save_sora(&h);
    let m = h
        .manager("ok")
        .with_hints("H".into())
        .with_cache_window(None);
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    let secret = ["gl", "pat-"].concat() + &"c".repeat(20);
    std::fs::write(
        persona::path(&h.config.home),
        format!("Name: X\nOwner: {secret}"),
    )
    .unwrap();
    bounded(m.prompt(KEY_B, 2, Sender::Owner, "again")).await;
    bounded(m.prompt(KEY_A, 3, Sender::Owner, "more")).await;
    let appends = appends(&h);
    let code = persona::load(&h.config.home).unwrap_err().code;
    bounded(m.shutdown_all()).await;

    assert_eq!(appends.len(), 2);
    assert!(appends[0].contains("Name: Sora"));
    assert_eq!(appends[1], "H");
    assert_eq!(code, persona::E_PERSONA_SECRET);
}

#[tokio::test]
async fn onboarding_finish_applies_from_the_next_turn() {
    let h = Harness::new();
    h.store.lock().unwrap().start_onboarding().unwrap();
    let r1 = owner_row(&h, "m-on-1", "run-tools");
    let m = onboarding_manager(&h, &[finish_done_call()]).with_cache_window(None);
    bounded(m.prompt(
        KEY_A,
        r1,
        Sender::Owner,
        &lead::owner_input(KEY_A, r1, "run-tools"),
    ))
    .await;
    let after_first = h.start_pids().len();
    let r2 = owner_row(&h, "m-on-2", "again");
    bounded(m.prompt(
        KEY_A,
        r2,
        Sender::Owner,
        &lead::owner_input(KEY_A, r2, "again"),
    ))
    .await;
    let pids = h.start_pids();
    let appends = appends(&h);
    let tools = h.frames("set_host_tools");
    bounded(m.shutdown_all()).await;

    assert_eq!(after_first, 1);
    assert_eq!(pids.len(), 2);
    assert!(!appends[0].contains("Name: Sora"));
    assert!(appends[1].contains("Name: Sora"));
    assert_eq!(tools.len(), 2);
    for frame in &tools {
        let names: Vec<&str> = frame["tools"]
            .as_array()
            .expect("tool list")
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        assert!(names.contains(&onboarding::FINISH_TOOL), "{names:?}");
    }
}

#[tokio::test]
async fn unsettled_session_defers_the_restart_until_settled() {
    let mut h = Harness::new();
    h.config.omp_path = unsettled_once_wrapper(h.dir.path());
    save_sora(&h);
    let m = h.manager("ok").with_cache_window(None);
    let r1 = bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    save_persona(&h, "Kai", "", "");
    let r2 = bounded(m.prompt(KEY_B, 2, Sender::Owner, "again"))
        .await
        .main
        .outcome;
    let after_second = h.start_pids().len();
    let r3 = bounded(m.prompt(KEY_A, 3, Sender::Owner, "more"))
        .await
        .main
        .outcome;
    let pids = h.start_pids();
    let appends = appends(&h);
    bounded(m.shutdown_all()).await;

    assert_eq!(r1, Outcome::Reply("echo: hi".into()));
    // Not settled: no restart, so the change goes in as an update block.
    let Outcome::Reply(r2) = r2 else {
        panic!("{r2:?}")
    };
    assert!(
        r2.starts_with("echo: again") && r2.contains(moraebot::pin::UPDATE_OPEN),
        "{r2}"
    );
    // The restarted child of the session held the update: it hears the new prompt holds.
    assert_eq!(
        r3,
        Outcome::Reply(format!("echo: more\n\n{}", moraebot::pin::revert_notice()))
    );
    assert_eq!(after_second, 1);
    assert_eq!(pids.len(), 2);
    assert_eq!(appends.len(), 2);
    let first = &appends[0];
    let second = &appends[1];
    assert!(first.contains("Name: Sora") && !first.contains("Name: Kai"));
    assert!(second.contains("Name: Kai") && !second.contains("Name: Sora"));
}

#[tokio::test]
async fn failed_restart_reports_the_start_failure_then_starts_again() {
    let mut h = Harness::new();
    let marker = h.dir.path().join("refuse-start");
    h.config.omp_path = refusing_wrapper(h.dir.path(), &marker);
    save_sora(&h);
    let m = h.manager("ok").with_cache_window(None);
    let r1 = bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    std::fs::write(&marker, "").unwrap();
    save_persona(&h, "Kai", "", "");
    let r2 = bounded(m.prompt(KEY_B, 2, Sender::Owner, "again"))
        .await
        .main
        .outcome;
    let after_fail = h.start_pids();
    let first_alive = after_fail.first().map(|p| alive(*p));
    let child_after_fail = m.child_pid().await;
    std::fs::remove_file(&marker).unwrap();
    let r3 = bounded(m.prompt(KEY_A, 3, Sender::Owner, "more"))
        .await
        .main
        .outcome;
    let pids = h.start_pids();
    let appends = appends(&h);
    bounded(m.shutdown_all()).await;

    assert_eq!(r1, Outcome::Reply("echo: hi".into()));
    assert!(matches!(r2, Outcome::Failed { .. }), "{r2:?}");
    assert_eq!(r2.code(), Some(omp::E_OMP_SPAWN));
    assert_eq!(after_fail.len(), 1);
    assert_eq!(first_alive, Some(false));
    assert_eq!(child_after_fail, None);
    assert_eq!(r3, Outcome::Reply("echo: more".into()));
    assert_eq!(pids.len(), 2);
    assert_eq!(appends.len(), 2);
    assert!(appends[1].contains("Name: Kai"));
}

#[tokio::test]
async fn refused_persona_is_left_out() {
    let h = Harness::new();
    let secret = ["gl", "pat-"].concat() + &"c".repeat(20);
    std::fs::write(
        persona::path(&h.config.home),
        format!("Name: X\nOwner: {secret}"),
    )
    .unwrap();
    let manager = h.manager("ok").with_hints("H".into());
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert_eq!(appends(&h), vec!["H".to_owned()]);
    assert_eq!(
        persona::load(&h.config.home).unwrap_err().code,
        persona::E_PERSONA_SECRET
    );
    bounded(manager.shutdown_all()).await;
}

#[tokio::test]
async fn injection_payload_arrives_byte_for_byte_with_hints_set() {
    let h = Harness::new();
    let manager = h.manager("ok").with_hints("probe hints".into());
    let payload = "$(touch /tmp/moraebot-pwned-7f3a); touch /tmp/moraebot-pwned-7f3a-b";
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, payload))
        .await
        .main
        .outcome;
    assert_eq!(outcome, Outcome::Reply(format!("echo: {payload}")));
    let prompts = h.frames("prompt");
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0]["message"], payload);
    assert!(!Path::new("/tmp/moraebot-pwned-7f3a").exists());
    assert!(!Path::new("/tmp/moraebot-pwned-7f3a-b").exists());
    manager.shutdown_all().await;
}

#[tokio::test]
async fn multibyte_text_round_trips() {
    let h = Harness::new();
    let manager = h.manager("ok");
    let text = "안녕하세요, 모래봇 🌊";
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, text))
        .await
        .main
        .outcome;
    assert_eq!(outcome, Outcome::Reply(format!("echo: {text}")));
    manager.shutdown_all().await;
}

#[tokio::test]
async fn injection_payload_arrives_byte_for_byte_and_never_runs() {
    let h = Harness::new();
    let manager = h.manager("ok");
    let payload = "$(touch /tmp/moraebot-pwned-7f3a); touch /tmp/moraebot-pwned-7f3a-b";
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, payload))
        .await
        .main
        .outcome;
    assert_eq!(outcome, Outcome::Reply(format!("echo: {payload}")));
    let prompts = h.frames("prompt");
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0]["message"], payload);
    let start = h
        .log_lines()
        .into_iter()
        .find(|l| l.starts_with("START pid="))
        .unwrap();
    assert!(
        start_argv(&start)
            .iter()
            .all(|a| !a.contains("moraebot-pwned"))
    );
    assert!(!Path::new("/tmp/moraebot-pwned-7f3a").exists());
    assert!(!Path::new("/tmp/moraebot-pwned-7f3a-b").exists());
    manager.shutdown_all().await;
}

#[tokio::test]
async fn not_invoked_is_no_agent() {
    let h = Harness::new();
    let manager = h.manager("not_invoked");
    assert_eq!(
        bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
            .await
            .main
            .outcome,
        Outcome::NoAgent
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn error_status_is_failed() {
    let h = Harness::new();
    let manager = h.manager("error");
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    assert!(matches!(outcome, Outcome::Failed { .. }));
    assert_eq!(outcome.code(), Some(omp::E_PROMPT_ERROR));
    manager.shutdown_all().await;
}

/// Shell stand-in for omp whose error turn carries `error.code: "provider_error"`: it runs
/// fake-omp (mode `error`) and adds the code to the `prompt_result` frame on its way out.
/// Written into the test's temp dir.
fn coded_error_wrapper(root: &Path) -> PathBuf {
    let script = root.join("omp-coded-error.sh");
    let fake = env!("CARGO_BIN_EXE_fake-omp");
    let body = format!(
        r#"#!/bin/sh
'{fake}' "$@" | awk '{{ sub(/"message":"fake provider failure"/, "\"code\":\"provider_error\",\"message\":\"fake provider failure\""); print; fflush() }}'
"#
    );
    std::fs::write(&script, body).expect("write wrapper");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    script
}

/// Each child's first `prompt_result` reports `sessionSettled: false`; later ones are unchanged.
fn unsettled_once_wrapper(root: &Path) -> PathBuf {
    let script = root.join("omp-unsettled-once.sh");
    let fake = env!("CARGO_BIN_EXE_fake-omp");
    let body = format!(
        r#"#!/bin/sh
'{fake}' "$@" | awk '{{ if (!n && sub(/"sessionSettled":true/, "\"sessionSettled\":false")) n = 1; print; fflush() }}'
"#
    );
    std::fs::write(&script, body).expect("write wrapper");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    script
}

/// Exits before omp's `ready` frame while `marker` exists, so the child start fails.
fn refusing_wrapper(root: &Path, marker: &Path) -> PathBuf {
    let script = root.join("omp-refusing.sh");
    let fake = env!("CARGO_BIN_EXE_fake-omp");
    let marker = marker.display();
    let body = format!(
        r#"#!/bin/sh
if [ -e '{marker}' ]; then exit 3; fi
exec '{fake}' "$@"
"#
    );
    std::fs::write(&script, body).expect("write wrapper");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    script
}

#[tokio::test]
async fn coded_non_cost_cap_error_is_prompt_error() {
    let mut h = Harness::new();
    h.config.omp_path = coded_error_wrapper(h.dir.path());
    let manager = h.manager("error");
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    assert!(matches!(outcome, Outcome::Failed { .. }), "{outcome:?}");
    assert_eq!(outcome.code(), Some(omp::E_PROMPT_ERROR));
    assert!(
        manager.child_pid().await.is_some(),
        "a finished turn keeps its child"
    );
    assert_eq!(h.frames("prompt").len(), 1);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn hang_with_turn_timeout_is_needs_check_and_child_is_reaped() {
    let h = Harness::new();
    let manager = h.manager("hang").with_turn_timeout(Duration::from_secs(1));
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    assert!(matches!(outcome, Outcome::NeedsCheck { .. }));
    assert_eq!(outcome.code(), Some(omp::E_TURN_TIMEOUT));
    let pids = h.start_pids();
    assert_eq!(pids.len(), 1);
    assert!(!alive(pids[0]), "timed-out child must be gone");
    assert_eq!(manager.child_pid().await, None);
    assert_eq!(h.frames("prompt").len(), 1);
}

#[tokio::test]
async fn open_session_failure_is_failed_without_prompt() {
    let h = Harness::new();
    let manager = h.manager("open_session_fail");
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    assert!(matches!(outcome, Outcome::Failed { .. }));
    assert_eq!(outcome.code(), Some(omp::E_OPEN_SESSION));
    assert!(h.frames("prompt").is_empty());
    assert_eq!(manager.child_pid().await, None);
    assert!(!alive(h.start_pids()[0]));
}

#[tokio::test]
async fn held_lease_is_needs_check_without_prompt() {
    let h = Harness::new();
    h.set_session("fake-session-1");
    let manager = h.manager("lease_held");
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    assert!(matches!(outcome, Outcome::NeedsCheck { .. }));
    assert_eq!(outcome.code(), Some(omp::E_SESSION_BUSY));
    let leases = h.frames("get_session_lease");
    assert_eq!(leases.len(), 1);
    assert_eq!(
        leases[0]["sessionIds"],
        serde_json::json!(["fake-session-1"])
    );
    assert!(h.frames("open_session").is_empty());
    assert!(h.frames("prompt").is_empty());
    assert!(!alive(h.start_pids()[0]));
}

#[tokio::test]
async fn reopen_checks_free_lease_then_prompts() {
    let h = Harness::new();
    h.set_session("fake-session-1");
    let manager = h.manager("ok");
    // A resumed session may hold an earlier memory update: the input says the prompt holds.
    assert_eq!(
        bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
            .await
            .main
            .outcome,
        Outcome::Reply(format!("echo: hi\n\n{}", moraebot::pin::revert_notice()))
    );
    assert_eq!(h.frames("get_session_lease").len(), 1);
    assert_eq!(h.frames("prompt").len(), 1);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn fresh_conversation_skips_lease_check() {
    let h = Harness::new();
    let manager = h.manager("ok");
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert!(h.frames("get_session_lease").is_empty());
    assert_eq!(h.frames("open_session").len(), 1);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn one_lead_child_serves_every_conversation() {
    let h = Harness::new();
    let manager = h.manager("ok");

    let first = bounded(manager.prompt("discord:th:111", 1, Sender::Owner, "one")).await;
    assert_eq!(first.main.outcome, Outcome::Reply("echo: one".into()));
    let pid = manager.child_pid().await.expect("live lead child");
    let second = bounded(manager.prompt("discord:dm:222", 2, Sender::Owner, "two")).await;
    assert_eq!(second.main.outcome, Outcome::Reply("echo: two".into()));
    assert_eq!(
        manager.child_pid().await,
        Some(pid),
        "the lead child is kept"
    );

    let starts: Vec<String> = h
        .log_lines()
        .into_iter()
        .filter(|l| l.starts_with("START pid="))
        .collect();
    assert_eq!(starts.len(), 1, "one lead child for every conversation");
    assert!(start_argv(&starts[0])[4].ends_with("lead/session"));

    manager.shutdown_all().await;
    assert!(!alive(pid));
    assert_eq!(manager.child_pid().await, None);
}

#[tokio::test]
async fn lead_session_id_is_stored_and_lease_checked() {
    let h = Harness::new();
    let first = h.manager("ok");
    bounded(first.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert_eq!(
        h.store
            .lock()
            .unwrap()
            .session_id(lead::LEAD_SESSION)
            .unwrap()
            .as_deref(),
        Some("fake-session-1")
    );
    first.shutdown_all().await;

    let second = h.manager("ok");
    assert_eq!(
        bounded(second.prompt(KEY_B, 2, Sender::Owner, "again"))
            .await
            .main
            .outcome,
        Outcome::Reply(format!("echo: again\n\n{}", moraebot::pin::revert_notice()))
    );
    second.shutdown_all().await;
    let lines = h.log_lines();
    let second_start = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.starts_with("START pid="))
        .nth(1)
        .expect("second START line")
        .0;
    let opening: Vec<Value> = lines[second_start..]
        .iter()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|f| f["type"] == "get_session_lease" || f["type"] == "open_session")
        .collect();
    assert_eq!(opening[0]["type"], "get_session_lease");
    assert_eq!(
        opening[0]["sessionIds"],
        serde_json::json!(["fake-session-1"])
    );
    assert_eq!(opening[1]["type"], "open_session");

    let third = h.manager("lease_held");
    let outcome = bounded(third.prompt(KEY_A, 3, Sender::Owner, "busy"))
        .await
        .main
        .outcome;
    assert!(matches!(outcome, Outcome::NeedsCheck { .. }), "{outcome:?}");
    assert_eq!(outcome.code(), Some(omp::E_SESSION_BUSY));
}

#[tokio::test]
async fn killed_child_is_lost_then_respawned() {
    let h = Harness::new();
    let manager = h.manager("ok");
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "first")).await;
    let pid = manager.child_pid().await.expect("live child");
    let killed = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .expect("run kill");
    assert!(killed.success());

    let outcome = bounded(manager.prompt(KEY_A, 2, Sender::Owner, "second"))
        .await
        .main
        .outcome;
    assert!(matches!(outcome, Outcome::NeedsCheck { .. }));
    assert_eq!(outcome.code(), Some(omp::E_CHILD_LOST));
    assert_eq!(manager.child_pid().await, None);
    assert!(!alive(pid));

    let outcome = bounded(manager.prompt(KEY_A, 3, Sender::Owner, "third"))
        .await
        .main
        .outcome;
    assert_eq!(outcome, Outcome::Reply("echo: third".into()));
    assert_eq!(h.start_pids().len(), 2);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn exit1_is_lost() {
    let h = Harness::new();
    let manager = h.manager("exit1");
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    assert!(matches!(outcome, Outcome::NeedsCheck { .. }), "{outcome:?}");
    assert_eq!(outcome.code(), Some(omp::E_CHILD_LOST));
    assert_eq!(manager.child_pid().await, None);
    let pids = h.start_pids();
    assert_eq!(pids.len(), 1);
    assert!(!alive(pids[0]), "exited child must be gone");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn missing_omp_binary_is_spawn_failure() {
    let mut h = Harness::new();
    h.config.omp_path = h.dir.path().join("no-such-omp");
    let manager = h.manager("ok");
    let outcome = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi"))
        .await
        .main
        .outcome;
    assert_eq!(outcome.code(), Some(omp::E_OMP_SPAWN));
}

// --- send_file host tool -------------------------------------------------------------

impl Harness {
    /// A `send_file` manager whose fake omp calls the tool once per line of `lines`.
    fn send_file_manager(&self, lines: &[String]) -> OmpManager {
        self.manager("send_file")
            .with_extra_env([("FAKE_OMP_SEND_FILE", lines.join("\n"))])
    }

    /// Creates `rel` (and its parents) under the work dir and returns its canonical path.
    fn work_file(&self, rel: &str) -> PathBuf {
        let path = self.config.work_dir.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).expect("parent dir");
        std::fs::write(&path, b"report").expect("write work file");
        path.canonicalize().expect("canonical work file")
    }

    /// `(isError, text)` of every `host_tool_result` frame, in order.
    fn tool_results(&self) -> Vec<(bool, String)> {
        self.frames("host_tool_result")
            .iter()
            .map(|f| {
                let text = f["result"]["content"][0]["text"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                (f["isError"] == true, text)
            })
            .collect()
    }
}

#[tokio::test]
async fn send_file_tool_records_inside_path() {
    let h = Harness::new();
    let file = h.work_file("s3-report.txt");
    let manager = h.send_file_manager(&[file.to_string_lossy().into_owned()]);
    let report = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert_eq!(report.main.outcome, Outcome::Reply("echo: hi".into()));
    assert_eq!(report.main.files, [file]);
    let results = h.tool_results();
    assert_eq!(results.len(), 1);
    assert!(!results[0].0, "{results:?}");
    let tools = h.frames("set_host_tools");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["tools"][0]["name"], omp::SEND_FILE_TOOL);
    assert_eq!(tools[0]["tools"][0]["loadMode"], "essential");
    // QA evidence (shown with --nocapture): the host tool frames fake omp received.
    for frame in tools.iter().chain(h.frames("host_tool_result").iter()) {
        println!("FRAME {frame}");
    }
    manager.shutdown_all().await;
}

#[tokio::test]
async fn send_file_tool_accepts_relative_path() {
    let h = Harness::new();
    assert!(
        !Path::new("out/s3-rel-report.txt").exists(),
        "the test's own current folder must not hold the file"
    );
    let file = h.work_file("out/s3-rel-report.txt");
    std::fs::write(h.dir.path().join("s3-outside.txt"), b"outside").expect("outside file");
    let manager = h.send_file_manager(&[
        "out/s3-rel-report.txt".to_owned(),
        "../s3-outside.txt".to_owned(),
    ]);
    let report = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert_eq!(report.main.outcome, Outcome::Reply("echo: hi".into()));
    assert_eq!(report.main.files, [file]);
    let results = h.tool_results();
    assert_eq!(results.len(), 2);
    assert!(!results[0].0, "{results:?}");
    assert!(results[1].0, "{results:?}");
    assert!(results[1].1.starts_with(files::E_FILE_PATH), "{results:?}");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn send_file_tool_rejects_outside_path() {
    let h = Harness::new();
    let other = tempfile::tempdir().expect("other tempdir");
    let outside = other.path().join("s3-elsewhere.txt");
    std::fs::write(&outside, b"elsewhere").expect("outside file");
    let manager = h.send_file_manager(&[outside.to_string_lossy().into_owned()]);
    let report = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert_eq!(report.main.outcome, Outcome::Reply("echo: hi".into()));
    assert!(report.main.files.is_empty());
    let results = h.tool_results();
    assert_eq!(results.len(), 1);
    assert!(results[0].0, "{results:?}");
    assert!(results[0].1.starts_with(files::E_FILE_PATH), "{results:?}");
    assert!(
        !results[0].1.contains("s3-elsewhere"),
        "the refusal never names the path"
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn send_file_tool_rejects_home() {
    let mut h = Harness::new();
    // A moraebot home inside the work folder: the location rule alone must refuse it.
    h.config.home = h.config.work_dir.join("mb-home");
    let secret = h.work_file("mb-home/secrets/s3-token.txt");
    let manager = h.send_file_manager(&[secret.to_string_lossy().into_owned()]);
    let report = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert_eq!(report.main.outcome, Outcome::Reply("echo: hi".into()));
    assert!(report.main.files.is_empty());
    let results = h.tool_results();
    assert_eq!(results.len(), 1);
    assert!(results[0].0, "{results:?}");
    assert!(results[0].1.starts_with(files::E_FILE_PATH), "{results:?}");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn send_file_tool_caps_files_per_turn() {
    let h = Harness::new();
    let paths: Vec<PathBuf> = (0..=files::MAX_FILES)
        .map(|n| h.work_file(&format!("s3-file-{n}.txt")))
        .collect();
    let lines: Vec<String> = paths
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect();
    let manager = h.send_file_manager(&lines);
    let report = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert_eq!(report.main.outcome, Outcome::Reply("echo: hi".into()));
    assert_eq!(report.main.files, paths[..files::MAX_FILES]);
    let results = h.tool_results();
    assert_eq!(results.len(), files::MAX_FILES + 1);
    assert!(results[..files::MAX_FILES].iter().all(|(err, _)| !err));
    let (last_err, last_text) = &results[files::MAX_FILES];
    assert!(*last_err);
    assert!(last_text.starts_with(files::E_FILE_TOO_MANY), "{last_text}");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn send_file_bucket_is_per_turn() {
    let h = Harness::new();
    let a = h.work_file("s3-a.txt");
    let b = h.work_file("s3-b.txt");
    let manager = h.send_file_manager(&[
        a.to_string_lossy().into_owned(),
        b.to_string_lossy().into_owned(),
    ]);
    let first = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "one")).await;
    assert_eq!(first.main.files, [a.clone(), b.clone()]);
    let second = bounded(manager.prompt(KEY_A, 2, Sender::Owner, "two")).await;
    assert_eq!(second.main.outcome, Outcome::Reply("echo: two".into()));
    assert_eq!(
        second.main.files,
        [a, b],
        "only the second turn's own files"
    );
    assert_eq!(h.start_pids().len(), 1, "both turns ran on one child");
    manager.shutdown_all().await;
}

/// Shell stand-in for omp that runs fake-omp and drops its `prompt_result` frame, so a
/// turn never ends on its own. Written into the test's temp dir.
fn no_result_wrapper(root: &Path) -> PathBuf {
    let script = root.join("omp-no-result.sh");
    let fake = env!("CARGO_BIN_EXE_fake-omp");
    let body = format!(
        r#"#!/bin/sh
'{fake}' "$@" | awk '!/"type":"prompt_result"/ {{ print; fflush() }}'
"#
    );
    std::fs::write(&script, body).expect("write wrapper");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    script
}

#[tokio::test]
async fn files_dropped_when_turn_fails() {
    let mut h = Harness::new();
    h.config.omp_path = no_result_wrapper(h.dir.path());
    let file = h.work_file("s3-dropped.txt");
    let manager = h
        .send_file_manager(&[file.to_string_lossy().into_owned()])
        .with_turn_timeout(Duration::from_secs(5));
    let report = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert!(
        matches!(report.main.outcome, Outcome::NeedsCheck { .. }),
        "{report:?}"
    );
    assert_eq!(report.main.outcome.code(), Some(omp::E_TURN_TIMEOUT));
    // The tool accepted the file during the turn (the fake logged the success result,
    // which the handler writes only after queueing), yet the report carries none.
    let results = h.tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(!results[0].0, "{results:?}");
    assert!(report.main.files.is_empty());
    assert_eq!(manager.child_pid().await, None);
}

#[tokio::test]
async fn send_file_tool_never_runs_shell_payload() {
    let h = Harness::new();
    let manager = h.send_file_manager(&["$(touch /tmp/moraebot-pwned-3c1d)".to_owned()]);
    let report = bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    assert!(report.main.files.is_empty());
    let results = h.tool_results();
    assert_eq!(results.len(), 1);
    assert!(results[0].0);
    assert!(results[0].1.starts_with(files::E_FILE_PATH), "{results:?}");
    assert!(!Path::new("/tmp/moraebot-pwned-3c1d").exists());
    manager.shutdown_all().await;
}

#[tokio::test]
async fn argv_unchanged_by_host_tools() {
    let h = Harness::new();
    let manager = h.manager("ok");
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    let start = h
        .log_lines()
        .into_iter()
        .find(|l| l.starts_with("START pid="))
        .expect("START line");
    assert_eq!(start_argv(&start), expected_argv(&h.config));
    let tools = h.frames("set_host_tools");
    assert_eq!(
        tools.len(),
        1,
        "the tool is registered once, right after startup"
    );
    assert_eq!(tools[0]["tools"][0]["name"], omp::SEND_FILE_TOOL);
    manager.shutdown_all().await;
}

// --- lead turns: follow-up, steer, turn context --------------------------------------

const KEY_STEER: &str = "discord:th:111";

impl Harness {
    /// A shared `steer`-mode manager whose fake answers a steer by `case`.
    fn steer_manager(&self, case: &str) -> Arc<OmpManager> {
        Arc::new(
            self.manager("steer")
                .with_extra_env([("FAKE_OMP_STEER_CASE", case)]),
        )
    }

    /// Prompt frames whose `streamingBehavior` is `steer`.
    fn steer_frames(&self) -> Vec<Value> {
        self.frames("prompt")
            .into_iter()
            .filter(|f| f["streamingBehavior"] == "steer")
            .collect()
    }

    fn admit(&self, message_id: &str, text: &str) -> i64 {
        match self
            .store
            .lock()
            .unwrap()
            .admit("discord", message_id, KEY_STEER, "222", "111", text)
            .expect("admit")
        {
            moraebot::store::Admitted::New(id) => id,
            moraebot::store::Admitted::Duplicate => panic!("duplicate message id"),
        }
    }
}

/// Starts the main prompt on its own task.
fn spawn_prompt(
    manager: &Arc<OmpManager>,
    key: &'static str,
    row_id: i64,
    text: &'static str,
) -> tokio::task::JoinHandle<GroupReport> {
    let manager = Arc::clone(manager);
    tokio::spawn(async move { manager.prompt(key, row_id, Sender::Owner, text).await })
}

/// Waits until the lead reported `agent_start` for a turn of `key`.
async fn turn_running(manager: &OmpManager, key: &str) {
    let mut running = manager.running_turn();
    bounded(running.wait_for(|k| k.as_deref() == Some(key)))
        .await
        .expect("running-turn watch open");
}

fn codes(report: &GroupReport) -> (Option<&'static str>, Option<(i64, Option<&'static str>)>) {
    (
        report.main.outcome.code(),
        report
            .steered
            .as_ref()
            .map(|(id, outcome)| (*id, outcome.code())),
    )
}

#[tokio::test]
async fn main_prompt_uses_follow_up() {
    let h = Harness::new();
    let manager = h.manager("ok");
    bounded(manager.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    let prompts = h.frames("prompt");
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0]["streamingBehavior"], "followUp");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn steer_merged_gives_both_rows_one_text() {
    let h = Harness::new();
    let manager = h.steer_manager("merged");
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, "m2", |_| true));
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(report.main.outcome, Outcome::Reply("echo: m1 + m2".into()));
    assert_eq!(
        report.steered,
        Some((2, Outcome::Reply("echo: m1 + m2".into())))
    );
    let steers = h.steer_frames();
    assert_eq!(steers.len(), 1);
    assert_eq!(steers[0]["message"], "m2");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn steer_separate_gives_each_row_its_text() {
    let h = Harness::new();
    let manager = h.steer_manager("separate");
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, "m2", |_| true));
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(report.main.outcome, Outcome::Reply("echo: m1".into()));
    assert_eq!(report.steered, Some((2, Outcome::Reply("echo: m2".into()))));
    assert_eq!(h.steer_frames().len(), 1);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn steer_refused_for_other_conversation_or_closed_turn() {
    let h = Harness::new();
    let manager = h.steer_manager("merged");
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;

    let mut called = false;
    assert!(
        !manager.try_steer("discord:th:999", 2, Sender::Owner, "other", |_| {
            called = true;
            true
        })
    );
    assert!(!called, "another conversation never reaches the claim");

    assert!(manager.try_steer(KEY_STEER, 3, Sender::Owner, "m2", |_| true));
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(report.steered.map(|(id, _)| id), Some(3));

    assert!(
        !manager.try_steer(KEY_STEER, 4, Sender::Owner, "late", |_| {
            called = true;
            true
        })
    );
    assert!(!called, "a closed turn never reaches the claim");
    assert_eq!(*manager.running_turn().borrow(), None);
    assert_eq!(h.steer_frames().len(), 1);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn steer_refused_from_another_sender() {
    let h = Harness::new();
    let manager = h.steer_manager("merged");
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;

    let mut called = false;
    let chat = Sender::Listed(Role::Chat);
    assert!(!manager.try_steer(KEY_STEER, 2, chat, "chat", |_| {
        called = true;
        true
    }));
    assert!(!called, "a non-owner message never joins the owner's turn");

    assert!(manager.try_steer(KEY_STEER, 3, Sender::Owner, "m2", |_| true));
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(report.steered.map(|(id, _)| id), Some(3));
    let steers = h.steer_frames();
    assert_eq!(steers.len(), 1);
    assert_eq!(steers[0]["message"], "m2");
    manager.shutdown_all().await;
}

#[tokio::test]
async fn turn_context_is_set_during_the_turn() {
    let h = Harness::new();
    let base = h.manager("tools").with_extra_env([(
        "FAKE_OMP_TOOL_CALLS",
        r#"{"tool": "ctx_probe", "arguments": {}}"#,
    )]);
    let context = base.turn_context();
    let handler_context = Arc::clone(&context);
    let parameters = serde_json::from_value(serde_json::json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    }))
    .expect("schema object");
    let probe = omp_rpc::HostTool::new("ctx_probe", "test probe", parameters, move |_, _| {
        let ctx = handler_context.lock().unwrap().clone();
        let text = match ctx {
            Some(ctx) => serde_json::json!({"row": ctx.row_id, "key": ctx.conversation_key}),
            None => serde_json::json!(null),
        };
        Ok(text.to_string().into())
    });
    let manager = base.with_host_tools(vec![probe]);

    let report = bounded(manager.prompt(KEY_STEER, 7, Sender::Owner, "run-tools ctx")).await;
    assert_eq!(
        report.main.outcome,
        Outcome::Reply("echo: run-tools ctx".into())
    );
    let results = h.tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(!results[0].0, "{results:?}");
    let seen: Value = serde_json::from_str(&results[0].1).expect("probe json");
    assert_eq!(seen, serde_json::json!({"row": 7, "key": KEY_STEER}));
    let tools = h.frames("set_host_tools");
    assert_eq!(tools[0]["tools"][0]["name"], omp::SEND_FILE_TOOL);
    assert_eq!(tools[0]["tools"][1]["name"], "ctx_probe");
    assert_eq!(*context.lock().unwrap(), None, "cleared after the turn");
    manager.shutdown_all().await;
}

/// A `tools` lead that makes `calls` (one JSON line each) and has the onboarding tool.
fn onboarding_manager(h: &Harness, calls: &[Value]) -> OmpManager {
    let lines: Vec<String> = calls.iter().map(Value::to_string).collect();
    let m = h
        .manager("tools")
        .with_extra_env([("FAKE_OMP_TOOL_CALLS", lines.join("\n"))]);
    let tools = onboarding::host_tools(
        Arc::clone(&h.store),
        h.config.home.clone(),
        m.turn_context(),
    );
    m.with_host_tools(tools)
}

/// Admits a claimed owner row on `KEY_A` and returns its id.
fn owner_row(h: &Harness, message_id: &str, text: &str) -> i64 {
    let mut store = h.store.lock().unwrap();
    let Admitted::New(id) = store
        .admit("discord", message_id, KEY_A, "222", "111", text)
        .expect("admit owner row")
    else {
        panic!("owner row is new");
    };
    assert!(store.claim(id).unwrap());
    id
}

fn finish_done_call() -> Value {
    serde_json::json!({
        "tool": onboarding::FINISH_TOOL,
        "arguments": {
            "outcome": "done",
            "name": "Sora",
            "tone": "short and warm",
            "owner": "Owner 111 likes short replies"
        }
    })
}

#[tokio::test]
async fn onboarding_finish_through_the_lead() {
    let h = Harness::new();
    assert_eq!(h.store.lock().unwrap().start_onboarding().unwrap(), 2);
    let r = owner_row(&h, "m-on-1", "run-tools");
    assert_eq!(r, 3);
    let m = onboarding_manager(&h, &[finish_done_call()]);
    bounded(m.prompt(
        KEY_A,
        r,
        Sender::Owner,
        &lead::owner_input(KEY_A, r, "run-tools"),
    ))
    .await;

    let tools = h.frames("set_host_tools");
    assert_eq!(tools.len(), 1);
    let names: Vec<&str> = tools[0]["tools"]
        .as_array()
        .expect("tool list")
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert!(names.contains(&onboarding::FINISH_TOOL), "{names:?}");
    let results = h.tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(!results[0].0, "{results:?}");
    let result: Value = serde_json::from_str(&results[0].1).expect("result json");
    assert_eq!(
        result,
        serde_json::json!({"onboarding": "done", "persona": "saved", "applies": "next_turn"})
    );
    assert_eq!(
        std::fs::read_to_string(persona::path(&h.config.home)).expect("persona file"),
        persona::render("Sora", "short and warm", "Owner 111 likes short replies").unwrap()
    );
    assert_eq!(
        h.store.lock().unwrap().onboarding().unwrap(),
        Some(OnboardingRow {
            state: OnboardingState::Done,
            after_row: 2
        })
    );
    assert_eq!(h.start_pids().len(), 1);
    m.shutdown_all().await;
}

#[tokio::test]
async fn onboarding_finish_refusal_reaches_the_lead() {
    let h = Harness::new();
    let r = owner_row(&h, "m-on-1", "run-tools");
    let m = onboarding_manager(&h, &[finish_done_call()]);
    bounded(m.prompt(
        KEY_A,
        r,
        Sender::Owner,
        &lead::owner_input(KEY_A, r, "run-tools"),
    ))
    .await;

    let results = h.tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(results[0].0, "{results:?}");
    assert!(
        results[0].1.starts_with(onboarding::E_ONBOARDING_STATE),
        "{results:?}"
    );
    assert!(!persona::path(&h.config.home).exists());
    m.shutdown_all().await;
}

#[tokio::test]
async fn steer_claim_race_runs_row_once() {
    let h = Harness::new();
    let a = h.admit("race-a", "mA");
    let r = h.admit("race-r", "mR");
    let s = h.admit("race-s", "mS");
    {
        let store = h.store.lock().unwrap();
        assert!(store.claim(a).unwrap());
        assert!(store.claim(r).unwrap(), "the worker took R first");
    }
    let manager = h.steer_manager("merged");
    let turn = spawn_prompt(&manager, KEY_STEER, a, "mA");
    turn_running(&manager, KEY_STEER).await;

    let mut called = None;
    let steered_r = manager.try_steer(KEY_STEER, r, Sender::Owner, "mR", |main| {
        called = Some(main);
        h.store.lock().unwrap().steer_into(r, main).unwrap()
    });
    assert!(!steered_r);
    assert_eq!(called, Some(a), "the claim got the open turn's main row");

    let steered_s = manager.try_steer(KEY_STEER, s, Sender::Owner, "mS", |main| {
        h.store.lock().unwrap().steer_into(s, main).unwrap()
    });
    assert!(steered_s);
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(
        report.steered,
        Some((s, Outcome::Reply("echo: mA + mS".into())))
    );

    let steers = h.steer_frames();
    assert_eq!(steers.len(), 1);
    assert_eq!(steers[0]["message"], "mS");
    assert!(h.frames("prompt").iter().all(|f| f["message"] != "mR"));
    manager.shutdown_all().await;
}

#[tokio::test]
async fn shutdown_all_does_not_wait_for_the_running_turn() {
    let h = Harness::new();
    let manager = h.steer_manager("hold");
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, "m2", |_| true));

    bounded(manager.shutdown_all()).await;
    let report = bounded(turn).await.expect("prompt task");
    assert!(matches!(report.main.outcome, Outcome::NeedsCheck { .. }));
    assert!(matches!(
        report.steered,
        Some((2, Outcome::NeedsCheck { .. }))
    ));
    assert_eq!(
        codes(&report),
        (Some(omp::E_CHILD_LOST), Some((2, Some(omp::E_CHILD_LOST))))
    );
    assert_eq!(manager.child_pid().await, None);
    assert!(!alive(h.start_pids()[0]));
}

/// The dispatcher drops the running `prompt` at shutdown. `shutdown_all` must then still
/// join the lead worker promptly, not after the `ABORT_JOIN_BOUND` fallback.
#[tokio::test]
async fn shutdown_all_after_a_cancelled_turn_joins_the_lead_promptly() {
    let h = Harness::new();
    let manager = h.steer_manager("hold");
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, "m2", |_| true));
    turn.abort();
    assert!(bounded(turn).await.expect_err("aborted").is_cancelled());

    tokio::time::timeout(omp::ABORT_JOIN_BOUND / 10, manager.shutdown_all())
        .await
        .expect("shutdown_all joins the lead worker well before ABORT_JOIN_BOUND");
    assert_eq!(manager.child_pid().await, None);
    assert!(!alive(h.start_pids()[0]));
}

#[tokio::test]
async fn second_steer_in_one_turn_is_refused() {
    let h = Harness::new();
    let manager = h.steer_manager("hold");
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, "m2", |_| true));

    let mut called = false;
    assert!(!manager.try_steer(KEY_STEER, 3, Sender::Owner, "m3", |_| {
        called = true;
        true
    }));
    assert!(!called, "a turn that holds a steer never reaches the claim");

    bounded(manager.shutdown_all()).await;
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(
        codes(&report),
        (Some(omp::E_CHILD_LOST), Some((2, Some(omp::E_CHILD_LOST))))
    );
    // The kill may land before the steer frame is written, so at most one steer frame.
    let steers = h.steer_frames();
    assert!(steers.len() <= 1, "{steers:?}");
    assert!(h.frames("prompt").iter().all(|f| f["message"] != "m3"));
}

#[tokio::test]
async fn turn_timeout_with_steer_kills_lead_before_waiting() {
    let h = Harness::new();
    let manager = Arc::new(
        h.manager("steer")
            .with_extra_env([("FAKE_OMP_STEER_CASE", "hold")])
            .with_turn_timeout(Duration::from_secs(1)),
    );
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, "m2", |_| true));

    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(
        codes(&report),
        (
            Some(omp::E_TURN_TIMEOUT),
            Some((2, Some(omp::E_CHILD_LOST)))
        )
    );
    let pids = h.start_pids();
    assert_eq!(pids.len(), 1);
    assert!(!alive(pids[0]), "the lead child is gone");
    assert_eq!(manager.child_pid().await, None);
}

fn qa_memory_store(h: &Harness) -> moraebot::memory::MemoryStore {
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    moraebot::memory::MemoryStore::open(
        &h.config.home,
        moraebot::memory::Redactor::default(),
        Arc::new(move || today),
    )
    .expect("memory store")
}

fn qa_note(text: &str, bucket: moraebot::memory::Bucket) -> moraebot::memory::NewEntry {
    moraebot::memory::NewEntry {
        text: text.into(),
        prov: moraebot::memory::Prov::Owner,
        bucket,
        src: moraebot::memory::Source::row(1),
        expires: None,
        why: None,
    }
}

#[tokio::test]
async fn memory_write_through_the_lead_decides_provenance_in_code() {
    use moraebot::memory::{Bucket, MemoryHandle, Prov};
    use moraebot::memory_tools::{MEMORY_SEARCH_TOOL, MEMORY_WRITE_TOOL};

    let h = Harness::new();
    let memory = MemoryHandle::spawn(qa_memory_store(&h)).expect("memory worker");
    let r = owner_row(&h, "m-mw-1", "run-tools. 릴리스는 금요일에 하자");
    let write = |text: &str, hint: &str| {
        serde_json::json!({
            "tool": MEMORY_WRITE_TOOL,
            "arguments": {"text": text, "bucket_hint": hint, "why": "qa"}
        })
    };
    let search =
        |arguments: Value| serde_json::json!({"tool": MEMORY_SEARCH_TOOL, "arguments": arguments});
    let calls = [
        write("릴리스는 금요일에 하자", "core"),
        write("릴리스는 금요일", "core"),
        search(serde_json::json!({"query": "  "})),
        search(serde_json::json!({"query": "x", "scope": "people"})),
    ];
    let lines: Vec<String> = calls.iter().map(Value::to_string).collect();
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let m = h
        .manager("tools")
        .with_extra_env([("FAKE_OMP_TOOL_CALLS", lines.join("\n"))])
        .with_memory_clock(Arc::new(move || today))
        .with_memory(memory.clone());
    let report = bounded(m.prompt(
        KEY_A,
        r,
        Sender::Owner,
        &lead::owner_input(KEY_A, r, "run-tools"),
    ))
    .await;

    // The lead's memory tools are exactly these two: nothing deletes, corrects or restores.
    let names: Vec<String> = h.frames("set_host_tools")[0]["tools"]
        .as_array()
        .expect("tool list")
        .iter()
        .filter_map(|t| t["name"].as_str().map(str::to_owned))
        .filter(|n| n.starts_with("memory"))
        .collect();
    assert_eq!(names, [MEMORY_SEARCH_TOOL, MEMORY_WRITE_TOOL]);
    let results = h.tool_results();
    assert_eq!(results.len(), 4, "{results:?}");
    assert!(results[..2].iter().all(|(error, _)| !error), "{results:?}");
    for (error, text) in &results[2..] {
        assert!(*error && text.starts_with("E_MEMORY_ARGS"), "{text}");
    }
    let whole: Value = serde_json::from_str(&results[0].1).expect("write json");
    let part: Value = serde_json::from_str(&results[1].1).expect("write json");
    assert_eq!(whole["provenance"], "owner");
    assert_eq!(whole["bucket"], "core/owner");
    assert_eq!(part["provenance"], "inference");
    assert_eq!(part["bucket"], format!("thread:{KEY_A}"));
    assert!(part["note"].is_string(), "{part}");
    // The worker takes jobs in order, so once a later add is answered both writes are done.
    let flush = qa_note("flush", Bucket::Thread(KEY_B.into()));
    bounded(memory.add(flush)).await.expect("flush");
    let owner = memory
        .show(whole["id"].as_str().unwrap())
        .expect("show")
        .expect("owner entry saved");
    assert_eq!(owner.bucket, Bucket::Owner);
    assert_eq!(owner.entry.prov, Prov::Owner);
    assert_eq!(
        owner.entry.src.as_deref(),
        Some(format!("row={r} s=1 turn={r}").as_str())
    );
    let guess = memory
        .show(part["id"].as_str().unwrap())
        .expect("show")
        .expect("inference entry saved");
    assert_eq!(guess.entry.prov, Prov::Inference);
    assert!(guess.entry.expires.is_some());
    // Saves stay internal: the reply never names them.
    assert!(
        matches!(&report.main.outcome, Outcome::Reply(reply) if !reply.contains("[memory]")),
        "{report:?}"
    );
    bounded(m.shutdown_all()).await;
}

const TRACE_SENTINEL: &str = "TRACE_SENTINEL_41d9";

/// Runs one owner turn of KEY_A in which omp reports a `web_search` and a `task` call (both
/// with [`TRACE_SENTINEL`] in their arguments) and the lead then calls `memory_write`, whose
/// handler waits until the event drain applied its own start, so every earlier frame is
/// applied before the turn can end. The turn ends as FAKE_OMP_TOOLS_END=`end` says.
async fn traced_turn(end: &str, timeout: Duration) -> GroupReport {
    use moraebot::memory::MemoryHandle;
    use moraebot::memory_tools::MEMORY_WRITE_TOOL;

    let h = Harness::new();
    let memory = MemoryHandle::spawn(qa_memory_store(&h)).expect("memory worker");
    let start = |id: &str, name: &str, args: Value| {
        serde_json::json!({"frame": {
            "type": "tool_execution_start",
            "toolCallId": id,
            "toolName": name,
            "args": args
        }})
        .to_string()
    };
    let write = serde_json::json!({
        "tool": MEMORY_WRITE_TOOL,
        "arguments": {"text": "rusqlite 0.43 needs Rust 1.89", "bucket_hint": "thread", "why": "qa"}
    })
    .to_string();
    let script = [
        start(
            "web-1",
            "web_search",
            serde_json::json!({"query": TRACE_SENTINEL}),
        ),
        start(
            "task-1",
            "task",
            serde_json::json!({"prompt": TRACE_SENTINEL}),
        ),
        start("web-2", "web_search", serde_json::json!({"query": "again"})),
        write,
    ]
    .join("\n");
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let m = h
        .manager("tools")
        .with_extra_env([
            ("FAKE_OMP_TOOL_CALLS", script),
            ("FAKE_OMP_TOOLS_END", end.to_owned()),
        ])
        .with_turn_timeout(timeout)
        .with_memory_clock(Arc::new(move || today))
        .with_memory(memory);
    let row = owner_row(&h, "m-trace-1", "run-tools");
    let report = bounded(m.prompt(
        KEY_A,
        row,
        Sender::Owner,
        &lead::owner_input(KEY_A, row, "run-tools"),
    ))
    .await;
    assert_eq!(h.tool_results().len(), 1, "the write ran");
    bounded(m.shutdown_all()).await;
    report
}

fn expected_tools() -> Vec<moraebot::store::ToolUse> {
    use moraebot::memory_tools::MEMORY_WRITE_TOOL;
    use moraebot::store::{ToolSource, ToolUse};
    vec![
        ToolUse::new("web_search", ToolSource::Web),
        ToolUse::new("task", ToolSource::Task),
        ToolUse::new(MEMORY_WRITE_TOOL, ToolSource::Host),
    ]
}

#[tokio::test]
async fn turn_trace_lists_tool_names_and_sources_without_arguments() {
    let report = traced_turn("", Duration::from_secs(30)).await;
    assert_eq!(report.main.outcome.code(), None, "{report:?}");
    assert_eq!(
        report.trace.tools,
        expected_tools(),
        "distinct, first seen first"
    );
    assert!(report.trace.task);
    let json = serde_json::to_string(&report.trace).expect("trace json");
    assert!(!json.contains(TRACE_SENTINEL), "{json}");
    assert!(!format!("{:?}", report.trace).contains(TRACE_SENTINEL));
    let value: Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        value["tools"][0],
        serde_json::json!({"name": "web_search", "source": "web"})
    );
}

#[tokio::test]
async fn turn_trace_survives_a_timed_out_turn() {
    let report = traced_turn("hang", Duration::from_secs(1)).await;
    assert_eq!(report.main.outcome.code(), Some(omp::E_TURN_TIMEOUT));
    assert_eq!(report.trace.tools, expected_tools());
    assert!(report.trace.task);
}

#[tokio::test]
async fn an_outside_tool_call_makes_later_writes_tool_provenance() {
    use moraebot::memory::MemoryHandle;
    use moraebot::memory_tools::MEMORY_WRITE_TOOL;

    let h = Harness::new();
    let memory = MemoryHandle::spawn(qa_memory_store(&h)).expect("memory worker");
    let write = serde_json::json!({
        "tool": MEMORY_WRITE_TOOL,
        "arguments": {"text": "rusqlite 0.43 needs Rust 1.89", "bucket_hint": "thread", "why": "qa"}
    });
    // omp reports a web search the lead ran, then the lead saves what it read.
    let web = serde_json::json!({"frame": {
        "type": "tool_execution_start",
        "toolCallId": "web-1",
        "toolName": "web_search",
        "args": {"query": "rusqlite msrv"}
    }});
    let script = [web, write.clone()].map(|v| v.to_string()).join("\n");
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let m = h
        .manager("tools")
        .with_extra_env([("FAKE_OMP_TOOL_CALLS", script)])
        .with_memory_clock(Arc::new(move || today))
        .with_memory(memory.clone());
    let first = owner_row(&h, "m-out-1", "run-tools");
    bounded(m.prompt(
        KEY_A,
        first,
        Sender::Owner,
        &lead::owner_input(KEY_A, first, "run-tools"),
    ))
    .await;
    let results = h.tool_results();
    assert_eq!(results.len(), 1, "{results:?}");
    let saved: Value = serde_json::from_str(&results[0].1).expect("write json");
    assert_eq!(saved["provenance"], "tool", "{saved}");
    bounded(m.shutdown_all()).await;
}

#[tokio::test]
async fn an_owner_row_steered_into_a_lane_result_turn_gives_owner_provenance() {
    use moraebot::memory::{Bucket, MemoryHandle, Prov};
    use moraebot::memory_tools::MEMORY_WRITE_TOOL;

    let h = Harness::new();
    let memory = MemoryHandle::spawn(qa_memory_store(&h)).expect("memory worker");
    // A lane result row of KEY_STEER, the main row of the turn.
    let lane_row = {
        let mut store = h.store.lock().unwrap();
        store
            .start_lane("call-1", KEY_STEER, "t", None, None, 3)
            .expect("lane");
        assert!(
            store
                .finish_lane(
                    1,
                    1,
                    "CI는 cargo test를 돌린다. 끝",
                    moraebot::store::LaneState::Idle,
                    None
                )
                .expect("finish")
        );
        // Row ids count up: the lane result is the newest row.
        let id = store.count().expect("count");
        let row = store.get(id).expect("get").expect("lane row");
        assert_eq!(row.origin, "lane");
        id
    };
    let owner_text = "알았어. 릴리스는 금요일에 하자";
    let owner = h.admit("m-steer-owner", owner_text);
    let write = |text: &str, hint: &str| {
        serde_json::json!({
            "tool": MEMORY_WRITE_TOOL,
            "arguments": {"text": text, "bucket_hint": hint, "why": "qa"}
        })
        .to_string()
    };
    let script = [
        write("릴리스는 금요일에 하자", "core"),
        write("CI는 cargo test를 돌린다.", "core"),
    ]
    .join("\n");
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let manager = Arc::new(
        h.manager("steer")
            .with_extra_env([
                ("FAKE_OMP_STEER_CASE", "tools".to_owned()),
                ("FAKE_OMP_TOOL_CALLS", script),
            ])
            .with_memory_clock(Arc::new(move || today))
            .with_memory(memory.clone()),
    );
    let turn = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move {
            manager
                .prompt(KEY_STEER, lane_row, Sender::Owner, "lane result")
                .await
        })
    };
    turn_running(&manager, KEY_STEER).await;
    assert!(manager.try_steer(
        KEY_STEER,
        owner,
        Sender::Owner,
        &lead::owner_input(KEY_STEER, owner, owner_text),
        |_| true
    ));
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(codes(&report), (None, Some((owner, None))));

    let results = h.tool_results();
    assert_eq!(results.len(), 2, "{results:?}");
    let owned: Value = serde_json::from_str(&results[0].1).expect("write json");
    let lane: Value = serde_json::from_str(&results[1].1).expect("write json");
    assert_eq!(owned["provenance"], "owner", "{owned}");
    assert_eq!(owned["bucket"], "core/owner");
    assert_eq!(
        lane["provenance"], "tool",
        "a lane row never gives owner: {lane}"
    );
    assert_eq!(lane["bucket"], format!("thread:{KEY_STEER}"));
    let flush = qa_note("flush", Bucket::Thread(KEY_B.into()));
    bounded(memory.add(flush)).await.expect("flush");
    let saved = memory
        .show(owned["id"].as_str().unwrap())
        .expect("show")
        .expect("owner entry saved");
    assert_eq!(saved.entry.prov, Prov::Owner);
    assert_eq!(
        saved.entry.src.as_deref(),
        Some(format!("row={owner} s=1 turn={lane_row}").as_str())
    );
    bounded(manager.shutdown_all()).await;
}

#[tokio::test]
async fn conversation_memory_follows_the_body_once_until_it_changes() {
    let h = Harness::new();
    let mut store = qa_memory_store(&h);
    let thread = moraebot::memory::Bucket::Thread(KEY_A.into());
    store
        .add(qa_note("reply in Korean", thread.clone()))
        .expect("add");
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let manager = h.manager("ok").with_memory_clock(Arc::new(move || today));
    let inputs = [
        lead::owner_input(KEY_A, 1, "first"),
        lead::owner_input(KEY_A, 2, "second"),
        lead::owner_input(KEY_B, 3, "other conversation"),
    ];
    for (n, input) in inputs.iter().enumerate() {
        bounded(manager.prompt(
            if n == 2 { KEY_B } else { KEY_A },
            n as i64 + 1,
            Sender::Owner,
            input,
        ))
        .await;
    }
    store
        .add(qa_note("keep PRs small", thread.clone()))
        .expect("add");
    let last = lead::owner_input(KEY_A, 4, "third");
    bounded(manager.prompt(KEY_A, 4, Sender::Owner, &last)).await;

    let messages: Vec<String> = h
        .frames("prompt")
        .iter()
        .map(|f| f["message"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(messages.len(), 4);
    let open = omp::thread_memory_open(KEY_A);
    let (body, block) = messages[0].split_once("\n\n").expect("memory block");
    assert_eq!(body, inputs[0], "the owner body comes first");
    assert!(
        block.starts_with(&open) && block.contains("reply in Korean"),
        "{block}"
    );
    assert_eq!(messages[1], inputs[1], "unchanged memory is not sent again");
    assert_eq!(messages[2], inputs[2], "KEY_B has no memory");
    assert!(messages[3].starts_with(&format!("{last}\n\n{open}")));
    assert!(messages[3].contains("keep PRs small"));
    bounded(manager.shutdown_all()).await;
}

/// Saves one owner note in KEY_A's conversation memory.
fn remember_in_key_a(h: &Harness) {
    let thread = moraebot::memory::Bucket::Thread(KEY_A.into());
    qa_memory_store(h)
        .add(qa_note("reply in Korean", thread))
        .expect("add");
}

/// The messages of every `prompt` frame the lead received, in order.
fn prompt_messages(h: &Harness) -> Vec<String> {
    h.frames("prompt")
        .iter()
        .map(|f| f["message"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn a_rejected_prompt_gives_its_memory_block_again() {
    let h = Harness::new();
    remember_in_key_a(&h);
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let manager = h
        .manager("ok")
        .with_extra_env([("FAKE_OMP_REJECT_TRIGGER", "reject-me".to_owned())])
        .with_memory_clock(Arc::new(move || today));
    let refused = bounded(manager.prompt(
        KEY_A,
        1,
        Sender::Owner,
        &lead::owner_input(KEY_A, 1, "reject-me"),
    ))
    .await;
    assert_eq!(refused.main.outcome.code(), Some(omp::E_PROMPT_REJECTED));
    let pid = manager.child_pid().await;
    assert!(pid.is_some(), "a rejected prompt keeps its child");
    bounded(manager.prompt(
        KEY_A,
        2,
        Sender::Owner,
        &lead::owner_input(KEY_A, 2, "second"),
    ))
    .await;
    assert_eq!(manager.child_pid().await, pid, "the same child");

    let messages = prompt_messages(&h);
    assert_eq!(messages.len(), 2);
    let open = omp::thread_memory_open(KEY_A);
    for message in &messages {
        assert!(
            message.contains(&open) && message.contains("reply in Korean"),
            "{message}"
        );
    }
    bounded(manager.shutdown_all()).await;
}

#[tokio::test]
async fn compaction_gives_the_memory_block_again() {
    let h = Harness::new();
    remember_in_key_a(&h);
    // omp compacts the lead's context during the second turn.
    let compaction = serde_json::json!({"frame": {
        "type": "auto_compaction_end",
        "action": "context-full",
        "aborted": false,
        "willRetry": false,
        "result": {"summary": "earlier turns", "firstKeptEntryId": "e9", "tokensBefore": 190000}
    }});
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let manager = h
        .manager("tools")
        .with_extra_env([("FAKE_OMP_TOOL_CALLS", compaction.to_string())])
        .with_memory_clock(Arc::new(move || today));
    for (row, body) in [(1, "first"), (2, "run-tools"), (3, "third")] {
        let report = bounded(manager.prompt(
            KEY_A,
            row,
            Sender::Owner,
            &lead::owner_input(KEY_A, row, body),
        ))
        .await;
        assert!(
            matches!(report.main.outcome, Outcome::Reply(_)),
            "{report:?}"
        );
    }

    let messages = prompt_messages(&h);
    assert_eq!(messages.len(), 3);
    let open = omp::thread_memory_open(KEY_A);
    assert!(messages[0].contains(&open), "{}", messages[0]);
    assert!(
        !messages[1].contains(&open),
        "unchanged memory is not sent again before the compaction"
    );
    assert!(
        messages[2].contains(&open) && messages[2].contains("reply in Korean"),
        "the compaction may have summarised the block away: {}",
        messages[2]
    );
    bounded(manager.shutdown_all()).await;
}

/// One owner turn in KEY_STEER, whose conversation memory holds one owner note, with a
/// steer joined to it (fake-omp `steer` / `merged`, plus `env`): the main input's
/// `followUp` frame message and the steer frame message the lead received.
async fn main_and_steer_messages(h: &Harness, env: Vec<(&str, String)>) -> (String, String) {
    let thread = moraebot::memory::Bucket::Thread(KEY_STEER.into());
    qa_memory_store(h)
        .add(qa_note("reply in Korean", thread))
        .expect("add");
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let manager = Arc::new(
        h.manager("steer")
            .with_extra_env([("FAKE_OMP_STEER_CASE", "merged".to_owned())])
            .with_extra_env(env)
            .with_memory_clock(Arc::new(move || today)),
    );
    let main = lead::owner_input(KEY_STEER, 1, "m1");
    let turn = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move { manager.prompt(KEY_STEER, 1, Sender::Owner, &main).await })
    };
    turn_running(&manager, KEY_STEER).await;
    let steer = lead::owner_input(KEY_STEER, 2, "m2");
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, &steer, |_| true));
    let report = bounded(turn).await.expect("prompt task");
    assert!(
        matches!(report.main.outcome, Outcome::Reply(_)),
        "{report:?}"
    );
    bounded(manager.shutdown_all()).await;

    let mains: Vec<String> = h
        .frames("prompt")
        .iter()
        .filter(|f| f["streamingBehavior"] == "followUp")
        .map(|f| f["message"].as_str().unwrap().to_owned())
        .collect();
    let steers = h.steer_frames();
    assert_eq!((mains.len(), steers.len()), (1, 1));
    let steer_message = steers[0]["message"].as_str().unwrap().to_owned();
    (mains[0].clone(), steer_message)
}

#[tokio::test]
async fn a_steer_counts_the_main_inputs_memory_block_as_given() {
    let h = Harness::new();
    let (main, steer) = main_and_steer_messages(&h, Vec::new()).await;
    let open = omp::thread_memory_open(KEY_STEER);
    assert!(
        main.contains(&open) && main.contains("reply in Korean"),
        "{main}"
    );
    assert_eq!(
        steer,
        lead::owner_input(KEY_STEER, 2, "m2"),
        "the block went in with the main input of the same turn"
    );
}

#[tokio::test]
async fn a_steer_after_a_mid_turn_compaction_carries_the_memory_block_again() {
    let h = Harness::new();
    // omp compacts the lead's context after it accepted the main prompt, before the steer.
    let compaction = serde_json::json!({
        "type": "auto_compaction_end",
        "action": "context-full",
        "aborted": false,
        "willRetry": false,
        "result": {"summary": "earlier turns", "firstKeptEntryId": "e9", "tokensBefore": 190000}
    });
    let (main, steer) =
        main_and_steer_messages(&h, vec![("FAKE_OMP_STEER_FRAME", compaction.to_string())]).await;
    let open = omp::thread_memory_open(KEY_STEER);
    assert!(main.contains(&open), "{main}");
    assert!(
        steer.contains(&open) && steer.contains("reply in Korean"),
        "the compaction may have summarised the main input's block away: {steer}"
    );
}

// ---- Recall advisor in shadow mode (#59, PR 2) ----

fn advisor_log(h: &Harness) -> PathBuf {
    h.dir.path().join("advisor.log")
}

/// An advisor on the fake omp with model `qa-small`, its own log, scripted by `vars`.
fn qa_advisor(h: &Harness, vars: &[(&str, &str)]) -> Arc<moraebot::recall::Advisor> {
    qa_advisor_with(h, vars, |_| {})
}

/// [`qa_advisor`] with `adjust` applied to its config.
fn qa_advisor_with(
    h: &Harness,
    vars: &[(&str, &str)],
    adjust: impl FnOnce(&mut Config),
) -> Arc<moraebot::recall::Advisor> {
    let mut config = h.config.clone();
    config.memory.recall.model = "qa-small".into();
    config.memory.recall.mode = Some(moraebot::config::RecallMode::Shadow);
    adjust(&mut config);
    let mut env: Vec<(String, String)> = vars
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    env.push((
        "FAKE_OMP_LOG".into(),
        advisor_log(h).to_string_lossy().into_owned(),
    ));
    Arc::new(
        moraebot::recall::Advisor::new(
            &config,
            Arc::clone(&h.store),
            moraebot::memory::Redactor::default(),
        )
        .with_extra_env(env)
        .with_clock(Arc::new(|| {
            moraebot::memory::Day::parse("2026-11-21").expect("date")
        })),
    )
}

fn advisor_lines(h: &Harness) -> Vec<String> {
    std::fs::read_to_string(advisor_log(h))
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn advisor_prompts(h: &Harness) -> Vec<String> {
    advisor_lines(h)
        .iter()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|f| f["type"] == "prompt")
        .map(|f| f["message"].as_str().unwrap_or_default().to_owned())
        .collect()
}

fn logged(h: &Harness, code: &str, detail: &str) -> bool {
    h.store.lock().unwrap().memory_logged(code, detail).unwrap()
}

/// The ledger rows of lead context `(generation, 0)` as (id, state).
fn ledger_rows(h: &Harness, generation: i64) -> Vec<(String, String)> {
    h.store
        .lock()
        .unwrap()
        .recall_seen(KEY_A, generation, 0, 0)
        .unwrap()
        .into_iter()
        .map(|r| (r.entry_id, r.state.as_str().to_owned()))
        .collect()
}

/// The project of the advisor tests' notes: no test message names it, so the lead never holds
/// its section.
const ZEPHYR: &str = "usunmn/zephyr";

/// One note in a project no message names: a candidate the lead does not hold.
fn seed_project_note(h: &Harness) -> String {
    let mut store = qa_memory_store(h);
    let added = store
        .add(qa_note(
            "Releases go out on Fridays only",
            moraebot::memory::Bucket::Project(ZEPHYR.into()),
        ))
        .expect("note");
    added.id
}

async fn wakes_done(progress: &mut tokio::sync::watch::Receiver<u64>, count: u64) {
    bounded(progress.wait_for(|n| *n >= count))
        .await
        .expect("advisor progress");
}

#[tokio::test]
async fn shadow_recall_logs_its_picks_and_never_changes_the_lead_input() {
    let h = Harness::new();
    let id = seed_project_note(&h);
    let reply = format!(r#"{{"pick":["{id}","m-ffffff"],"reject":[]}}"#);
    let advisor = qa_advisor(&h, &[("FAKE_OMP_REPLY", reply.as_str())]);
    let mut progress = advisor.progress();
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    let input = lead::owner_input(KEY_A, 1, "Can we ship the release today?");
    let outcome = bounded(m.prompt(KEY_A, 1, Sender::Owner, &input))
        .await
        .main
        .outcome;
    assert_eq!(
        outcome,
        Outcome::Reply("echo: Can we ship the release today?".into())
    );
    wakes_done(&mut progress, 1).await;

    // The lead got its input exactly as without an advisor: nothing recalled, nothing in chat.
    let lead_prompts = h.frames("prompt");
    assert_eq!(lead_prompts.len(), 1);
    assert_eq!(lead_prompts[0]["message"], input.as_str());

    // The advisor ran locked, on the recall model, and was offered the project note.
    let start = advisor_lines(&h)
        .into_iter()
        .find(|l| l.starts_with("START pid="))
        .expect("advisor started");
    let argv = start_argv(&start);
    assert!(argv.contains(&"--model=qa-small".to_owned()), "{argv:?}");
    assert!(argv.contains(&"--no-tools".to_owned()), "{argv:?}");
    let asked = advisor_prompts(&h);
    assert_eq!(asked.len(), 1);
    assert!(
        asked[0].contains("Can we ship the release today?"),
        "{}",
        asked[0]
    );
    assert!(
        asked[0].contains(&format!("- {id} ["))
            && asked[0].contains("Releases go out on Fridays only"),
        "{}",
        asked[0]
    );

    // The pick is logged with its score and the lead context; the unoffered id is dropped.
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_PICK,
        &format!("gen=1 epoch=0 row=1 offered=1 picks={id}:2 rejected=0 unoffered=1 blocked=0")
    ));
    assert_eq!(ledger_rows(&h, 1), [(id, "shadow".to_owned())]);
    m.shutdown_all().await;
}

/// moraedang's point 1 on #59: the ledger's lead context is a persisted generation, not the
/// in-memory compaction epoch (which is 0 again for every new lead child).
#[tokio::test]
async fn a_shadow_pick_is_offered_again_only_to_a_new_lead_generation() {
    let h = Harness::new();
    let id = seed_project_note(&h);
    let reply = format!(r#"{{"pick":["{id}"]}}"#);
    let advisor = qa_advisor(&h, &[("FAKE_OMP_REPLY", reply.as_str())]);
    let mut progress = advisor.progress();
    let m = h
        .manager("ok")
        .with_recall(Arc::clone(&advisor))
        .with_cache_window(None);
    let ask = |row: i64, text: &str| lead::owner_input(KEY_A, row, text);

    bounded(m.prompt(KEY_A, 1, Sender::Owner, &ask(1, "first"))).await;
    wakes_done(&mut progress, 1).await;
    bounded(m.prompt(KEY_A, 1, Sender::Owner, &ask(1, "second"))).await;
    wakes_done(&mut progress, 2).await;
    assert_eq!(
        advisor_prompts(&h).len(),
        1,
        "the same lead child already holds the pick: nothing left to offer, no call"
    );

    // A persona change restarts the lead between turns: a new generation, epoch 0 again.
    save_persona(&h, "Kai", "", "");
    bounded(m.prompt(KEY_A, 1, Sender::Owner, &ask(1, "third"))).await;
    wakes_done(&mut progress, 3).await;
    let asked = advisor_prompts(&h);
    assert_eq!(asked.len(), 2, "the new lead child never saw the pick");
    assert!(asked[1].contains(&id), "{}", asked[1]);
    for generation in [1, 2] {
        let detail = format!(
            "gen={generation} epoch=0 row=1 offered=1 picks={id}:2 rejected=0 unoffered=0 blocked=0"
        );
        assert!(
            logged(&h, moraebot::recall::I_RECALL_PICK, &detail),
            "{detail}"
        );
        assert_eq!(
            ledger_rows(&h, generation),
            [(id.clone(), "shadow".to_owned())]
        );
    }
    m.shutdown_all().await;
}

/// The failure rule: a hanging advisor never holds the lead turn, and never makes the next
/// owner wake busy; that wake finds nothing new and asks nothing.
#[tokio::test]
async fn a_hanging_advisor_never_holds_the_lead_turn() {
    let h = Harness::new();
    seed_project_note(&h);
    let advisor = qa_advisor(&h, &[("FAKE_OMP_MODE", "hang")]);
    let mut progress = advisor.progress();
    let mut asked = advisor.asked();
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    let input = lead::owner_input(KEY_A, 1, "hello");
    let outcome = bounded(m.prompt(KEY_A, 1, Sender::Owner, &input))
        .await
        .main
        .outcome;
    assert_eq!(outcome, Outcome::Reply("echo: hello".into()));
    bounded(asked.wait_for(|n| *n >= 1)).await.expect("asked");
    let outcome = bounded(m.prompt(KEY_A, 1, Sender::Owner, &input))
        .await
        .main
        .outcome;
    assert_eq!(outcome, Outcome::Reply("echo: hello".into()));
    wakes_done(&mut progress, 1).await;
    assert_eq!(*progress.borrow(), 1, "the first wake still runs");
    assert_eq!(*asked.borrow(), 1, "nothing new for the second wake");
    assert!(!logged(
        &h,
        moraebot::recall::W_RECALL_CAP,
        "reason=busy gen=1 epoch=0 row=1"
    ));
    assert!(ledger_rows(&h, 1).is_empty());
    m.shutdown_all().await;
}

/// Scope: a note in another conversation's thread (a DM) is never offered here.
#[tokio::test]
async fn another_conversations_thread_is_never_offered() {
    let h = Harness::new();
    qa_memory_store(&h)
        .add(qa_note(
            "My bank PIN hint is the cat's name",
            moraebot::memory::Bucket::Thread(KEY_B.into()),
        ))
        .expect("note");
    let advisor = qa_advisor(&h, &[]);
    let mut progress = advisor.progress();
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    let input = lead::owner_input(KEY_A, 1, "what is my bank PIN hint?");
    bounded(m.prompt(KEY_A, 1, Sender::Owner, &input)).await;
    wakes_done(&mut progress, 1).await;
    assert!(advisor_prompts(&h).is_empty(), "no candidate, no call");
    assert_eq!(lead_messages(&h), [input.as_str()]);
    m.shutdown_all().await;
}

#[tokio::test]
async fn an_advisor_that_lists_tools_is_refused_and_logged() {
    let h = Harness::new();
    seed_project_note(&h);
    let advisor = qa_advisor(&h, &[("FAKE_OMP_DUMP_TOOLS", "bash")]);
    let mut progress = advisor.progress();
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    let input = lead::owner_input(KEY_A, 1, "hello");
    let outcome = bounded(m.prompt(KEY_A, 1, Sender::Owner, &input))
        .await
        .main
        .outcome;
    assert_eq!(outcome, Outcome::Reply("echo: hello".into()));
    wakes_done(&mut progress, 1).await;
    assert!(
        advisor_prompts(&h).is_empty(),
        "no prompt reaches a child with tools"
    );
    assert!(logged(
        &h,
        moraebot::recall::E_RECALL_SPAWN,
        "gen=1 epoch=0 row=1"
    ));
    assert!(ledger_rows(&h, 1).is_empty());
    m.shutdown_all().await;
}

#[tokio::test]
async fn lane_results_and_untagged_inputs_never_wake_the_advisor() {
    for on in [false, true] {
        let h = Harness::new();
        seed_project_note(&h);
        let advisor = if on {
            on_advisor(&h, &[], moraebot::config::MAX_RECALL_WAIT_MS)
        } else {
            qa_advisor(&h, &[])
        };
        let progress = advisor.progress();
        let m = h.manager("ok").with_recall(Arc::clone(&advisor));
        let lane = lead::lane_result(KEY_A, 7, &lead::LaneResult::Text("done"));
        bounded(m.prompt(KEY_A, 1, Sender::Owner, &lane)).await;
        bounded(m.prompt(KEY_A, 1, Sender::Owner, "untagged")).await;
        m.shutdown_all().await;
        assert_eq!(*progress.borrow(), 0, "on={on}");
        assert!(advisor_lines(&h).is_empty(), "on={on}");
        let messages: Vec<Value> = h
            .frames("prompt")
            .iter()
            .map(|f| f["message"].clone())
            .collect();
        assert_eq!(
            messages,
            [Value::from(lane), Value::from("untagged")],
            "on={on}"
        );
    }
}

/// A turn with an advisor that fails is the same turn as without one; the failure is logged.
async fn turn_with_failing_advisor(h: &Harness, advisor: Arc<moraebot::recall::Advisor>) {
    let mut progress = advisor.progress();
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    let input = lead::owner_input(KEY_A, 1, "hello");
    let outcome = bounded(m.prompt(KEY_A, 1, Sender::Owner, &input))
        .await
        .main
        .outcome;
    assert_eq!(outcome, Outcome::Reply("echo: hello".into()));
    wakes_done(&mut progress, 1).await;
    let lead_prompts = h.frames("prompt");
    assert_eq!(lead_prompts.len(), 1);
    assert_eq!(lead_prompts[0]["message"], input.as_str());
    assert!(ledger_rows(h, 1).is_empty());
    m.shutdown_all().await;
}

#[tokio::test]
async fn an_advisor_past_its_time_limit_is_logged_and_the_turn_is_unaffected() {
    let h = Harness::new();
    seed_project_note(&h);
    let advisor = qa_advisor_with(&h, &[("FAKE_OMP_MODE", "hang")], |c| {
        c.memory.recall.timeout_seconds = 1;
    });
    turn_with_failing_advisor(&h, advisor).await;
    assert_eq!(advisor_prompts(&h).len(), 1, "the advisor was asked");
    assert!(logged(
        &h,
        moraebot::recall::E_RECALL_TIMEOUT,
        "gen=1 epoch=0 row=1"
    ));
}

#[tokio::test]
async fn an_advisor_answer_without_json_is_logged_and_the_turn_is_unaffected() {
    let h = Harness::new();
    seed_project_note(&h);
    let advisor = qa_advisor(&h, &[("FAKE_OMP_REPLY", "I would pick the first entry.")]);
    turn_with_failing_advisor(&h, advisor).await;
    assert_eq!(advisor_prompts(&h).len(), 1, "the advisor was asked");
    assert!(logged(
        &h,
        moraebot::recall::E_RECALL_OUTPUT,
        "gen=1 epoch=0 row=1"
    ));
    assert!(!logged(
        &h,
        moraebot::recall::I_RECALL_PICK,
        "gen=1 epoch=0 row=1 offered=1 picks= rejected=0 unoffered=0 blocked=0"
    ));
}

// ---- Recall advisor in on mode (#59, PR 3) ----

/// An advisor in on mode that waits at most `wait_ms` per owner turn.
fn on_advisor(h: &Harness, vars: &[(&str, &str)], wait_ms: u64) -> Arc<moraebot::recall::Advisor> {
    qa_advisor_with(h, vars, |c| {
        c.memory.recall.mode = Some(moraebot::config::RecallMode::On);
        c.memory.recall.wait_ms = wait_ms;
    })
}

/// The file whose existence lets the gated advisor answer (FAKE_OMP_REPLY_GATE).
fn reply_gate(h: &Harness) -> PathBuf {
    h.dir.path().join("advisor.gate")
}

/// One owner note in project [`ZEPHYR`]: a candidate no input gives the lead.
fn seed_key_c(h: &Harness) -> String {
    seed_project_note(h)
}

/// The lead's prompt messages, in order.
fn lead_messages(h: &Harness) -> Vec<String> {
    h.frames("prompt")
        .iter()
        .map(|f| f["message"].as_str().unwrap_or_default().to_owned())
        .collect()
}

/// The delivered ledger rows of lead context `(generation, 0)` as (id, inbox row).
fn delivered_rows(h: &Harness, generation: i64) -> Vec<(String, Option<i64>)> {
    h.store
        .lock()
        .unwrap()
        .recall_seen(KEY_A, generation, 0, i64::MAX)
        .unwrap()
        .into_iter()
        .filter(|r| r.state == moraebot::store::RecallState::Delivered)
        .map(|r| (r.entry_id, r.row_id))
        .collect()
}

/// The recalled block for the [`ZEPHYR`] owner note `id` as a lead input carries it.
fn recalled_for(id: &str) -> String {
    use moraebot::recall::{RECALLED_CLOSE, RECALLED_LABEL, RECALLED_OPEN};
    format!(
        "{RECALLED_OPEN}\n{}\n- {id} [2026-11-20, project {ZEPHYR}, owner; {RECALLED_LABEL}] Releases go out on Fridays only\n{RECALLED_CLOSE}",
        moraebot::memory::MEMORY_GUARD
    )
}

/// An on-mode advisor answering `pick` for `id`, its reply held back until [`reply_gate`] exists.
fn gated_advisor(h: &Harness, id: &str, wait_ms: u64) -> Arc<moraebot::recall::Advisor> {
    let reply = format!(r#"{{"pick":["{id}"]}}"#);
    let gate = reply_gate(h).to_string_lossy().into_owned();
    on_advisor(
        h,
        &[
            ("FAKE_OMP_REPLY", reply.as_str()),
            ("FAKE_OMP_REPLY_GATE", gate.as_str()),
        ],
        wait_ms,
    )
}

#[tokio::test]
async fn on_mode_appends_the_picks_as_a_guarded_block_and_records_the_delivery() {
    let h = Harness::new();
    let id = seed_key_c(&h);
    let reply = format!(r#"{{"pick":["{id}"],"reject":[]}}"#);
    let advisor = on_advisor(
        &h,
        &[("FAKE_OMP_REPLY", reply.as_str())],
        moraebot::config::MAX_RECALL_WAIT_MS,
    );
    let mut progress = advisor.progress();
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    let input = lead::owner_input(KEY_A, 1, "Can we ship the release today?");
    let report = bounded(m.prompt(KEY_A, 1, Sender::Owner, &input)).await;
    assert!(
        matches!(report.main.outcome, Outcome::Reply(_)),
        "{report:?}"
    );
    wakes_done(&mut progress, 1).await;
    assert_eq!(
        lead_messages(&h),
        [format!("{input}\n\n{}", recalled_for(&id))],
        "the block follows the owner's input, before the turn started"
    );
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_PICK,
        &format!(
            "gen=1 epoch=0 row=1 offered=1 picks={id}:2 rejected=0 unoffered=0 blocked=0 mode=on"
        )
    ));
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_DELIVERED,
        &format!("gen=1 epoch=0 row=1 ids={id}")
    ));
    assert_eq!(ledger_rows(&h, 1), [(id.clone(), "delivered".to_owned())]);
    assert_eq!(delivered_rows(&h, 1), [(id.clone(), Some(1))]);

    // The same lead context never gets it again: nothing left to offer, no call, no block.
    let again = lead::owner_input(KEY_A, 2, "And the release notes?");
    bounded(m.prompt(KEY_A, 2, Sender::Owner, &again)).await;
    wakes_done(&mut progress, 2).await;
    assert_eq!(lead_messages(&h)[1], again);
    assert_eq!(advisor_prompts(&h).len(), 1);
    assert_eq!(delivered_rows(&h, 1).len(), 1);
    m.shutdown_all().await;
}

/// The wait cap and late picks: a turn waits at most `wait_ms` and starts without the picks;
/// they go with that conversation's next owner input, never with another conversation's.
#[tokio::test]
async fn late_picks_go_with_the_conversations_next_owner_message_only() {
    let h = Harness::new();
    let id = seed_key_c(&h);
    let gate = reply_gate(&h);
    let advisor = gated_advisor(&h, &id, 50);
    let mut progress = advisor.progress();
    let mut asked = advisor.asked();
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));

    // The advisor cannot answer yet: the turn runs after the wait, without a block.
    let first = lead::owner_input(KEY_A, 1, "Can we ship the release today?");
    let outcome = bounded(m.prompt(KEY_A, 1, Sender::Owner, &first))
        .await
        .main
        .outcome;
    assert_eq!(
        outcome,
        Outcome::Reply("echo: Can we ship the release today?".into())
    );
    assert_eq!(lead_messages(&h), [first.as_str()]);
    bounded(asked.wait_for(|n| *n >= 1)).await.expect("asked");
    assert!(delivered_rows(&h, 1).is_empty());

    // It answers after the turn: the picks are kept.
    std::fs::write(&gate, "").expect("open the gate");
    wakes_done(&mut progress, 1).await;
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_PICK,
        &format!(
            "gen=1 epoch=0 row=1 offered=1 picks={id}:2 rejected=0 unoffered=0 blocked=0 mode=on"
        )
    ));

    // Another conversation's turn never gets them (its own call waits: the gate is shut again).
    std::fs::remove_file(&gate).expect("shut the gate");
    let other = lead::owner_input(KEY_B, 2, "hello from b");
    bounded(m.prompt(KEY_B, 2, Sender::Owner, &other)).await;
    assert_eq!(lead_messages(&h)[1], other);

    // The conversation's next owner message carries them, recorded with its row.
    let next = lead::owner_input(KEY_A, 3, "ok, go on");
    bounded(m.prompt(KEY_A, 3, Sender::Owner, &next)).await;
    assert_eq!(
        lead_messages(&h)[2],
        format!("{next}\n\n{}", recalled_for(&id))
    );
    assert_eq!(delivered_rows(&h, 1), [(id.clone(), Some(3))]);
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_DELIVERED,
        &format!("gen=1 epoch=0 row=3 ids={id}")
    ));
    m.shutdown_all().await;
}

/// Late picks chosen for a lead context that is gone (a restart) are dropped.
#[tokio::test]
async fn late_picks_of_an_earlier_lead_generation_are_dropped() {
    let h = Harness::new();
    let id = seed_key_c(&h);
    let gate = reply_gate(&h);
    let advisor = gated_advisor(&h, &id, 50);
    let mut progress = advisor.progress();
    let mut asked = advisor.asked();
    let m = h
        .manager("ok")
        .with_recall(Arc::clone(&advisor))
        .with_cache_window(None);
    let first = lead::owner_input(KEY_A, 1, "Can we ship the release today?");
    bounded(m.prompt(KEY_A, 1, Sender::Owner, &first)).await;
    bounded(asked.wait_for(|n| *n >= 1)).await.expect("asked");
    std::fs::write(&gate, "").expect("open the gate");
    wakes_done(&mut progress, 1).await;
    std::fs::remove_file(&gate).expect("shut the gate");

    // A persona change restarts the lead between turns: generation 2.
    save_persona(&h, "Kai", "", "");
    let next = lead::owner_input(KEY_A, 2, "ok, go on");
    bounded(m.prompt(KEY_A, 2, Sender::Owner, &next)).await;
    assert_eq!(
        lead_messages(&h)[1],
        next,
        "the old lead's late picks never reach the new one"
    );
    assert!(delivered_rows(&h, 1).is_empty());
    assert!(delivered_rows(&h, 2).is_empty());
    m.shutdown_all().await;
}

/// Runs one KEY_A owner turn whose advisor answers after the wait (picking the KEY_C note `id`),
/// so the pick is kept late; returns with the gate shut again, so the next turn's own call
/// waits and only the late pick can reach it.
async fn turn_with_a_late_pick(
    h: &Harness,
    m: &OmpManager,
    advisor: &Arc<moraebot::recall::Advisor>,
) {
    let gate = reply_gate(h);
    let mut progress = advisor.progress();
    let mut asked = advisor.asked();
    let first = lead::owner_input(KEY_A, 1, "Can we ship the release today?");
    bounded(m.prompt(KEY_A, 1, Sender::Owner, &first)).await;
    bounded(asked.wait_for(|n| *n >= 1)).await.expect("asked");
    std::fs::write(&gate, "").expect("open the gate");
    wakes_done(&mut progress, 1).await;
    std::fs::remove_file(&gate).expect("shut the gate");
    assert_eq!(lead_messages(h), [first.as_str()]);
}

/// moraedang on #70: a late pick is read again before it is rendered; one the owner forgot
/// after the advisor answered is never given to the lead.
#[tokio::test]
async fn a_late_pick_forgotten_before_the_next_message_is_not_delivered() {
    let h = Harness::new();
    let id = seed_key_c(&h);
    let advisor = gated_advisor(&h, &id, 50);
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    turn_with_a_late_pick(&h, &m, &advisor).await;

    qa_memory_store(&h).forget(&id).expect("forget");
    let next = lead::owner_input(KEY_A, 2, "ok, go on");
    bounded(m.prompt(KEY_A, 2, Sender::Owner, &next)).await;
    assert_eq!(
        lead_messages(&h)[1],
        next,
        "the forgotten note never reaches the lead"
    );
    assert!(delivered_rows(&h, 1).is_empty());
    m.shutdown_all().await;
}

/// moraedang on #70: a late pick the owner corrected after the advisor answered goes in as
/// the store holds it now: replaced, so labelled no longer in force.
#[tokio::test]
async fn a_late_pick_corrected_before_the_next_message_is_delivered_as_no_longer_in_force() {
    use moraebot::recall::{RECALLED_CLOSE, RECALLED_LABEL, RECALLED_OPEN};
    let h = Harness::new();
    let id = seed_key_c(&h);
    let advisor = gated_advisor(&h, &id, 50);
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    turn_with_a_late_pick(&h, &m, &advisor).await;

    let corrected = qa_memory_store(&h)
        .correct(
            &id,
            "Releases go out on Thursdays now",
            moraebot::memory::Source::row(9),
        )
        .expect("correct");
    let next = lead::owner_input(KEY_A, 2, "ok, go on");
    bounded(m.prompt(KEY_A, 2, Sender::Owner, &next)).await;
    let block = format!(
        "{RECALLED_OPEN}\n{}\n- {id} [2026-11-20, project {ZEPHYR}, replaced by the owner, no longer in force, owner; {RECALLED_LABEL}] Releases go out on Fridays only\n{RECALLED_CLOSE}",
        moraebot::memory::MEMORY_GUARD
    );
    assert_eq!(lead_messages(&h)[1], format!("{next}\n\n{block}"));
    assert_eq!(delivered_rows(&h, 1), [(id.clone(), Some(2))]);
    assert_ne!(corrected.new.id, id, "the correction is a new entry");
    m.shutdown_all().await;
}

/// moraedang's point 7 on #59: during the wait the steer gate is closed, so a second owner
/// message of the conversation is not steered into a turn that has not started (the
/// dispatcher queues it as usual).
#[tokio::test]
async fn no_steer_joins_a_turn_while_it_waits_for_the_advisor() {
    let h = Harness::new();
    let id = seed_key_c(&h);
    let gate = reply_gate(&h);
    let advisor = gated_advisor(&h, &id, moraebot::config::MAX_RECALL_WAIT_MS);
    let mut asked = advisor.asked();
    let m = Arc::new(h.manager("ok").with_recall(Arc::clone(&advisor)));
    let input = lead::owner_input(KEY_A, 1, "Can we ship the release today?");
    let turn = {
        let (m, input) = (Arc::clone(&m), input.clone());
        tokio::spawn(async move { m.prompt(KEY_A, 1, Sender::Owner, &input).await })
    };
    bounded(asked.wait_for(|n| *n >= 1)).await.expect("asked");
    let mut claimed = false;
    let steer = lead::owner_input(KEY_A, 2, "also the notes");
    assert!(!m.try_steer(KEY_A, 2, Sender::Owner, &steer, |_| {
        claimed = true;
        true
    }));
    assert!(!claimed, "nothing was claimed: the row stays queued");
    assert_eq!(*m.running_turn().borrow(), None);
    assert!(h.frames("prompt").is_empty(), "the lead has no input yet");

    std::fs::write(&gate, "").expect("open the gate");
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(report.steered, None);
    assert_eq!(
        lead_messages(&h),
        [format!("{input}\n\n{}", recalled_for(&id))]
    );
    m.shutdown_all().await;
}

#[tokio::test]
async fn a_failing_advisor_in_on_mode_leaves_the_input_unchanged() {
    let h = Harness::new();
    seed_project_note(&h);
    let advisor = on_advisor(
        &h,
        &[("FAKE_OMP_REPLY", "I would pick the first entry.")],
        moraebot::config::MAX_RECALL_WAIT_MS,
    );
    turn_with_failing_advisor(&h, advisor).await;
    assert!(logged(
        &h,
        moraebot::recall::E_RECALL_OUTPUT,
        "gen=1 epoch=0 row=1"
    ));
    assert!(delivered_rows(&h, 1).is_empty());
}

// ---- moraebot's own notes in a running turn (rowless steers, #59 PR 3b / #68) ----

fn flag() -> (Arc<std::sync::atomic::AtomicBool>, moraebot::omp::OnSettled) {
    let taken = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let set = Arc::clone(&taken);
    (
        taken,
        Box::new(move |took| set.store(took, std::sync::atomic::Ordering::SeqCst)),
    )
}

/// The lead's steer messages, sorted (concurrent steers reach omp in no fixed order).
fn steer_messages(h: &Harness) -> Vec<String> {
    sorted(
        h.steer_frames()
            .iter()
            .map(|f| f["message"].as_str().unwrap_or_default().to_owned())
            .collect(),
    )
}

fn sorted(mut items: Vec<String>) -> Vec<String> {
    items.sort();
    items
}

fn is_set(taken: &std::sync::atomic::AtomicBool) -> bool {
    taken.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test]
async fn a_note_joins_the_running_turn_without_taking_the_owners_steer_slot() {
    let h = Harness::new();
    let manager = h.steer_manager("merged");
    let notes = manager.notes();
    assert_eq!(manager.turn_id(), None, "no turn runs yet");
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    let id = manager.turn_id().expect("the turn runs");
    let (taken, on_taken) = flag();
    assert!(notes.send(
        id,
        moraebot::omp::NoteKind::Recall,
        "qa note body",
        Some(on_taken)
    ));
    // The owner's steer slot is still free: the next owner message is steered in as before.
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, "m2", |_| true));
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(report.main.outcome, Outcome::Reply("echo: m1 + m2".into()));
    assert_eq!(
        report.steered,
        Some((2, Outcome::Reply("echo: m1 + m2".into()))),
        "the note has no row and no reply of its own"
    );
    // Each prompt frame is written by its own wait thread, so the two steers may arrive in
    // either order.
    let note = format!("{}\nqa note body", moraebot::omp::RECALL_NOTE_TAG);
    assert_eq!(
        steer_messages(&h),
        sorted(vec![note.clone(), "m2".to_owned()])
    );
    assert!(is_set(&taken), "the lead took the note");
    assert_eq!(
        lead::parse_tag(&note),
        None,
        "a note never reads as an owner or lane input"
    );

    // The turn ended: a note for it is dropped, and nothing more reaches the lead.
    assert_eq!(manager.turn_id(), None);
    let (late, on_taken) = flag();
    assert!(!notes.send(
        id,
        moraebot::omp::NoteKind::Recall,
        "too late",
        Some(on_taken)
    ));
    assert!(!is_set(&late));
    assert_eq!(h.steer_frames().len(), 2);
    manager.shutdown_all().await;
}

#[tokio::test]
async fn a_note_never_reaches_the_turn_of_a_restarted_lead() {
    let h = Harness::new();
    let manager = Arc::new(
        h.manager("steer")
            .with_extra_env([("FAKE_OMP_STEER_CASE", "merged")])
            .with_cache_window(None),
    );
    let notes = manager.notes();
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    let first = manager.turn_id().expect("first turn");
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, "m2", |_| true));
    bounded(turn).await.expect("prompt task");

    // A persona change restarts the lead between turns; its new child holds the next turn.
    save_persona(&h, "Kai", "", "");
    let turn = spawn_prompt(&manager, KEY_STEER, 3, "m3");
    turn_running(&manager, KEY_STEER).await;
    assert_eq!(h.start_pids().len(), 2, "the lead restarted");
    let second = manager.turn_id().expect("second turn");
    assert_ne!(first, second);
    assert!(!notes.send(first, moraebot::omp::NoteKind::Recall, "old", None));
    assert!(notes.send(second, moraebot::omp::NoteKind::Recall, "new", None));
    assert!(manager.try_steer(KEY_STEER, 4, Sender::Owner, "m4", |_| true));
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(
        report.steered,
        Some((4, Outcome::Reply("echo: m3 + m4".into())))
    );
    let tag = moraebot::omp::RECALL_NOTE_TAG;
    assert_eq!(
        steer_messages(&h),
        sorted(vec![
            "m2".to_owned(),
            format!("{tag}\nnew"),
            "m4".to_owned()
        ]),
        "the old turn's note never went out"
    );
    manager.shutdown_all().await;
}

#[tokio::test]
async fn shutdown_does_not_wait_for_a_note_in_flight() {
    let h = Harness::new();
    let manager = h.steer_manager("merged");
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    let id = manager.turn_id().expect("the turn runs");
    assert!(
        manager
            .notes()
            .send(id, moraebot::omp::NoteKind::Recall, "held", None)
    );
    // The fake holds the note with the main prompt; shutdown kills the lead without waiting.
    bounded(manager.shutdown_all()).await;
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(codes(&report), (Some(omp::E_CHILD_LOST), None));
    assert_eq!(manager.turn_id(), None);
}

#[tokio::test]
async fn a_note_omp_refuses_is_not_taken_and_the_turn_goes_on() {
    let h = Harness::new();
    let manager = Arc::new(h.manager("steer").with_extra_env([
        ("FAKE_OMP_STEER_CASE", "merged"),
        ("FAKE_OMP_REJECT_TRIGGER", "qa-refuse"),
    ]));
    let turn = spawn_prompt(&manager, KEY_STEER, 1, "m1");
    turn_running(&manager, KEY_STEER).await;
    let id = manager.turn_id().expect("the turn runs");
    let (taken, on_taken) = flag();
    assert!(manager.notes().send(
        id,
        moraebot::omp::NoteKind::Recall,
        "qa-refuse body",
        Some(on_taken)
    ));
    assert!(manager.try_steer(KEY_STEER, 2, Sender::Owner, "m2", |_| true));
    let report = bounded(turn).await.expect("prompt task");
    assert_eq!(report.main.outcome, Outcome::Reply("echo: m1 + m2".into()));
    assert_eq!(
        report.steered,
        Some((2, Outcome::Reply("echo: m1 + m2".into())))
    );
    assert!(!is_set(&taken), "a refused note never counts as delivered");
    manager.shutdown_all().await;
}

// ---- Mid-task recall (#59, PR 3b) ----

/// The lead's tool events of a mid-task turn: a bash call with a bearer token in its arguments,
/// then its failure.
const MID_EVENTS: &str = r#"{"type":"tool_execution_start","toolCallId":"mid-1","toolName":"bash","args":{"command":"curl -H 'Authorization: Bearer qatokenabcdefghijklmnopqrstuvwxyz0123'"}}
{"type":"tool_execution_end","toolCallId":"mid-1","toolName":"bash","result":{"content":[{"type":"text","text":"curl: (7) Failed to connect"}]},"isError":true}"#;

/// X: related only to the tool call's arguments (`curl`), never to the owner's message.
const MID_X: &str = "curl calls need vpn first";
const MID_Y: &str = "Quokka habitat survey";

/// Two owner notes in project [`ZEPHYR`] (X, Y); only X shares a word, `curl`, and only with
/// the turn's tool events.
fn seed_mid(h: &Harness) -> (String, String) {
    let mut store = qa_memory_store(h);
    let mut add = |text: &str| {
        store
            .add(qa_note(
                text,
                moraebot::memory::Bucket::Project(ZEPHYR.into()),
            ))
            .expect("note")
            .id
    };
    (add(MID_X), add(MID_Y))
}

/// The file whose existence lets the lead fake write its tool events.
fn events_gate(h: &Harness) -> PathBuf {
    h.dir.path().join("events.gate")
}

/// An advisor answering pick Y then X with one pick per answer: the wake before the turn is
/// offered both (neither related to the message) and picks Y; the tool call makes X related,
/// so a mid wake asks again, is not offered Y and picks X.
fn mid_advisor(
    h: &Harness,
    vars: &[(&str, &str)],
    on: bool,
    adjust: impl FnOnce(&mut Config),
) -> Arc<moraebot::recall::Advisor> {
    use moraebot::config::{MidTask, RecallMode};
    qa_advisor_with(h, vars, |c| {
        let r = &mut c.memory.recall;
        r.max_items = 1;
        if on {
            r.mode = Some(RecallMode::On);
            r.mid_task = Some(MidTask::On);
            r.wait_ms = moraebot::config::MAX_RECALL_WAIT_MS;
        } else {
            r.mode = Some(RecallMode::Shadow);
            r.mid_task = Some(MidTask::Shadow);
        }
        adjust(c);
    })
}

fn pick_y_then_x(x: &str, y: &str) -> String {
    format!(r#"{{"pick":["{y}","{x}"]}}"#)
}

/// A lead in steer mode answering a steer by `case`, writing [`MID_EVENTS`] once
/// [`events_gate`] exists.
fn mid_lead(h: &Harness, case: &str, advisor: &Arc<moraebot::recall::Advisor>) -> Arc<OmpManager> {
    Arc::new(
        h.manager("steer")
            .with_extra_env([
                ("FAKE_OMP_STEER_CASE", case.to_owned()),
                ("FAKE_OMP_STEER_EVENTS", MID_EVENTS.to_owned()),
                (
                    "FAKE_OMP_EVENTS_GATE",
                    events_gate(h).to_string_lossy().into_owned(),
                ),
            ])
            .with_recall(Arc::clone(advisor)),
    )
}

fn spawn_owner_turn(
    manager: &Arc<OmpManager>,
    input: &str,
) -> tokio::task::JoinHandle<GroupReport> {
    let (manager, input) = (Arc::clone(manager), input.to_owned());
    tokio::spawn(async move { manager.prompt(KEY_A, 1, Sender::Owner, &input).await })
}

/// Ends a waiting steer-mode turn with an owner steer.
async fn end_with_owner_steer(
    manager: &OmpManager,
    turn: tokio::task::JoinHandle<GroupReport>,
) -> GroupReport {
    let steer = lead::owner_input(KEY_A, 2, "thanks");
    assert!(manager.try_steer(KEY_A, 2, Sender::Owner, &steer, |_| true));
    bounded(turn).await.expect("prompt task")
}

/// The note the lead gets for the [`ZEPHYR`] owner note `id` saying `text`.
fn recalled_note(id: &str, text: &str) -> String {
    use moraebot::recall::{RECALLED_CLOSE, RECALLED_LABEL, RECALLED_OPEN};
    format!(
        "{}\n{RECALLED_OPEN}\n{}\n- {id} [2026-11-20, project {ZEPHYR}, owner; {RECALLED_LABEL}] {text}\n{RECALLED_CLOSE}",
        moraebot::omp::RECALL_NOTE_TAG,
        moraebot::memory::MEMORY_GUARD
    )
}

/// The wakes of a mid-task turn with [`MID_EVENTS`]: the owner wake, then one per tool event
/// (start and end).
const MID_TURN_WAKES: u64 = 3;

#[tokio::test]
async fn shadow_mid_task_logs_its_picks_and_never_steers() {
    let h = Harness::new();
    let (x, y) = seed_mid(&h);
    let reply = pick_y_then_x(&x, &y);
    let advisor = mid_advisor(&h, &[("FAKE_OMP_REPLY", reply.as_str())], false, |_| {});
    let mut progress = advisor.progress();
    let m = mid_lead(&h, "merged", &advisor);
    let input = lead::owner_input(KEY_A, 1, "check the server");
    let turn = spawn_owner_turn(&m, &input);
    turn_running(&m, KEY_A).await;
    wakes_done(&mut progress, 1).await;
    std::fs::write(events_gate(&h), "").expect("let the tool events come");
    wakes_done(&mut progress, MID_TURN_WAKES).await;
    let report = end_with_owner_steer(&m, turn).await;
    assert!(
        matches!(report.main.outcome, Outcome::Reply(_)),
        "{report:?}"
    );

    // One mid call for the two tool events: X became related once, then nothing was new.
    let asked = advisor_prompts(&h);
    assert_eq!(asked.len(), 2, "{asked:?}");
    let mid = &asked[1];
    assert!(
        mid.starts_with(&format!(
            "<recall-wake max_picks=1>\n[entries]\n- {x} [2026-11-20, project {ZEPHYR}, owner] {MID_X}\n[wake] phase=mid conversation={KEY_A}\n[owner message]\ncheck the server\n[tool events]\ntool bash started: {{\"command\":\"curl -H '[redacted:bearer_token]'\"}}"
        )),
        "{mid}"
    );
    assert!(!mid.contains("qatoken"), "{mid}");
    assert!(!mid.contains(&y), "Y was picked by the turn's wake: {mid}");
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_PICK,
        &format!(
            "gen=1 epoch=0 row=1 offered=1 picks={x}:12 rejected=0 unoffered=1 blocked=0 phase=mid"
        )
    ));
    // Both wakes wrote their shadow rows.
    let mut rows = ledger_rows(&h, 1);
    rows.sort();
    let mut want = vec![(x, "shadow".to_owned()), (y, "shadow".to_owned())];
    want.sort();
    assert_eq!(rows, want);
    // Shadow never touches the lead: its input as sent, and the owner's steer only.
    assert_eq!(lead_messages(&h)[0], input);
    assert_eq!(h.steer_frames().len(), 1);
    m.shutdown_all().await;
}

#[tokio::test]
async fn a_fresh_candidate_at_a_tool_start_is_steered_into_the_running_turn() {
    let h = Harness::new();
    let (x, y) = seed_mid(&h);
    let reply = pick_y_then_x(&x, &y);
    let advisor = mid_advisor(&h, &[("FAKE_OMP_REPLY", reply.as_str())], true, |_| {});
    let mut progress = advisor.progress();
    let m = mid_lead(&h, "note", &advisor);
    let input = lead::owner_input(KEY_A, 1, "check the server");
    let turn = spawn_owner_turn(&m, &input);
    turn_running(&m, KEY_A).await;
    wakes_done(&mut progress, 1).await;
    std::fs::write(events_gate(&h), "").expect("let the tool events come");
    // The fake ends the run when the note comes.
    let report = bounded(turn).await.expect("prompt task");
    wakes_done(&mut progress, MID_TURN_WAKES).await;
    assert!(
        matches!(report.main.outcome, Outcome::Reply(_)),
        "{report:?}"
    );
    assert_eq!(report.steered, None, "a note is not an owner row");

    // Y went with the input; X, which the mid wake picked, came as one note.
    let messages = lead_messages(&h);
    assert_eq!(messages.len(), 2);
    assert!(messages[0].contains(&format!("- {y} [")), "{}", messages[0]);
    let steers = h.steer_frames();
    assert_eq!(steers.len(), 1);
    assert_eq!(steers[0]["message"], recalled_note(&x, MID_X));
    // The mid wake saw the tool start and was never offered Y.
    let asked = advisor_prompts(&h);
    assert_eq!(asked.len(), 2, "{asked:?}");
    assert!(asked[1].contains("tool bash started: "), "{}", asked[1]);
    assert!(!asked[1].contains(&y));
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_PICK,
        &format!(
            "gen=1 epoch=0 row=1 offered=1 picks={x}:12 rejected=0 unoffered=1 blocked=0 phase=mid mode=on"
        )
    ));
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_DELIVERED,
        &format!("gen=1 epoch=0 row=1 ids={x} phase=mid")
    ));
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_DELIVERED,
        &format!("gen=1 epoch=0 row=1 ids={y}")
    ));
    let mut delivered = delivered_rows(&h, 1);
    delivered.sort();
    let mut want = vec![(x, Some(1)), (y, Some(1))];
    want.sort();
    assert_eq!(delivered, want);
    m.shutdown_all().await;
}

/// Unrelated owner notes in [`ZEPHYR`] that take the pool past
/// [`moraebot::recall::POOL_ALL_BYTES`]: from then on a wake is offered only the candidates
/// that share a term with it, so each mid wake below is offered exactly what its events name.
fn pad_pool(h: &Harness) {
    let mut store = qa_memory_store(h);
    let mut bytes = 0;
    for n in 0.. {
        if bytes > moraebot::recall::POOL_ALL_BYTES {
            break;
        }
        let text = (0..150)
            .map(|i| format!("pad{n}w{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        bytes += text.len();
        store
            .add(qa_note(
                &text,
                moraebot::memory::Bucket::Project(ZEPHYR.into()),
            ))
            .expect("pad note");
    }
}

/// Z: related only to the tool result of [`TWO_PICK_EVENTS`] (`zebra`, `paint`).
const MID_Z: &str = "Zebra paint dries overnight";

/// One tool call whose start names X's `curl` and whose end names Z's `zebra paint`: over a
/// padded pool the start's wake is offered X alone, the end's wake X and Z.
const TWO_PICK_EVENTS: &str = r#"{"type":"tool_execution_start","toolCallId":"mid-1","toolName":"bash","args":{"command":"curl example"}}
{"type":"tool_execution_end","toolCallId":"mid-1","toolName":"bash","result":{"content":[{"type":"text","text":"zebra paint failed"}]},"isError":true}"#;

/// X, Z and the padding; Z's id is returned.
fn seed_two_picks(h: &Harness) -> (String, String) {
    let (x, _) = seed_mid(h);
    let z = qa_memory_store(h)
        .add(qa_note(
            MID_Z,
            moraebot::memory::Bucket::Project(ZEPHYR.into()),
        ))
        .expect("note")
        .id;
    pad_pool(h);
    (x, z)
}

/// [`mid_lead`] writing [`TWO_PICK_EVENTS`] instead.
fn two_pick_lead(h: &Harness, advisor: &Arc<moraebot::recall::Advisor>) -> Arc<OmpManager> {
    Arc::new(
        h.manager("steer")
            .with_extra_env([
                ("FAKE_OMP_STEER_CASE", "merged".to_owned()),
                ("FAKE_OMP_STEER_EVENTS", TWO_PICK_EVENTS.to_owned()),
                (
                    "FAKE_OMP_EVENTS_GATE",
                    events_gate(h).to_string_lossy().into_owned(),
                ),
            ])
            .with_recall(Arc::clone(advisor)),
    )
}

fn recall_notes(h: &Harness) -> Vec<String> {
    h.steer_frames()
        .iter()
        .filter_map(|f| f["message"].as_str())
        .filter(|m| m.starts_with(moraebot::omp::RECALL_NOTE_TAG))
        .map(str::to_owned)
        .collect()
}

/// The cooldown counts a note when it is sent: within one turn two mid wakes each pick a
/// different entry (X at the tool start, Z at its end), both calls run, and with
/// `mid_max_delivered = 1` only one note goes in. A fresh lead child sharing the advisor then
/// starts no mid wake in that conversation while the window lasts.
#[tokio::test]
async fn the_cooldown_holds_within_one_turn_and_across_turns() {
    let h = Harness::new();
    let (x, z) = seed_two_picks(&h);
    let reply = format!(r#"{{"pick":["{z}","{x}"]}}"#);
    let advisor = mid_advisor(&h, &[("FAKE_OMP_REPLY", reply.as_str())], true, |c| {
        c.memory.recall.mid_max_delivered = 1;
    });
    let mut progress = advisor.progress();
    let m = two_pick_lead(&h, &advisor);
    let turn = spawn_owner_turn(&m, &lead::owner_input(KEY_A, 1, "check the server"));
    turn_running(&m, KEY_A).await;
    wakes_done(&mut progress, 1).await;
    std::fs::write(events_gate(&h), "").expect("let the tool events come");
    wakes_done(&mut progress, MID_TURN_WAKES).await;
    end_with_owner_steer(&m, turn).await;

    // Both mid wakes asked (X alone, then X and Z): each had its own pick.
    let asked = advisor_prompts(&h);
    assert_eq!(asked.len(), 2, "no owner call, two mid calls: {asked:?}");
    assert!(
        asked.iter().any(|a| a.contains(&x) && !a.contains(&z)),
        "{asked:?}"
    );
    assert!(asked.iter().any(|a| a.contains(&z)), "{asked:?}");
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_PICK,
        &format!(
            "gen=1 epoch=0 row=1 offered=1 picks={x}:12 rejected=0 unoffered=1 blocked=0 phase=mid mode=on"
        )
    ));
    // One note only, though neither had settled when the second was ready.
    let notes = recall_notes(&h);
    assert_eq!(notes.len(), 1, "{notes:?}");
    assert_eq!(delivered_rows(&h, 1).len(), 1);
    m.shutdown_all().await;

    // A fresh lead child (generation 2) sharing the advisor: its tool events wake nothing.
    let before = *progress.borrow();
    let m = two_pick_lead(&h, &advisor);
    let turn = spawn_owner_turn(&m, &lead::owner_input(KEY_A, 3, "check the server again"));
    turn_running(&m, KEY_A).await;
    let steer = lead::owner_input(KEY_A, 4, "thanks");
    bounded(async {
        while !m.try_steer(KEY_A, 4, Sender::Owner, &steer, |_| true) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    bounded(turn).await.expect("prompt task");
    wakes_done(&mut progress, before + 1).await;
    assert_eq!(
        *progress.borrow(),
        before + 1,
        "only the owner wake: no mid wake started"
    );
    assert_eq!(advisor_prompts(&h).len(), 2);
    assert_eq!(recall_notes(&h).len(), 1);
    m.shutdown_all().await;
}

#[tokio::test]
async fn a_mid_pick_for_a_turn_that_ended_is_dropped() {
    let h = Harness::new();
    let (x, y) = seed_mid(&h);
    let reply = pick_y_then_x(&x, &y);
    let gate = reply_gate(&h);
    let gate_path = gate.to_string_lossy().into_owned();
    let advisor = mid_advisor(
        &h,
        &[
            ("FAKE_OMP_REPLY", reply.as_str()),
            ("FAKE_OMP_REPLY_GATE", gate_path.as_str()),
        ],
        true,
        |_| {},
    );
    let mut progress = advisor.progress();
    let mut asked = advisor.asked();
    let m = mid_lead(&h, "merged", &advisor);
    std::fs::write(&gate, "").expect("the turn's wake answers at once");
    let input = lead::owner_input(KEY_A, 1, "check the server");
    let turn = spawn_owner_turn(&m, &input);
    turn_running(&m, KEY_A).await;
    wakes_done(&mut progress, 1).await;
    std::fs::remove_file(&gate).expect("hold the mid wake's answer");
    std::fs::write(events_gate(&h), "").expect("let the tool events come");
    bounded(asked.wait_for(|n| *n >= 2))
        .await
        .expect("mid wake asked");
    let report = end_with_owner_steer(&m, turn).await;
    assert!(
        matches!(report.main.outcome, Outcome::Reply(_)),
        "{report:?}"
    );

    // The answer comes after the turn: nothing is steered, the pick is dropped and logged.
    std::fs::write(&gate, "").expect("release the answer");
    wakes_done(&mut progress, MID_TURN_WAKES).await;
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_DROPPED,
        &format!("gen=1 epoch=0 row=1 ids={x} phase=mid")
    ));
    assert_eq!(delivered_rows(&h, 1), [(y, Some(1))]);
    assert_eq!(h.steer_frames().len(), 1, "the owner's steer only");
    m.shutdown_all().await;
}

#[tokio::test]
async fn mid_wakes_count_against_the_hourly_cap() {
    let h = Harness::new();
    let (x, y) = seed_mid(&h);
    let reply = pick_y_then_x(&x, &y);
    let advisor = mid_advisor(&h, &[("FAKE_OMP_REPLY", reply.as_str())], false, |c| {
        c.memory.recall.max_calls_per_hour = 1;
    });
    let mut progress = advisor.progress();
    let m = mid_lead(&h, "merged", &advisor);
    let turn = spawn_owner_turn(&m, &lead::owner_input(KEY_A, 1, "check the server"));
    turn_running(&m, KEY_A).await;
    wakes_done(&mut progress, 1).await;
    std::fs::write(events_gate(&h), "").expect("let the tool events come");
    wakes_done(&mut progress, MID_TURN_WAKES).await;
    end_with_owner_steer(&m, turn).await;
    assert_eq!(
        advisor_prompts(&h).len(),
        1,
        "the turn's wake used the hour"
    );
    assert!(logged(
        &h,
        moraebot::recall::W_RECALL_CAP,
        "reason=hour gen=1 epoch=0 row=1 phase=mid"
    ));
    m.shutdown_all().await;
}

#[tokio::test]
async fn a_failing_advisor_mid_turn_never_affects_the_turn() {
    let h = Harness::new();
    seed_mid(&h);
    let advisor = mid_advisor(
        &h,
        &[("FAKE_OMP_REPLY", "I would pick the first entry.")],
        true,
        |_| {},
    );
    let mut progress = advisor.progress();
    let m = mid_lead(&h, "merged", &advisor);
    let input = lead::owner_input(KEY_A, 1, "check the server");
    let turn = spawn_owner_turn(&m, &input);
    turn_running(&m, KEY_A).await;
    wakes_done(&mut progress, 1).await;
    std::fs::write(events_gate(&h), "").expect("let the tool events come");
    wakes_done(&mut progress, MID_TURN_WAKES).await;
    let report = end_with_owner_steer(&m, turn).await;
    assert_eq!(
        report.main.outcome,
        Outcome::Reply("echo: check the server + thanks".into())
    );
    assert!(logged(
        &h,
        moraebot::recall::E_RECALL_OUTPUT,
        "gen=1 epoch=0 row=1 phase=mid"
    ));
    assert_eq!(lead_messages(&h)[0], input, "no block, no note");
    assert_eq!(h.steer_frames().len(), 1);
    assert!(delivered_rows(&h, 1).is_empty());
    m.shutdown_all().await;
}

/// R1 M2: a mid wake leaves ledger rows like an owner wake (a shadow pick, a reject), so its
/// verdicts are auditable and an entry judged once is not offered again in this lead context.
#[tokio::test]
async fn mid_wakes_write_shadow_and_rejected_ledger_rows() {
    let h = Harness::new();
    let (x, y) = seed_mid(&h);
    let reply = pick_y_then_x(&x, &y);
    let advisor = mid_advisor(&h, &[("FAKE_OMP_REPLY", reply.as_str())], true, |c| {
        c.memory.recall.mid_task = Some(moraebot::config::MidTask::Shadow);
    });
    let mut progress = advisor.progress();
    let m = mid_lead(&h, "merged", &advisor);
    let turn = spawn_owner_turn(&m, &lead::owner_input(KEY_A, 1, "check the server"));
    turn_running(&m, KEY_A).await;
    wakes_done(&mut progress, 1).await;
    std::fs::write(events_gate(&h), "").expect("let the tool events come");
    wakes_done(&mut progress, MID_TURN_WAKES).await;
    end_with_owner_steer(&m, turn).await;
    assert_eq!(h.steer_frames().len(), 1, "shadow sends no note");
    assert_eq!(delivered_rows(&h, 1), [(y.clone(), Some(1))]);
    assert!(
        ledger_rows(&h, 1).contains(&(x.clone(), "shadow".to_owned())),
        "{:?}",
        ledger_rows(&h, 1)
    );
    let phases: Vec<(String, moraebot::store::RecallPhase)> = h
        .store
        .lock()
        .unwrap()
        .recall_seen(KEY_A, 1, 0, 0)
        .unwrap()
        .into_iter()
        .map(|r| (r.entry_id, r.phase))
        .collect();
    assert!(
        phases.contains(&(x.clone(), moraebot::store::RecallPhase::Mid)),
        "{phases:?}"
    );
    assert!(
        phases.contains(&(y.clone(), moraebot::store::RecallPhase::Owner)),
        "{phases:?}"
    );

    // A rejecting mid wake writes a `rejected` row.
    let h = Harness::new();
    let (x, y) = seed_mid(&h);
    let reply = h.dir.path().join("advisor.reply");
    std::fs::write(&reply, format!(r#"{{"pick":["{y}"]}}"#)).expect("turn reply");
    let reply_path = reply.to_string_lossy().into_owned();
    let advisor = mid_advisor(
        &h,
        &[("FAKE_OMP_REPLY_FILE", reply_path.as_str())],
        true,
        |_| {},
    );
    let mut progress = advisor.progress();
    let m = mid_lead(&h, "merged", &advisor);
    let turn = spawn_owner_turn(&m, &lead::owner_input(KEY_A, 1, "check the server"));
    turn_running(&m, KEY_A).await;
    wakes_done(&mut progress, 1).await;
    std::fs::write(&reply, format!(r#"{{"pick":[],"reject":["{x}"]}}"#)).expect("mid reply");
    std::fs::write(events_gate(&h), "").expect("let the tool events come");
    wakes_done(&mut progress, MID_TURN_WAKES).await;
    end_with_owner_steer(&m, turn).await;
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_PICK,
        "gen=1 epoch=0 row=1 offered=1 picks= rejected=1 unoffered=0 blocked=0 phase=mid mode=on"
    ));
    assert!(
        ledger_rows(&h, 1).contains(&(x.clone(), "rejected".to_owned())),
        "{:?}",
        ledger_rows(&h, 1)
    );
    m.shutdown_all().await;

    // The reject judged tool events, not a message: a later owner wake (on a new lead child,
    // so this turn's own offers are gone) is still offered the entry.
    let asked = advisor_prompts(&h).len();
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    let input = lead::owner_input(KEY_A, 2, "why do curl calls hang?");
    bounded(m.prompt(KEY_A, 2, Sender::Owner, &input)).await;
    wakes_done(&mut progress, MID_TURN_WAKES + 1).await;
    let later = advisor_prompts(&h);
    assert!(
        later[asked..].iter().any(|p| p.contains(&x)),
        "{:?}",
        &later[asked..]
    );
    m.shutdown_all().await;
}

/// R1 M4: calls still running (here two mid wakes') never make the next owner wake busy: mid
/// wakes leave one place for owner wakes.
#[tokio::test]
async fn calls_still_running_never_make_the_next_owner_wake_busy() {
    let h = Harness::new();
    let (x, z) = seed_two_picks(&h);
    let reply = format!(r#"{{"pick":["{z}","{x}"]}}"#);
    let gate = reply_gate(&h);
    let gate_path = gate.to_string_lossy().into_owned();
    let advisor = mid_advisor(
        &h,
        &[
            ("FAKE_OMP_REPLY", reply.as_str()),
            ("FAKE_OMP_REPLY_GATE", gate_path.as_str()),
        ],
        true,
        // 0 never waits: no turn depends on how fast the fake advisor answers.
        |c| c.memory.recall.wait_ms = 0,
    );
    let mut progress = advisor.progress();
    let mut asked = advisor.asked();
    let m = two_pick_lead(&h, &advisor);
    let turn = spawn_owner_turn(&m, &lead::owner_input(KEY_A, 1, "check the server"));
    turn_running(&m, KEY_A).await;
    wakes_done(&mut progress, 1).await;
    std::fs::write(events_gate(&h), "").expect("let the tool events come");
    bounded(asked.wait_for(|n| *n >= 2))
        .await
        .expect("both mid wakes asked");
    end_with_owner_steer(&m, turn).await;

    // A new note makes the next owner wake fresh; both mid calls still run.
    qa_memory_store(&h)
        .add(qa_note(
            "Walrus tusks grow slowly",
            moraebot::memory::Bucket::Project(ZEPHYR.into()),
        ))
        .expect("note");
    let next = lead::owner_input(KEY_A, 3, "how do walrus tusks grow?");
    bounded(m.prompt(KEY_A, 3, Sender::Owner, &next)).await;
    bounded(asked.wait_for(|n| *n >= 3))
        .await
        .expect("the owner wake reached the advisor");
    assert!(!logged(
        &h,
        moraebot::recall::W_RECALL_CAP,
        "reason=busy gen=1 epoch=0 row=3"
    ));
    std::fs::write(&gate, "").expect("release the answers");
    wakes_done(&mut progress, MID_TURN_WAKES + 1).await;
    m.shutdown_all().await;
}

/// A call that failed offered nothing: the next wake with the same candidates asks again.
#[tokio::test]
async fn after_a_failed_call_the_next_wake_asks_again() {
    let h = Harness::new();
    seed_project_note(&h);
    let reply = h.dir.path().join("advisor.reply");
    std::fs::write(&reply, "I would pick the first entry.").expect("bad reply");
    let reply_path = reply.to_string_lossy().into_owned();
    let advisor = qa_advisor(&h, &[("FAKE_OMP_REPLY_FILE", reply_path.as_str())]);
    let mut progress = advisor.progress();
    let m = h.manager("ok").with_recall(Arc::clone(&advisor));
    let input = lead::owner_input(KEY_A, 1, "Can we ship the release today?");
    m.prompt(KEY_A, 1, Sender::Owner, &input).await;
    wakes_done(&mut progress, 1).await;
    assert!(logged(
        &h,
        moraebot::recall::E_RECALL_OUTPUT,
        "gen=1 epoch=0 row=1"
    ));
    std::fs::write(&reply, r#"{"pick":[]}"#).expect("good reply");
    let input = lead::owner_input(KEY_A, 2, "Can we ship the release today?");
    m.prompt(KEY_A, 2, Sender::Owner, &input).await;
    wakes_done(&mut progress, 2).await;
    assert_eq!(
        advisor_prompts(&h).len(),
        2,
        "the same candidates asked again"
    );
    // An answered call keeps them offered: a third wake has nothing new.
    let input = lead::owner_input(KEY_A, 3, "Can we ship the release today?");
    m.prompt(KEY_A, 3, Sender::Owner, &input).await;
    wakes_done(&mut progress, 3).await;
    assert_eq!(advisor_prompts(&h).len(), 2);
    m.shutdown_all().await;
}

/// On mode: an owner wake that misses `wait_ms` while its turn still runs steers its picks
/// into that turn as a recalled note, recorded as delivered once the lead took it.
#[tokio::test]
async fn a_late_owner_pick_is_steered_into_its_running_turn() {
    let h = Harness::new();
    let (_, y) = seed_mid(&h);
    let reply = format!(r#"{{"pick":["{y}"]}}"#);
    let gate = reply_gate(&h);
    let gate_path = gate.to_string_lossy().into_owned();
    let advisor = mid_advisor(
        &h,
        &[
            ("FAKE_OMP_REPLY", reply.as_str()),
            ("FAKE_OMP_REPLY_GATE", gate_path.as_str()),
        ],
        true,
        |c| {
            c.memory.recall.wait_ms = 0;
            c.memory.recall.mid_task = Some(moraebot::config::MidTask::Off);
        },
    );
    let mut progress = advisor.progress();
    let mut asked = advisor.asked();
    let m = Arc::new(
        h.manager("steer")
            .with_extra_env([("FAKE_OMP_STEER_CASE", "merged")])
            .with_recall(Arc::clone(&advisor)),
    );
    let input = lead::owner_input(KEY_A, 1, "check the server");
    let turn = spawn_owner_turn(&m, &input);
    turn_running(&m, KEY_A).await;
    bounded(asked.wait_for(|n| *n >= 1))
        .await
        .expect("owner wake asked");
    std::fs::write(&gate, "").expect("the answer comes after the wait");
    wakes_done(&mut progress, 1).await;
    let report = end_with_owner_steer(&m, turn).await;
    assert!(
        matches!(report.main.outcome, Outcome::Reply(_)),
        "{report:?}"
    );
    assert_eq!(lead_messages(&h)[0], input, "the input went without it");
    let steers = h.steer_frames();
    assert_eq!(steers.len(), 2, "{steers:?}");
    assert_eq!(steers[0]["message"], recalled_note(&y, MID_Y));
    assert_eq!(delivered_rows(&h, 1), [(y.clone(), Some(1))]);
    assert!(logged(
        &h,
        moraebot::recall::I_RECALL_DELIVERED,
        &format!("gen=1 epoch=0 row=1 ids={y}")
    ));
    // Not kept for the next owner input as well.
    let next = lead::owner_input(KEY_A, 3, "and now?");
    bounded(m.prompt(KEY_A, 3, Sender::Owner, &next)).await;
    let messages = lead_messages(&h);
    let last = messages.last().expect("the next input");
    assert!(last.contains("and now?") && !last.contains(&y), "{last}");
    m.shutdown_all().await;
}

// --- Prompt-cache pinning ---

/// Why the lead child was last closed to be started again, if it was.
fn last_restart(m: &OmpManager) -> Option<&'static str> {
    m.lead_cache().last_restart.map(|(_, reason)| reason)
}

#[tokio::test]
async fn a_warm_lead_takes_a_persona_change_as_one_update_block() {
    let h = Harness::new();
    save_sora(&h);
    let m = h.manager("ok");
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    let pid = m.child_pid().await;
    save_persona(&h, "Kai", "", "");
    assert!(
        m.lead_cache().memory_waiting,
        "the change waits for a restart"
    );
    bounded(m.prompt(KEY_A, 2, Sender::Owner, "again")).await;
    bounded(m.prompt(KEY_A, 3, Sender::Owner, "more")).await;
    let same = m.child_pid().await;
    let waiting = m.lead_cache().memory_waiting;
    bounded(m.shutdown_all()).await;

    assert_eq!(same, pid, "the warm lead keeps its child and its prompt");
    assert_eq!(h.start_pids().len(), 1);
    assert!(waiting);
    let messages = prompt_messages(&h);
    assert_eq!(messages.len(), 3);
    assert!(!messages[0].contains(moraebot::pin::UPDATE_OPEN));
    assert!(
        messages[1].starts_with("again")
            && messages[1].contains(moraebot::pin::UPDATE_OPEN)
            && messages[1].contains("Name: Kai")
            && messages[1].contains("Name: Sora"),
        "{}",
        messages[1]
    );
    assert!(
        !messages[2].contains(moraebot::pin::UPDATE_OPEN),
        "the update goes in once: {}",
        messages[2]
    );
    assert_eq!(last_restart(&m), None);
}

#[tokio::test]
async fn a_change_back_to_the_pinned_prompt_says_so_once() {
    let h = Harness::new();
    save_sora(&h);
    let m = h.manager("ok");
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "one")).await;
    save_persona(&h, "Kai", "", "");
    bounded(m.prompt(KEY_A, 2, Sender::Owner, "two")).await;
    save_sora(&h);
    bounded(m.prompt(KEY_A, 3, Sender::Owner, "three")).await;
    bounded(m.prompt(KEY_A, 4, Sender::Owner, "four")).await;
    bounded(m.shutdown_all()).await;

    assert_eq!(h.start_pids().len(), 1);
    let messages = prompt_messages(&h);
    assert!(messages[1].contains(moraebot::pin::UPDATE_OPEN));
    assert!(
        messages[2].contains(moraebot::pin::REVERT_HEADER)
            && !messages[2].contains(moraebot::pin::UPDATE_HEADER),
        "{}",
        messages[2]
    );
    assert!(
        !messages[3].contains(moraebot::pin::REVERT_HEADER)
            && !messages[3].contains(moraebot::pin::UPDATE_OPEN),
        "{}",
        messages[3]
    );
}

/// An update block taken in the turn a compaction ran in may survive it: a later change back
/// to the pinned prompt still says so.
#[tokio::test]
async fn a_change_back_after_a_compaction_still_says_so() {
    let h = Harness::new();
    save_sora(&h);
    let compaction = serde_json::json!({"frame": {
        "type": "auto_compaction_end",
        "action": "context-full",
        "aborted": false,
        "willRetry": false,
        "result": {"summary": "earlier turns", "firstKeptEntryId": "e9", "tokensBefore": 190000}
    }});
    let m = h
        .manager("tools")
        .with_extra_env([("FAKE_OMP_TOOL_CALLS", compaction.to_string())]);
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "one")).await;
    save_persona(&h, "Kai", "", "");
    bounded(m.prompt(KEY_A, 2, Sender::Owner, "run-tools")).await;
    save_sora(&h);
    bounded(m.prompt(KEY_A, 3, Sender::Owner, "three")).await;
    bounded(m.shutdown_all()).await;

    assert_eq!(
        h.start_pids().len(),
        1,
        "back to the pinned prompt: no restart"
    );
    let messages = prompt_messages(&h);
    assert!(
        messages[1].contains(moraebot::pin::UPDATE_HEADER),
        "{}",
        messages[1]
    );
    assert!(
        messages[2].contains(moraebot::pin::REVERT_HEADER),
        "{}",
        messages[2]
    );
}

/// A lead started on its earlier session may hold update blocks from before: its first input
/// says the system prompt holds; a new session starts with nothing to withdraw.
#[tokio::test]
async fn a_resumed_lead_session_hears_the_prompt_holds_once() {
    let h = Harness::new();
    save_sora(&h);
    let m1 = h.manager("ok");
    bounded(m1.prompt(KEY_A, 1, Sender::Owner, "one")).await;
    bounded(m1.shutdown_all()).await;
    let m2 = h.manager("ok");
    bounded(m2.prompt(KEY_A, 2, Sender::Owner, "two")).await;
    bounded(m2.prompt(KEY_A, 3, Sender::Owner, "three")).await;
    bounded(m2.new_session()).await.expect("new session");
    bounded(m2.prompt(KEY_A, 4, Sender::Owner, "four")).await;
    bounded(m2.shutdown_all()).await;

    let messages = prompt_messages(&h);
    assert_eq!(messages.len(), 4);
    assert!(!messages[0].contains(moraebot::pin::UPDATE_OPEN));
    assert!(
        messages[1].starts_with("two") && messages[1].contains(moraebot::pin::REVERT_HEADER),
        "{}",
        messages[1]
    );
    assert!(
        !messages[2].contains(moraebot::pin::UPDATE_OPEN),
        "{}",
        messages[2]
    );
    assert!(
        !messages[3].contains(moraebot::pin::UPDATE_OPEN),
        "{}",
        messages[3]
    );
}

#[tokio::test]
async fn a_lead_idle_past_the_cache_window_restarts_with_the_new_prompt() {
    let h = Harness::new();
    save_sora(&h);
    let m = h.manager("ok").with_cache_window(Some(Duration::ZERO));
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    bounded(m.prompt(KEY_A, 2, Sender::Owner, "unchanged")).await;
    let before = h.start_pids().len();
    save_persona(&h, "Kai", "", "");
    let r = bounded(m.prompt(KEY_A, 3, Sender::Owner, "again")).await;
    let reason = last_restart(&m);
    let waiting = m.lead_cache().memory_waiting;
    bounded(m.shutdown_all()).await;

    assert_eq!(before, 1, "no change, no restart");
    assert_eq!(r.main.outcome, Outcome::Reply("echo: again".into()));
    assert_eq!(h.start_pids().len(), 2);
    let appends = appends(&h);
    assert!(appends[1].contains("Name: Kai") && !appends[1].contains("Name: Sora"));
    let messages = prompt_messages(&h);
    assert!(!messages[2].contains(moraebot::pin::UPDATE_OPEN));
    assert_eq!(reason, Some("memory_idle"));
    assert!(!waiting);
}

#[tokio::test]
async fn a_compacted_lead_restarts_at_the_next_prompt_after_a_change() {
    let h = Harness::new();
    save_sora(&h);
    let compaction = serde_json::json!({"frame": {
        "type": "auto_compaction_end",
        "action": "context-full",
        "aborted": false,
        "willRetry": false,
        "result": {"summary": "earlier turns", "firstKeptEntryId": "e9", "tokensBefore": 190000}
    }});
    let m = h
        .manager("tools")
        .with_extra_env([("FAKE_OMP_TOOL_CALLS", compaction.to_string())]);
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "run-tools")).await;
    bounded(m.prompt(KEY_A, 2, Sender::Owner, "unchanged")).await;
    let before = h.start_pids().len();
    save_persona(&h, "Kai", "", "");
    bounded(m.prompt(KEY_A, 3, Sender::Owner, "again")).await;
    let reason = last_restart(&m);
    bounded(m.shutdown_all()).await;

    assert_eq!(before, 1, "a compaction alone never restarts the lead");
    assert_eq!(h.start_pids().len(), 2);
    assert!(appends(&h)[1].contains("Name: Kai"));
    assert_eq!(reason, Some("memory_compacted"));
}

#[tokio::test]
async fn the_refresh_command_restarts_a_warm_lead_once() {
    let h = Harness::new();
    save_sora(&h);
    let m = h.manager("ok");
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    m.refresh_memory();
    bounded(m.prompt(KEY_A, 2, Sender::Owner, "same prompt")).await;
    let unchanged = h.start_pids().len();
    save_persona(&h, "Kai", "", "");
    m.refresh_memory();
    bounded(m.prompt(KEY_A, 3, Sender::Owner, "again")).await;
    let reason = last_restart(&m);
    save_persona(&h, "Mia", "", "");
    bounded(m.prompt(KEY_A, 4, Sender::Owner, "later")).await;
    bounded(m.shutdown_all()).await;

    assert_eq!(unchanged, 1, "nothing to set again: no restart");
    assert_eq!(h.start_pids().len(), 2, "the request is used up");
    assert!(appends(&h)[1].contains("Name: Kai"));
    assert_eq!(reason, Some("memory_requested"));
    let messages = prompt_messages(&h);
    assert!(!messages[2].contains(moraebot::pin::UPDATE_OPEN));
    assert!(messages[3].contains(moraebot::pin::UPDATE_OPEN) && messages[3].contains("Name: Mia"));
}

#[tokio::test]
async fn cache_minutes_zero_restarts_on_any_change() {
    let mut h = Harness::new();
    h.config.lead.cache_minutes = Some(0);
    save_sora(&h);
    let m = h.manager("ok");
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "hi")).await;
    save_persona(&h, "Kai", "", "");
    bounded(m.prompt(KEY_A, 2, Sender::Owner, "again")).await;
    let reason = last_restart(&m);
    bounded(m.shutdown_all()).await;

    assert_eq!(h.start_pids().len(), 2);
    assert!(appends(&h)[1].contains("Name: Kai"));
    assert!(!prompt_messages(&h)[1].contains(moraebot::pin::UPDATE_OPEN));
    assert_eq!(reason, Some("unpinned"));
}

#[tokio::test]
async fn outside_text_provenance_survives_a_restart_until_a_new_session() {
    use moraebot::memory::MemoryHandle;
    use moraebot::memory_tools::MEMORY_WRITE_TOOL;

    let h = Harness::new();
    save_sora(&h);
    let memory = MemoryHandle::spawn(qa_memory_store(&h)).expect("memory worker");
    let write = serde_json::json!({
        "tool": MEMORY_WRITE_TOOL,
        "arguments": {"text": "rusqlite 0.43 needs Rust 1.89", "bucket_hint": "thread", "why": "qa"}
    });
    // Each tool turn saves first, then omp reports a web search the lead ran.
    let web = serde_json::json!({"frame": {
        "type": "tool_execution_start",
        "toolCallId": "web-1",
        "toolName": "web_search",
        "args": {"query": "rusqlite msrv"}
    }});
    let script = [write, web].map(|v| v.to_string()).join("\n");
    let today = moraebot::memory::Day::parse("2026-11-20").unwrap();
    let m = h
        .manager("tools")
        .with_extra_env([("FAKE_OMP_TOOL_CALLS", script)])
        .with_memory_clock(Arc::new(move || today))
        .with_memory(memory.clone())
        .with_cache_window(None);
    let turn = |n: i64| {
        let row = owner_row(&h, &format!("m-pin-{n}"), "run-tools");
        (row, lead::owner_input(KEY_A, row, "run-tools"))
    };
    let (row, input) = turn(1);
    bounded(m.prompt(KEY_A, row, Sender::Owner, &input)).await;
    // A persona change restarts the lead: a new child of the same session.
    save_persona(&h, "Kai", "", "");
    let (row, input) = turn(2);
    bounded(m.prompt(KEY_A, row, Sender::Owner, &input)).await;
    let restarted = h.start_pids().len();
    bounded(m.new_session()).await.expect("new session");
    let (row, input) = turn(3);
    bounded(m.prompt(KEY_A, row, Sender::Owner, &input)).await;
    let results = h.tool_results();
    bounded(m.shutdown_all()).await;

    assert_eq!(restarted, 2);
    assert_eq!(results.len(), 3, "{results:?}");
    let prov: Vec<String> = results
        .iter()
        .map(|(_, text)| {
            let saved: Value = serde_json::from_str(text).expect("write json");
            saved["provenance"].as_str().expect("provenance").to_owned()
        })
        .collect();
    assert_ne!(prov[0], "tool", "nothing outside was read yet");
    assert_eq!(
        prov[1], "tool",
        "the session read outside text before the restart"
    );
    assert_eq!(prov[2], prov[0], "a new session starts clean");
}

#[tokio::test]
async fn usage_of_lead_turns_is_summed_for_the_cache_hit_rate() {
    let h = Harness::new();
    let usage = r#"{"input":10,"output":5,"cacheRead":970,"cacheWrite":20}"#;
    let m = h
        .manager("ok")
        .with_extra_env([("FAKE_OMP_USAGE", usage.to_owned())]);
    let empty = m.lead_cache();
    bounded(m.prompt(KEY_A, 1, Sender::Owner, "one")).await;
    bounded(m.prompt(KEY_A, 2, Sender::Owner, "two")).await;
    let cache = m.lead_cache();
    bounded(m.shutdown_all()).await;

    assert_eq!(empty.turns, 0);
    assert_eq!(empty.recent.hit_percent(), None);
    assert_eq!(cache.turns, 2);
    assert_eq!(
        cache.recent,
        omp::TokenUsage {
            input: 20,
            output: 10,
            cache_read: 1940,
            cache_write: 40,
        }
    );
    assert_eq!(cache.recent.hit_percent(), Some(97));
}
