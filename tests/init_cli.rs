use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use moraebot::config::Platform;
use moraebot::init::{install_command, next_steps};

const SENTINEL: &str = "TOKEN_SENTINEL_9f3a";

/// A temp folder short enough for the default bridge socket under it: on macOS the
/// default is `~/Library/Application Support/moraebot/slack-bridge.sock`, and a home under
/// the per-user temp folder (`/var/folders/..`) pushes it past the 103 bytes
/// `sockaddr_un` allows there.
fn short_tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("mb")
        .tempdir_in("/tmp")
        .expect("tempdir")
}

fn moraebot(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_moraebot"));
    cmd.env("MORAEBOT_HOME", home)
        .env("HOME", home)
        .env_remove("XDG_RUNTIME_DIR");
    cmd
}

/// Answers the overlay check of `check-config` (`omp_overlay::check`) like omp 18.22.2.
fn fake_executable(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("omp-stand-in");
    let script = "#!/bin/sh\n\
                  if [ \"$1\" = --version ]; then echo omp/18.22.2; exit 0; fi\n\
                  printf '{\"key\": \"%s\"}\\n' \"$3\"\n";
    fs::write(&path, script).expect("write executable");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod");
    path
}

fn run_init(home: &Path, input: &str) -> std::process::Output {
    let mut child = moraebot(home)
        .arg("init")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn moraebot");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write answers");
    child.wait_with_output().expect("wait")
}

#[test]
fn piped_init_writes_valid_config_and_private_token() {
    let tmp = short_tempdir();
    let home = tmp.path().join("home");
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());

    let output = run_init(&home, &input);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout: {stdout}\nstderr: {stderr}"
    );

    let token_path = home.join("secrets/discord.token");
    let mode = fs::metadata(&token_path)
        .expect("token file")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
    assert_eq!(fs::read_to_string(&token_path).expect("token"), SENTINEL);

    let config_text = fs::read_to_string(home.join("config.toml")).expect("config.toml");
    for (label, text) in [("stdout", &stdout), ("stderr", &stderr)] {
        assert!(!text.contains(SENTINEL), "token leaked to {label}");
    }
    assert!(
        !config_text.contains(SENTINEL),
        "token leaked to config.toml"
    );

    let check = moraebot(&home)
        .arg("check-config")
        .output()
        .expect("check-config");
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stdout)
    );
}

fn mode_of(path: &Path) -> u32 {
    fs::metadata(path).expect("metadata").permissions().mode() & 0o777
}

#[test]
fn discord_init_works_where_no_bridge_socket_fits() {
    let tmp = short_tempdir();
    // Too deep for any bridge socket path: init and setup discord never need one.
    let home = tmp.path().join("d".repeat(120)).join("home");
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());
    let output = run_init(&home, &input);
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let output = run_with_input(&home, &["setup", "discord"], "\n\n\n");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let before = fs::read(home.join("config.toml")).expect("config");
    let output = run_with_input(&home, &["setup", "slack"], "");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("E_BRIDGE_SOCKET"));
    assert_eq!(fs::read(home.join("config.toml")).expect("config"), before);
}

#[test]
fn init_writes_the_onboarding_request() {
    let tmp = short_tempdir();
    let home = tmp.path().join("qa-mbhome");
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());

    let output = run_init(&home, &input);
    assert_eq!(output.status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .ends_with(&next_steps(std::env::consts::OS, Platform::Discord))
    );
    assert_eq!(mode_of(&home.join("onboarding.request")), 0o600);
    assert_eq!(
        fs::read_to_string(home.join("onboarding.request")).expect("request"),
        "dm\n"
    );
}

#[test]
fn onboarding_command_writes_the_request() {
    let tmp = short_tempdir();
    let home = home_without_request(tmp.path());
    let request = home.join("onboarding.request");

    let output = moraebot(&home)
        .arg("onboarding")
        .output()
        .expect("onboarding");
    assert_eq!(output.status.code(), Some(0));
    let printed: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout is JSON");
    assert_eq!(
        printed,
        serde_json::json!({"onboarding": "requested", "greet": "dm"})
    );
    assert_eq!(mode_of(&request), 0o600);
    assert_eq!(fs::read_to_string(&request).expect("request"), "dm\n");
    assert!(!home.join("inbox.sqlite").exists());
}

/// A home set up by `init` whose onboarding request was removed again.
fn home_without_request(tmp: &Path) -> std::path::PathBuf {
    let home = tmp.join("qa-mbhome");
    let omp = fake_executable(tmp);
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());
    assert!(run_init(&home, &input).status.success());
    fs::remove_file(home.join("onboarding.request")).expect("init left a request");
    home
}

#[test]
fn onboarding_command_can_greet_in_the_channel() {
    let tmp = short_tempdir();
    let home = home_without_request(tmp.path());

    let output = moraebot(&home)
        .args(["onboarding", "--greet", "channel", "--config"])
        .arg(home.join("config.toml"))
        .output()
        .expect("onboarding");
    assert_eq!(output.status.code(), Some(0));
    let printed: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout is JSON");
    assert_eq!(
        printed,
        serde_json::json!({"onboarding": "requested", "greet": "channel"})
    );
    assert_eq!(
        fs::read_to_string(home.join("onboarding.request")).expect("request"),
        "channel\n"
    );
}

#[test]
fn onboarding_command_refuses_an_unknown_place() {
    let tmp = short_tempdir();
    let home = home_without_request(tmp.path());

    let output = moraebot(&home)
        .args(["onboarding", "--greet", "thread", "--config"])
        .arg(home.join("config.toml"))
        .output()
        .expect("onboarding");
    assert_ne!(output.status.code(), Some(0));
    assert!(!home.join("onboarding.request").exists());
}

#[test]
fn onboarding_command_without_config_fails() {
    let tmp = short_tempdir();
    let home = tmp.path().join("qa-mbhome");
    fs::create_dir(&home).expect("mkdir");
    let output = moraebot(&home)
        .arg("onboarding")
        .output()
        .expect("onboarding");
    assert_eq!(output.status.code(), Some(1));
    assert!(!home.join("onboarding.request").exists());
}

#[test]
fn check_config_fails_on_loose_token_mode() {
    let tmp = short_tempdir();
    let home = tmp.path().join("home");
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());
    assert!(run_init(&home, &input).status.success());

    let token_path = home.join("secrets/discord.token");
    fs::set_permissions(&token_path, fs::Permissions::from_mode(0o644)).expect("chmod");
    let check = moraebot(&home)
        .arg("check-config")
        .output()
        .expect("check-config");
    assert_eq!(check.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&check.stdout).contains("E_TOKEN_MODE"));
}

/// moraedang on #46: `check-config` never prints a key written into `config.toml`.
#[test]
fn check_config_never_prints_a_key_written_into_the_config() {
    let tmp = short_tempdir();
    let home = tmp.path().join("home");
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());
    assert!(run_init(&home, &input).status.success());
    let config = home.join("config.toml");
    let mut text = fs::read_to_string(&config).expect("config.toml");
    text.push_str("\n[memory]\napi_key = \"JEV_KEY_SENTINEL_5a0c\"\n");
    fs::write(&config, text).expect("write config");

    let check = moraebot(&home)
        .arg("check-config")
        .output()
        .expect("check-config");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&check.stdout),
        String::from_utf8_lossy(&check.stderr)
    );
    assert_eq!(check.status.code(), Some(1), "{out}");
    assert!(out.contains("E_CONFIG_PARSE"), "{out}");
    assert!(!out.contains("JEV_KEY_SENTINEL_5a0c"), "{out}");
}

#[test]
fn unknown_subcommand_exits_2() {
    let tmp = short_tempdir();
    let output = moraebot(tmp.path()).arg("bogus").output().expect("run");
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn config_carries_its_own_home() {
    let tmp = short_tempdir();
    let home = tmp.path().join("home");
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());
    assert!(run_init(&home, &input).status.success());

    let config_text = fs::read_to_string(home.join("config.toml")).expect("config.toml");
    assert!(config_text.contains("home = "));

    let other_home = tmp.path().join("other-home");
    fs::create_dir(&other_home).expect("mkdir");
    let check = Command::new(env!("CARGO_BIN_EXE_moraebot"))
        .env_remove("MORAEBOT_HOME")
        .env("HOME", &other_home)
        .arg("check-config")
        .arg("--config")
        .arg(home.join("config.toml"))
        .output()
        .expect("check-config");
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stdout)
    );
    assert!(!other_home.join(".moraebot").exists());
}

const BOT: &str = "xoxb-test-SENTINEL1";
const APP: &str = "xapp-test-SENTINEL2";

fn run_with_input(home: &Path, args: &[&str], input: &str) -> std::process::Output {
    let mut child = moraebot(home)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn moraebot");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write answers");
    child.wait_with_output().expect("wait")
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut decoded = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).expect("hex digits");
            decoded.push(u8::from_str_radix(hex, 16).expect("hex"));
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(decoded).expect("utf-8")
}

#[test]
fn init_then_setup_slack_writes_both_configs_and_activates_the_bridge() {
    let tmp = short_tempdir();
    let home = tmp.path().join("home");
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());
    assert!(run_init(&home, &input).status.success());

    let output = run_with_input(
        &home,
        &["setup", "slack"],
        &format!("U111\nC222\n{BOT}\n{APP}\n"),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}");

    for name in ["slack-bot.token", "slack-app.token"] {
        let mode = fs::metadata(home.join("secrets").join(name))
            .expect("token file")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "{name}");
    }
    let config_text = fs::read_to_string(home.join("config.toml")).expect("config.toml");
    let bridge_text = fs::read_to_string(home.join("slack-bridge.toml")).expect("bridge config");
    let config: toml::Table = toml::from_str(&config_text).expect("config TOML");
    let bridge_file: toml::Table = toml::from_str(&bridge_text).expect("bridge TOML");
    assert_eq!(config["messenger"].as_str(), Some("bridge"));
    assert!(config.contains_key("discord") && !config.contains_key("slack"));
    let socket = config["bridge"]["socket"].as_str().expect("bridge.socket");
    assert_eq!(
        bridge_file["socket"].as_str(),
        Some(socket),
        "one socket path"
    );
    // The OS default (`XDG_RUNTIME_DIR` is removed, so Linux falls back to the home).
    let default_socket = if cfg!(target_os = "macos") {
        home.join("Library/Application Support/moraebot/slack-bridge.sock")
    } else {
        home.join("run/slack-bridge.sock")
    };
    assert_eq!(socket, default_socket.to_str().expect("utf-8"));
    assert_eq!(bridge_file["channel_id"].as_str(), Some("C222"));
    for text in [stdout.as_ref(), config_text.as_str(), bridge_text.as_str()] {
        assert!(!text.contains("SENTINEL"), "token leaked");
    }

    let check = moraebot(&home)
        .arg("check-config")
        .output()
        .expect("check-config");
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stdout)
    );
}

#[test]
fn setup_prints_manifest_link_that_decodes_to_manifest() {
    let tmp = short_tempdir();
    let home = tmp.path().join("home");
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());
    assert!(run_init(&home, &input).status.success());

    let output = run_with_input(
        &home,
        &["setup", "slack"],
        &format!("U111\nC222\n{BOT}\n{APP}\n"),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let link = stdout
        .lines()
        .find(|line| line.starts_with("https://api.slack.com/apps?new_app=1&manifest_json="))
        .expect("link line");
    let encoded = link.split("manifest_json=").nth(1).expect("parameter");
    let printed: serde_json::Value =
        serde_json::from_str(&percent_decode(encoded)).expect("decoded manifest is JSON");
    let file: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/docs/guides/slack-manifest.json"
        ))
        .expect("manifest file"),
    )
    .expect("manifest JSON");
    assert_eq!(printed, file);

    let description = printed["features"]["agent_view"]["agent_description"]
        .as_str()
        .expect("agent_description");
    assert!((1..=300).contains(&description.chars().count()));
    let scopes = printed["oauth_config"]["scopes"]["bot"]
        .as_array()
        .expect("bot scopes");
    assert!(scopes.iter().any(|scope| scope == "assistant:write"));
}

#[test]
fn failed_setup_leaves_config_byte_identical() {
    let tmp = short_tempdir();
    let home = tmp.path().join("home");
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());
    assert!(run_init(&home, &input).status.success());
    let before = fs::read(home.join("config.toml")).expect("config");

    let output = run_with_input(&home, &["setup", "slack"], "U111\nC222\nxapp-test-x\n");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("E_SLACK_TOKEN_KIND"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("E_INIT_EOF"));
    assert_eq!(fs::read(home.join("config.toml")).expect("config"), before);
}

/// A home with a pre-bridge config: `messenger = "slack"` and a `[slack]` section with
/// an allowlist, plus both token files. Returns the home and the config bytes.
fn legacy_slack_home(tmp: &Path) -> (PathBuf, Vec<u8>) {
    let home = tmp.join("home");
    let secrets = home.join("secrets");
    fs::create_dir_all(&secrets).expect("mkdir");
    for dir in [&home, &secrets] {
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).expect("chmod dir");
    }
    for (name, token) in [("slack-bot.token", BOT), ("slack-app.token", APP)] {
        let path = secrets.join(name);
        fs::write(&path, token).expect("token");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("chmod token");
    }
    let omp = fake_executable(tmp);
    let text = format!(
        "messenger = \"slack\"\nomp_path = {}\naccess_mode = \"allowlist\"\n\n\
         [slack]\nowner_id = \"U111\"\nchannel_id = \"C222\"\n\
         bot_token_file = \"~/secrets/slack-bot.token\"\n\
         app_token_file = \"~/secrets/slack-app.token\"\nmax_bot_replies = 5\n\n\
         [[slack.allow]]\nid = \"B0BOT\"\nkind = \"bot\"\n",
        toml::Value::from(omp.to_str().expect("utf-8 path"))
    );
    fs::write(home.join("config.toml"), &text).expect("legacy config");
    (home, text.into_bytes())
}

fn check_config(home: &Path) -> (bool, String) {
    let output = moraebot(home)
        .arg("check-config")
        .output()
        .expect("check-config");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), text)
}

#[test]
fn setup_slack_moves_a_legacy_config_once() {
    let tmp = short_tempdir();
    let (home, legacy) = legacy_slack_home(tmp.path());
    let backup = home.join("config.toml.pre-bridge");
    let (ok, out) = check_config(&home);
    assert!(!ok && out.contains("E_SLACK_MOVED"), "{out}");

    // Nothing is asked: empty input is enough.
    let output = run_with_input(&home, &["setup", "slack"], "");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!stdout.contains("Slack owner member id"), "{stdout}");
    assert!(!stdout.contains("SENTINEL"), "token leaked");
    assert_eq!(
        stdout.lines().last(),
        Some(format!("  {}", install_command(std::env::consts::OS)).as_str()),
        "{stdout}"
    );

    let config_text = fs::read_to_string(home.join("config.toml")).expect("config.toml");
    let config: toml::Table = toml::from_str(&config_text).expect("config TOML");
    assert_eq!(config["messenger"].as_str(), Some("bridge"));
    assert!(!config.contains_key("slack"), "{config_text}");
    let bridge = &config["bridge"];
    assert_eq!(bridge["owner_id"].as_str(), Some("U111"));
    assert_eq!(bridge["max_bot_replies"].as_integer(), Some(5));
    let allow = bridge["allow"].as_array().expect("bridge.allow");
    assert_eq!(allow.len(), 1);
    assert_eq!(allow[0]["id"].as_str(), Some("B0BOT"));
    assert_eq!(allow[0]["kind"].as_str(), Some("bot"));

    let bridge_path = home.join("slack-bridge.toml");
    assert_eq!(mode_of(&bridge_path), 0o600);
    let bridge_file: toml::Table =
        toml::from_str(&fs::read_to_string(&bridge_path).expect("bridge config"))
            .expect("bridge TOML");
    let socket = bridge["socket"].as_str().expect("bridge.socket");
    assert!(Path::new(socket).is_absolute(), "{socket}");
    assert_eq!(
        bridge_file["socket"].as_str(),
        Some(socket),
        "one socket path"
    );
    assert_eq!(bridge_file["channel_id"].as_str(), Some("C222"));
    for (key, name) in [
        ("bot_token_file", "slack-bot.token"),
        ("app_token_file", "slack-app.token"),
    ] {
        assert_eq!(
            bridge_file[key].as_str().map(PathBuf::from),
            Some(home.join("secrets").join(name)),
            "{key}"
        );
    }

    assert_eq!(fs::read(&backup).expect("backup"), legacy);
    assert_eq!(mode_of(&backup), 0o600);
    let (ok, out) = check_config(&home);
    assert!(ok, "{out}");

    // A second run finds no [slack] section: it asks the normal questions (Enter keeps
    // every value) and writes no second backup.
    let output = run_with_input(&home, &["setup", "slack"], "\n\n\n\n");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "{stdout}");
    assert!(stdout.contains("Slack owner member id"), "{stdout}");
    assert!(!stdout.contains("pre-bridge"), "{stdout}");
    assert_eq!(fs::read(&backup).expect("backup"), legacy);

    // No alias: messenger = "slack" stays refused after the move.
    let moved = fs::read_to_string(home.join("config.toml")).expect("config.toml");
    let slack_messenger = moved.replace("messenger = \"bridge\"", "messenger = \"slack\"");
    assert_ne!(slack_messenger, moved);
    fs::write(home.join("config.toml"), &slack_messenger).expect("write config");
    let (ok, out) = check_config(&home);
    assert!(!ok && out.contains("E_SLACK_MOVED"), "{out}");
    let output = run_with_input(&home, &["setup", "slack"], "\n\n\n\n");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("E_SLACK_MOVED"));
    assert_eq!(
        fs::read_to_string(home.join("config.toml")).expect("config"),
        slack_messenger
    );
}

#[test]
fn failed_migration_leaves_the_legacy_config_for_the_next_run() {
    let tmp = short_tempdir();
    let (home, legacy) = legacy_slack_home(tmp.path());
    // A folder where slack-bridge.toml belongs makes the first write fail.
    let blocker = home.join("slack-bridge.toml");
    fs::create_dir(&blocker).expect("mkdir");
    fs::write(blocker.join("keep"), "x").expect("file in folder");

    let output = run_with_input(&home, &["setup", "slack"], "");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("E_IO"));
    assert_eq!(fs::read(home.join("config.toml")).expect("config"), legacy);
    assert!(!home.join("config.toml.pre-bridge").exists());

    fs::remove_dir_all(&blocker).expect("remove folder");
    let output = run_with_input(&home, &["setup", "slack"], "");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let config: toml::Table =
        toml::from_str(&fs::read_to_string(home.join("config.toml")).expect("config"))
            .expect("config TOML");
    assert_eq!(config["messenger"].as_str(), Some("bridge"));
    assert!(!config.contains_key("slack"));
    assert_eq!(
        fs::read(home.join("config.toml.pre-bridge")).expect("backup"),
        legacy
    );
}

#[test]
fn a_socket_path_too_long_only_stops_the_slack_setup() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let home = tmp.path().join("h".repeat(100));
    let omp = fake_executable(tmp.path());
    let input = format!("1\n111\n222\n{SENTINEL}\n{}\n\n", omp.display());
    let output = run_init(&home, &input);
    assert!(
        output.status.success(),
        "a Discord init needs no socket: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let before = fs::read(home.join("config.toml")).expect("config");

    let output = run_with_input(
        &home,
        &["setup", "slack"],
        &format!("U111\nC222\n{BOT}\n{APP}\n"),
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("E_BRIDGE_SOCKET"), "{stderr}");
    assert!(stderr.contains("this OS allows at most"), "{stderr}");
    assert_eq!(fs::read(home.join("config.toml")).expect("config"), before);
}

#[test]
fn setup_without_config_fails_with_its_code() {
    let tmp = short_tempdir();
    let output = run_with_input(&tmp.path().join("home"), &["setup", "discord"], "");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("E_SETUP_NO_CONFIG"));
}

#[test]
fn guide_prints_embedded_guides() {
    let tmp = short_tempdir();
    for name in ["discord", "slack"] {
        let output = moraebot(tmp.path())
            .args(["guide", name])
            .output()
            .expect("guide");
        assert!(output.status.success());
        let file = fs::read_to_string(format!(
            "{}/docs/guides/{name}.md",
            env!("CARGO_MANIFEST_DIR")
        ))
        .expect("guide file");
        assert_eq!(String::from_utf8_lossy(&output.stdout), file);
    }
}

#[test]
fn unknown_messenger_is_usage_error() {
    let tmp = short_tempdir();
    for args in [["setup", "telegram"], ["guide", "telegram"]] {
        let output = moraebot(tmp.path()).args(args).output().expect("run");
        assert_eq!(output.status.code(), Some(2), "{args:?}");
    }
}
