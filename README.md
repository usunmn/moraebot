# moraebot

moraebot lets you talk to a coding agent on your Mac or Linux machine from a chat app. You send a message in Discord or Slack, moraebot hands it to omp, a coding agent CLI, running on that machine, and the reply comes back in the same conversation.

## Status

This is slice 3. It adds files: you can attach files to a message, and omp can send files back.

- Discord or Slack, one at a time. Other messengers come later. Slack runs through a separate bridge process, `moraebot-slack-bridge`, installed and started next to `moraebot` (see [docs/bridge.md](docs/bridge.md)).
- Apple Silicon macOS. It runs as a per-user LaunchAgent.
- Linux x86_64 or aarch64 with glibc 2.35 or newer and OpenSSL 3. It runs as a systemd user service.
- One owner. Only you can talk to the bot (`access_mode = "owner_only"`, the default). An allowlist for other people and bots is on the way; see [Access](#access).
- One task at a time. Messages wait in a queue and run in order.

## Read this first: yolo mode

> **Warning**
>
> moraebot runs omp without asking for approval. Anyone who can talk to the bot can run commands on this Mac, with your user's permissions.
>
> - Keep `access_mode = "owner_only"` unless you need the allowlist. It's the default.
> - Use a private channel that only you can see.
> - Turn on two-factor login for your Discord or Slack account. That account is now a key to your Mac.
> - The work folder is not a sandbox. omp can read and change files outside it.
> - Never share the bot token. Anyone holding it can pretend to be your bot.

The bot only runs while you're logged in to macOS and the Mac is awake. On Linux it runs while you're logged in, or all the time once you turn on linger (see step 8).

## Install

You'll need Apple Silicon macOS or Linux (x86_64 or aarch64), a Discord or Slack account, git, and omp already installed and working in your terminal.

### 1. Install moraebot

```sh
curl -fsSL https://github.com/useonmn/moraebot/releases/latest/download/install.sh | sh
```

This picks the macOS or Linux build for your machine, downloads the latest release, checks it against the release's SHA-256 checksum (with `sha256sum`, or `shasum` when that's missing), makes sure it runs, and only then puts it at `~/.local/bin/moraebot`. If anything fails, your existing moraebot stays as it was. When `~/.local/bin` isn't on your `PATH` yet, the script prints the line to add to your shell startup file.

On a fresh install (no `~/.moraebot/config.toml` yet) the script ends with `Next steps:`: run `moraebot init`, then `moraebot check-config`, then `moraebot install-launchd` (`moraebot install-systemd` on Linux), and send the bot a message. When moraebot isn't on your `PATH`, those lines use its full path. An upgrade prints no next steps.

Two settings change what it does. Put them right before `sh`, not before `curl`:

- `MORAEBOT_VERSION` picks a release instead of the latest one.
- `MORAEBOT_INSTALL_DIR` puts the binary in another folder.

```sh
curl -fsSL https://github.com/useonmn/moraebot/releases/latest/download/install.sh | MORAEBOT_VERSION=v0.1.0 sh
```

A release made before the Slack bridge existed, such as v0.1.0, holds only `moraebot`. The script installs it alone and prints a warning: Discord works, but Slack needs a release that ships `moraebot-slack-bridge`.

On macOS only: download it with curl like this, not with a browser. A file saved by a browser can be flagged by macOS Gatekeeper, and the unsigned binary may then refuse to open.

### 2. Build from source (optional)

Skip this if you used the line above. It's for developers who want to build moraebot themselves.

Install Rust:

```sh
brew install rust
```

Or use rustup if you prefer:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

On Linux, the build also needs the OpenSSL headers. On Debian or Ubuntu:

```sh
sudo apt-get install pkg-config libssl-dev
```

Then build moraebot:

```sh
git clone https://github.com/useonmn/moraebot.git
cd moraebot
cargo build --release
```

The binary ends up at `target/release/moraebot`. The commands below call it `moraebot`. Either run it by that path or copy it somewhere on your `PATH`.

### 3. Pick a messenger

moraebot talks to one messenger at a time: Discord or Slack. Each one has its own setup guide with the exact app settings, in the order the portal shows them:

- Discord: [docs/guides/discord.md](docs/guides/discord.md)
- Slack: [docs/guides/slack.md](docs/guides/slack.md)

Follow the guide for the messenger you want, then come back here. The guides are the only place the fixed settings are written down, so this README doesn't repeat them.

You can also print a guide in the terminal:

```sh
moraebot guide discord
moraebot guide slack
```

### 4. Set up with your agent (optional)

Each guide ends with an "Agent-assisted setup (optional)" section. It's a prompt you can hand to a coding agent so it walks you through the portal clicks. The agent never needs your tokens. You type those into moraebot yourself.

### 5. Run `moraebot init`

On the first run:

```sh
moraebot init
```

It asks a few questions, in order:

1. Which messenger to use (Discord or Slack).
2. That messenger's questions (see the list below). Tokens aren't echoed while you type or paste them. Each one is saved to its own file under `~/.moraebot/secrets/` with mode `600` (readable only by you).
3. The path to `omp` (found on your `PATH`, you confirm it) and the work folder. Press Enter to accept the default `~/moraebot-work`.

Then it creates `~/.moraebot` (mode `700`), writes `~/.moraebot/config.toml`, and runs the same checks as `check-config`. If a config already exists, it asks before overwriting it. The default answer is No.

When it writes a new config, `init` also leaves a small request file, `~/.moraebot/onboarding.request`, and when every check passes it prints the next steps last: `check-config`, `install-launchd` (or `moraebot run` to try it in the terminal), then message the bot. The next time moraebot starts, it says hello to you in a DM (in the channel if the DM fails) and begins the first conversation (see First conversation). Keeping your old config, or running `moraebot setup`, leaves no request.

### 6. Change or add a messenger

To switch messengers, or to fix one messenger's answers, run setup for it:

```sh
moraebot setup slack
moraebot setup discord
```

setup asks only that messenger's questions, keeps the rest of your config, and makes that messenger the active one. When a section already exists, its current ids are offered as defaults and pressing Enter keeps a saved token.

Discord asks:

1. Your Discord owner user id.
2. The Discord channel id.
3. The Discord bot token (saved to `~/.moraebot/secrets/discord.token`).

Slack first prints a link that creates the app with the manifest filled in, then asks:

1. Your Slack owner member id (starts with `U`).
2. The Slack channel id (starts with `C` or `G`).
3. The Slack bot token, `xoxb-...` (saved to `~/.moraebot/secrets/slack-bot.token`).
4. The Slack app-level token, `xapp-...` (saved to `~/.moraebot/secrets/slack-app.token`).

For Slack, setup writes two files: `config.toml` with `messenger = "bridge"` and a `[bridge]` section (socket, owner, access list), and `~/.moraebot/slack-bridge.toml` for the bridge (the same socket path, the channel id and the two token files). The socket path is written out in full in both, on macOS too.

If your `config.toml` still has a `[slack]` section from an earlier version, `setup slack` asks nothing. It moves that section into the two files, keeps the old config as `config.toml.pre-bridge`, and prints the install command. This happens once; see [Upgrading](#upgrading-from-an-earlier-version).

setup ends by printing `moraebot install-launchd` (macOS) or `moraebot install-systemd` (Linux). Run it so the services pick up the change. For Slack it installs both services.

### The config file

The written config looks roughly like this. You can edit it by hand later. Only the section named by `messenger` is used. The other one can stay in the file, so switching back is a one-word change.

```toml
# The active messenger: "discord" or "bridge" (Slack). Only one runs at a time.
messenger = "discord"
# Folder omp starts in. This is not a sandbox.
work_dir = "~/moraebot-work"
# Absolute path to omp.
omp_path = "<PATH_TO_OMP>"
# "owner_only" (default) or "allowlist". See Access below.
access_mode = "owner_only"

[discord]
# Your Discord user id. Only this account can talk to the bot.
owner_id = <YOUR_DISCORD_USER_ID>
# The one channel the bot listens in. Threads under it and your DMs also work.
channel_id = <YOUR_CHANNEL_ID>
# File holding the bot token. Must be mode 600.
token_file = "~/.moraebot/secrets/discord.token"

# [bridge]
# # The bridge's socket. The same absolute path as `socket` in slack-bridge.toml.
# socket = "<SOCKET_PATH>"
# # Your Slack member id. Only this account can talk to the bot.
# owner_id = "<YOUR_SLACK_MEMBER_ID>"

[omp]
# Optional. No time limit when left out.
# turn_timeout_minutes = 30
# Optional. Minutes without progress before a running turn gets one stall notice. 20 when left out.
# stall_after_minutes = 20
# Optional. No cost limit when left out.
# max_cost_usd = 5.0

[lead]
# Optional. Minutes a lead turn may keep your waiting message unanswered before moraebot asks it
# to hand off to a lane. 5 when left out; 0 turns it off.
# handoff_after_minutes = 5
# Optional. Minutes the lead keeps the system prompt it started with after its last turn. A memory
# or persona change meanwhile reaches it as an update block in its next input, so the provider's
# prompt cache stays valid; after this long idle (or after a compaction, or `refresh memory`) the
# lead restarts with the new prompt. 60 when left out; 0 restarts the lead on any change.
# cache_minutes = 60
# Optional. Whether moraebot answers bare status questions itself. true when left out;
# false sends them to the lead like any other message.
# direct_status = true
```

For Slack, the channel and the tokens live in `~/.moraebot/slack-bridge.toml`, not here:

```toml
socket = "<SOCKET_PATH>"
# The one channel the bot listens in. Thread replies under it and your DMs also work.
channel_id = "<YOUR_CHANNEL_ID>"
# Token files. Both must be mode 600.
bot_token_file = "~/.moraebot/secrets/slack-bot.token"
app_token_file = "~/.moraebot/secrets/slack-app.token"
```

A config with `messenger = "slack"` or a `[slack]` section fails to load. Run `moraebot setup slack` once to move it.

Configs written by slice 1 (top-level `owner_id` and `token_file`, no `messenger` line) still load as Discord. You don't need to rewrite them.

### Access

There are two access modes:

- `owner_only` (the default): only `owner_id` can talk to the bot. Every config without an `access_mode` line works this way, exactly as before.
- `allowlist`: the owner, plus the people and bots you list under the active messenger.

```toml
access_mode = "allowlist"

[bridge]
socket = "<SOCKET_PATH>"
owner_id = "<YOUR_SLACK_MEMBER_ID>"
# Optional. At most this many replies to bot messages per thread in any 10 minutes.
# Leave it out for no limit (check-config warns when bots are listed without it).
max_bot_replies = 30

[[bridge.allow]]           # Discord: [[discord.allow]]
id = "<THEIR_MEMBER_ID>"   # Discord: a number, as for owner_id
kind = "user"              # "user" or "bot"
role = "chat"              # only "chat" exists; it's also the default
```

Rules, checked when the config loads:

- `allow` and `max_bot_replies` need `access_mode = "allowlist"`. With `owner_only` they are an error, so a list never looks active when it isn't.
- Each entry needs an `id` in the messenger's format (Slack `U...` or `W...`, and for a bot also its `B...` bot id; Discord a non-zero number) and a `kind`. An unknown `kind` or `role`, a duplicate id, or the owner's own id is an error.
- `max_bot_replies` must be greater than zero.
- Older moraebot versions reject a config with these keys.

`moraebot check-config` prints a `WARN` line for every listed id with its kind and role, so you can see who else can reach the bot.

The `chat` role is for conversation only. A chat sender gets answers from a separate omp with no tools, never from the lead that works on your machine, and can't stop or close lanes. Chat senders may attach files. Bots in the list should not talk to each other forever: `max_bot_replies` is the hard limit, and the lead is also told to wind down endless bot-to-bot exchanges.

How a chat sender is answered: each chat message starts its own omp with every tool switched off (`--no-tools --no-extensions --no-skills --no-rules --no-context-files --no-lsp --no-pty`), in its own empty folder (`<home>/chat/cwd/<turn>`, removed after the turn), with an overlay config that turns off every omp config source (so no MCP server, plugin, hook or skill is loaded), memory and the advisor, and with none of moraebot's tools. That overlay's `disabledProviders` list replaces your own omp `disabledProviders` for chat children only. At most two chat omps run at once; later chat messages wait for a slot. A chat message delivered twice by the messenger is answered once. Before it answers, moraebot asks it for its tool list and refuses the turn if anything is there. Each conversation keeps its own omp session under `<home>/chat/`, apart from the lead's. A chat message posted straight in the channel gets its own thread. Bot messages posted directly in the channel count toward `max_bot_replies` per channel, since each one would start its own thread. Chat messages are never stored in the inbox and never reach the lead. Attached files go into the prompt only (images up to 5 MB as images, text files up to 64 KiB inline, anything else or larger by name; a file over its limit is not downloaded), are kept under `<home>/chat/` only while the turn runs, and never land in your work folder. The chat omp is closed after each answer, and stopped with everything else at shutdown.

Set `MORAEBOT_HOME` if you want moraebot's home somewhere other than `~/.moraebot`.

### 7. Check the config

```sh
moraebot check-config
```

Every line should start with `OK`. A `FAIL` line says what to fix, for example a token file that other users can read (`chmod 600 ~/.moraebot/secrets/slack-bot.token`). The command exits non-zero if anything fails. Use `--config <path>` to check a different file.

### 8. Start it in the background

```sh
moraebot install-launchd
```

This copies the binary to `~/.moraebot/bin/moraebot`, writes `~/Library/LaunchAgents/dev.moraebot.gateway.plist`, and loads it with `launchctl`. macOS keeps moraebot running from now on and starts it again after a crash or a new login. Add `--dry-run` to print the plist and the `launchctl` commands without running them.

With Slack (`messenger = "bridge"`) it installs a second agent too: `moraebot-slack-bridge` is copied next to `moraebot` and runs under the label `dev.moraebot.gateway.slack-bridge`. The two start in any order; the core waits for the bridge and reconnects when it restarts.

On Linux:

```sh
moraebot install-systemd
```

This copies the binary to `~/.moraebot/bin/moraebot`, writes `~/.config/systemd/user/moraebot.service` (under `$XDG_CONFIG_HOME/systemd/user` when that's set to an absolute path), then runs:

```sh
systemctl --user daemon-reload
systemctl --user enable moraebot.service
systemctl --user restart moraebot.service
```

With Slack (`messenger = "bridge"`) it also copies `moraebot-slack-bridge` to `~/.moraebot/bin/` and writes `moraebot-slack-bridge.service`. Then the commands run for the bridge unit first and the core unit second. The core unit `Wants=` the bridge, so stopping the bridge never stops the core:

```sh
systemctl --user daemon-reload
systemctl --user enable moraebot-slack-bridge.service
systemctl --user restart moraebot-slack-bridge.service
systemctl --user enable moraebot.service
systemctl --user restart moraebot.service
```

If the bridge binary isn't next to `moraebot`, the command fails with `E_BRIDGE_BINARY`. Run the install line from Install again; it puts both binaries in place.

systemd starts moraebot again 10 seconds after it stops and starts it when you log in. Add `--dry-run` to print the unit and those commands without running them, and `--unit NAME` to use another unit name than `moraebot`.

A user service stops when you log out. To keep it running after logout, run this once (some machines need admin rights for it):

```sh
loginctl enable-linger
```

To see whether it's running:

```sh
systemctl --user status moraebot.service
```

You can also run it in the foreground, which is handy for a first try:

```sh
moraebot run
```

Stop it with Ctrl+C.

## Lead and lanes

One omp, the lead, answers every thread and DM of the active messenger. It knows which conversation each message came from, and its reply always goes back to that conversation. Because it's one omp, it remembers what was said in all of them.

- **Lanes for long work.** When a task is long, the lead hands it to a lane: a separate omp that works in the background while the lead keeps answering you. At most 3 lanes run at once. Lanes share the lead's work folder unless the lead passes `repo` (a git checkout) to `lane_start`. A lane that shares the folder could touch the same files as another one, so the lead is told not to give two such lanes the same files at the same time. A lane with `repo` gets its own git worktree of that checkout, the folder `.moraebot-lanes/lane-<id>` inside the work folder on the branch `moraebot/lane-<id>`, made from the checkout's current `HEAD` before the lane starts. The lane works there, its Done checks run there, and later work given to the same lane reuses that folder. The folders and branches are kept for now; moraebot doesn't remove them, so clean them up yourself with `git worktree remove` when you no longer need them. This needs `git` on the machine (on macOS run `xcode-select --install`, on Linux install your distro's `git` package). Without it, only lanes started with `repo` fail, with `E_LANE_GIT`.
- **Done checks.** Every lane starts with a `done` line that says how to tell the work is finished. There are six kinds of check. `command ["program","arg"]` passes when the program exits with code 0. `command ["cargo","test"] contains "0 failed"` also needs that exact text in the command's standard output; stderr isn't searched, and a failing command fails before any search. `file <path>` passes when the path exists, and `file report.txt contains "status: ok"` also needs that exact text inside the file. `url http://127.0.0.1:8080/health` sends one GET and passes on a 2xx status. It doesn't follow redirects, ignores proxy settings, doesn't read the answer's body, and gives up after 120 s. Only `http` and `https` addresses are accepted, and an address with a user name or password is refused. `merged acme/app#12` asks GitHub (`api.github.com`) once whether that pull request is merged and passes only if it is. It doesn't follow redirects, so a renamed repo gives `E_CHECK_STATUS`; write the new name instead. It ignores proxy settings and gives up after 120 s. For a token it reads `GH_TOKEN`, then `GITHUB_TOKEN`, from the environment the checks get. The token is only sent in the request header, never stored or logged. Without one, only public repos can be seen, and GitHub allows 60 requests an hour. The launchd and systemd services pass no token, so under them `merged` works for public repos only; for a private repo, run `moraebot run` in a terminal that has the token set. Texts are matched exactly, never as regular expressions. Join several with `;`. When the lane thinks it's done, it calls `lane_done` with its evidence (what it did and how it checked it). moraebot then runs every check in the work folder, with the same environment omp gets minus the messenger variables (model-provider credentials stay, as they do for omp itself), and gives each command 120 s. If a check fails, the claim is refused with the reasons and the lane keeps working. There's no limit on how often it may try again. A lane that stops without a passed claim still goes to the lead: unchecked if it never claimed, or with `E_DONE_UNMET` and the reasons if its last claim failed. Anything a check command leaves running in its own process group is stopped when the check ends. A process that detached itself into a new session is not. A lane started with `repo` gets a git check too, run before its Done checks: every change to tracked files must be committed, and when the repo has a remote, its commits must be pushed (under any branch name, usually `moraebot/lane-<id>`). New untracked files don't count. A repo with no remote is checked for commits only. moraebot never fetches or pushes for the lane. A failed Done check reports `E_CHECK_TEXT` when the text isn't there, `E_CHECK_READ` when the file or the command's output can't be read, `E_CHECK_STATUS` when the address answers outside 2xx, and `E_CHECK_HTTP` when the request itself fails or GitHub's answer isn't the expected JSON. A `merged` check reports `E_CHECK_UNMERGED` when the pull request isn't merged, `E_CHECK_NOT_FOUND` when GitHub answers 404 (no such pull request, or a private repo the token can't see), `E_CHECK_AUTH` when GitHub refuses the token or the token can't be sent, and `E_CHECK_RATE` when GitHub's request limit is used up. A failed git check is listed in the same refusal as the Done checks, with `E_CHECK_UNCOMMITTED`, `E_CHECK_UNPUSHED`, or `E_CHECK_GIT` when the git state can't be read. Lanes without `repo` aren't git-checked.
- **Steering a working lane.** The lead can send a running lane more instructions with `lane_message`, and that text is folded into the turn already in progress instead of waiting for it. omp answers the steer inside the same run, so the lane still writes one result and its prompt count doesn't grow. A text that arrives too late for that turn is not dropped: it runs as the lane's next prompt. A lane that is not running takes the text as a new prompt instead and picks up its own history. The text carries no Done-line, progress-note or git footers; those belong to the prompt that started the work.
- **Lane results.** When a lane finishes, its result goes to the lead, not straight to you. The result says whether the lane passed its Done checks. The lead checks it and answers in the conversation that started the lane. A lane that finished can be given more work later; it picks up its own history.
- **Follow-ups while it works.** If you send another message in the same conversation while the lead is working on it, that message is folded into the answer it's writing, and you get one reply. Only one follow-up is folded in per answer; anything after that waits for the next turn. Messages from other conversations, and lane results, also wait their turn.
- **Closing lanes.** When the lead decides a lane is finished, or no longer needed, it closes it with `lane_close` and a short reason. moraebot never closes a lane on its own. A running lane can't be closed; the lead waits for its result first. A closed lane takes no more work, and it stays in `lane_status` marked closed with its reason. For a lane with `repo`, moraebot removes the folder `.moraebot-lanes/lane-<id>` only when every change is committed and pushed (just committed when the repo has no remote) and there are no untracked files. Otherwise it keeps the folder and reports why: `E_CHECK_UNCOMMITTED`, `E_CHECK_UNPUSHED`, `E_CHECK_GIT` or `E_LANE_REMOVE`. Files listed in `.gitignore` go with the folder. The branch `moraebot/lane-<id>` always stays. If the folder was kept, save the work and ask the lead to close the lane again; that retries the cleanup.
- **Stuck lanes.** Every 30 seconds moraebot looks at each running lane. If a lane sent no activity for 20 minutes, or kept busy without a new tool call for 20 minutes (`stall_after_minutes` in `[omp]`, 20 when left out), the lead gets one notice (`N_LANE_QUIET`, `N_LANE_SPINNING`, or `N_LANE_RETRYING` when the model service keeps failing) and tells you in the lane's thread. moraebot never stops the lane; its result still comes. The lead may stop it with `lane_stop` (see Stopping lanes). One notice per stretch: after the lane makes progress, a new stall gets a new notice. It's off when `turn_timeout_minutes` is `stall_after_minutes` or less. When a notice is sent, the log (see Logs) gets one `lane looks stalled` line with the lane's number and the reason code. You can't easily make a lane stall on purpose: omp moves a shell command that runs longer than 60 seconds to the background and ends a command after 5 minutes unless it's given another limit, so a lane rarely stays silent that long.
- **Stuck lead turns.** The same 30-second check looks at the lead's running turn. If omp sent nothing for that turn (no text, no tool call, no other event) for `stall_after_minutes` (20 when left out), you get one notice in that conversation: "No progress for 20 minutes. The task may be stuck. It keeps running and nothing was stopped." moraebot never stops, restarts or interrupts the turn; its reply still comes when the lead answers. One notice per stretch: once the lead shows progress again, a later stall gets a new notice. It's off when `turn_timeout_minutes` is `stall_after_minutes` or less. The log (see Logs) gets one warn line `lead turn looks stalled` with `code=W_TURN_STALLED` (stalled lanes log the same code).
- **Busy lead turns.** The same check also watches how long a lead turn keeps you waiting. When a turn has run for 5 minutes (`handoff_after_minutes` in `[lead]`) and a message of yours in the same conversation is waiting (folded into that turn, or queued behind it), moraebot sends the lead one `[moraebot lead busy]` note: hand the rest of the work to a lane, answer you, and end the turn. One note per turn, and the turn is never stopped. Messages from other conversations don't count, since one turn can only answer its own conversation; they get their own turn after it. It's off with `handoff_after_minutes = 0`, or when `turn_timeout_minutes` is `handoff_after_minutes` or less. The log (see Logs) gets one warn line with `code=W_LEAD_BUSY`, separate from `W_TURN_STALLED`.
- **Status questions.** A message that is only a status question (`뭐 하고 있어`, `뭐해`, `상태`, `status`, `what are you doing`, with or without a question mark) is answered by moraebot itself, right away, even while the lead is busy. The answer says how long the lead has worked on this conversation and its current step, which lanes of this conversation are running (number, time, the first line of the task), and how many lanes run for other conversations. For another conversation's work it only says that the lead is busy elsewhere. It ends with the lead's prompt cache hit rate over its last 20 turns (and whether a memory change waits for the next lead restart) and one line about the recall advisor: its modes, its calls in the last hour and when it last delivered a note. The lead never sees these messages. Anything longer, like `status of PR 12?`, goes to the lead as usual. To have the lead answer them instead, set `direct_status = false` in `[lead]`.
- **New session.** When the lead's session has grown too long or gone wrong, send only `새 세션` (also `새 세션 시작`, `새 세션으로 시작`, `new session` or `/new`). moraebot handles it itself; the lead never sees it. It waits for the lead's running turn to end, then stops only the lead's omp (never the service, the lanes or chat), moves `~/.moraebot/lead/session` to `~/.moraebot/lead/session-backup-<UTC time>` (nothing is deleted) and forgets the stored session id. You get one line back saying the new session is open, your memory is kept, and where the backup is; your next message starts the lead with an empty session. If lanes are running, moraebot first tells you how many and changes nothing: send `새 세션 확인` or `new session confirm` within 10 minutes to go ahead; any other message you send in that conversation first cancels the question. The lanes keep running and their results go to the new lead, which won't remember starting them. Messages you send after the command go to the new lead. The command takes no attachments: sent with a file, it gets one notice and nothing is done. A longer message like `새 세션 열어줄래?` goes to the lead as usual. The log (see Logs) gets one info line with `code=I_LEAD_NEW_SESSION` and the backup folder; if the folder can't be moved, you get `E_LEAD_NEW_SESSION` and the lead keeps its old session.
- **Stopping lanes.** The lead can stop a lane's current work with `lane_stop` and a short reason, for example when you ask it to, or when a lane looks stuck (`N_LANE_QUIET` or `N_LANE_SPINNING`). moraebot ends that lane's omp at once, the same way it does when it shuts down, together with any Done check the lane was running. Files the lane already wrote stay (in a `repo` lane they stay uncommitted in its folder); work in progress in that turn may be lost. The result comes to the lead as `failed code=E_LANE_STOPPED` with the reason, and the lead tells you in the lane's thread. The lane stays open: the lead can give it more work with `lane_message`, and it picks up its own history, or close it with `lane_close`. A lane that isn't running can't be stopped (`E_LANE_IDLE`). If the lane finished on its own just before the stop, its real result comes instead. When a stop is requested, the log (see Logs) gets one `lane stop requested` line with the lane's number. moraebot itself still never stops a lane.
- **Progress news.** A lane can leave a one-line progress note at big steps with `lane_report`. When the conversation that started the lane has been quiet for 20 minutes (no message from you, and no reply or news from moraebot), moraebot hands the newest note it hasn't handed on yet to the lead, and the lead tells you in one short line in your language. If the lead is idle, or busy with another conversation, the note waits as its own input tagged `[moraebot conversation=<key> lane=<id> news]` and gets a lead turn when its time comes, so each note costs one short lead turn. If the lead is working on that same conversation, the note goes into the running turn as a `[moraebot lane news]` note instead; it counts as passed on only once the lead took it, and a note omp refused, or that a cut turn never answered, waits as its own input like above. The same note is stored only once while the lane works, and a note whose lane has finished by the time its turn comes is dropped: the lane's result follows it anyway. moraebot keeps a note to 200 characters on one line, replaces keys and passwords it recognizes with `[secret]`, writes the work folder as `.` and your home folder as `~`, and changes `@` to `＠` so no mention fires. Only when the lead can't start (its last start failed) does moraebot post the note itself as `[lane <id>] <note>`; if the messenger refuses that post, it tries again 30 seconds later. Notes are kept in memory only; after a restart nothing is resent. Whether a lane leaves notes is up to the lane's model, so there is no step in the first-use checklist for it.
- **Restarts.** A lane that was running when moraebot stopped isn't resumed. You get one notice in the conversation that started it. Ask the lead to try again if you still need it.

The lead controls lanes only through five tools moraebot gives it: `lane_start`, `lane_message`, `lane_status`, `lane_close` and `lane_stop`. Lanes themselves get `lane_report`, and `lane_done` when they have a Done line. Each lane runs a full omp, so three busy lanes cost about as much as four omp sessions.

## First conversation

The first time moraebot starts after `moraebot init`, it sends you one fixed hello in a DM. When the DM can't be opened or the send fails, the same hello goes once to the configured channel instead. Nothing is retried, and restarting doesn't repeat it.

Reply in that DM: the whole first conversation stays in one conversation there. On Discord the bot can DM you only while you share its server and your privacy settings allow DMs from server members; otherwise the hello goes to the channel.

When you reply, the lead asks one thing per message: what to call it, how it should talk to you (tone, length, language), and a few basics about you. While this runs, each of your messages reaches the lead with an extra second line, `[moraebot onboarding turn=<n>]`, for your first 10 messages. After the 10th the line stops, but the lead can still finish.

Once it has your answers, the lead saves them with its `onboarding_finish` tool into `~/.moraebot/persona.md` (mode `600`):

```text
Name: Sora
Tone: short and warm
Owner: Owner 111 likes short replies
```

`Name:` is always there. `Tone:` and `Owner:` appear only when you gave them. The tool refuses a file over 2048 bytes, a field with a line break, and anything that looks like a key, token or password. It also works only while the lead is answering one of your messages. Say "skip" and the lead ends the first conversation without writing the file.

The persona applies from your next message. Before each turn the lead reads `persona.md` (and core memory) again. When the text changed and the lead worked within the last hour (`cache_minutes` in `[lead]`), the change goes into the lead's next input as a `<<<moraebot-memory update>>>` block that lists the changed lines, so the lead's prompt cache stays valid; the block goes in once per change, and a later change back says the earlier update no longer applies. Once the lead was idle that long, its context was compacted, or you send `refresh memory` (`기억 새로고침`), moraebot closes the idle lead and starts it again on the same session with the new text, so it keeps the conversation. This happens only between turns, never while the lead is working. If omp reports that the last turn left background work pending, the restart waits until a turn ends with nothing pending. The restart adds a short pause (about 0.2 s in a bare test setup; more with many plugins). With `cache_minutes = 0` every change restarts the lead before the next message. Only the lead gets the persona; lanes don't, and running lanes keep working.

You can edit `persona.md` by hand. They're checked before every turn. The same rules apply when it's read: a file that's over 2048 bytes, holds secret-like text, or isn't a plain UTF-8 file is left out and the log gets one line with a code (`E_PERSONA_SIZE`, `E_PERSONA_SECRET` or `E_PERSONA_READ`), never the text. Delete the file to drop the persona from the next message on.

To run the first conversation again, or to start it on a home you already use:

```sh
moraebot onboarding
moraebot onboarding --greet channel
```

It writes the request file with one word, `dm` (the default) or `channel`, prints `{"onboarding":"requested","greet":"dm"}` (or `"channel"`) and changes nothing else. Restart moraebot and the hello comes at the next start: in your DM for `dm`, in the configured channel for `--greet channel`. An empty request file left by an older version means DM. Add `--config <path>` for a different config.

Files involved:

- `~/.moraebot/onboarding.request`: left by `init` or `moraebot onboarding`, holding one word, `dm` or `channel`. It's removed at the start that sends the hello.
- `~/.moraebot/persona.md`: the saved persona.
- The `onboarding` table in `~/.moraebot/inbox.sqlite`: one row saying whether the first conversation is running, done or skipped.

## Memory

moraebot keeps what you ask it to remember in `~/.moraebot/memory/`, a small git repository. Tell it in plain words (`기억해: ...`, `항상 기억해: ...`, `뭐 기억해?`, `m-1a2b3c 지워`); moraebot answers these itself, so they never reach the lead. Your core rules are in the system prompt the lead started with; a change reaches it as an update block in its next input until its prompt is set again (see the persona paragraph above; `refresh memory` or `기억 새로고침` does it at once). The notes of the conversation and of the projects you name follow your message, and the lead and lanes can look things up with `memory_search` (only the lead can save, with `memory_write`, and moraebot decides whose words a saved line is). moraebot trusts only the memory commits it made itself: a commit made by something else is undone and kept as a patch in `memory/.rejected/`. See [docs/memory.md](docs/memory.md) for the commands, the folder layout and how to remove something from the history for good.

When one of your messages corrects moraebot (`왜 또 ...`, `몇 번 말해 ...`, `I told you ...`; the phrases are `[memory.self] correction_patterns`), moraebot saves it in your own words in `self/observations.md`; no model reads or rewrites it. Quoted lines and code blocks don't count, and a correction steered into a running answer counts too. At the third similar correction within 90 days, the lead is asked to offer you, once, to keep it as a standing rule (`기억해: ...`). `너 자신에 대해 뭐 기억해?` lists the corrections. A lane result that failed (an error, unmet Done checks, or a stop) is kept as one short reference note in the lane's project, written by moraebot from the error codes, and expires in 30 days. Safety is at read time, the way omo does it: every memory block the lead or a lane reads says that your rules are still followed but a remembered approval never approves a new action, and that anything destructive, irreversible, sent in your name or spending money is confirmed with you in the current conversation. Saves are never posted in the chat; `뭐 기억해?` lists what was saved. See [docs/memory.md](docs/memory.md#corrections).

The recall advisor is on by default. On a small fast model (omp's `@smol` role) it looks for saved notes the lead doesn't have in front of it but that would change its next step: a project note, one of your corrections, a rule you replaced. Its picks go into the lead's input after your message, and while the lead works into the running turn as a short note (at most 2 such notes per conversation in 10 minutes), always marked as reference only. It never offers core memory and offers other conversations' notes only in your DM. `recent recalls` (`최근 떠올린 기억`) lists what it recently picked. Set `mode = "shadow"` in `[memory.recall]` to only log its picks, or `"off"` to turn it off. See [docs/memory.md](docs/memory.md#the-recall-advisor).

The lead's and lanes' system prompts, which carry your core memory, are passed to omp as private files (`~/.moraebot/lead/append.md`, mode 0600), not on the command line, so other users of the machine can't read them in the process list.

The lead and the lanes are ordinary omp processes that read your own omp settings, except for omp's own memory: moraebot runs them with `--config ~/.moraebot/omp-overlay.yml`, which turns `memory.backend` and `autolearn` off, so nothing they do lands in your terminal omp's memory. If a newer omp no longer knows those keys, `moraebot run` refuses to start (`E_OMP_OVERLAY`); if omp is another version than the one moraebot was checked against, it starts and tells you once in your DM. See [docs/memory.md](docs/memory.md#omps-own-memory-is-off).

## Behavior hints

moraebot gives omp a short built-in note about the messenger it's answering in, for example how long a reply may be and which formatting renders. To replace it, write your own text to:

```text
~/.moraebot/hints/discord.md
~/.moraebot/hints/slack.md
```

- The file for the active messenger replaces the built-in text.
- It can be at most 8 KiB. A bigger file fails `check-config`.
- An empty file turns the hints off.
- Restart moraebot to apply a change.

The built-in hints also tell omp how files work here: where received files are saved and how to send one back. An override file replaces all of that text, so keep those file sentences in your own file if you write one.

The lead also gets a built-in role note: how to read the tag line at the start of each input, when to answer directly and when to start a lane, and how to use the lane tools. To replace it, write your own text to `~/.moraebot/hints/lead.md`. The same rules apply (8 KiB at most, empty turns it off, restart to apply). If you write one, keep the lane tool names (`lane_start`, `lane_message`, `lane_status`, `lane_close`, `lane_stop`), the description of the `done` field, and the description of the tag line and its lane result tags (`done`, `failed code=E_DONE_UNMET`, `failed code=E_LANE_STOPPED`, `result`, `stalled reason=...`, `news`) and of the `[moraebot memory recalled]`, `[moraebot lead busy]` and `[moraebot lane news]` notes, and the line that tells it never to stop or restart its own service or edit session files and to send you to `새 세션` for a fresh session, or the lead won't know how to use lanes, what a lane result means, where a message came from, that a recalled note is reference only, or how to get you a fresh session safely. The messenger hints file still applies on top of it.

## Logs

Logs live in `~/.moraebot/logs/`. moraebot writes `moraebot.log` files there, plus the stdout and stderr files that launchd (macOS) or systemd (Linux) captures, `moraebot.out.log` and `moraebot.err.log`. Message text is never written to the logs.

Warnings and errors from the libraries moraebot uses (for Discord, Slack and the network) appear in the log only as a short line naming the library (libraries that write through the common `log` interface, such as the network library, are named `log`), without their details, so message text, file names, file links and tokens never reach the log.

```sh
ls ~/.moraebot/logs/
tail -f ~/.moraebot/logs/moraebot.log*
```

## First-use checklist

After installing, check these by hand once:

- [ ] **Real round trip.** Send a short message in the bot's channel. A reply comes back.
- [ ] **Strangers get nothing.** Send a message from a second Discord account. The bot doesn't reply and doesn't store it.
- [ ] **Restart notice.** Send something long, then restart moraebot mid-task (`launchctl kickstart -k gui/$(id -u)/dev.moraebot.gateway`, or `systemctl --user restart moraebot.service` on Linux). After it comes back you get one notice saying the task was interrupted.
- [ ] **Send a file.** Attach a small file to a message. omp's answer shows it read the file.
- [ ] **Get a file back.** Ask omp to send you a file from the work folder. It shows up in the same thread, after the reply text.
- [ ] **Lane result in the same thread.** In a thread, ask for something long, such as a task with several steps. The lead says it started a lane, and later the lane's result is answered in that same thread.
- [ ] **A lane claims done, a failing Done check is refused, and the lead reports the outcome.** Ask for long work whose Done line can't pass at first, for example a file the lane still has to create. The lane's first claim is refused, it keeps working, and the lead's answer in that thread says whether the lane's checks passed.
- [ ] **A `url` Done check sees your local service.** Start a small web service on your machine, then ask for long work whose Done line is `url http://127.0.0.1:<port>/<path>`. The lane's claim passes only while the service answers with a 2xx status. To check the answer's text too, add `; command ["curl","-s","http://127.0.0.1:<port>/<path>"] contains "ok"` (the `url` check itself never reads the body).
- [ ] **A `merged` Done check asks GitHub.** Ask for long work whose Done line is `merged <your public repo>#<a merged PR number>`, for example `merged acme/app#12`. The lane's claim passes. With the number of a PR that's still open, the claim is refused with `E_CHECK_UNMERGED`.
- [ ] **A lane works in its own folder.** Ask for a change in a git repo. Check that the lane worked in `.moraebot-lanes/lane-<id>` inside the work folder, on the branch `moraebot/lane-<id>`, and that the repo's own folder was left alone.
- [ ] **A lane with a repo pushes before it is done.** Ask for a change in a git repo that has a remote, and check that the branch `moraebot/lane-<id>` is on the remote when the lead reports the lane done.
- [ ] **The lead closes a finished repo lane.** After a repo lane is reported done, ask the lead to close it. Check that `.moraebot-lanes/lane-<id>` is gone and the branch `moraebot/lane-<id>` still exists.
- [ ] **Follow-up while it works.** While the lead is still answering in a thread, send a second message in that thread. You get one reply that covers both.
- [ ] **First conversation.** After `moraebot init` and the first start, the bot says hello once in a DM (in its channel if the DM fails). Answer its questions (or reply skip), then restart moraebot and check that it uses the name you gave it.
- [ ] **omp works under launchd or systemd.** launchd or systemd starts moraebot with a minimal environment, not your shell's. After your first message, check the logs and confirm omp reached its model. If it can't find its credentials or tools, fix that before relying on the bot.

If you use Slack, also check these:

- [ ] **Invite the bot.** In the bot's channel, run `/invite @<bot name>`. Without it the bot sees nothing there.
- [ ] **Channel post.** Post in the channel. The reply arrives in a thread under your message.
- [ ] **Thread reply.** Reply in that thread. The answer stays in the same thread and the same conversation.
- [ ] **DM.** Send the bot a direct message. A reply comes back in the DM.
- [ ] **Strangers get nothing.** Post from a second Slack account. The bot doesn't reply and doesn't store it.
- [ ] **Reconnect (optional).** For a quick test, add `&debug_reconnects=true` to the end of the Socket Mode `wss` URL (for example in a debug build). Slack then drops the connection on purpose, and the logs show moraebot reconnecting within 360 seconds.

## Files

You can attach files to a message, and omp can send files back to you.

**Files you send.** When a message from you has attachments, moraebot first stores the message, then downloads each file into the work folder:

```text
<work_dir>/.moraebot-inbox/<conversation>/<message>/<file name>
```

The file name is cleaned up first, so it can't point outside that folder. moraebot then hands omp your text plus a short list of the saved paths. A message with only files and no text works too.

- At most 10 files per message. A message with more gets one notice and isn't run.
- moraebot sets no size limit. The messenger's own upload limit still applies.
- Received files are written to disk in small pieces, so a large download doesn't need much memory.
- While a file downloads, your other incoming messages wait. They wait for as long as the file keeps arriving. A download fails only after 2 minutes with no data at all, so a large, slow file holds later messages until it's done.
- If any file of a message can't be received (a dropped download, a full disk, a missing permission), the message isn't run and you get one notice. Its folder under `.moraebot-inbox` can be deleted by hand.
- `.moraebot-inbox` is never cleaned up automatically. Delete old folders yourself when you no longer need them.
- If the work folder is a git repository, add `.moraebot-inbox/` to its `.gitignore` so received files don't end up in a commit.

**Files omp sends back.** omp has a `send_file` tool. When it calls it with a path, the file is sent to the same thread after the reply text.

- Only files inside the work folder can be sent, and never anything inside the moraebot home. If `work_dir` is set somewhere under the moraebot home, every `send_file` is refused.
- A file to send is read whole into memory and sent in one go, the way other chat bots do it. Sending it uses extra memory about equal to its size on Slack, and about twice its size on Discord, because the Discord library keeps a second copy while it sends.
- Later incoming messages also wait while files are sent.
- One file's upload may take up to 30 minutes. After that it's skipped with one notice and not tried again.
- Other short failures, such as a dropped connection, are retried at the next reconnect. Each file gets at most 3 attempts in total, restarts included. After that it's skipped with one notice. A file can still arrive after that notice, if a later attempt went through.
- If a stop or crash (running out of memory included) cut an upload short, that file is skipped with one notice instead of being sent twice.
- If the messenger refuses a file, for example because it's over the upload limit, the file is skipped with one notice.

**Privacy.** The log never holds file contents, file names or tokens. Detailed logs from other libraries are always off; only their warnings and errors are kept, as described under Logs.

**Missing permission.** If the bot lacks the file permission, every file you send or ask for shows up as a file notice. Do the upgrade steps below to fix it.

## Upgrading from an earlier version

Re-run the install line from Install, then `moraebot install-launchd` (macOS) or `moraebot install-systemd` (Linux) if you use the background service:

```sh
moraebot install-launchd
moraebot install-systemd
```

Run the one for your system from a terminal, not by asking the bot. It restarts the bot's own process.

Files need one more permission than earlier versions asked for. If you set up the bot before this version:

- Discord: invite the bot again with the invite link from [docs/guides/discord.md](docs/guides/discord.md), or give its role the Attach Files permission in the channel.
- Slack: update the app from the manifest in [docs/guides/slack.md](docs/guides/slack.md), then reinstall it to your workspace.

The guides hold the exact settings. Restart moraebot afterwards.

Slack now runs through the bridge. If you use Slack, after the install line:

1. Run `moraebot setup slack`. It finds the old `[slack]` section and moves it without asking: it writes `~/.moraebot/slack-bridge.toml`, saves the old config as `config.toml.pre-bridge`, and writes a `config.toml` with `[bridge]` and no `[slack]`. It runs once. There's no alias: `messenger = "slack"` stays an error. Do step 2 right after: the background service still running cannot read the moved config, so it fails on its next restart until the new services are installed.
2. Run the install command it prints, `moraebot install-launchd` or `moraebot install-systemd`. That installs both services.
3. Update the Slack app from [`slack-manifest.json`](docs/guides/slack-manifest.json) and reinstall it to your workspace. The manifest now asks for `assistant:write` and turns on Slack's agent view. **That switch can't be undone**; read the Agents section of [docs/guides/slack.md](docs/guides/slack.md) first.

After the move, each Slack DM is answered in a thread, and each DM thread is its own conversation.

This version replaces the separate omp per thread or DM with one lead (see Lead and lanes). After the upgrade:

- The lead starts with an empty history. It doesn't pick up what earlier per-thread sessions remembered.
- The old `~/.moraebot/conversations/*/session` folders are kept as they were and are no longer used. Delete them by hand if you don't need them.
- Your stored messages and settings keep working without any change.

If you're coming from 0.2.0: the old memory is not carried over. The new memory store uses `~/.moraebot/memory/` with a different layout, so while the old files (`~/.moraebot/memory/system` and its sibling folders, `~/.moraebot/omp/config-overlay.yml`) are there, moraebot refuses to start and prints the `rm -rf` command for exactly those paths. moraebot never deletes them itself; run the command, then start again (see [docs/memory.md](docs/memory.md)). The old memory tables in the database are left unused. The first conversation's state carries over as it was. To make a `persona.md`, run `moraebot onboarding` and restart moraebot. omp memory now follows your own omp settings (see Memory).

## Stop and uninstall

```sh
moraebot uninstall-launchd
```

This unloads the LaunchAgent and removes its plist, and does the same for the bridge agent if there is one. On Linux:

```sh
moraebot uninstall-systemd
```

This runs `systemctl --user disable --now moraebot.service`, removes the unit file and reloads systemd. It removes `moraebot-slack-bridge.service` the same way. To remove everything else too:

```sh
rm -rf ~/.moraebot
rm ~/.local/bin/moraebot ~/.local/bin/moraebot-slack-bridge
```

That deletes the installed binary, the config, the token file, the message database, and the logs. Your work folder is left alone. Finally, delete the Discord application or the Slack app if you no longer need the bot.
