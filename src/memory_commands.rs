//! Owner memory commands in plain words, handled by moraebot itself (never by the lead).
//!
//! The dispatcher asks [`parse`] about the text of every owner row (`origin = 'owner'`)
//! before the row can be steered into a running turn or queued for the lead. Lane-result
//! rows and chat senders never reach it, so the same words in a lane report or a web page do
//! nothing. The rules come first:
//!
//! - remember: `기억해: <text>`, `<text> 기억해(줘/둬/놔)`, `remember: <text>`,
//!   `remember that <text>`. Saved in the conversation's thread bucket; `기억해(<repo>):` /
//!   `remember(<repo>):` saves in that project's bucket. Only an explicit "always"
//!   (`항상 기억해`, `always remember`) saves in core, because core goes into the lead's
//!   system prompt; an "always" rule that says not to do something goes to the boundaries file.
//! - show: `뭐 기억해?`, `what do you remember?` (core, boundaries and this conversation),
//!   or `m-xxxxxx 보여줘` / `show m-xxxxxx` for one entry. `너 자신에 대해 뭐 기억해?` /
//!   `what do you know about yourself?` lists the corrections the owner gave
//!   (`self/observations.md`, [`crate::lessons`]).
//! - correct: `m-xxxxxx 고쳐: <new text>`, `correct m-xxxxxx: <new text>`.
//! - forget: `m-xxxxxx 지워(줘)`, `forget m-xxxxxx`; the dispatcher asks once and acts on the
//!   owner's next message in that conversation ([`confirmation`]).
//! - restore: `m-xxxxxx 되살려`, `restore m-xxxxxx`.
//! - refresh: the whole message `기억 새로고침` / `refresh memory` ([`refresh_request`]); the
//!   lead's system prompt is set again before its next turn instead of waiting for its prompt
//!   cache to go cold. The dispatcher answers it like a status question.
//! - recent recalls: the whole message `최근 떠올린 기억` / `recent recalls`
//!   ([`recent_recalls_request`]) lists the last [`RECENT_RECALLS`] recall ledger rows with
//!   each entry's current text.
//!
//! A question (`... 기억해?`) is not a remember request, and id commands must be short (at
//! most [`MAX_ID_COMMAND_WORDS`] words) so a sentence that merely mentions an id and a verb
//! still goes to the lead.
//!
//! A message the rules do not match goes to the lead.

use crate::memory::{
    Bucket, E_MEMORY_ARCHIVED, E_MEMORY_NOT_ARCHIVED, E_MEMORY_NOT_FOUND, Entry, Found,
    MemoryError, MemoryHandle, NewEntry, Prov, Source,
};

/// An id command (show one, forget, restore) has at most this many words.
pub const MAX_ID_COMMAND_WORDS: usize = 4;

/// Where a remember request saves its text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    /// The thread bucket of the conversation the request came from.
    Conversation,
    /// `projects/<repo>`.
    Project(String),
    /// `core/owner.md`: only on an explicit "always".
    Core,
    /// `core/boundaries.md`: an "always" rule that says not to do something.
    Boundaries,
}

/// One owner memory command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Remember {
        text: String,
        target: Target,
    },
    /// Core, boundaries and this conversation's entries.
    Show,
    /// The corrections the owner gave (`self/observations.md`).
    ShowSelf,
    ShowOne(String),
    Correct {
        id: String,
        text: String,
    },
    /// Asks for confirmation; the dispatcher forgets on the owner's yes.
    Forget(String),
    Restore(String),
}

const SHOW_PHRASES: [&str; 16] = [
    "뭐 기억해",
    "뭘 기억해",
    "뭐 기억해?",
    "뭐 기억하고 있어",
    "뭘 기억하고 있어",
    "뭐 기억하고있어",
    "기억 보여줘",
    "기억한 거 보여줘",
    "기억한거 보여줘",
    "기억 목록",
    "what do you remember",
    "what do you remember about me",
    "show memory",
    "show your memory",
    "show memories",
    "list memories",
];

const SHOW_SELF_PHRASES: [&str; 8] = [
    "너 자신에 대해 뭐 기억해",
    "너 자신에 대해 뭐 기억하고 있어",
    "너에 대해 뭐 기억해",
    "너 자신에 대해 뭐 알아",
    "what do you know about yourself",
    "what do you remember about yourself",
    "show your self file",
    "show self memory",
];

/// Suffix forms: `<text> 기억해줘`. Only the explicit imperatives: a bare `<text> 기억해` is
/// often a question asked without `?` ("어제 한 얘기 기억해"), so it goes to the lead.
const KO_SUFFIXES: [&str; 6] = [
    "기억해 줘",
    "기억해 둬",
    "기억해 놔",
    "기억해줘",
    "기억해둬",
    "기억해놔",
];

/// Prefix forms, each followed by `:` (or `(<repo>):`).
const KO_PREFIXES: [&str; 3] = ["기억해줘", "기억해 줘", "기억해"];
const EN_PREFIXES: [&str; 4] = [
    "remember this",
    "remember that",
    "remember",
    "please remember",
];

const FORGET_VERBS: [&str; 12] = [
    "지워",
    "지워줘",
    "삭제",
    "삭제해",
    "삭제해줘",
    "잊어",
    "잊어줘",
    "잊어버려",
    "forget",
    "delete",
    "remove",
    "drop",
];
const RESTORE_VERBS: [&str; 8] = [
    "되살려",
    "되살려줘",
    "복구",
    "복구해",
    "복구해줘",
    "restore",
    "undelete",
    "unforget",
];
const CORRECT_VERBS: [&str; 9] = [
    "고쳐",
    "고쳐줘",
    "수정",
    "수정해",
    "수정해줘",
    "바꿔",
    "바꿔줘",
    "correct",
    "fix",
];
const SHOW_VERBS: [&str; 4] = ["보여줘", "보여", "show", "보기"];
/// Words that only soften an id command (`m-1f3a2b 지워 줘`, `forget m-1f3a2b please`).
const FILLER: [&str; 4] = ["줘", "please", "it", "좀"];
/// Korean particles that may stick to an id (`m-1f3a2b를 지워`).
const PARTICLES: [&str; 8] = ["", "를", "을", "는", "은", "이", "가", "도"];

const YES: [&str; 14] = [
    "응",
    "네",
    "예",
    "ㅇㅇ",
    "ㅇ",
    "그래",
    "좋아",
    "지워",
    "지워줘",
    "삭제해",
    "yes",
    "y",
    "ok",
    "sure",
];
const NO: [&str; 9] = [
    "아니",
    "아니요",
    "아니야",
    "ㄴㄴ",
    "취소",
    "no",
    "n",
    "cancel",
    "nope",
];

/// Lowercase, trailing `?!.~` and spaces dropped, inner spaces collapsed.
fn normalize(text: &str) -> String {
    let lowered = text.trim().to_lowercase();
    let trimmed = lowered.trim_end_matches(|c: char| "?!.~ ".contains(c) || c.is_whitespace());
    trimmed.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The memory command in an owner message, if it is one.
pub fn parse(text: &str) -> Option<Command> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let norm = normalize(text);
    if SHOW_PHRASES.contains(&norm.as_str()) {
        return Some(Command::Show);
    }
    if SHOW_SELF_PHRASES.contains(&norm.as_str()) {
        return Some(Command::ShowSelf);
    }
    parse_correct(text)
        .or_else(|| parse_id_command(&norm))
        .or_else(|| parse_remember(text))
}

/// The owner's answer to a forget question: `Some(true)` yes, `Some(false)` no, `None` when
/// the message is something else (the question is then dropped).
pub fn confirmation(text: &str) -> Option<bool> {
    let norm = normalize(text);
    if YES.contains(&norm.as_str()) {
        Some(true)
    } else if NO.contains(&norm.as_str()) {
        Some(false)
    } else {
        None
    }
}

/// The owner's refresh command: the whole message is `기억 새로고침` or `refresh memory` (case,
/// spaces and closing punctuation ignored, like the new-session command). Returns the language
/// it was given in.
pub fn refresh_request(text: &str) -> Option<crate::status::Lang> {
    match normalize(text).as_str() {
        "기억 새로고침" => Some(crate::status::Lang::Korean),
        "refresh memory" => Some(crate::status::Lang::English),
        _ => None,
    }
}

/// The answer to the refresh command.
pub fn refresh_text(lang: crate::status::Lang) -> &'static str {
    match lang {
        crate::status::Lang::Korean => {
            "리드가 다음에 턴 사이에서 쉴 때 저장된 기억을 시스템 프롬프트에 다시 반영할게요."
        }
        crate::status::Lang::English => {
            "Saved memory goes into the lead's system prompt again when the lead is next idle between turns."
        }
    }
}

/// How many ledger rows the recent-recalls answer lists.
pub const RECENT_RECALLS: usize = 10;

/// The owner's recent-recalls command: the whole message is `최근 떠올린 기억` or
/// `recent recalls` (normalized like [`refresh_request`]). Returns the language it was given in.
pub fn recent_recalls_request(text: &str) -> Option<crate::status::Lang> {
    match normalize(text).as_str() {
        "최근 떠올린 기억" => Some(crate::status::Lang::Korean),
        "recent recalls" => Some(crate::status::Lang::English),
        _ => None,
    }
}

/// The answer to the recent-recalls command: one line per ledger row of `rows` (newest
/// first) with its age at `now` (unix seconds), wake phase, state, entry id and the entry's
/// current text, or `(gone)` when it was forgotten, replaced or removed.
pub fn recent_recalls(
    memory: &MemoryHandle,
    lang: crate::status::Lang,
    rows: &[crate::store::RecallRow],
    now: i64,
) -> String {
    use crate::status::Lang;
    use crate::store::{RecallPhase, RecallState};
    if rows.is_empty() {
        return match lang {
            Lang::Korean => "아직 떠올린 기억이 없어요.".into(),
            Lang::English => "No recalls yet.".into(),
        };
    }
    let mut out = vec![match lang {
        Lang::Korean => format!("최근 떠올린 기억 {}개 (최신순):", rows.len()),
        Lang::English => format!("Last {} recalls, newest first:", rows.len()),
    }];
    for row in rows {
        let text = match memory.show(&row.entry_id) {
            Ok(Some(found)) if !found.archived => found.entry.text,
            Ok(_) => match lang {
                Lang::Korean => "(없어짐)".to_owned(),
                Lang::English => "(gone)".to_owned(),
            },
            Err(err) => failure(&row.entry_id, &err),
        };
        let (phase, state) = match lang {
            Lang::English => (row.phase.as_str(), row.state.as_str()),
            Lang::Korean => (
                match row.phase {
                    RecallPhase::Owner => "오너 메시지",
                    RecallPhase::Mid => "작업 중",
                },
                match row.state {
                    RecallState::Delivered => "전달됨",
                    RecallState::Rejected => "거절됨",
                    RecallState::Shadow => "기록만",
                },
            ),
        };
        out.push(format!(
            "- {} {phase} {state} {} {text}",
            crate::status::ago(lang, now - row.at),
            row.entry_id
        ));
    }
    out.join("\n")
}

/// The memory id in `word` (`m-` and 6 to 16 lowercase hex digits, maybe a particle after
/// it), stripped of surrounding punctuation.
fn id_of(word: &str) -> Option<String> {
    let word = word.trim_matches(|c: char| ",.:!?()\"'`".contains(c));
    let rest = word.strip_prefix("m-")?;
    let hex_len = rest
        .char_indices()
        .find(|(_, c)| !matches!(c, '0'..='9' | 'a'..='f'))
        .map_or(rest.len(), |(i, _)| i);
    let (hex, tail) = rest.split_at(hex_len);
    ((6..=16).contains(&hex.len()) && PARTICLES.contains(&tail)).then(|| format!("m-{hex}"))
}

/// `m-xxxxxx 지워`, `forget m-xxxxxx`, `restore ...`, `show ...`: exactly one id, one verb,
/// otherwise only filler words.
fn parse_id_command(norm: &str) -> Option<Command> {
    let words: Vec<&str> = norm.split_whitespace().collect();
    if words.len() > MAX_ID_COMMAND_WORDS {
        return None;
    }
    let mut id = None;
    let mut verb: Option<fn(String) -> Command> = None;
    for word in words {
        let bare = word.trim_matches(|c: char| ",.:!?".contains(c));
        if let Some(found) = id_of(word) {
            if id.replace(found).is_some() {
                return None;
            }
            continue;
        }
        let this: fn(String) -> Command = if FORGET_VERBS.contains(&bare) {
            Command::Forget
        } else if RESTORE_VERBS.contains(&bare) {
            Command::Restore
        } else if SHOW_VERBS.contains(&bare) {
            Command::ShowOne
        } else if FILLER.contains(&bare) {
            continue;
        } else {
            return None;
        };
        if verb.replace(this).is_some() {
            return None;
        }
    }
    Some(verb?(id?))
}

/// `m-xxxxxx 고쳐: <new text>` / `correct m-xxxxxx: <new text>`.
fn parse_correct(text: &str) -> Option<Command> {
    let (head, new_text) = text.split_once(':')?;
    let new_text = new_text.trim();
    if new_text.is_empty() {
        return None;
    }
    let words: Vec<String> = normalize(head)
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    if words.len() > MAX_ID_COMMAND_WORDS {
        return None;
    }
    let mut id = None;
    let mut verb = false;
    for word in &words {
        if let Some(found) = id_of(word) {
            if id.replace(found).is_some() {
                return None;
            }
        } else if CORRECT_VERBS.contains(&word.as_str()) {
            verb = true;
        } else if !FILLER.contains(&word.as_str()) {
            return None;
        }
    }
    verb.then_some(())?;
    Some(Command::Correct {
        id: id?,
        text: new_text.to_owned(),
    })
}

/// The remember forms. A question is never one.
fn parse_remember(text: &str) -> Option<Command> {
    if text.trim_end().ends_with('?') {
        return None;
    }
    let (always, rest) = strip_always_prefix(text);
    if let Some((repo, body)) = prefixed(rest) {
        return remember(body, always, repo);
    }
    // Suffix form: `<text> (항상) 기억해줘` (or 둬 / 놔).
    let trimmed = text.trim_end_matches(|c: char| "!.~ ".contains(c) || c.is_whitespace());
    let suffix = KO_SUFFIXES.iter().find(|s| trimmed.ends_with(*s))?;
    let body = trimmed[..trimmed.len() - suffix.len()].trim_end();
    let (body, always) = match body.strip_suffix("항상") {
        Some(body) => (body, true),
        None => (body, false),
    };
    let body = body.trim_end_matches(|c: char| c == ',' || c.is_whitespace());
    remember(body, always, None)
}

/// The words that make a remember request a standing (core) rule.
const ALWAYS_WORDS: [&str; 2] = ["항상", "always"];

/// `항상 ` / `always ` at the start (case ignored).
fn strip_always_prefix(text: &str) -> (bool, &str) {
    let text = text.trim_start();
    for word in ALWAYS_WORDS {
        if let Some(rest) = text
            .get(..word.len())
            .filter(|head| head.eq_ignore_ascii_case(word))
            .map(|_| &text[word.len()..])
            && rest.starts_with(char::is_whitespace)
        {
            return (true, rest.trim_start());
        }
    }
    (false, text)
}

/// `기억해: <body>`, `기억해(<repo>): <body>`, `remember that <body>`; returns the repo, if
/// named, and the body.
fn prefixed(text: &str) -> Option<(Option<String>, &str)> {
    let lower = text.to_lowercase();
    for prefix in KO_PREFIXES.iter().chain(EN_PREFIXES.iter()) {
        if !lower.starts_with(prefix) {
            continue;
        }
        // ASCII and Hangul prefixes keep their byte length through `to_lowercase`.
        let rest = &text[prefix.len()..];
        let (repo, rest) = match rest.strip_prefix('(') {
            Some(inner) => {
                let (repo, after) = inner.split_once(')')?;
                (Some(repo.trim().to_owned()), after)
            }
            None => (None, rest),
        };
        let rest = rest.trim_start();
        if let Some(body) = rest.strip_prefix(':') {
            return Some((repo, body));
        }
        // `remember that <body>` needs no colon; a bare `remember <word>` does.
        if repo.is_none() && (*prefix == "remember that" || *prefix == "remember this") {
            return Some((None, rest));
        }
    }
    None
}

fn remember(body: &str, always: bool, repo: Option<String>) -> Option<Command> {
    let body = body.trim();
    if body.is_empty() {
        return None;
    }
    let target = match (always, repo) {
        (_, Some(repo)) if !repo.is_empty() => Target::Project(repo),
        (_, Some(_)) => return None,
        (true, None) if says_dont(body) => Target::Boundaries,
        (true, None) => Target::Core,
        (false, None) => Target::Conversation,
    };
    Some(Command::Remember {
        text: body.to_owned(),
        target,
    })
}

/// Whether a rule says not to do something.
pub(crate) fn says_dont(text: &str) -> bool {
    let lower = text.to_lowercase();
    [
        "하지 마",
        "하지마",
        "하지 말",
        "하지말",
        "never ",
        "don't ",
        "do not ",
    ]
    .iter()
    .any(|w| lower.contains(w))
}

/// The bucket of a remember request from conversation `key`. A key the store cannot use as a
/// file name falls back to `projects/_general.md`.
pub fn bucket_for(target: &Target, key: &str) -> Bucket {
    let bucket = match target {
        Target::Conversation => Bucket::Thread(key.to_owned()),
        Target::Project(repo) => Bucket::Project(repo.clone()),
        Target::Core => Bucket::Owner,
        Target::Boundaries => Bucket::Boundaries,
    };
    if bucket.validate().is_ok() {
        bucket
    } else {
        Bucket::General
    }
}

fn place(bucket: &Bucket) -> String {
    match bucket {
        Bucket::Owner => "always (core rules; the lead gets it with its next message)".into(),
        Bucket::Boundaries => {
            "always, as something not to do (core; the lead gets it with its next message)".into()
        }
        Bucket::Project(repo) => format!("project {repo}"),
        Bucket::General => "general notes".into(),
        Bucket::Thread(_) => "this conversation".into(),
        Bucket::SelfObs => "a correction you gave me".into(),
    }
}

fn entry_line(entry: &Entry) -> String {
    format!("- {} [{}] {}", entry.id, entry.day, entry.text)
}

fn found_line(found: &Found) -> String {
    let state = if found.archived {
        ", forgotten or replaced"
    } else {
        ""
    };
    format!(
        "{} ({}{state}) [{}] {}",
        found.entry.id,
        place(&found.bucket),
        found.entry.day,
        found.entry.text
    )
}

fn failure(id: &str, err: &MemoryError) -> String {
    match err.code {
        E_MEMORY_NOT_FOUND => format!("No memory entry {id}."),
        E_MEMORY_ARCHIVED => format!("{id} is forgotten or replaced; restore it first."),
        E_MEMORY_NOT_ARCHIVED => format!("{id} is not forgotten."),
        code => {
            tracing::warn!(code, detail = %err.detail, "memory command failed");
            format!("The memory command failed ({code}).")
        }
    }
}

/// What a command did: the reply text, and the id the dispatcher must ask about before
/// forgetting it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub reply: String,
    pub confirm_forget: Option<String>,
}

impl Answer {
    fn say(reply: String) -> Self {
        Self {
            reply,
            confirm_forget: None,
        }
    }
}

/// Runs `command` from owner row `row_id` of conversation `key`. Forget only asks (see
/// [`forget`]).
pub async fn run(memory: &MemoryHandle, key: &str, row_id: i64, command: Command) -> Answer {
    match command {
        Command::Remember { text, target } => {
            let entry = NewEntry {
                text,
                prov: Prov::Owner,
                bucket: bucket_for(&target, key),
                src: Source::row(row_id),
                expires: None,
                why: None,
            };
            Answer::say(match memory.add(entry).await {
                Ok(added) => {
                    let mut reply = if added.created {
                        format!("Remembered {} ({}).", added.id, place(&added.bucket))
                    } else {
                        format!("Already remembered as {}.", added.id)
                    };
                    if added.redacted {
                        reply.push_str(" Secret-like text was saved as a [redacted] marker.");
                    }
                    reply
                }
                Err(err) => failure("the entry", &err),
            })
        }
        Command::Show => Answer::say(list_all(memory, key)),
        Command::ShowSelf => Answer::say(show_self(memory)),
        Command::ShowOne(id) => Answer::say(match memory.show(&id) {
            Ok(Some(found)) => found_line(&found),
            Ok(None) => format!("No memory entry {id}."),
            Err(err) => failure(&id, &err),
        }),
        Command::Correct { id, text } => Answer::say(
            match memory.correct(&id, &text, Source::row(row_id)).await {
                Ok(corrected) => format!(
                    "Replaced {} with {} ({}).",
                    corrected.old_id,
                    corrected.new.id,
                    place(&corrected.new.bucket)
                ),
                Err(err) => failure(&id, &err),
            },
        ),
        Command::Forget(id) => match memory.show(&id) {
            Ok(Some(found)) if found.archived => Answer::say(format!("{id} is already forgotten.")),
            Ok(Some(found)) => Answer {
                reply: format!(
                    "Forget this? {}\nReply yes to forget it or no to keep it.",
                    found_line(&found)
                ),
                confirm_forget: Some(id),
            },
            Ok(None) => Answer::say(format!("No memory entry {id}.")),
            Err(err) => Answer::say(failure(&id, &err)),
        },
        Command::Restore(id) => Answer::say(match memory.restore(&id).await {
            Ok(found) => format!("Restored {}.", found_line(&found)),
            Err(err) => failure(&id, &err),
        }),
    }
}

/// Acts on the owner's answer to the forget question about `id`.
pub async fn forget(memory: &MemoryHandle, id: &str, yes: bool) -> String {
    if !yes {
        return format!("Kept {id}.");
    }
    match memory.forget(id).await {
        Ok(_) => format!("Forgot {id}. Say \"restore {id}\" to bring it back."),
        Err(err) => failure(id, &err),
    }
}

/// The corrections the owner gave (`self/observations.md`) with their ids, newest last.
fn show_self(memory: &MemoryHandle) -> String {
    let entries = match memory.entries(&Bucket::SelfObs) {
        Ok(entries) => entries,
        Err(err) => return failure("this list", &err),
    };
    if entries.is_empty() {
        return "No corrections recorded yet.".into();
    }
    let mut out = vec!["Corrections you gave me (your own words):".to_owned()];
    out.extend(entries.iter().map(entry_line));
    out.push(
        "Say \"m-xxxxxx 지워\" / \"forget m-xxxxxx\" to forget one, or \"기억해: <rule>\" to keep a standing rule."
            .into(),
    );
    out.join("\n")
}

fn list_all(memory: &MemoryHandle, key: &str) -> String {
    let mut sections = vec![
        ("Always (core rules):", Bucket::Owner),
        ("Not to do (core):", Bucket::Boundaries),
    ];
    let thread = Bucket::Thread(key.to_owned());
    if thread.validate().is_ok() {
        sections.push(("This conversation:", thread));
    }
    let mut out = Vec::new();
    for (heading, bucket) in sections {
        match memory.entries(&bucket) {
            Ok(entries) if entries.is_empty() => {}
            Ok(entries) => {
                out.push(heading.to_owned());
                out.extend(entries.iter().map(entry_line));
            }
            Err(err) => out.push(format!("{heading} {}", failure("this list", &err))),
        }
    }
    if out.is_empty() {
        return "Nothing remembered yet.".into();
    }
    out.push(
        "Say \"m-xxxxxx 지워\" / \"forget m-xxxxxx\" to forget, \"m-xxxxxx 고쳐: <new text>\" to correct."
            .into(),
    );
    out.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{Day, MemoryStore, Redactor};
    use std::sync::Arc;

    fn remember_in(text: &str) -> Option<(String, Target)> {
        match parse(text)? {
            Command::Remember { text, target } => Some((text, target)),
            other => panic!("expected Remember, got {other:?}"),
        }
    }

    #[test]
    fn remember_defaults_to_the_conversation() {
        for text in [
            "기억해: 회의는 3시",
            "회의는 3시 기억해놔",
            "회의는 3시, 기억해줘!",
            "회의는 3시 기억해 둬",
            "remember: 회의는 3시",
            "remember that 회의는 3시",
        ] {
            assert_eq!(
                remember_in(text),
                Some(("회의는 3시".into(), Target::Conversation)),
                "{text}"
            );
        }
        assert_eq!(
            remember_in("이거 기억해줘"),
            Some(("이거".into(), Target::Conversation))
        );
    }

    #[test]
    fn only_an_explicit_always_reaches_core() {
        assert_eq!(
            remember_in("항상 기억해: 존댓말로 답해"),
            Some(("존댓말로 답해".into(), Target::Core))
        );
        assert_eq!(
            remember_in("존댓말로 답해 항상 기억해줘"),
            Some(("존댓말로 답해".into(), Target::Core))
        );
        assert_eq!(
            remember_in("always remember: answer in Korean"),
            Some(("answer in Korean".into(), Target::Core))
        );
        assert_eq!(
            remember_in("항상 기억해: 금요일엔 배포하지 마"),
            Some(("금요일엔 배포하지 마".into(), Target::Boundaries))
        );
        assert_eq!(
            remember_in("기억해(usunmn/moraebot): CI는 cargo test"),
            Some((
                "CI는 cargo test".into(),
                Target::Project("usunmn/moraebot".into())
            ))
        );
    }

    #[test]
    fn questions_and_plain_sentences_are_not_commands() {
        for text in [
            "그거 기억해?",
            "어제 한 얘기 기억해?",
            "어제 한 얘기 기억해",
            "그거 기억해",
            "회의는 3시 기억해 두어",
            "이거 기억해서 나중에 PR 열어줘",
            "remember to open the PR",
            "m-1f3a2b 지워진 이유가 뭐야",
            "m-1f3a2b 이건 lane이 지워 달래서 지운 거야 확인해봐",
            "기억해:",
            "지워",
            "",
        ] {
            assert_eq!(parse(text), None, "{text}");
        }
    }

    #[test]
    fn id_commands_and_show() {
        assert_eq!(parse("뭐 기억해?"), Some(Command::Show));
        assert_eq!(parse("What do you remember?"), Some(Command::Show));
        assert_eq!(
            parse("m-1f3a2b를 지워 줘"),
            Some(Command::Forget("m-1f3a2b".into()))
        );
        assert_eq!(
            parse("forget m-1f3a2b"),
            Some(Command::Forget("m-1f3a2b".into()))
        );
        assert_eq!(
            parse("되살려 m-0c4d5e"),
            Some(Command::Restore("m-0c4d5e".into()))
        );
        assert_eq!(
            parse("m-0c4d5e 보여줘"),
            Some(Command::ShowOne("m-0c4d5e".into()))
        );
        assert_eq!(
            parse("m-1f3a2b 고쳐: 회의는 4시"),
            Some(Command::Correct {
                id: "m-1f3a2b".into(),
                text: "회의는 4시".into()
            })
        );
        assert_eq!(parse("m-1f3a2b m-0c4d5e 지워"), None, "two ids");
        assert_eq!(parse("m-1f3a2b 지워 되살려"), None, "two verbs");
    }

    #[test]
    fn only_the_whole_refresh_phrase_is_the_refresh_command() {
        use crate::status::Lang;
        assert_eq!(refresh_request("기억 새로고침"), Some(Lang::Korean));
        assert_eq!(refresh_request("  Refresh   memory! "), Some(Lang::English));
        for text in [
            "refresh memory please",
            "기억 새로고침 해줘",
            "refresh",
            "memory",
        ] {
            assert_eq!(refresh_request(text), None, "{text}");
        }
    }

    #[test]
    fn confirmation_words() {
        assert_eq!(confirmation("응"), Some(true));
        assert_eq!(confirmation("Yes!"), Some(true));
        assert_eq!(confirmation("아니"), Some(false));
        assert_eq!(confirmation("cancel"), Some(false));
        assert_eq!(confirmation("응 근데 PR은 열어줘"), None);
    }

    #[test]
    fn unusable_thread_keys_fall_back_to_general() {
        assert_eq!(
            bucket_for(&Target::Conversation, "discord:th:111"),
            Bucket::Thread("discord:th:111".into())
        );
        assert_eq!(
            bucket_for(&Target::Conversation, "slack:th:C1/../x"),
            Bucket::General
        );
    }

    #[tokio::test]
    async fn remember_show_correct_forget_restore_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let today = Day::from_ymd(2026, 11, 20).expect("day");
        let store = MemoryStore::open(dir.path(), Redactor::default(), Arc::new(move || today))
            .expect("open");
        let memory = MemoryHandle::spawn(store).expect("spawn");
        let key = "discord:th:111";
        let run_text = |text: &str| parse(text).expect("a command");

        let saved = run(&memory, key, 7, run_text("회의는 3시 기억해줘")).await;
        assert!(saved.reply.starts_with("Remembered m-"), "{}", saved.reply);
        let id = memory
            .entries(&Bucket::Thread(key.into()))
            .expect("entries")[0]
            .id
            .clone();
        assert!(memory.entries(&Bucket::Owner).expect("core").is_empty());

        let shown = run(&memory, key, 8, Command::Show).await.reply;
        assert!(
            shown.contains(&id) && shown.contains("회의는 3시"),
            "{shown}"
        );

        let fixed = run(&memory, key, 9, run_text(&format!("{id} 고쳐: 회의는 4시"))).await;
        assert!(fixed.reply.starts_with(&format!("Replaced {id} with m-")));
        let new_id = memory
            .entries(&Bucket::Thread(key.into()))
            .expect("entries")[0]
            .id
            .clone();

        let asked = run(&memory, key, 10, run_text(&format!("{new_id} 지워"))).await;
        assert_eq!(asked.confirm_forget.as_deref(), Some(new_id.as_str()));
        assert!(
            !memory
                .entries(&Bucket::Thread(key.into()))
                .expect("entries")
                .is_empty(),
            "asking forgets nothing"
        );
        assert_eq!(
            forget(&memory, &new_id, false).await,
            format!("Kept {new_id}.")
        );
        assert!(forget(&memory, &new_id, true).await.starts_with("Forgot"));
        assert!(
            memory
                .entries(&Bucket::Thread(key.into()))
                .expect("entries")
                .is_empty()
        );

        let back = run(&memory, key, 11, Command::Restore(new_id.clone())).await;
        assert!(back.reply.starts_with("Restored"), "{}", back.reply);
        assert_eq!(
            run(&memory, key, 12, Command::Forget("m-ffffff".into()))
                .await
                .reply,
            "No memory entry m-ffffff."
        );
    }

    #[test]
    fn the_self_question_is_a_command() {
        for text in [
            "너 자신에 대해 뭐 기억해?",
            "What do you know about yourself?",
        ] {
            assert_eq!(parse(text), Some(Command::ShowSelf), "{text}");
        }
        assert_eq!(parse("뭐 기억해?"), Some(Command::Show));
    }

    #[tokio::test]
    async fn show_self_lists_the_recorded_corrections() {
        let dir = tempfile::tempdir().expect("tempdir");
        let today = Day::from_ymd(2026, 11, 21).expect("day");
        let store = MemoryStore::open(dir.path(), Redactor::default(), Arc::new(move || today))
            .expect("open");
        let memory = MemoryHandle::spawn(store).expect("spawn");
        let empty = run(&memory, "k", 1, Command::ShowSelf).await.reply;
        assert_eq!(empty, "No corrections recorded yet.");

        let added = memory
            .add(NewEntry {
                text: "왜 또 영어로 답해?".into(),
                prov: Prov::Owner,
                bucket: Bucket::SelfObs,
                src: Source::row(7),
                expires: None,
                why: None,
            })
            .await
            .expect("add");
        let shown = run(&memory, "k", 3, Command::ShowSelf).await.reply;
        assert!(shown.contains(&added.id), "{shown}");
        assert!(shown.contains("왜 또 영어로 답해?"), "{shown}");
        let one = run(&memory, "k", 4, Command::ShowOne(added.id.clone()))
            .await
            .reply;
        assert!(one.contains("a correction you gave me"), "{one}");
    }

    #[tokio::test]
    async fn recent_recalls_lists_ledger_rows_with_entry_text() {
        use crate::status::Lang;
        use crate::store::{RecallPhase, RecallRow, RecallState, Store};

        assert_eq!(
            recent_recalls_request("Recent recalls?"),
            Some(Lang::English)
        );
        assert_eq!(
            recent_recalls_request("최근 떠올린 기억"),
            Some(Lang::Korean)
        );
        assert_eq!(recent_recalls_request("show recent recalls please"), None);

        let dir = tempfile::tempdir().expect("tempdir");
        let today = Day::from_ymd(2026, 11, 22).expect("day");
        let store = MemoryStore::open(
            &dir.path().join("memory"),
            Redactor::default(),
            Arc::new(move || today),
        )
        .expect("open");
        let memory = MemoryHandle::spawn(store).expect("spawn");
        let added = memory
            .add(NewEntry {
                text: "deploys go through make release".into(),
                prov: Prov::Owner,
                bucket: Bucket::General,
                src: Source::row(3),
                expires: None,
                why: None,
            })
            .await
            .expect("add");
        let mut ledger = Store::open(&dir.path().join("inbox.sqlite")).expect("store");
        let row = |id: &str, state, at, phase| RecallRow {
            conversation: "k".into(),
            entry_id: id.into(),
            entry_hash: String::new(),
            state,
            lead_generation: 2,
            lead_epoch: 0,
            row_id: Some(9),
            at,
            phase,
        };
        ledger
            .record_recall(&[
                row(&added.id, RecallState::Delivered, 1000, RecallPhase::Owner),
                row("m-abcdef", RecallState::Shadow, 1600, RecallPhase::Mid),
            ])
            .expect("ledger");
        let rows = ledger.recent_recalls(RECENT_RECALLS).expect("rows");
        let text = recent_recalls(&memory, Lang::English, &rows, 1660);
        assert_eq!(
            text,
            format!(
                "Last 2 recalls, newest first:\n- 1 min ago mid shadow m-abcdef (gone)\n\
                 - 11 min ago owner delivered {} deploys go through make release",
                added.id
            )
        );
        let text = recent_recalls(&memory, Lang::Korean, &rows, 1660);
        assert!(
            text.contains("작업 중 기록만 m-abcdef (없어짐)")
                && text.contains("오너 메시지 전달됨"),
            "{text}"
        );
    }
}
