//! Owner status questions answered by moraebot itself (#68), so the owner gets an answer
//! while the lead is busy. The matcher is rule-based and conservative: only the whole message,
//! after trimming whitespace and end punctuation and ignoring case, compared with a fixed list.
//! "status of PR 12?" or "뭐 하고 있어? 그거 말고 X 해줘" still go to the lead.
//!
//! The answer is built from the lead's turn state ([`LeadStatus`]) and the running lanes. The
//! lead's current step is shown only when its turn belongs to the asking conversation; a turn
//! of another conversation is named without details, and lanes of other conversations are only
//! counted, so a shared channel never sees another conversation's work.

use std::time::Duration;

use crate::omp::{LeadCache, LeadStatus};

/// The language a status question was asked in; the answer uses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lang {
    Korean,
    English,
}

const KOREAN: &[&str] = &[
    "뭐 하고 있어",
    "뭐하고 있어",
    "뭐 하고있어",
    "뭐하고있어",
    "지금 뭐 하고 있어",
    "지금 뭐하고 있어",
    "뭐해",
    "뭐 해",
    "지금 뭐해",
    "지금 뭐 해",
    "상태",
    "지금 상태",
];

const ENGLISH: &[&str] = &[
    "status",
    "what are you doing",
    "what're you doing",
    "what are you doing now",
];

/// A task's first line is cut to this many characters in a status answer.
const TASK_CHARS: usize = 80;

/// The language of `text` when the whole message is a status question; `None` otherwise.
pub fn request(text: &str) -> Option<Lang> {
    let trimmed = text.trim().trim_end_matches(|c: char| {
        c.is_whitespace() || matches!(c, '?' | '!' | '.' | '~' | '？' | '！' | '。')
    });
    let words = trimmed.split_whitespace().collect::<Vec<_>>().join(" ");
    let words = words.to_lowercase();
    if KOREAN.contains(&words.as_str()) {
        Some(Lang::Korean)
    } else if ENGLISH.contains(&words.as_str()) {
        Some(Lang::English)
    } else {
        None
    }
}

/// One running lane of the asking conversation.
pub struct LaneLine<'a> {
    pub id: i64,
    /// How long its current job has run; `None` when moraebot does not know.
    pub running: Option<Duration>,
    /// The task the lead gave it.
    pub task: &'a str,
}

/// The status answer for conversation `key`: the lead's turn (`lead`, its intent already
/// cleaned), the running lanes of this conversation (`lanes`, each task's first line cleaned by
/// `clean`, which scrubs secrets and paths like a lane note), the number of running lanes of
/// other conversations, then one line from `cache`: the lead's prompt cache hit rate over
/// its recent turns and whether a memory change waits for the next lead restart, and one from
/// `recall` when given: the recall advisor's modes and recent use.
#[allow(clippy::too_many_arguments)]
pub fn reply(
    lang: Lang,
    key: &str,
    lead: Option<&LeadStatus>,
    cache: &LeadCache,
    recall: Option<&RecallStatus>,
    lanes: &[LaneLine],
    others: usize,
    clean: &dyn Fn(&str) -> String,
) -> String {
    let mut lines = Vec::new();
    match lead {
        Some(turn) if turn.conversation == key => {
            let minutes = minutes(turn.running);
            lines.push(match lang {
                Lang::Korean => format!("이 대화 일을 {minutes}분째 하고 있어요"),
                Lang::English => format!("Working on this conversation for {minutes} min."),
            });
            if let Some((name, intent)) = &turn.tool {
                let step = intent.as_deref().unwrap_or(name);
                lines.push(match lang {
                    Lang::Korean => format!("지금 단계: {step}"),
                    Lang::English => format!("Current step: {step}"),
                });
            }
        }
        Some(turn) => {
            let minutes = minutes(turn.running);
            lines.push(match lang {
                Lang::Korean => format!("다른 대화 일을 {minutes}분째 하고 있어요"),
                Lang::English => {
                    format!("Working on another conversation for {minutes} min.")
                }
            });
        }
        None => lines.push(
            match lang {
                Lang::Korean => "지금 하고 있는 일은 없어요",
                Lang::English => "Not working on anything right now.",
            }
            .to_owned(),
        ),
    }
    if !lanes.is_empty() {
        lines.push(match lang {
            Lang::Korean => format!("이 대화의 백그라운드 작업 {}개:", lanes.len()),
            Lang::English => format!("Background lanes of this conversation: {}", lanes.len()),
        });
        for lane in lanes {
            let task = first_line(lane.task, clean);
            lines.push(match (lang, lane.running) {
                (Lang::Korean, Some(running)) => {
                    format!("- lane {} ({}분째): {task}", lane.id, minutes(running))
                }
                (Lang::English, Some(running)) => {
                    format!("- lane {} ({} min): {task}", lane.id, minutes(running))
                }
                (_, None) => format!("- lane {}: {task}", lane.id),
            });
        }
    }
    if others > 0 {
        lines.push(match lang {
            Lang::Korean => format!("다른 대화의 백그라운드 작업도 {others}개 돌고 있어요"),
            Lang::English => format!("{others} more lanes run for other conversations."),
        });
    }
    if lanes.is_empty() && others == 0 {
        lines.push(
            match lang {
                Lang::Korean => "돌고 있는 백그라운드 작업은 없어요",
                Lang::English => "No background lanes are running.",
            }
            .to_owned(),
        );
    }
    lines.push(cache_line(lang, cache));
    if let Some(recall) = recall {
        lines.push(recall_line(lang, recall));
    }
    lines.join("\n")
}

/// What the status answer tells about the recall advisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecallStatus {
    /// `memory.recall.mode` (`off`, `shadow`, `on`).
    pub mode: &'static str,
    /// The mode while a lead turn runs (`off`, `shadow`, `on`).
    pub mid: &'static str,
    /// Advisor calls answered in the last hour.
    pub calls_last_hour: i64,
    /// Seconds since the newest delivered pick; `None` when none was ever delivered.
    pub last_delivered: Option<i64>,
}

/// The recall advisor line of a status answer.
fn recall_line(lang: Lang, recall: &RecallStatus) -> String {
    let last = recall.last_delivered.map(|secs| ago(lang, secs));
    match lang {
        Lang::Korean => format!(
            "기억 떠올리기: {} (작업 중 {}), 최근 1시간 호출 {}번, 마지막 전달 {}",
            recall.mode,
            recall.mid,
            recall.calls_last_hour,
            last.as_deref().unwrap_or("없음")
        ),
        Lang::English => format!(
            "Recall: {} (mid-task {}), {} calls in the last hour, last delivered pick {}.",
            recall.mode,
            recall.mid,
            recall.calls_last_hour,
            last.as_deref().unwrap_or("never")
        ),
    }
}

/// The memory curator line of a status answer at `now` (unix seconds): `None` when
/// `memory.curate.mode = off`; otherwise the last run's age and counts, the failed runs on the
/// current window when there are any, and whether the last run only logged (shadow).
pub fn curate_line(lang: Lang, curate: Option<&crate::store::CurateStatus>, now: i64) -> String {
    let Some(curate) = curate else {
        return match lang {
            Lang::Korean => "기억 정리: 꺼짐".to_owned(),
            Lang::English => "Memory tidy: off.".to_owned(),
        };
    };
    let mut parts = vec![match (lang, curate.last_run_at) {
        (Lang::Korean, Some(at)) => format!("마지막 실행 {}", ago(lang, now - at)),
        (Lang::English, Some(at)) => format!("last run {}", ago(lang, now - at)),
        (Lang::Korean, None) => "아직 실행 안 함".to_owned(),
        (Lang::English, None) => "not run yet".to_owned(),
    }];
    for (op, n) in &curate.last_counts {
        let (ko, en) = match op.as_str() {
            "save" => ("저장", "saved"),
            "replace" => ("교체", "replaced"),
            "promote" => ("핵심으로 올림", "promoted"),
            "demote" => ("핵심에서 내림", "demoted"),
            "unsave" => ("저장 취소", "unsaved"),
            "forget" => ("지움", "forgot"),
            "conflict" => ("충돌", "conflicts"),
            "skipped" => ("건너뛴 구간", "skipped windows"),
            _ => continue,
        };
        parts.push(match lang {
            Lang::Korean => format!("{ko} {n}"),
            Lang::English => format!("{en} {n}"),
        });
    }
    if curate.window_failures > 0 {
        parts.push(match lang {
            Lang::Korean => format!("실패 {}", curate.window_failures),
            Lang::English => format!("failures {}", curate.window_failures),
        });
    }
    let mut line = match lang {
        Lang::Korean => format!("기억 정리: {}", parts.join(", ")),
        Lang::English => format!("Memory tidy: {}.", parts.join(", ")),
    };
    if curate.last_shadow {
        line.push_str(match lang {
            Lang::Korean => " (기록만 하고 적용은 안 했어요)",
            Lang::English => " Shadow mode: nothing was applied.",
        });
    }
    line
}

/// How long ago `secs` seconds is, in whole minutes, hours or days.
pub fn ago(lang: Lang, secs: i64) -> String {
    let secs = secs.max(0);
    let (n, ko, en) = if secs < 3600 {
        (secs / 60, "분", "min")
    } else if secs < 86_400 {
        (secs / 3600, "시간", "h")
    } else {
        (secs / 86_400, "일", "d")
    };
    match lang {
        Lang::Korean => format!("{n}{ko} 전"),
        Lang::English => format!("{n} {en} ago"),
    }
}

/// The prompt cache line of a status answer.
fn cache_line(lang: Lang, cache: &LeadCache) -> String {
    let mut line = match (lang, cache.recent.hit_percent()) {
        (Lang::Korean, Some(percent)) => {
            format!("프롬프트 캐시 적중률: 최근 {}턴 {percent}%", cache.turns)
        }
        (Lang::English, Some(percent)) => format!(
            "Prompt cache hit rate: {percent}% over the last {} turns.",
            cache.turns
        ),
        (Lang::Korean, None) => "프롬프트 캐시: 아직 잰 사용량이 없어요".to_owned(),
        (Lang::English, None) => "Prompt cache: no usage measured yet.".to_owned(),
    };
    if cache.memory_waiting {
        line.push_str(match lang {
            Lang::Korean => " (바뀐 기억은 다음 lead 재시작 때 시스템 프롬프트에 반영돼요)",
            Lang::English => " A memory change waits for the next lead restart.",
        });
    }
    line
}

/// Whole minutes, at least one.
fn minutes(running: Duration) -> u64 {
    (running.as_secs() / 60).max(1)
}

/// The task's first non-empty line, cleaned by `clean` and cut to [`TASK_CHARS`].
fn first_line(task: &str, clean: &dyn Fn(&str) -> String) -> String {
    let line = task
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default();
    let line = clean(line);
    if line.chars().count() <= TASK_CHARS {
        return line;
    }
    let cut: String = line.chars().take(TASK_CHARS).collect();
    format!("{cut}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::CurateStatus;

    #[test]
    fn curate_line_tells_the_last_run_its_counts_failures_and_off() {
        let now = 100_000;
        let mut status = CurateStatus {
            last_run_at: Some(now - 2 * 3600),
            last_run_id: Some(4),
            last_counts: vec![
                ("save".into(), 3),
                ("replace".into(), 2),
                ("promote".into(), 1),
                ("conflict".into(), 1),
            ],
            last_shadow: false,
            open_conflicts: vec![("m-aaaaaa".into(), "m-bbbbbb".into())],
            window_failures: 0,
            failed_runs: 5,
        };
        assert_eq!(
            curate_line(Lang::English, Some(&status), now),
            "Memory tidy: last run 2 h ago, saved 3, replaced 2, promoted 1, conflicts 1."
        );
        assert_eq!(
            curate_line(Lang::Korean, Some(&status), now),
            "기억 정리: 마지막 실행 2시간 전, 저장 3, 교체 2, 핵심으로 올림 1, 충돌 1"
        );
        status.window_failures = 2;
        status.last_shadow = true;
        assert_eq!(
            curate_line(Lang::English, Some(&status), now),
            "Memory tidy: last run 2 h ago, saved 3, replaced 2, promoted 1, conflicts 1, \
             failures 2. Shadow mode: nothing was applied."
        );
        assert_eq!(curate_line(Lang::English, None, now), "Memory tidy: off.");
        assert_eq!(curate_line(Lang::Korean, None, now), "기억 정리: 꺼짐");
    }
    use crate::lane::news_text;
    use std::path::Path;

    /// Cleans like a lane note with `/qa/work` as the work folder.
    fn clean(text: &str) -> String {
        news_text(text, &[(Path::new("/qa/work"), ".")])
    }

    #[test]
    fn only_whole_status_questions_match() {
        for (text, lang) in [
            ("뭐 하고 있어?", Lang::Korean),
            ("  뭐해  ", Lang::Korean),
            ("뭐하고있어??", Lang::Korean),
            ("상태", Lang::Korean),
            ("Status", Lang::English),
            ("what are you doing?", Lang::English),
            ("What  are you   doing!", Lang::English),
        ] {
            assert_eq!(request(text), Some(lang), "{text}");
        }
        for text in [
            "status of PR 12?",
            "뭐 하고 있어? 그거 말고 X 해줘",
            "뭐해 이거",
            "what are you doing with the build",
            "",
            "?",
        ] {
            assert_eq!(request(text), None, "{text}");
        }
    }

    fn turn(key: &str, tool: Option<(&str, Option<&str>)>) -> LeadStatus {
        LeadStatus {
            conversation: key.to_owned(),
            running: Duration::from_secs(7 * 60 + 5),
            tool: tool.map(|(name, intent)| (name.to_owned(), intent.map(str::to_owned))),
        }
    }

    #[test]
    fn the_answer_shows_this_conversations_turn_and_lanes() {
        let lead = turn("p:th:1", Some(("bash", Some("running the tests"))));
        let lanes = [LaneLine {
            id: 4,
            running: Some(Duration::from_secs(30)),
            task: "\nfix the build in /qa/work/src\nmore detail",
        }];
        let none = LeadCache::default();
        assert_eq!(
            reply(
                Lang::English,
                "p:th:1",
                Some(&lead),
                &none,
                None,
                &lanes,
                2,
                &clean
            ),
            "Working on this conversation for 7 min.\nCurrent step: running the tests\n\
             Background lanes of this conversation: 1\n- lane 4 (1 min): fix the build in ./src\n\
             2 more lanes run for other conversations.\nPrompt cache: no usage measured yet."
        );
        let no_intent = turn("p:th:1", Some(("read", None)));
        assert_eq!(
            reply(
                Lang::Korean,
                "p:th:1",
                Some(&no_intent),
                &none,
                None,
                &[],
                0,
                &clean
            ),
            "이 대화 일을 7분째 하고 있어요\n지금 단계: read\n돌고 있는 백그라운드 작업은 없어요\n\
             프롬프트 캐시: 아직 잰 사용량이 없어요"
        );
    }

    #[test]
    fn another_conversations_turn_is_named_without_details() {
        let lead = turn("p:th:2", Some(("bash", Some("secret plan"))));
        let none = LeadCache::default();
        let text = reply(
            Lang::English,
            "p:th:1",
            Some(&lead),
            &none,
            None,
            &[],
            0,
            &clean,
        );
        assert_eq!(
            text,
            "Working on another conversation for 7 min.\nNo background lanes are running.\n\
             Prompt cache: no usage measured yet."
        );
        assert_eq!(
            reply(Lang::Korean, "p:th:1", None, &none, None, &[], 1, &clean),
            "지금 하고 있는 일은 없어요\n다른 대화의 백그라운드 작업도 1개 돌고 있어요\n\
             프롬프트 캐시: 아직 잰 사용량이 없어요"
        );
    }

    #[test]
    fn the_answer_ends_with_the_cache_hit_rate_and_a_waiting_memory_change() {
        let cache = LeadCache {
            turns: 3,
            recent: crate::omp::TokenUsage {
                input: 10,
                output: 50,
                cache_read: 970,
                cache_write: 20,
            },
            last_restart: None,
            memory_waiting: true,
        };
        let english = reply(Lang::English, "p:th:1", None, &cache, None, &[], 0, &clean);
        assert_eq!(
            english.lines().last(),
            Some(
                "Prompt cache hit rate: 97% over the last 3 turns. A memory change waits for the next lead restart."
            )
        );
        let korean = reply(Lang::Korean, "p:th:1", None, &cache, None, &[], 0, &clean);
        assert_eq!(
            korean.lines().last(),
            Some(
                "프롬프트 캐시 적중률: 최근 3턴 97% (바뀐 기억은 다음 lead 재시작 때 시스템 프롬프트에 반영돼요)"
            )
        );
    }

    #[test]
    fn the_recall_line_follows_the_cache_line() {
        let recall = RecallStatus {
            mode: "on",
            mid: "shadow",
            calls_last_hour: 4,
            last_delivered: Some(125),
        };
        let none = LeadCache::default();
        let english = reply(
            Lang::English,
            "p:th:1",
            None,
            &none,
            Some(&recall),
            &[],
            0,
            &clean,
        );
        assert_eq!(
            english.lines().last(),
            Some(
                "Recall: on (mid-task shadow), 4 calls in the last hour, last delivered pick 2 min ago."
            )
        );
        let never = RecallStatus {
            last_delivered: None,
            ..recall
        };
        let korean = reply(
            Lang::Korean,
            "p:th:1",
            None,
            &none,
            Some(&never),
            &[],
            0,
            &clean,
        );
        assert_eq!(
            korean.lines().last(),
            Some("기억 떠올리기: on (작업 중 shadow), 최근 1시간 호출 4번, 마지막 전달 없음")
        );
    }

    #[test]
    fn a_long_task_line_is_cut() {
        let task = "x".repeat(200);
        let line = first_line(&task, &clean);
        assert_eq!(line.chars().count(), TASK_CHARS + 1);
        assert!(line.ends_with('…'));
    }
}
