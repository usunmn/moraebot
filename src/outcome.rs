//! Outcome memory: a lane result that ends in an error, an unmet done check or a stop is saved
//! as one reference entry of the lane's project (`prov = lane`), so a later lane or the lead
//! can recall what went wrong. The text is built here from codes and moraebot's own fields,
//! never from the lane's words; the memory store redacts and escapes it as for every entry.
//! A successful result saves nothing.

use crate::memory::{Bucket, NewEntry, Prov, Source};

/// At most this many characters of the task's first line.
pub const MAX_TASK_CHARS: usize = 120;
/// At most this many characters of the failure detail (check names or error line).
pub const MAX_DETAIL_CHARS: usize = 200;

/// Why a lane result failed, as far as the entry tells it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure<'a> {
    /// An error outcome; its detail text (only the first line is kept).
    Error(&'a str),
    /// The last `lane_done` claim failed; the claim's receipt ([`crate::done::receipt`]).
    Unmet(&'a str),
    /// The lead stopped the job; the reason it gave.
    Stopped(&'a str),
}

/// The first line of `text`, trimmed and cut to `max` characters.
fn first_line(text: &str, max: usize) -> String {
    let line = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    line.chars().take(max).collect()
}

/// The signals of the failed checks in a done receipt (`- <signal> -> <code>: <detail>`).
fn failed_checks(receipt: &str) -> Vec<&str> {
    receipt
        .lines()
        .filter_map(|line| line.strip_prefix("- "))
        .filter_map(|line| line.split_once(" -> "))
        .filter(|(_, result)| *result != "ok")
        .map(|(signal, _)| signal.trim())
        .collect()
}

/// The entry text for lane `lane_id`'s failed result.
pub fn text(lane_id: i64, task: &str, code: &str, failure: &Failure) -> String {
    let detail = match failure {
        Failure::Error(detail) => {
            format!("Error: {}", first_line(detail, MAX_DETAIL_CHARS))
        }
        Failure::Unmet(receipt) => {
            let names = failed_checks(receipt).join(", ");
            format!(
                "Failed checks: {}",
                names.chars().take(MAX_DETAIL_CHARS).collect::<String>()
            )
        }
        Failure::Stopped(reason) => {
            format!("Stopped: {}", first_line(reason, MAX_DETAIL_CHARS))
        }
    };
    format!(
        "Lane {lane_id} result failed with {code}. Task: {}. {detail}",
        first_line(task, MAX_TASK_CHARS)
    )
}

/// The entry for result `seq` of lane `lane_id`, saved in `projects` (the lane's project
/// buckets, first one used) or the general project bucket.
pub fn entry(
    lane_id: i64,
    seq: i64,
    projects: &[String],
    task: &str,
    code: &str,
    failure: &Failure,
) -> NewEntry {
    NewEntry {
        text: text(lane_id, task, code, failure),
        prov: Prov::Lane,
        bucket: projects
            .first()
            .map_or(Bucket::General, |name| Bucket::Project(name.clone())),
        src: Source::lane(lane_id, seq),
        expires: None,
        why: Some(format!("lane {lane_id} result {seq} failed")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unmet_text_names_only_the_failed_checks() {
        let receipt = "evidence: ran it\n- file a -> ok\n- cmd make test -> E_CHECK_EXIT: exit 2";
        let text = text(
            7,
            "Fix the build\nmore",
            "E_DONE_UNMET",
            &Failure::Unmet(receipt),
        );
        assert_eq!(
            text,
            "Lane 7 result failed with E_DONE_UNMET. Task: Fix the build. Failed checks: cmd make test"
        );
    }

    #[test]
    fn long_task_and_error_are_cut() {
        let task = "t".repeat(300);
        let err = format!("{}\nsecond", "e".repeat(300));
        let text = text(1, &task, "E_X", &Failure::Error(&err));
        assert!(text.contains(&format!("Task: {}.", "t".repeat(MAX_TASK_CHARS))));
        assert!(text.ends_with(&format!("Error: {}", "e".repeat(MAX_DETAIL_CHARS))));
    }
}
