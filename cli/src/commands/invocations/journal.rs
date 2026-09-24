// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::str::FromStr;

use anyhow::Result;
use chrono::{DateTime, Local};
use cling::prelude::*;
use serde_json::{Value, json};

use restate_cli_util::ui::watcher::Watch;
use restate_cli_util::{CliContext, c_println, c_title, exit};

use crate::cli_env::CliEnv;
use crate::clients::datafusion_helpers::{
    Invocation, JournalEntryRow, JournalFetch, get_invocation, get_journal, get_journal_length,
};
use crate::ui::fmt::{
    Formatter, JournalRow, JournalScope, OutputFormatter, compact_duration, journal_time,
};
use crate::ui::invocations::rich_invocation_status;

const DEFAULT_HEAD: u32 = 5;
const DEFAULT_TAIL: u32 = 15;

/// Which entries to select: a single index, or an inclusive range with optional open
/// ends (`5`, `1..3`, `..10`, `90..`).
#[derive(Clone, Debug, PartialEq, Eq)]
enum EntrySelector {
    One(u32),
    Range(Option<u32>, Option<u32>),
}

impl FromStr for EntrySelector {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.trim();
        if let Some((start, end)) = s.split_once("..") {
            let start = parse_bound(start)?;
            let end = parse_bound(end)?;
            if let (Some(start), Some(end)) = (start, end)
                && start > end
            {
                return Err(format!(
                    "invalid range '{s}': start {start} is greater than end {end}"
                ));
            }
            Ok(EntrySelector::Range(start, end))
        } else {
            let index = s.parse::<u32>().map_err(|_| {
                format!("invalid entry index '{s}', expected a number or a range like '1..3'")
            })?;
            Ok(EntrySelector::One(index))
        }
    }
}

impl std::fmt::Display for EntrySelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bound = |b: &Option<u32>| b.map(|b| b.to_string()).unwrap_or_default();
        match self {
            EntrySelector::One(index) => write!(f, "{index}"),
            EntrySelector::Range(start, end) => write!(f, "{}..{}", bound(start), bound(end)),
        }
    }
}

fn parse_bound(s: &str) -> std::result::Result<Option<u32>, String> {
    let s = s.trim();
    if s.is_empty() {
        Ok(None)
    } else {
        s.parse::<u32>()
            .map(Some)
            .map_err(|_| format!("invalid range bound '{s}', expected a number"))
    }
}

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_journal")]
pub struct Journal {
    /// The ID of the invocation
    invocation_id: String,

    /// Entry index or inclusive range to show, e.g. `5`, `1..3`, `..10`, `90..`.
    /// Omit to show a head+tail preview of the journal.
    selector: Option<EntrySelector>,

    /// Include entry payloads (input/output/state values). Pipe with --json into jq.
    #[clap(long, short = 'p')]
    payload: bool,

    /// Show the whole journal instead of a head+tail preview
    #[clap(long)]
    all: bool,

    /// Number of leading entries shown in the preview
    #[clap(long, default_value_t = DEFAULT_HEAD)]
    head: u32,

    /// Number of trailing entries shown in the preview
    #[clap(long, default_value_t = DEFAULT_TAIL)]
    tail: u32,

    #[clap(flatten)]
    watch: Watch,
}

pub async fn run_journal(State(env): State<CliEnv>, opts: &Journal) -> Result<()> {
    opts.watch.run(|| journal(&env, opts)).await
}

async fn journal(env: &CliEnv, opts: &Journal) -> Result<()> {
    super::parse_invocation_id(&opts.invocation_id)?;
    let sql_client = crate::clients::DataFusionHttpClient::new(env).await?;

    let (fetch, scope) = match &opts.selector {
        Some(EntrySelector::One(index)) => (JournalFetch::One(*index), JournalScope::Full),
        Some(EntrySelector::Range(start, end)) => {
            (JournalFetch::Range(*start, *end), JournalScope::Full)
        }
        None if opts.all => (JournalFetch::All, JournalScope::Full),
        None => (
            JournalFetch::Preview {
                head: opts.head,
                tail: opts.tail,
            },
            JournalScope::Preview,
        ),
    };

    let entries = get_journal(&sql_client, &opts.invocation_id, fetch, opts.payload).await?;
    if entries.is_empty()
        && let Some(selector) = &opts.selector
    {
        let length = get_journal_length(&sql_client, &opts.invocation_id).await?;
        return Err(exit::NotFound(format!(
            "Journal entries {selector} not found: the journal of {} has {length} entries{}",
            opts.invocation_id,
            if length > 0 {
                format!(" (0..{})", length - 1)
            } else {
                String::new()
            }
        ))
        .into());
    }

    // The header/footer are human-only; skip the extra lookup for JSON.
    let json_output = CliContext::get().json_output();
    let invocation = if json_output {
        None
    } else {
        get_invocation(&sql_client, &opts.invocation_id).await?
    };
    if !json_output {
        print_journal_header();
    }

    let rows = journal_rows(&entries, opts.payload);

    let mut f = Formatter::new();
    f.journal("journal", &rows, scope);
    print_journal_footer(invocation.as_ref());
    f.finish()
}

/// Human-only closing line of a journal view: the invocation's current status.
pub(super) fn print_journal_footer(invocation: Option<&Invocation>) {
    if let Some(inv) = invocation {
        c_println!(
            " >> {}",
            rich_invocation_status(inv.status, inv.completion.as_ref())
        );
    }
}

/// Human-only heading of a journal view.
pub(super) fn print_journal_header() {
    c_title!("🚂", "Journal");
}

/// Build the rows for `entries`. Notifications are linked back to the command that owns
/// their completion id, when that command is among `entries`.
pub(super) fn journal_rows(entries: &[JournalEntryRow], include_payload: bool) -> Vec<JournalRow> {
    let owners = completion_owners(entries);
    // Index references are zero-padded like the rows' own indices (`[07]`).
    let index_digits = entries
        .iter()
        .map(|entry| entry.index.to_string().len())
        .max()
        .unwrap_or_default();
    entries
        .iter()
        .map(|entry| journal_row(entry, include_payload, &owners, index_digits))
        .collect()
}

fn journal_row(
    entry: &JournalEntryRow,
    include_payload: bool,
    owners: &HashMap<u64, (u32, Option<String>)>,
    index_digits: usize,
) -> JournalRow {
    // Byte-array payloads (e.g. `SetState.value`, `Input.payload`) are serialized as
    // `[123, 34, …]`; decode them to JSON/UTF-8 for readability.
    let full_decoded = entry.full.as_ref().map(|value| {
        let mut value = value.clone();
        decode_payloads(&mut value);
        value
    });

    let entry_value = if include_payload {
        full_decoded.clone().or_else(|| entry.lite.clone())
    } else {
        entry.lite.clone()
    };
    let record = json!({
        "index": entry.index,
        "type": entry.entry_type,
        "name": entry.name,
        "appended_at": entry.appended_at.map(|t| t.to_rfc3339()),
        "entry": entry_value,
    });

    let payload = if include_payload {
        match &full_decoded {
            Some(value) => {
                Some(serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()))
            }
            None => Some("(no payload)".to_owned()),
        }
    } else {
        None
    };

    let (label, name, details) = entry_cells(entry, owners, index_digits);
    JournalRow {
        index: u64::from(entry.index),
        appended_at: entry.appended_at,
        entry: label,
        name,
        details,
        record,
        payload,
    }
}

/// The human cells of an entry: its label (`Run command`), its name, and detail lines
/// derived from the lite entry. Notifications take the name of the command they
/// complete, and link back to it.
fn entry_cells(
    entry: &JournalEntryRow,
    owners: &HashMap<u64, (u32, Option<String>)>,
    index_digits: usize,
) -> (String, Option<String>, Vec<String>) {
    let label = match entry.entry_type.split_once(": ") {
        Some((kind, ty)) => format!("{ty} {}", kind.to_lowercase()),
        None => entry.entry_type.clone(),
    };
    let lite = entry.lite.as_ref();
    let (name, details) = if let Some((command, body)) =
        lite.and_then(|l| l.get("Command")).and_then(single_variant)
    {
        let details = command_details(command, body, entry.appended_at);
        (entry_name(entry), details.into_iter().collect())
    } else if let Some(body) = lite.and_then(|l| l.get("Notification")) {
        notification_cells(body, owners, index_digits)
    } else {
        (entry_name(entry), Vec::new())
    };
    (label, name, details)
}

/// The entry name: the `name` column, falling back to the lite entry's `name` (set on
/// named runs, sleeps and calls).
fn entry_name(entry: &JournalEntryRow) -> Option<String> {
    entry.name.clone().filter(|n| !n.is_empty()).or_else(|| {
        let (_, body) = entry
            .lite
            .as_ref()
            .and_then(|l| l.get("Command"))
            .and_then(single_variant)?;
        body.get("name")
            .and_then(Value::as_str)
            .filter(|n| !n.is_empty())
            .map(str::to_owned)
    })
}

/// The detail line of a command, if it has something to add. Plain text: detail lines
/// are rendered dimmed.
fn command_details(
    kind: &str,
    body: &Value,
    appended_at: Option<DateTime<Local>>,
) -> Option<String> {
    let str_field = |key: &str| body.get(key).and_then(Value::as_str).unwrap_or_default();
    let failed = |key: &str| {
        if str_field(key) == "Failure" {
            "failed"
        } else {
            ""
        }
    };
    let details = match kind {
        "Sleep" => body
            .get("wake_up_time")
            .and_then(Value::as_i64)
            .and_then(DateTime::from_timestamp_millis)
            .map(|t| {
                let wake_up = t.with_timezone(&Local);
                let at = journal_time(wake_up);
                let left = wake_up.signed_duration_since(Local::now());
                if left.num_milliseconds() >= 0 {
                    format!("until {at} (in {})", compact_duration(left))
                } else {
                    format!("until {at}")
                }
            })
            .unwrap_or_default(),
        "Call" | "OneWayCall" => {
            let mut details = format!(
                "→ {} {}",
                body.get("invocation_target")
                    .map(format_target)
                    .unwrap_or_default(),
                str_field("invocation_id")
            );
            // Delayed sends carry a future invoke time (0 means "immediately").
            if let Some(invoke_at) = body
                .get("invoke_time")
                .and_then(Value::as_i64)
                .and_then(DateTime::from_timestamp_millis)
                .map(|t| t.with_timezone(&Local))
                && appended_at.is_some_and(|appended| invoke_at > appended)
            {
                details.push_str(&format!(" at {}", journal_time(invoke_at)));
            }
            details
        }
        "SendSignal" => format!(
            "{} → {} {}",
            body.get("signal_id").map(format_signal).unwrap_or_default(),
            str_field("target_invocation_id"),
            failed("result")
        )
        .trim_end()
        .to_owned(),
        "CompleteAwakeable" => format!("{} {}", str_field("id"), failed("result"))
            .trim_end()
            .to_owned(),
        "GetEagerState" if str_field("result") == "Void" => {
            format!("key {} (empty)", str_field("key"))
        }
        "GetLazyState" | "GetEagerState" | "SetState" | "ClearState" | "GetPromise"
        | "PeekPromise" | "CompletePromise" => format!("key {}", str_field("key")),
        "AttachInvocation" | "GetInvocationOutput" => body
            .get("target")
            .map(format_attach_target)
            .unwrap_or_default(),
        "Output" => failed("result").to_owned(),
        _ => String::new(),
    };
    (!details.is_empty()).then_some(details)
}

/// A notification's name (the completed command's, or the signal's) and detail lines
/// (the command it completes, a failure).
fn notification_cells(
    body: &Value,
    owners: &HashMap<u64, (u32, Option<String>)>,
    index_digits: usize,
) -> (Option<String>, Vec<String>) {
    let mut name = None;
    let mut details = Vec::new();
    let id = body.get("id");
    if let Some(completion_id) = id
        .and_then(|id| id.get("CompletionId"))
        .and_then(Value::as_u64)
        && let Some((index, label)) = owners.get(&completion_id)
    {
        name.clone_from(label);
        details.push(format!("completion of [{index:0index_digits$}]"));
    } else if let Some((kind, value)) = id.and_then(single_variant) {
        let signal = format_signal_parts(kind, value);
        name = Some(signal.trim_matches(['[', ']']).to_owned()).filter(|s| !s.is_empty());
    }
    if body.get("result").and_then(Value::as_str) == Some("Failure") {
        details.push("failed".to_owned());
    }
    (name, details)
}

/// completion id → (index of the owning command, its name).
fn completion_owners(entries: &[JournalEntryRow]) -> HashMap<u64, (u32, Option<String>)> {
    let mut owners = HashMap::new();
    for entry in entries {
        let Some((_, body)) = entry
            .lite
            .as_ref()
            .and_then(|l| l.get("Command"))
            .and_then(single_variant)
        else {
            continue;
        };
        let label = entry_name(entry);
        for key in [
            "completion_id",
            "result_completion_id",
            "invocation_id_completion_id",
        ] {
            if let Some(id) = body.get(key).and_then(Value::as_u64) {
                owners.insert(id, (entry.index, label.clone()));
            }
        }
    }
    owners
}

/// Each completion id owned by a command in `entries`, described as that command, e.g.
/// `Sleep [nap] (entry #3)`.
pub(super) fn completion_commands(entries: &[JournalEntryRow]) -> HashMap<u32, String> {
    let types: HashMap<u32, &str> = entries
        .iter()
        .map(|e| {
            (
                e.index,
                e.entry_type.rsplit(": ").next().unwrap_or_default(),
            )
        })
        .collect();
    completion_owners(entries)
        .into_iter()
        .filter_map(|(id, (index, label))| {
            let ty = types.get(&index)?;
            let label = label.map(|l| format!(" [{l}]")).unwrap_or_default();
            Some((
                u32::try_from(id).ok()?,
                format!("{ty}{label} (entry #{index})"),
            ))
        })
        .collect()
}

/// An externally-tagged serde enum value `{"Variant": body}` as `("Variant", body)`.
fn single_variant(value: &Value) -> Option<(&str, &Value)> {
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    object.iter().next().map(|(k, v)| (k.as_str(), v))
}

/// `InvocationTarget` as `Service/handler` or `Object/key/handler`.
fn format_target(target: &Value) -> String {
    let Some((_, body)) = single_variant(target) else {
        return String::new();
    };
    let field = |key: &str| body.get(key).and_then(Value::as_str);
    match (field("name"), field("key"), field("handler")) {
        (Some(name), Some(key), Some(handler)) => format!("{name}/{key}/{handler}"),
        (Some(name), None, Some(handler)) => format!("{name}/{handler}"),
        _ => String::new(),
    }
}

/// `AttachInvocationTarget`: an invocation id, an idempotent request, or a workflow.
fn format_attach_target(target: &Value) -> String {
    let Some((kind, body)) = single_variant(target) else {
        return String::new();
    };
    let field = |key: &str| body.get(key).and_then(Value::as_str);
    match kind {
        "InvocationId" => body.as_str().unwrap_or_default().to_owned(),
        "IdempotentRequest" => {
            let service = match field("service_key") {
                Some(key) => format!("{}/{key}", field("service_name").unwrap_or_default()),
                None => field("service_name").unwrap_or_default().to_owned(),
            };
            format!(
                "{service}/{} idempotency key {}",
                field("service_handler").unwrap_or_default(),
                field("idempotency_key").unwrap_or_default()
            )
        }
        "Workflow" => format!(
            "{}/{}",
            field("service_name").unwrap_or_default(),
            field("key").unwrap_or_default()
        ),
        _ => String::new(),
    }
}

/// `SignalId` (`{"Index": n}` / `{"Name": "x"}`) as a readable label.
fn format_signal(signal: &Value) -> String {
    single_variant(signal)
        .map(|(kind, value)| format_signal_parts(kind, value))
        .unwrap_or_default()
}

fn format_signal_parts(kind: &str, value: &Value) -> String {
    match (kind, value) {
        // Index 1 is the built-in cancel signal.
        ("Index" | "SignalIndex", Value::Number(n)) if n.as_u64() == Some(1) => {
            "[cancel]".to_owned()
        }
        ("Index" | "SignalIndex", Value::Number(n)) => format!("[signal {n}]"),
        ("Name" | "SignalName", Value::String(name)) => format!("[{name}]"),
        _ => String::new(),
    }
}

/// Recursively replace byte-array fields (`[123, 34, …]`) with their decoded value —
/// parsed JSON if the bytes are JSON, otherwise a UTF-8 string — leaving anything that
/// isn't a printable byte buffer untouched.
fn decode_payloads(value: &mut Value) {
    match value {
        Value::Array(items) => {
            if let Some(bytes) = as_byte_buffer(items)
                && let Some(decoded) = decode_bytes(&bytes)
            {
                *value = decoded;
                return;
            }
            for item in items {
                decode_payloads(item);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                decode_payloads(item);
            }
        }
        _ => {}
    }
}

/// A non-empty array whose every element is an integer in `0..=255`, as raw bytes.
fn as_byte_buffer(items: &[Value]) -> Option<Vec<u8>> {
    if items.is_empty() {
        return None;
    }
    items
        .iter()
        .map(|item| u8::try_from(item.as_u64()?).ok())
        .collect()
}

fn decode_bytes(bytes: &[u8]) -> Option<Value> {
    // Prefer structured JSON.
    if let Ok(json) = serde_json::from_slice::<Value>(bytes) {
        return Some(json);
    }
    // Otherwise a printable UTF-8 string (reject binary/control-heavy blobs).
    let text = std::str::from_utf8(bytes).ok()?;
    if text.chars().all(|c| !c.is_control() || c.is_whitespace()) {
        Some(Value::String(text.to_owned()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes_value(bytes: &[u8]) -> Value {
        Value::Array(bytes.iter().map(|b| json!(b)).collect())
    }

    #[test]
    fn decode_payloads_decodes_byte_buffers_and_leaves_the_rest() {
        let mut value = json!({
            "json_payload": bytes_value(br#"{"a":1}"#),
            "text_payload": bytes_value(b"hello"),
            "binary": bytes_value(&[0, 1, 2, 3]),
            "small_numbers": [1, 2, 3],
            "count": 5,
            "empty": [],
        });

        decode_payloads(&mut value);

        // JSON bytes become structured JSON; text bytes become a string.
        assert_eq!(value["json_payload"], json!({"a": 1}));
        assert_eq!(value["text_payload"], json!("hello"));
        // Non-printable bytes, ordinary numeric arrays and scalars are untouched.
        assert_eq!(value["binary"], json!([0, 1, 2, 3]));
        assert_eq!(value["small_numbers"], json!([1, 2, 3]));
        assert_eq!(value["count"], json!(5));
        assert_eq!(value["empty"], json!([]));
    }

    #[test]
    fn entry_selector_parses_indices_and_ranges() {
        assert_eq!("5".parse(), Ok(EntrySelector::One(5)));
        assert_eq!("1..3".parse(), Ok(EntrySelector::Range(Some(1), Some(3))));
        assert_eq!("..10".parse(), Ok(EntrySelector::Range(None, Some(10))));
        assert_eq!("90..".parse(), Ok(EntrySelector::Range(Some(90), None)));
        assert!("9..3".parse::<EntrySelector>().is_err());
        assert!("x".parse::<EntrySelector>().is_err());
    }

    fn lite_entry(
        index: u32,
        entry_type: &str,
        name: Option<&str>,
        lite: Value,
    ) -> JournalEntryRow {
        JournalEntryRow {
            index,
            entry_type: entry_type.to_owned(),
            name: name.map(str::to_owned),
            appended_at: None,
            lite: Some(lite),
            full: None,
        }
    }

    #[test]
    fn journal_rows_summarize_lite_entries() {
        let entries = vec![
            lite_entry(
                0,
                "Command: Call",
                None,
                json!({"Command": {"Call": {
                    "invocation_id": "inv_1",
                    "invocation_target": {"VirtualObject": {"name": "Counter", "key": "k", "handler": "add"}},
                    "invocation_id_completion_id": 1,
                    "result_completion_id": 2,
                }}}),
            ),
            lite_entry(
                1,
                "Notification: Call",
                None,
                json!({"Notification": {"ty": {"Completion": "Call"}, "id": {"CompletionId": 2}, "result": "Failure"}}),
            ),
            lite_entry(
                2,
                "Command: SendSignal",
                None,
                json!({"Command": {"SendSignal": {"target_invocation_id": "inv_2", "signal_id": {"Name": "approved"}, "result": "Void"}}}),
            ),
            lite_entry(
                3,
                "Notification: Signal",
                None,
                json!({"Notification": {"ty": "Signal", "id": {"SignalIndex": 1}, "result": "Void"}}),
            ),
            lite_entry(
                5,
                "Command: Sleep",
                None,
                json!({"Command": {"Sleep": {"completion_id": 3, "name": "nap"}}}),
            ),
            lite_entry(
                4,
                "Command: GetEagerState",
                None,
                json!({"Command": {"GetEagerState": {"key": "count", "result": "Void"}}}),
            ),
        ];

        let cells: Vec<(String, Option<String>, Vec<String>)> = journal_rows(&entries, false)
            .into_iter()
            .map(|row| (row.entry, row.name, row.details))
            .collect();
        let row = |entry: &str, name: Option<&str>, details: &[&str]| {
            (
                entry.to_owned(),
                name.map(str::to_owned),
                details.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
            )
        };

        assert_eq!(
            cells,
            vec![
                row("Call command", None, &["→ Counter/k/add inv_1"]),
                row("Call notification", None, &["completion of [0]", "failed"]),
                row("SendSignal command", None, &["[approved] → inv_2"]),
                row("Signal notification", Some("cancel"), &[]),
                row("Sleep command", Some("nap"), &[]),
                row("GetEagerState command", None, &["key count (empty)"]),
            ]
        );
    }
}
