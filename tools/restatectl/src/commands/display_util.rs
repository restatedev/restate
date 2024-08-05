use chrono::{DateTime, Local};
use restate_cli_util::_comfy_table::{Cell, Color};
use restate_cli_util::ui::{timestamp_as_human_duration, Tense};
use std::time::SystemTime;

pub fn render_as_duration(ts: Option<prost_types::Timestamp>, tense: Tense) -> Cell {
    let ts: Option<SystemTime> = ts
        .map(TryInto::try_into)
        .transpose()
        .expect("valid timestamp");
    if let Some(ts) = ts {
        let ts: DateTime<Local> = ts.into();
        Cell::new(timestamp_as_human_duration(ts, tense))
    } else {
        Cell::new("??").fg(Color::Red)
    }
}
