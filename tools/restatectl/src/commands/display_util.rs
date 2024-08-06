use chrono::{DateTime, Local};
use restate_cli_util::_comfy_table::{Cell, Color};
use restate_cli_util::ui::{timestamp_as_human_duration, Tense};
use restate_types::logs::metadata::Segment;
use restate_types::logs::Lsn;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::time::SystemTime;

pub struct LsnRange {
    start: Lsn,
    end: Option<Lsn>,
}

impl LsnRange {
    pub fn from(segment: &Segment) -> Self {
        Self {
            start: segment.base_lsn,
            end: segment.tail_lsn,
        }
    }
}

impl Display for LsnRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}..{}",
            self.start,
            self.end
                .map(|lsn| format!("{}]", lsn))
                .unwrap_or(String::from("∞)"))
        )
    }
}

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
