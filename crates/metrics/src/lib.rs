// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod macros;

use std::fmt::Display;
use std::str::FromStr;
use std::sync::atomic::{AtomicU8, Ordering};

pub use metrics;
pub use metrics::{describe_counter, describe_gauge, describe_histogram};

// Use a simple enum to represent the level as a u8
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Level {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl FromStr for Level {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "trace" => Ok(Level::Trace),
            "debug" => Ok(Level::Debug),
            "info" => Ok(Level::Info),
            "warn" | "warning" => Ok(Level::Warn),
            "error" => Ok(Level::Error),
            _ => Err(format!(
                "Unknown level: '{}'. Expected one of: trace, debug, info, warn, error",
                s
            )),
        }
    }
}

impl Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Level::Trace => write!(f, "trace"),
            Level::Debug => write!(f, "debug"),
            Level::Info => write!(f, "info"),
            Level::Warn => write!(f, "warn"),
            Level::Error => write!(f, "error"),
        }
    }
}

impl From<metrics::Level> for Level {
    fn from(level: metrics::Level) -> Self {
        match level {
            metrics::Level::TRACE => Level::Trace,
            metrics::Level::DEBUG => Level::Debug,
            metrics::Level::INFO => Level::Info,
            metrics::Level::WARN => Level::Warn,
            metrics::Level::ERROR => Level::Error,
        }
    }
}

impl From<Level> for metrics::Level {
    fn from(level_value: Level) -> Self {
        match level_value {
            Level::Trace => metrics::Level::TRACE,
            Level::Debug => metrics::Level::DEBUG,
            Level::Info => metrics::Level::INFO,
            Level::Warn => metrics::Level::WARN,
            Level::Error => metrics::Level::ERROR,
        }
    }
}

static METRICS_CARDINALITY_LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);

/// Sets the global metrics cardinality level.
///
/// This function allows you to dynamically change the metrics cardinality level globally.
/// The level affects how metrics are filtered and processed.
///
/// # Examples
///
/// ```ignore
/// use restate_metrics::{set_metrics_cardinality_level, get_metrics_cardinality_level, Level};
///
/// // Set the cardinality level to DEBUG
/// set_metrics_cardinality_level(Level::Debug);
/// assert_eq!(get_metrics_cardinality_level(), Level::Debug as u8);
///
/// // Set the cardinality level to ERROR
/// set_metrics_cardinality_level(Level::Error);
/// assert_eq!(get_metrics_cardinality_level(), Level::Debug as u8);
/// ```
pub fn set_metrics_cardinality_level(level: impl Into<Level>) {
    let level: Level = level.into();
    METRICS_CARDINALITY_LEVEL.store(level as u8, Ordering::Relaxed);
}

#[doc(hidden)]
#[inline]
pub fn get_metrics_cardinality_level() -> u8 {
    METRICS_CARDINALITY_LEVEL.load(Ordering::Relaxed)
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use metrics::{
        Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };

    use crate::{Level, counter, set_metrics_cardinality_level};

    #[derive(Default, Clone)]
    struct TestRecorder {
        registry: Arc<Mutex<HashMap<String, Key>>>,
    }

    impl TestRecorder {
        fn get_key(&self, key: &str) -> Option<Key> {
            self.registry.lock().unwrap().get(key).cloned()
        }

        fn reset(&self) {
            self.registry.lock().unwrap().clear();
        }
    }

    impl Recorder for TestRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        /// Registers a counter.
        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            self.registry
                .lock()
                .unwrap()
                .insert(key.name().to_string(), key.clone());
            Counter::noop()
        }

        /// Registers a gauge.
        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            self.registry
                .lock()
                .unwrap()
                .insert(key.name().to_string(), key.clone());
            Gauge::noop()
        }

        /// Registers a histogram.
        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            self.registry
                .lock()
                .unwrap()
                .insert(key.name().to_string(), key.clone());
            Histogram::noop()
        }
    }

    #[test]
    fn test_metrics() {
        let recorder = TestRecorder::default();
        metrics::set_global_recorder(recorder.clone()).unwrap();

        let _counter = counter!("my_counter", "label" => "value");
        let key = recorder.get_key("my_counter");
        assert!(key.is_some());
        let key = key.unwrap();
        let labels = key.labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].key(), "label");
        assert_eq!(labels[0].value(), "value");

        recorder.reset();
        // Set the cardinality level to info, so that the "info" label is included
        set_metrics_cardinality_level(Level::Info);

        let _counter = counter!("my_counter", "label" => "value", "info"; info => "value2", "debug"; debug => "value3");
        let key = recorder.get_key("my_counter");
        assert!(key.is_some());
        let key = key.unwrap();
        let labels = key.labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].key(), "label");
        assert_eq!(labels[0].value(), "value");
        assert_eq!(labels[1].key(), "info");
        assert_eq!(labels[1].value(), "value2");

        recorder.reset();
        set_metrics_cardinality_level(Level::Warn);

        let _counter = counter!(
            "my_counter",
            "label" => "value",
            "info"; info => "value2",
            "debug"; debug => "value3",
            "warn"; warn => "value4",
            "error"; error => "value5"
        );

        let key = recorder.get_key("my_counter");
        assert!(key.is_some());
        let key = key.unwrap();
        let labels = key.labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].key(), "warn");
        assert_eq!(labels[0].value(), "value4");
        assert_eq!(labels[1].key(), "error");
        assert_eq!(labels[1].value(), "value5");

        recorder.reset();
        set_metrics_cardinality_level(Level::Info);
        let mut side_effect = 0;
        let _counter = counter!("my_counter", "info" => "value", "debug"; debug => {
            side_effect += 1;
            "value3"
        });

        assert_eq!(side_effect, 0);
        let key = recorder.get_key("my_counter");
        assert!(key.is_some());
        let key = key.unwrap();
        let labels = key.labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].key(), "info");
        assert_eq!(labels[0].value(), "value");

        recorder.reset();
        set_metrics_cardinality_level(Level::Debug);
        let mut side_effect = 0;
        let _counter = counter!("my_counter", "info" => "value", "debug"; debug => {
            side_effect += 1;
            "value3"
        });

        assert_eq!(side_effect, 1);
        let key = recorder.get_key("my_counter");
        assert!(key.is_some());
        let key = key.unwrap();
        let labels = key.labels().collect::<Vec<_>>();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].key(), "info");
        assert_eq!(labels[0].value(), "value");
        assert_eq!(labels[1].key(), "debug");
        assert_eq!(labels[1].value(), "value3");
    }

    #[test]
    fn test_global_metrics_level() {
        use super::{get_metrics_cardinality_level, set_metrics_cardinality_level};

        // Test initial state
        assert_eq!(get_metrics_cardinality_level(), Level::Info as u8);

        // Test setting different levels
        set_metrics_cardinality_level(metrics::Level::DEBUG);
        assert_eq!(get_metrics_cardinality_level(), Level::Debug as u8);

        set_metrics_cardinality_level(metrics::Level::ERROR);
        assert_eq!(get_metrics_cardinality_level(), Level::Error as u8);

        set_metrics_cardinality_level(metrics::Level::TRACE);
        assert_eq!(get_metrics_cardinality_level(), Level::Trace as u8);

        set_metrics_cardinality_level(metrics::Level::WARN);
        assert_eq!(get_metrics_cardinality_level(), Level::Warn as u8);
    }

    #[test]
    fn test_level_from_str() {
        use super::Level;
        use std::str::FromStr;

        // Test valid lowercase strings
        assert_eq!(Level::from_str("trace").unwrap(), Level::Trace);
        assert_eq!(Level::from_str("debug").unwrap(), Level::Debug);
        assert_eq!(Level::from_str("info").unwrap(), Level::Info);
        assert_eq!(Level::from_str("warn").unwrap(), Level::Warn);
        assert_eq!(Level::from_str("error").unwrap(), Level::Error);

        // Test valid uppercase strings (should be case-insensitive)
        assert_eq!(Level::from_str("TRACE").unwrap(), Level::Trace);
        assert_eq!(Level::from_str("DEBUG").unwrap(), Level::Debug);
        assert_eq!(Level::from_str("INFO").unwrap(), Level::Info);
        assert_eq!(Level::from_str("WARN").unwrap(), Level::Warn);
        assert_eq!(Level::from_str("ERROR").unwrap(), Level::Error);

        // Test valid mixed case strings
        assert_eq!(Level::from_str("Trace").unwrap(), Level::Trace);
        assert_eq!(Level::from_str("Debug").unwrap(), Level::Debug);
        assert_eq!(Level::from_str("Info").unwrap(), Level::Info);
        assert_eq!(Level::from_str("Warn").unwrap(), Level::Warn);
        assert_eq!(Level::from_str("Error").unwrap(), Level::Error);

        // Test "warning" as an alias for "warn"
        assert_eq!(Level::from_str("warning").unwrap(), Level::Warn);
        assert_eq!(Level::from_str("WARNING").unwrap(), Level::Warn);
        assert_eq!(Level::from_str("Warning").unwrap(), Level::Warn);

        // Test invalid strings
        assert!(Level::from_str("invalid").is_err());
        assert!(Level::from_str("").is_err());
        assert!(Level::from_str("TRAC").is_err());
        assert!(Level::from_str("debugg").is_err());
        assert!(Level::from_str("inf").is_err());
        assert!(Level::from_str("warnings").is_err());
        assert!(Level::from_str("errors").is_err());

        // Test error message format
        let err = Level::from_str("invalid").unwrap_err();
        assert!(err.contains("Unknown level: 'invalid'"));
        assert!(err.contains("Expected one of: trace, debug, info, warn, error"));
    }

    #[test]
    fn test_level_display() {
        use super::Level;
        use std::fmt::Write;
        use std::str::FromStr;

        // Test each level displays correctly
        assert_eq!(Level::Trace.to_string(), "trace");
        assert_eq!(Level::Debug.to_string(), "debug");
        assert_eq!(Level::Info.to_string(), "info");
        assert_eq!(Level::Warn.to_string(), "warn");
        assert_eq!(Level::Error.to_string(), "error");

        // Test using format! macro
        assert_eq!(format!("{}", Level::Trace), "trace");
        assert_eq!(format!("{}", Level::Debug), "debug");
        assert_eq!(format!("{}", Level::Info), "info");
        assert_eq!(format!("{}", Level::Warn), "warn");
        assert_eq!(format!("{}", Level::Error), "error");

        // Test round-trip: Display -> FromStr
        assert_eq!(
            Level::from_str(&Level::Trace.to_string()).unwrap(),
            Level::Trace
        );
        assert_eq!(
            Level::from_str(&Level::Debug.to_string()).unwrap(),
            Level::Debug
        );
        assert_eq!(
            Level::from_str(&Level::Info.to_string()).unwrap(),
            Level::Info
        );
        assert_eq!(
            Level::from_str(&Level::Warn.to_string()).unwrap(),
            Level::Warn
        );
        assert_eq!(
            Level::from_str(&Level::Error.to_string()).unwrap(),
            Level::Error
        );

        // Test writing to a string buffer
        let mut buffer = String::new();
        write!(&mut buffer, "{}", Level::Info).unwrap();
        assert_eq!(buffer, "info");
    }
}
