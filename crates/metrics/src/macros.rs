// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// **NOTE** This macro is internal and should not be used directly.
///
/// Creates a metric label with optional level-based filtering.
///
/// This macro creates metric labels that can be conditionally included based on the current
/// metrics label level configuration. Labels without a level are treated as `info` level by default.
///
/// # Syntax
///
/// ## Basic label (info level by default)
/// ```ignore
/// label!("key" => "value")
/// ```
///
/// ## Label with explicit level
/// ```ignore
/// label!("key"; trace => "value");    // Only included if level >= trace
/// label!("key"; debug => "value");    // Only included if level >= debug
/// label!("key"; info => "value");     // Only included if level >= info
/// label!("key"; warn => "value");     // Only included if level >= warn
/// label!("key"; error => "value");    // Only included if level >= error
/// ```
///
/// # Examples
///
/// ```ignore
/// use restate_metrics::{label, set_metrics_cardinality_level, Level};
///
/// // Set the metrics label level to info
/// set_metrics_cardinality_level(Level::Info);
///
/// // These labels will be included (info level >= current level)
/// let label1 = label!("service" => "user-service");
/// let label2 = label!("version"; info => "v1.0.0");
///
/// // This label will be filtered out (debug level < current level)
/// let label3 = label!("debug_info"; debug => "detailed_debug_data");
///
/// // This label will be included (warn level >= current level)
/// let label4 = label!("error_code"; warn => "connection_timeout");
/// ```
///
/// # Level Hierarchy
///
/// The levels follow this hierarchy (from most to least verbose):
/// - `trace` (most verbose)
/// - `debug`
/// - `info` (default for labels without level)
/// - `warn`
/// - `error` (least verbose)
///
/// Only labels with levels greater than or equal to the configured level are included.
#[doc(hidden)]
#[macro_export]
macro_rules! label {
    (@filter $key:expr => $value:tt) => {{
        let level = $crate::get_metrics_cardinality_level();
        if $crate::Level::Info as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; trace => $value:tt) => {{
        let level = $crate::get_metrics_cardinality_level();
        if $crate::Level::Trace as u8 >= level  {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; debug => $value:tt) => {{
        let level = $crate::get_metrics_cardinality_level();
        if $crate::Level::Debug as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; info => $value:tt) => {{
        let level = $crate::get_metrics_cardinality_level();
        if $crate::Level::Info as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; warn => $value:tt) => {{
        let level = $crate::get_metrics_cardinality_level();
        if $crate::Level::Warn as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};
    (@filter $key:expr; error => $value:tt) => {{
        let level = $crate::get_metrics_cardinality_level();
        if $crate::Level::Error as u8 >= level {
            Some($crate::label!(@label $key => $value))
        } else {
            None
        }
    }};

    (@label $key:literal => $value:literal) => {{
        $crate::metrics::Label::from_static_parts($key, $value)
    }};

    // Support for lazy evaluation of the value
    // This is useful for cases where the value is expensive to compute
    // and we want to avoid computing it if the label is not included
    // in the metrics cardinality level.
    (@label $key:expr => $value:block) => {{
        $crate::metrics::Label::new($key, $value)
    }};

    (@label $key:expr => $value:expr) => {{
        $crate::metrics::Label::new($key, $value)
    }};
}

/// Records a histogram metric with level-based label filtering.
///
/// This macro creates histogram metrics that can include labels filtered by the current
/// metrics label level configuration. It provides multiple syntax variants for different use cases.
///
/// # Syntax Variants
///
/// ## Standard metrics crate syntax (passes through to underlying metrics crate)
/// ```ignore
/// histogram!(target: "my_target", level: Level::INFO, "metric_name", "label" => "value");
/// histogram!(target: "my_target", "metric_name", "label" => "value");
/// histogram!(level: Level::INFO, "metric_name", "label" => "value");
/// histogram!("metric_name", "label" => "value");
/// ```
///
/// ## Custom syntax with level-based label filtering
/// ```ignore
/// histogram!("my_target",
///     "service" => "user-service",
///     "version"; info => "v1.0.0",
///     "debug_info"; debug => "detailed_data",
///     "error_code"; warn => "timeout"
/// );
/// ```
///
/// # Examples
///
/// ```ignore
/// use restate_metrics::{histogram, set_metrics_cardinality_level, Level};
/// use std::time::Instant;
///
/// // Set metrics label level to info
/// set_metrics_cardinality_level(Level::Info);
///
/// // Record a histogram with basic labels
/// histogram!("http_request_duration",
///     "method" => "GET",
///     "endpoint" => "/api/users"
/// );
///
/// // Record a histogram with level-filtered labels
/// histogram!("database_query_duration",
///     "table" => "users",
///     "query_type"; info => "select",
///     "debug_sql"; debug => "SELECT * FROM users WHERE id = ?",
///     "error_details"; warn => "connection_timeout"
/// );
///
/// // Record with explicit target and level
/// histogram!(target: "my_module", level: Level::DEBUG, "custom_metric", "label" => "value");
/// ```
///
/// # Label Filtering
///
/// Labels are filtered based on the current metrics label level:
/// - Labels without a level are treated as `info` level
/// - Only labels with levels >= the configured level are included
/// - This helps control metric cardinality in production environments
#[macro_export]
macro_rules! histogram {
    (target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        $crate::metrics::histogram!(target: $target, level: $level, $name $(, $label_key $(=> $label_value)?)*)
    }};
    (target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::histogram!(target: $target, level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
    (level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::histogram!(target: ::std::module_path!(), level: $level, $name $(, $label_key $(=> $label_value)?)*)
    };
    ($name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::histogram!(target: ::std::module_path!(), level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };

    ($target:expr $(, $label_key:expr $(; $label_level:ident)? $(=> $label_value:tt)?)* $(,)?) => {{
        let labels = vec![$($crate::label!(@filter $label_key $(; $label_level)? $(=> $label_value)?)),*]
                .into_iter().filter_map(|x| x).collect::<Vec<_>>();
        let metric_key = Key::from_parts($target, labels);
        let metadata = $crate::metrics::metadata_var!($target, $crate::metrics::Level::INFO);
        $crate::metrics::with_recorder(|recorder| recorder.register_histogram(&metric_key, metadata))
    }};
}

/// Records a counter metric with level-based label filtering.
///
/// This macro creates counter metrics that can include labels filtered by the current
/// metrics label level configuration. Counters are used to track cumulative values that
/// only increase over time.
///
/// # Syntax Variants
///
/// ## Standard metrics crate syntax (passes through to underlying metrics crate)
/// ```ignore
/// counter!(target: "my_target", level: Level::INFO, "metric_name", "label" => "value");
/// counter!(target: "my_target", "metric_name", "label" => "value");
/// counter!(level: Level::INFO, "metric_name", "label" => "value");
/// counter!("metric_name", "label" => "value");
/// ```
///
/// ## Custom syntax with level-based label filtering
/// ```ignore
/// counter!("my_target",
///     "operation" => "create_user",
///     "user_type"; info => "premium",
///     "debug_context"; debug => "detailed_context",
///     "error_type"; warn => "validation_failed"
/// );
/// ```
///
/// # Examples
///
/// ```ignore
/// use restate_metrics::{counter, set_metrics_cardinality_level, Level};
///
/// // Set metrics label level to info
/// set_metrics_cardinality_level(Level::Info);
///
/// // Record a simple counter
/// counter!("http_requests_total",
///     "method" => "POST",
///     "status" => "200"
/// );
///
/// // Record a counter with level-filtered labels
/// counter!("database_operations",
///     "operation" => "insert",
///     "table_name"; info => "users",
///     "query_hash"; debug => "abc123def456",
///     "error_code"; warn => "duplicate_key"
/// );
#[macro_export]
macro_rules! counter {
    (target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        $crate::metrics::counter!(target: $target, level: $level, $name $(, $label_key $(=> $label_value)?)*)
    }};
    (target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::counter!(target: $target, level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
    (level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::counter!(target: ::std::module_path!(), level: $level, $name $(, $label_key $(=> $label_value)?)*)
    };
    ($name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::counter!(target: ::std::module_path!(), level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };

    ($target:expr $(, $label_key:expr $(; $label_level:ident)? $(=> $label_value:tt)?)* $(,)?) => {{
        let labels = vec![$($crate::label!(@filter $label_key $(; $label_level)? $(=> $label_value)?)),*]
                .into_iter().filter_map(|x| x).collect::<Vec<_>>();
        let metric_key = Key::from_parts($target, labels);
        let metadata = $crate::metrics::metadata_var!($target, $crate::metrics::Level::INFO);
        $crate::metrics::with_recorder(|recorder| recorder.register_counter(&metric_key, metadata))
    }};
}

/// Records a gauge metric with level-based label filtering.
///
/// This macro creates gauge metrics that can include labels filtered by the current
/// metrics label level configuration. Gauges are used to track values that can go up and down.
///
/// # Syntax Variants
///
/// ## Standard metrics crate syntax (passes through to underlying metrics crate)
/// ```ignore
/// gauge!(target: "my_target", level: Level::INFO, "metric_name", "label" => "value");
/// gauge!(target: "my_target", "metric_name", "label" => "value");
/// gauge!(level: Level::INFO, "metric_name", "label" => "value");
/// gauge!("metric_name", "label" => "value");
/// ```
///
/// ## Custom syntax with level-based label filtering
/// ```ignore
/// gauge!("my_target",
///     "resource" => "memory",
///     "component"; info => "cache",
///     "debug_details"; debug => "detailed_memory_info",
///     "warning_threshold"; warn => "80_percent"
/// );
/// ```
///
/// # Examples
///
/// ```ignore
/// use restate_metrics::{gauge, set_metrics_cardinality_level, Level};
///
/// // Set metrics label level to info
/// set_metrics_cardinality_level(Level::Info);
///
/// // Record a simple gauge
/// gauge!("memory_usage_bytes",
///     "component" => "database"
/// ).set(1024 * 1024 * 100); // 100MB
///
/// // Record a gauge with level-filtered labels
/// gauge!("connection_pool_size",
///     "pool_name" => "main_db",
///     "pool_type"; info => "read_write",
///     "debug_config"; debug => "max_connections=50",
///     "warning_state"; warn => "near_capacity"
/// ).set(45);
/// ```
#[macro_export]
macro_rules! gauge {
    (target: $target:expr, level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {{
        $crate::metrics::gauge!(target: $target, level: $level, $name $(, $label_key $(=> $label_value)?)*)
    }};
    (target: $target:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::gauge!(target: $target, level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };
    (level: $level:expr, $name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::gauge!(target: ::std::module_path!(), level: $level, $name $(, $label_key $(=> $label_value)?)*)
    };
    ($name:expr $(, $label_key:expr $(=> $label_value:expr)?)* $(,)?) => {
        $crate::metrics::gauge!(target: ::std::module_path!(), level: $crate::metrics::Level::INFO, $name $(, $label_key $(=> $label_value)?)*)
    };

    ($target:expr $(, $label_key:expr $(; $label_level:ident)? $(=> $label_value:tt)?)* $(,)?) => {{
        let labels = vec![$($crate::label!(@filter $label_key $(; $label_level)? $(=> $label_value)?)),*]
                .into_iter().filter_map(|x| x).collect::<Vec<_>>();
        let metric_key = Key::from_parts($target, labels);
        let metadata = $crate::metrics::metadata_var!($target, $crate::metrics::Level::INFO);
        $crate::metrics::with_recorder(|recorder| recorder.register_gauge(&metric_key, metadata))
    }};
}
