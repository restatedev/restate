// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{ColumnStatistics, Statistics, stats::Precision},
    scalar::ScalarValue,
};

pub(super) const DEPLOYMENT_ROW_ESTIMATE: RowEstimate = RowEstimate::Tiny;
pub(super) const SERVICE_ROW_ESTIMATE: RowEstimate = RowEstimate::Tiny;

pub(super) enum RowEstimate {
    Tiny,
    Small,
    Large,
}

impl From<RowEstimate> for Precision<usize> {
    fn from(value: RowEstimate) -> Self {
        match value {
            RowEstimate::Tiny => Precision::Inexact(1024),
            // this size is chosen to be well under the 128k limit for collecting a build-side into one partition
            RowEstimate::Small => Precision::Inexact(64 * 1024), // 64K
            RowEstimate::Large => Precision::Inexact(64 * 64 * 1024), // 4M
        }
    }
}

/// Allows for non-default table/column statistics to be defined.
/// Most tables do not need this! These statistics are used at plan time to decide on join order.
/// The main benefit of this is that tables that are much smaller will be put on the build side.
/// The propagation of table size information *through* joins is important when there are multiple
/// joins in a query.
///
/// You might choose to set statistics on your table if:
/// a) You want to join it with some much smaller table (eg, sys_journal and sys_service)
/// b) You want to join it with a table of potentially similar size, but that result needs to be joined
///    with a much smaller table (eg, sys_invocation itself is a join, with its results being used in other joins).
///
/// The statistics needed are:
/// - The estimated row count (we use buckets that span several orders of magnitude)
/// - The estimated distinct count for every field being joined on. This is likely either the row count of
///   the current table (primary key), or of the table you're joining into (foreign key).
/// - The max and min value must be *set* but can be set as unbounded.
pub(super) struct TableStatisticsBuilder {
    schema: SchemaRef,
    statistics: Statistics,
}

impl TableStatisticsBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        let statistics = Statistics::new_unknown(&schema);
        Self { schema, statistics }
    }

    pub fn with_num_rows_estimate(mut self, num_rows_estimate: RowEstimate) -> Self {
        self.statistics.num_rows = num_rows_estimate.into();
        self
    }

    /// Adds statistics for a field named 'partition_key', currently this is assumed to be distinct
    /// for roughly row in the table. This would may not be appropriate for tables like journal.
    pub fn with_partition_key(mut self) -> Self {
        self.statistics
            .num_rows
            .get_value()
            .expect("with_num_rows_estimate must be called first");

        let (i, _) = self
            .schema
            .column_with_name("partition_key")
            .expect("partition_key to exist on table");
        self.statistics.column_statistics[i] =
            partition_key_column_statistics(self.statistics.num_rows);
        self
    }

    /// Describes a field as being the primary key of the table, with distinct count roughly equal to the
    /// number of rows in the table.
    pub fn with_primary_key(mut self, key_field: &str) -> Self {
        self.statistics
            .num_rows
            .get_value()
            .expect("with_num_rows_estimate must be called first");

        let (i, _) = self
            .schema
            .column_with_name(key_field)
            .expect("key field to exist on table");
        self.statistics.column_statistics[i] =
            primary_key_column_statistics(self.statistics.num_rows);
        self
    }

    /// Describes this field as being a reference to another table, where we can use the size of that table
    /// as an estimate of the number of distinct values of the key. For joins to propagate cardinality
    /// estimates correctly, all join keys must have statistics set.
    pub fn with_foreign_key(mut self, key_field: &str, foreign_row_estimate: RowEstimate) -> Self {
        self.statistics
            .num_rows
            .get_value()
            .expect("with_num_rows_estimate must be called first");

        let (i, _) = self
            .schema
            .column_with_name(key_field)
            .expect("key field to exist on table");
        let foreign_row_estimate: Precision<usize> = foreign_row_estimate.into();
        // we can't have more distinct values for a foreign key than we have rows
        let foreign_row_estimate = foreign_row_estimate.min(&self.statistics.num_rows);
        self.statistics.column_statistics[i] = foreign_key_column_statistics(foreign_row_estimate);
        self
    }

    pub fn build(self) -> Statistics {
        self.statistics
            .num_rows
            .get_value()
            .expect("with_num_rows_estimate must be called first");

        self.statistics
    }
}

fn partition_key_column_statistics(row_count: Precision<usize>) -> ColumnStatistics {
    ColumnStatistics {
        null_count: Precision::Exact(0),
        max_value: Precision::Inexact(ScalarValue::Null),
        min_value: Precision::Inexact(ScalarValue::Null),
        sum_value: Precision::Absent,
        distinct_count: row_count,
    }
}

fn primary_key_column_statistics(row_count: Precision<usize>) -> ColumnStatistics {
    ColumnStatistics {
        null_count: Precision::Exact(0),
        max_value: Precision::Inexact(ScalarValue::Null),
        min_value: Precision::Inexact(ScalarValue::Null),
        sum_value: Precision::Absent,
        distinct_count: row_count,
    }
}

fn foreign_key_column_statistics(foreign_row_estimate: Precision<usize>) -> ColumnStatistics {
    ColumnStatistics {
        null_count: Precision::Absent,
        max_value: Precision::Inexact(ScalarValue::Null),
        min_value: Precision::Inexact(ScalarValue::Null),
        sum_value: Precision::Absent,
        distinct_count: foreign_row_estimate,
    }
}
