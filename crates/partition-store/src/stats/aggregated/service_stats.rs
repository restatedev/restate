// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;

use restate_storage_api::vqueue_table::{Stage, Status};
use restate_storage_query_api::schema::{RecordProgress, row_written};
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::stats::service_stats::{
    ServiceStats as ServiceStatsSchema, ServiceStatsRow,
};
use restate_storage_query_api::{LogicalSchema, PreparedRead, RowSink, SchemaBinding, Table};

use crate::keys::codecs::{EntryKindCodec, ServiceNameCodec, StringCodec};
use crate::stats::macros::define_aggregated_stat;
use crate::{Result, break_on_err};

use super::StageStatusGauge;
use super::binding;

#[cfg(test)]
mod filter_tests;

define_aggregated_stat!(
    /// Key for current entry counts grouped by service, handler, and entry kind.
    ///
    /// The payload orders `service_name -> handler -> kind` within each physical
    /// partition. Counts combine scopes and queue identities; callers combine
    /// physical partitions for service-wide totals. Values are sparse stage/status
    /// counters, including finished entries until deletion.
    ///
    /// A NULL handler identifies an external state mutation, not a handler rollup.
    table: ServiceStats,
    definition: "service_stats/1",
    schema: ServiceStatsSchema,
    value: StageStatusGauge,
    key: ServiceStatsKey(
        service_name: ServiceNameCodec => str,
        handler: Option<StringCodec> => str,
        kind: EntryKindCodec,
    ),
);

pub enum ServiceStatsBinding {}

impl SchemaBinding for ServiceStatsBinding {
    type Schema = ServiceStatsSchema;
    type Table = ServiceStats;
    type Materializer = super::GaugeMaterializer<ServiceStatsRow>;

    fn materialize_next(
        (key, payload): <Self::Table as Table>::Record<'_>,
        state: &mut Self::Materializer,
        emit: &mut impl RowSink<Self::Schema>,
    ) -> Result<RecordProgress> {
        let Some((row, bucket, count, done)) = state.next(|| {
            let Some(counts) = binding::decode::<Self::Table>(payload)? else {
                return Ok(None);
            };
            let key = key.decode_all()?;
            Ok(Some((
                ServiceStatsRow {
                    service_name: key.service_name.into(),
                    handler: key.handler,
                    kind: key.kind,
                    stage: Stage::Unknown,
                    status: Status::Unknown,
                    num_entries: 0,
                },
                counts,
            )))
        })?
        else {
            return Ok(RecordProgress::Done);
        };
        row.stage = bucket.stage;
        row.status = bucket.status;
        row.num_entries = count;
        row_written(emit.append(row))?;
        Ok(if done {
            RecordProgress::Done
        } else {
            RecordProgress::More
        })
    }

    fn prepare(selection: &Selection<Self::Schema>) -> Result<PreparedRead<Self::Table>> {
        binding::prepare::<Self::Table, Self::Schema>(selection)
    }

    fn materialize(
        (key, payload): <Self::Table as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(
            <Self::Schema as LogicalSchema>::Row<'a>,
        ) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>> {
        let Some(counts) = break_on_err(binding::decode::<Self::Table>(payload))? else {
            return ControlFlow::Continue(());
        };
        let key = break_on_err(key.decode_all())?;
        let mut row = ServiceStatsRow {
            service_name: key.service_name.into(),
            handler: key.handler,
            kind: key.kind,
            stage: Stage::Unknown,
            status: Status::Unknown,
            num_entries: 0,
        };
        for (bucket, num_entries) in counts.iter() {
            row.stage = bucket.stage;
            row.status = bucket.status;
            row.num_entries = num_entries;
            emit(&row)?;
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use restate_types::ServiceName;
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::EntryKind;
    use restate_util_string::{ReString, encoded_mem_cmp_str};

    use crate::keys::KeyDecoder;
    use crate::keys::header::IndexScope;
    use crate::stats::{AggregateKeyPrefix, Stat};

    use super::*;

    const SCOPE: IndexScope = IndexScope {
        partition: PartitionId::MIN,
        index: 64,
    };

    #[test]
    fn service_stat_keys_encode_owned_and_sort_by_stable_dimensions() {
        let mut encoded = [
            ("beta", Some("handler-b")),
            ("alpha", Some("handler-b")),
            ("beta", None),
            ("alpha", None),
        ]
        .map(|(service_name, handler)| {
            let mut key = Vec::new();
            ServiceStats::encode_key(
                SCOPE,
                ServiceStatsKey::borrowed(service_name, handler, EntryKind::Invocation),
                &mut key,
            );
            (key, service_name, handler)
        });
        encoded.sort_by(|(left, ..), (right, ..)| left.cmp(right));

        assert_eq!(
            encoded.map(|(_, service_name, handler)| (service_name, handler)),
            [
                ("alpha", None),
                ("alpha", Some("handler-b")),
                ("beta", None),
                ("beta", Some("handler-b")),
            ]
        );

        let service_name = ServiceName::new("alpha");
        let mut borrowed_service_name = Vec::new();
        ServiceStats::encode_key(
            SCOPE,
            ServiceStatsKey::borrowed(&service_name, Some("handler"), EntryKind::Invocation),
            &mut borrowed_service_name,
        );
        let mut borrowed_str = Vec::new();
        ServiceStats::encode_key(
            SCOPE,
            ServiceStatsKey::borrowed("alpha", Some("handler"), EntryKind::Invocation),
            &mut borrowed_str,
        );
        let string = String::from("alpha");
        let handler_string = String::from("handler");
        let mut borrowed_string = Vec::new();
        ServiceStats::encode_key(
            SCOPE,
            ServiceStatsKey::borrowed(&string, Some(&handler_string), EntryKind::Invocation),
            &mut borrowed_string,
        );
        let re_string = ReString::from("alpha");
        let handler_re_string = ReString::from("handler");
        let mut borrowed_re_string = Vec::new();
        ServiceStats::encode_key(
            SCOPE,
            ServiceStatsKey::borrowed(&re_string, Some(&handler_re_string), EntryKind::Invocation),
            &mut borrowed_re_string,
        );
        let mut owned = Vec::new();
        ServiceStats::encode_key(
            SCOPE,
            ServiceStatsKey {
                service_name,
                kind: EntryKind::Invocation,
                handler: Some(ReString::from("handler")),
            },
            &mut owned,
        );
        assert_eq!(borrowed_service_name, owned);
        assert_eq!(borrowed_str, owned);
        assert_eq!(borrowed_string, owned);
        assert_eq!(borrowed_re_string, owned);

        let (prefix, kind, payload) = AggregateKeyPrefix::decode(&owned).unwrap();
        assert_eq!((prefix.id(), kind), (64, ServiceStats::STAT_KIND));
        let decoder = KeyDecoder::<ServiceStatsKey>::from_payload(payload);
        let (service_name, decoder) = decoder.take_service_name().unwrap();
        assert_eq!(service_name.encoded(), encoded_mem_cmp_str!("alpha"));
        assert_eq!(service_name.decode().unwrap().as_str(), "alpha");
        let (handler, decoder) = decoder.take_handler().unwrap();
        assert_eq!(handler.as_bytes()[0], 1);
        assert_eq!(
            &handler.as_bytes()[1..],
            encoded_mem_cmp_str!("handler").as_bytes()
        );
        assert_eq!(handler.decode().unwrap().unwrap(), "handler");
        let kind = decoder.take_kind().unwrap();
        assert_eq!(kind.as_bytes(), &[10]);
        assert_eq!(kind.decode().unwrap(), EntryKind::Invocation);

        let mut borrowed_none = Vec::new();
        ServiceStats::encode_key(
            SCOPE,
            ServiceStatsKey::borrowed("alpha", None::<&str>, EntryKind::Invocation),
            &mut borrowed_none,
        );
        let mut owned_none = Vec::new();
        ServiceStats::encode_key(
            SCOPE,
            ServiceStatsKey {
                service_name: ServiceName::new("alpha"),
                kind: EntryKind::Invocation,
                handler: None,
            },
            &mut owned_none,
        );
        assert_eq!(borrowed_none, owned_none);
    }

    #[test]
    fn service_prefixes_match_complete_key_field_boundaries() {
        let partition_id = IndexScope {
            partition: PartitionId::from(8),
            index: 64,
        };
        let mut service_key = Vec::new();
        ServiceStats::encode_key(
            partition_id,
            ServiceStatsKey::borrowed("alpha", Some("handler"), EntryKind::Invocation),
            &mut service_key,
        );
        let mut prefix = Vec::new();
        for fields in 0..=3 {
            prefix.clear();
            let builder = ServiceStatsKey::prefix(partition_id, &mut prefix);
            match fields {
                0 => {}
                1 => {
                    builder.service_name("alpha");
                }
                2 => {
                    builder.service_name("alpha").handler(Some("handler"));
                }
                _ => {
                    builder
                        .service_name("alpha")
                        .handler(Some("handler"))
                        .kind(EntryKind::Invocation);
                }
            }
            assert!(service_key.starts_with(&prefix));
        }
        assert_eq!(prefix, service_key);
    }
}
