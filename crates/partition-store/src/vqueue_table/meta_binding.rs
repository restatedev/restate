// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Query reads of vqueue metadata.
//!
//! Keys store `kind | partition_key | length | digest`: the vqueue ID's partition
//! key forms the header and the rest of the ID is a fixed-width, mem-comparable
//! ordered field. A selection narrows the header, and an ID restriction positions
//! the iterator at that queue's key. Many queues can share a partition key (every
//! queue of one scope), and they are skipped by seeking, not read.

use std::ops::ControlFlow;

use bilrost::BorrowedMessage;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::VQueueMetaRef;
use restate_storage_query_api::schema::RecordProgress;
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::tables::vqueue_meta::{
    VQueueMetaView, VqueueMeta, VqueueMetaClause, VqueueMetaColumn,
};
use restate_storage_query_api::{PreparedRead, RowSink, SchemaBinding, Table};
use restate_types::identifiers::partitioner::HashPartitioner;

use crate::keys::codecs::VQueueIdSuffixCodec;
use crate::keys::header::{KeySchema, PartitionKeyHeader, prepare_partition_keyed};
use crate::keys::selection::codec::FieldPredicateCodec;
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, KeyDecode, KeyDecoder, KeyKind};
use crate::table::PhysicalTable;
use crate::{Result, break_on_err};

use super::MetaKey;

/// Physical vqueue metadata records using the existing key and value encodings.
pub enum VQueueMetaTable {}

impl Table for VQueueMetaTable {
    type Record<'a> = (KeyDecoder<'a, MetaKey>, &'a [u8]);
    type Scan = KeySelection<MetaKey>;
}

impl PhysicalTable for VQueueMetaTable {
    type Key = MetaKey;
}

impl KeySchema for MetaKey {
    type Header = PartitionKeyHeader<Self>;

    fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
        // The vqueue ID includes the partition key. Retain the full key.
        Ok(KeyDecoder::from_payload(key))
    }
}

impl OrderedKeySchema for MetaKey {
    const KEY_KIND: KeyKind = KeyKind::VQueueMeta;
    const FIELDS: &'static [OrderedKeyField] = &[OrderedKeyField::new::<VQueueIdSuffixCodec>("id")];
}

impl KeyDecode for MetaKey {
    fn decode(source: &mut &[u8]) -> Result<Self> {
        Self::deserialize_from(source)
    }
}

pub enum VQueueMetaBinding {}

impl SchemaBinding for VQueueMetaBinding {
    type Schema = VqueueMeta;
    type Table = VQueueMetaTable;
    type Materializer = ();

    fn materialize_next(
        record: <Self::Table as Table>::Record<'_>,
        _: &mut (),
        emit: &mut impl RowSink<Self::Schema>,
    ) -> Result<RecordProgress> {
        if let ControlFlow::Break(result) = Self::materialize_projected(record, emit) {
            result?;
        }
        Ok(RecordProgress::Done)
    }

    fn prepare(selection: &Selection<VqueueMeta>) -> Result<PreparedRead<VQueueMetaTable>> {
        let (request, precision) = prepare_partition_keyed(
            selection,
            &[
                VqueueMetaColumn::PartitionKey,
                VqueueMetaColumn::Id,
                VqueueMetaColumn::Scope,
            ],
            |clause| match clause {
                VqueueMetaClause::PartitionKey(predicate) => Some(predicate.into()),
                VqueueMetaClause::Id(predicate) => {
                    Some(Domain::from(predicate).map_values(|id| id.partition_key()))
                }
                // A NULL scope leaves the partition key unknown.
                VqueueMetaClause::Scope(predicate) => {
                    Some(Domain::from(predicate).try_map_values(|scope| {
                        scope.as_deref().map(HashPartitioner::compute_partition_key)
                    }))
                }
                _ => None,
            },
            |clause, literals| match clause {
                VqueueMetaClause::Id(predicate) => {
                    VQueueIdSuffixCodec::try_prepare_value(predicate, literals)
                }
                _ => Ok(None),
            },
        )?;
        Ok(PreparedRead::new(request, precision))
    }

    fn materialize(
        (key, value): <Self::Table as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(VQueueMetaView<'a>) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>> {
        let (id,) = break_on_err(key.decode_all())?.split();
        let meta = break_on_err(
            VQueueMetaRef::decode_borrowed(value).map_err(StorageError::BilrostDecode),
        )?;
        emit(VQueueMetaView {
            id: &id,
            meta: &meta,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use restate_storage_query_api::selection::{Selection, SelectionPrecision, ValuePredicate};
    use restate_storage_query_api::tables::vqueue_meta::VqueueMetaClause;
    use restate_storage_query_api::{PreparedRead, SchemaBinding};
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::VQueueId;

    use crate::table::navigate;

    use super::{MetaKey, VQueueMetaBinding, VQueueMetaTable};

    #[test]
    fn vqueue_ids_position_metadata_reads() {
        // Equal digests across partition keys model the cross product the header
        // interval admits.
        let mut ids: Vec<_> = [6, 7, 8, 9]
            .into_iter()
            .flat_map(|partition_key| {
                (0..128).map(move |queue| VQueueId::custom(partition_key, format!("q{queue}")))
            })
            .collect();
        ids.sort();
        let keys: Vec<_> = ids.iter().map(|id| MetaKey::from(id).to_bytes()).collect();
        let read = |clause| {
            let prepared: PreparedRead<VQueueMetaTable> =
                VQueueMetaBinding::prepare(&Selection::All.and(clause)).unwrap();
            let (matches, visited) =
                navigate(Arc::clone(prepared.request()), KeyRange::FULL, &keys);
            let matches: Vec<_> = matches.into_iter().map(|index| &ids[index]).collect();
            (matches, visited, prepared.precision())
        };
        let id = |partition_key, queue: u32| VQueueId::custom(partition_key, format!("q{queue}"));

        // One queue among the 128 sharing its partition key.
        let (matches, visited, precision) =
            read(VqueueMetaClause::Id(ValuePredicate::Equal(id(7, 5))));
        assert_eq!(matches, [&id(7, 5)]);
        assert_eq!(precision, SelectionPrecision::Inexact);
        assert_eq!(visited, 1, "id_bounds_read_to_one_key");

        // IDs on two partition keys: the header covers 7..=8 and both suffixes are
        // sought under each, so residuals reject the crossed records.
        let (matches, visited, _) = read(VqueueMetaClause::Id(ValuePredicate::In(vec![
            id(7, 5),
            id(8, 9),
        ])));
        let mut crossed = [id(7, 5), id(7, 9), id(8, 5), id(8, 9)];
        crossed.sort();
        assert_eq!(matches, crossed.iter().collect::<Vec<_>>());
        assert_eq!(visited, 10, "ids_seek_within_and_between_partition_keys");
    }
}
