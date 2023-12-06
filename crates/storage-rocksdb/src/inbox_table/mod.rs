// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::codec::ProtoValue;
use crate::keys::{define_table_key, TableKey};
use crate::TableKind::Inbox;
use crate::{GetFuture, PutFuture, RocksDBTransaction};
use crate::{Result, TableScan, TableScanIterationDecision};
use bytes::Bytes;
use bytestring::ByteString;

use futures_util::FutureExt;
use prost::Message;
use restate_storage_api::inbox_table::{InboxEntry, InboxTable};
use restate_storage_api::{ready, GetStream, StorageError};
use restate_storage_proto::storage;
use restate_types::identifiers::{PartitionKey, ServiceId, WithPartitionKey};
use restate_types::invocation::{MaybeFullInvocationId, ServiceInvocation};
use std::io::Cursor;
use std::ops::RangeInclusive;
use tokio_stream::StreamExt;

define_table_key!(
    Inbox,
    InboxKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: Bytes,
        sequence_number: u64
    )
);

impl<'a> InboxTable for RocksDBTransaction<'a> {
    fn put_invocation(
        &mut self,
        service_id: &ServiceId,
        InboxEntry {
            inbox_sequence_number,
            service_invocation,
        }: InboxEntry,
    ) -> PutFuture {
        let key = InboxKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone())
            .sequence_number(inbox_sequence_number);

        self.put_kv(
            key,
            ProtoValue(storage::v1::InboxEntry::from(service_invocation)),
        );
        ready()
    }

    fn delete_invocation(&mut self, service_id: &ServiceId, sequence_number: u64) -> PutFuture {
        let key = InboxKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone())
            .sequence_number(sequence_number);

        self.delete_key(&key);
        ready()
    }

    fn peek_inbox(&mut self, service_id: &ServiceId) -> GetFuture<Option<InboxEntry>> {
        let key = InboxKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());

        self.get_first_blocking(TableScan::KeyPrefix(key), |kv| match kv {
            Some((k, v)) => {
                let entry = decode_inbox_key_value(k, v)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        })
    }

    fn inbox(&mut self, service_id: &ServiceId) -> GetStream<InboxEntry> {
        let key = InboxKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());

        self.for_each_key_value(TableScan::KeyPrefix(key), |k, v| {
            let inbox_entry = decode_inbox_key_value(k, v);
            TableScanIterationDecision::Emit(inbox_entry)
        })
    }

    fn all_inboxes(&mut self, range: RangeInclusive<PartitionKey>) -> GetStream<InboxEntry> {
        self.for_each_key_value(TableScan::PartitionKeyRange::<InboxKey>(range), |k, v| {
            let inbox_entry = decode_inbox_key_value(k, v);
            TableScanIterationDecision::Emit(inbox_entry)
        })
    }

    fn contains(
        &mut self,
        maybe_fid: impl Into<MaybeFullInvocationId>,
    ) -> GetFuture<Option<(ServiceId, u64)>> {
        let (inbox_key, invocation_uuid) = match maybe_fid.into() {
            MaybeFullInvocationId::Partial(invocation_id) => (
                InboxKey::default().partition_key(invocation_id.partition_key()),
                invocation_id.invocation_uuid(),
            ),
            MaybeFullInvocationId::Full(fid) => (
                InboxKey::default()
                    .partition_key(fid.partition_key())
                    .service_name(fid.service_id.service_name)
                    .service_key(fid.service_id.key),
                fid.invocation_uuid,
            ),
        };

        let mut result =
            self.for_each_key_value(TableScan::KeyPrefix(inbox_key), move |key, value| {
                let inbox_entry = decode_inbox_key_value(key, value);

                match inbox_entry {
                    Ok(inbox_entry) => {
                        let fid = inbox_entry.service_invocation.fid;
                        if fid.invocation_uuid == invocation_uuid {
                            TableScanIterationDecision::BreakWith(Ok((
                                fid.service_id,
                                inbox_entry.inbox_sequence_number,
                            )))
                        } else {
                            TableScanIterationDecision::Continue
                        }
                    }
                    Err(err) => TableScanIterationDecision::BreakWith(Err(err)),
                }
            });

        async move { result.next().await.transpose() }.boxed()
    }
}

fn decode_inbox_key_value(k: &[u8], v: &[u8]) -> Result<InboxEntry> {
    let key = InboxKey::deserialize_from(&mut Cursor::new(k))?;
    let sequence_number = *key.sequence_number_ok_or()?;

    let inbox_entry = ServiceInvocation::try_from(
        storage::v1::InboxEntry::decode(v).map_err(|error| StorageError::Generic(error.into()))?,
    )?;

    Ok(InboxEntry::new(sequence_number, inbox_entry))
}

#[cfg(test)]
mod tests {
    use crate::inbox_table::InboxKey;
    use crate::keys::TableKey;
    use bytes::{Bytes, BytesMut};
    use restate_types::identifiers::{ServiceId, WithPartitionKey};

    fn message_key(service_id: &ServiceId, sequence_number: u64) -> Bytes {
        let key = InboxKey {
            partition_key: Some(service_id.partition_key()),
            service_name: Some(service_id.service_name.clone()),
            service_key: Some(service_id.key.clone()),
            sequence_number: Some(sequence_number),
        };
        let mut buf = BytesMut::new();
        key.serialize_to(&mut buf);
        buf.freeze()
    }

    fn inbox_key(service_id: &ServiceId) -> Bytes {
        let key = InboxKey {
            partition_key: Some(service_id.partition_key()),
            service_name: Some(service_id.service_name.clone()),
            service_key: Some(service_id.key.clone()),
            sequence_number: None,
        };

        key.serialize().freeze()
    }

    #[test]
    fn inbox_key_covers_all_messages_of_a_service() {
        let prefix_key = inbox_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"));

        let low_key = message_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"), 0);
        assert!(low_key.starts_with(&prefix_key));

        let high_key = message_key(
            &ServiceId::with_partition_key(1337, "svc-1", "key-a"),
            u64::MAX,
        );
        assert!(high_key.starts_with(&prefix_key));
    }

    #[test]
    fn message_keys_sort_lex() {
        //
        // across services
        //
        assert!(
            message_key(&ServiceId::with_partition_key(1337, "svc-1", ""), 0)
                < message_key(&ServiceId::with_partition_key(1337, "svc-2", ""), 0)
        );
        //
        // same service across keys
        //
        assert!(
            message_key(&ServiceId::with_partition_key(1337, "svc-1", "a"), 0)
                < message_key(&ServiceId::with_partition_key(1337, "svc-1", "b"), 0)
        );
        //
        // within the same service and key
        //
        let mut previous_key =
            message_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"), 0);
        for i in 1..300 {
            let current_key =
                message_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"), i);
            assert!(previous_key < current_key);
            previous_key = current_key;
        }
    }
}
