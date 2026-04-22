// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use bilrost::{Message, OwnedMessage};
use bytes::{Buf, BufMut};

use restate_rocksdb::Priority;
use restate_storage_api::StorageError;
use restate_storage_api::lock_table::{LoadLocks, LockState, ScanLocksTable, WriteLockTable};
use restate_types::identifiers::{PartitionKey, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_types::{CanonicalLockId, LockName, Scope};
use restate_util_string::InternedReString;

use crate::TableKind::Locks;
use crate::keys::{
    DecodeTableKey, EncodeTableKeyPrefix, KeyDecode, KeyEncode, KeyKind, define_table_key,
};
use crate::scan::TableScan;
use crate::{
    PartitionDb, PartitionStore, PartitionStoreTransaction, Result, StorageAccess, break_on_err,
};

// 'lo' | PKEY | CANONICAL_LOCK_ID
// CanonicalLockId is serialized as follows:
// - one byte to denote whether this is scoped or not (b's' or b'u')
// - if scoped, the scope name is length-prefixed string
// - canonical lock-name encoded (without length prefix) (service/key)
//
// This design is chosen to allow for prefix scan locks by scope (or unscoped) and
// by service/owner name.
//
// What's not possible is a prefix scan by partial scope name, but partial lock-name is supported.
define_table_key!(
    Locks,
    KeyKind::Lock,
    LockKey (
        partition_key: PartitionKey,
        optional_scope: Option<Scope>,
        lock_name: LockName,
    )
);

// prefix is `s` or `u`
impl KeyEncode for Option<Scope> {
    fn encode<B: BufMut>(&self, target: &mut B) {
        if let Some(scope) = self {
            target.put_u8(b's');
            target.put_u32(scope.len() as u32);
            target.put_slice(scope.as_bytes());
        } else {
            target.put_u8(b'u');
        }
    }

    fn serialized_length(&self) -> usize {
        if self.is_some() {
            1 + std::mem::size_of::<u32>() + self.as_ref().map(|s| s.len()).unwrap_or_default()
        } else {
            1
        }
    }
}

impl KeyDecode for Option<Scope> {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let tag = source.get_u8();
        match tag {
            b's' => {
                let scope_len = source.get_u32() as usize;
                if scope_len == 0 {
                    return Err(StorageError::Generic(anyhow::anyhow!("empty scope")));
                }
                let mut string_data = source.take(scope_len);
                // SAFETY:
                // We are always decoding keys that we have serialized by this type, therefore
                // they are valid utf-8 strings.
                let raw = unsafe { std::str::from_utf8_unchecked(string_data.chunk()) };
                let scope = InternedReString::new(raw);
                string_data.advance(scope_len);
                Ok(Some(Scope::from(scope)))
            }
            b'u' => Ok(None),
            _ => Err(StorageError::Generic(anyhow::anyhow!(
                "unknown scope prefix: {tag:x?}"
            ))),
        }
    }
}

// Note that LockName *must* be used as the suffix portion of a key since it's not length-prefixed.
impl KeyEncode for LockName {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_slice(self.service_name().as_bytes());
        target.put_u8(b'/');
        target.put_slice(self.key().as_bytes());
    }

    fn serialized_length(&self) -> usize {
        self.service_name().len() + 1 + self.key().len()
    }
}

impl KeyDecode for LockName {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let string_data = source.take(source.remaining());
        // SAFETY:
        // We are always decoding keys that we have serialized by this type, therefore
        // they are valid utf-8 strings.
        let raw = unsafe { std::str::from_utf8_unchecked(string_data.chunk()) };
        // make the lock name a shared string (arc or inlined if small)
        LockName::parse(raw).map_err(|e| StorageError::Generic(e.into()))
    }
}

impl LoadLocks for PartitionDb {
    fn scan_all_locked(&self, mut on_item: impl FnMut(Option<Scope>, LockName)) -> Result<()> {
        let mut iterator_opts = rocksdb::ReadOptions::default();
        iterator_opts.fill_cache(false);
        iterator_opts.set_async_io(true);
        // this is not the place to be concerned about corruption, we favor speed
        // over safety for this particular use-case.
        iterator_opts.set_verify_checksums(false);
        // We scan the full partition-key range, which spans many fixed-prefix domains.
        // Prefix seek mode may miss keys outside the current prefix, so force total order.
        iterator_opts.set_total_order_seek(true);

        let mut key_buf = [0u8; KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>()];
        // Setting up iterator bounds
        EncodeTableKeyPrefix::serialize_to(
            &LockKey::builder().partition_key(self.partition().key_range.start()),
            &mut key_buf.as_mut(),
        );
        iterator_opts.set_iterate_lower_bound(key_buf);

        EncodeTableKeyPrefix::serialize_to(
            &LockKey::builder().partition_key(self.partition().key_range.end()),
            &mut key_buf.as_mut(),
        );
        // End key is exclusive in rocksdb iterators, so the end prefix is one byte
        // beyond the max partition key on this key kind prefix.
        let _success = crate::convert_to_upper_bound(&mut key_buf);
        iterator_opts.set_iterate_upper_bound(key_buf);

        let rocksdb = self.rocksdb().inner().as_raw_db();
        let cf = self.table_cf_handle(crate::TableKind::Locks);
        let mut it = rocksdb.raw_iterator_cf_opt(cf, iterator_opts);
        it.seek_to_first();

        while it.valid() {
            // safe to unwrap because the iterator is valid
            let mut key = it.key().unwrap();
            let lock_key = LockKey::deserialize_from(&mut key)?;
            let (_partition_key, opt_scope, lock_name) = lock_key.split();
            on_item(opt_scope, lock_name);
            it.next();
        }
        // ensures we didn't stop because of an iterator error
        it.status()
            .context("iterating over locks")
            .map_err(StorageError::Generic)
    }
}

impl WriteLockTable for PartitionStoreTransaction<'_> {
    fn acquire_lock(&mut self, scope: &Option<Scope>, lock_name: &LockName, state: &LockState) {
        let canonical = CanonicalLockId { scope, lock_name };
        let partition_key = canonical.partition_key();

        let key_buf = {
            let key = LockKey::builder_ref()
                .partition_key(&partition_key)
                .optional_scope(scope)
                .lock_name(lock_name);
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        let value_buf = {
            let value_buf = self.cleared_value_buffer_mut(state.encoded_len());
            state.encode(value_buf).unwrap();
            value_buf.split()
        };

        self.raw_put_cf(KeyKind::Lock, key_buf, value_buf);
    }

    fn release_lock(&mut self, scope: &Option<Scope>, lock_name: &LockName) {
        let canonical = CanonicalLockId { scope, lock_name };
        let partition_key = canonical.partition_key();

        let key = LockKey::builder_ref()
            .partition_key(&partition_key)
            .optional_scope(scope)
            .lock_name(lock_name);

        let key_buf = {
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        self.raw_delete_cf(KeyKind::Lock, key_buf);
    }
}

// Data fusion queries
impl ScanLocksTable for PartitionStore {
    fn for_each_lock<
        F: FnMut((PartitionKey, Option<Scope>, LockName, LockState)) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: KeyRange,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        self.iterator_for_each(
            "df-locks",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<LockKey>(range),
            move |(mut key, mut value)| {
                let lock_key = break_on_err(LockKey::deserialize_from(&mut key))?;
                let lock_state = break_on_err(
                    LockState::decode(&mut value).map_err(StorageError::BilrostDecode),
                )?;

                let (partition_key, opt_scope, lock_name) = lock_key.split();

                f((partition_key, opt_scope, lock_name, lock_state)).map_break(Ok)
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}
