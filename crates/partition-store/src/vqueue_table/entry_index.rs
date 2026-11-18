// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The entry indexes: secondary indexes over VQueue entries, released together
//! and switched on by `experimental-enable-track-entry-indexes`.
//!
//! **Entries by service and handler, newest created first**
//! (`entry_by_service_created/1`) holds one key for every entry, of both kinds,
//! in every stage, including finished entries kept for their retention:
//!
//! ```text
//! xI | partition | index ID | service | handler? | created_at ↓ | canonical entry ID
//!                             "Utils"   "call"     t9             inv_…
//!                             "Utils"   "call"     t7             inv_…
//!                             "Utils"   "run"      t8             inv_…
//!                             "object"  NULL       t6             (a state mutation)
//! ```
//!
//! So the entries of one service and handler are one contiguous run, newest
//! first, and a query naming only the service reads one run per handler. The
//! handler is NULL for state mutations, and NULL sorts first within its service.
//!
//! Service and handler come from the entry's target, which every write already
//! passes (`EntryContext`), so the entry header stays as it is. None of the key's
//! fields changes after the entry is created, so only creating, adopting and
//! deleting an entry writes the index; stage and status changes don't.

use std::cmp::Reverse;

use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef};
use restate_types::config::Configuration;

use crate::PartitionStoreTransaction;
use crate::keys::codecs::{
    CanonicalEntryIdCodec, ReverseUniqueTimestampCodec, ServiceNameCodec, StringCodec,
};
use crate::registry::{Family, IndexDefinition, IndexKind};

crate::index::macros::define_secondary_index!(
    /// Entries by service and handler, newest created first.
    EntryByServiceCreated,
    definition: "entry_by_service_created/1",
    key: EntryByServiceCreatedKey {
        target_service_name: ServiceNameCodec => str,
        target_handler_name: Option<StringCodec> => str,
        created_at: ReverseUniqueTimestampCodec,
        canonical_id: CanonicalEntryIdCodec (primary_key),
    }
);

// A query on invocations restricts the index through the invocation columns its
// fields are named after. Every invocation's key has a handler; only state
// mutations, which a query on invocations skips, have none.
crate::keys::macros::define_key_selection!(
    EntryByServiceCreatedKey;
    [restate_storage_query_api::tables::index::EntryByServiceCreated];
    target_service_name,
    target_handler_name => present,
    created_at,
);

/// Every entry by service and handler, newest created first.
pub(crate) static ENTRY_BY_SERVICE_CREATED: IndexDefinition = IndexDefinition {
    name: "entry_by_service_created/1",
    kind: IndexKind::Secondary,
    apply: apply_entry_by_service_created,
    can_fill: true,
};

/// The entry indexes, released and controlled together.
pub(crate) const ENTRY_INDEXES: Family = Family {
    definitions: &[&ENTRY_BY_SERVICE_CREATED],
    maintain: track_entry_indexes,
    reset: entry_indexes_reset,
};

fn track_entry_indexes(config: &Configuration) -> bool {
    config.common.experimental.is_track_entry_indexes_enabled()
}

fn entry_indexes_reset(config: &Configuration) -> bool {
    config.common.experimental.is_entry_indexes_reset_enabled()
}

/// Removes the key of `before` and adds the key of `after`. Both share the
/// entry's target, and the rest of the key never changes after creation, so an
/// update writes nothing.
fn apply_entry_by_service_created(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let project = |entry: &EntryStateRef<'_>| {
        (
            Reverse(entry.stats.created_at),
            entry
                .entry_key
                .to_canonical_entry_id(context.qid.partition_key()),
        )
    };
    let (old, new) = (before.map(project), after.map(project));
    if old != new {
        let key = |(created_at, canonical_id)| {
            EntryByServiceCreatedKey::borrowed(
                context.target.service(),
                context.target.handler(),
                created_at,
                canonical_id,
            )
        };
        storage.update_secondary_index_with_id(id, old.map(key).as_ref(), new.map(key).as_ref());
    }
}

#[cfg(test)]
mod tests;
