// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Resolves the targets of retained VQueue entries, for completeness passes over
//! entries written before an instance was maintained.
//!
//! An invocation's target comes from its invocation status. A state mutation's
//! comes from its input payload, without decoding its state map.

use anyhow::{Context, ensure};
use bilrost::OwnedMessage;

use restate_storage_api::protobuf_types::v1::lazy::{InvocationStatusV2Lazy, InvocationTargetLazy};
use restate_types::identifiers::{ServiceId, WithPartitionKey};
use restate_types::invocation::ServiceType;
use restate_types::storage::StorageCodecKind;
use restate_types::vqueues::{EntryKind, EntryTargetRef, HandlerRef, VQueueId};

use crate::PartitionDb;
use crate::invocation_status_table::InvocationStatusKey;
use crate::keys::EncodeTableKeyPrefix;
use crate::vqueue_table::InputPayloadKey;

// Decode only the target, skipping the potentially large state map in StateMutationInput.
#[derive(bilrost::Message)]
struct StateMutationTarget {
    #[bilrost(1)]
    service_id: ServiceId,
}

fn target_ref<'a>(target: &'a InvocationTargetLazy<'_>) -> anyhow::Result<EntryTargetRef<'a>> {
    let service = target.service_name();
    let handler = target.handler_name();
    let scope = target.scope();
    Ok(match target.service_ty()? {
        ServiceType::Service => EntryTargetRef::Service {
            service,
            handler,
            scope,
        },
        ServiceType::VirtualObject => EntryTargetRef::VirtualObject {
            service,
            handler: HandlerRef::UserHandler(handler),
            scope,
            key: target.key()?.context("virtual object target has no key")?,
        },
        ServiceType::Workflow => EntryTargetRef::Workflow {
            service,
            handler,
            scope,
            key: target.key()?.context("workflow target has no key")?,
        },
    })
}

/// Decodes the target of a stored invocation status (codec byte included).
fn with_invocation_target<R>(
    value: &[u8],
    f: impl FnOnce(&EntryTargetRef<'_>) -> anyhow::Result<R>,
) -> anyhow::Result<R> {
    let (&codec, value) = value.split_first().context("empty invocation status")?;
    ensure!(
        matches!(
            StorageCodecKind::try_from(codec)?,
            StorageCodecKind::Protobuf
        ),
        "unsupported invocation status codec"
    );
    let mut invocation = InvocationStatusV2Lazy::default();
    invocation.merge(value)?;
    let target = invocation
        .invocation_target()?
        .context("missing invocation target")?;
    f(&target_ref(&target)?)
}

/// Resolves the target of an entry with point lookups: its invocation status, or
/// the input of a state mutation. Entries without one are errors.
pub(crate) fn with_entry_target<R>(
    db: &PartitionDb,
    id: &restate_types::identifiers::BaseEntryId,
    qid: &VQueueId,
    seq: restate_types::vqueues::Seq,
    f: impl FnOnce(&EntryTargetRef<'_>) -> anyhow::Result<R>,
) -> anyhow::Result<R> {
    ensure!(
        id.partition_key() == qid.partition_key(),
        "entry and queue partition keys disagree"
    );
    let raw = db.rocksdb().inner().as_raw_db();
    let mut key = Vec::new();
    match id.kind() {
        EntryKind::Invocation => {
            let invocation_id = id
                .to_invocation_id()
                .context("invalid invocation entry id")?;
            InvocationStatusKey::builder()
                .partition_key(invocation_id.partition_key())
                .invocation_uuid(invocation_id.invocation_uuid())
                .serialize_to(&mut key);
            let value = raw
                .get_pinned_cf(db.cf_handle(), &key)?
                .with_context(|| format!("missing invocation status for {id}"))?;
            with_invocation_target(&value, f)
        }
        EntryKind::StateMutation => {
            InputPayloadKey::builder()
                .qid(qid.clone())
                .seq(seq)
                .id(id.to_entry_id())
                .serialize_to(&mut key);
            let value = raw
                .get_pinned_cf(db.cf_handle(), &key)?
                .context("missing state mutation input")?;
            let mutation = StateMutationTarget::decode(value.as_ref())?;
            f(&EntryTargetRef::VirtualObject {
                service: &mutation.service_id.service_name,
                scope: mutation.service_id.scope.as_ref().map(AsRef::as_ref),
                key: &mutation.service_id.key,
                handler: HandlerRef::StateMutation,
            })
        }
        EntryKind::Unknown => anyhow::bail!("unknown vqueue entry kind"),
    }
}

#[cfg(test)]
pub(crate) mod tests;
