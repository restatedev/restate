// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::stats::virtual_object_load::VirtualObjectLoad;
use restate_types::ServiceName;
use restate_types::sharding::PartitionKey;
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

use crate::stats::macros::define_aggregated_stat;

use super::StageGauge;

// Number of vqueue entries aggregated by VO
define_aggregated_stat!(
    table: VirtualObjectLoad,
    value: StageGauge,
    key: VirtualObjectLoadKey(
        service_name: ServiceName => str,
        key: ReString => str,
        scope: Option<ReString> => str,
        handler: Option<ReString> => str,
        kind: EntryKind,
        partition_key: PartitionKey,
    ),
);

#[cfg(test)]
mod filter_tests;

#[cfg(test)]
mod tests {
    use crate::stats::Stat;
    use restate_types::sharding::PartitionId;

    use super::*;

    #[test]
    fn virtual_object_prefixes_match_complete_key_field_boundaries() {
        let partition_id = PartitionId::from(8);
        let mut virtual_object_key = Vec::new();
        VirtualObjectLoad::encode_key(
            partition_id,
            VirtualObjectLoadKey::borrowed(
                "counter",
                "object",
                None::<&str>,
                None::<&str>,
                EntryKind::StateMutation,
                3337,
            ),
            &mut virtual_object_key,
        );
        let mut prefix = Vec::new();
        for fields in 0..=6 {
            prefix.clear();
            let builder = VirtualObjectLoadKey::prefix(partition_id, &mut prefix);
            match fields {
                0 => {}
                1 => {
                    builder.service_name("counter");
                }
                2 => {
                    builder.service_name("counter").key("object");
                }
                3 => {
                    builder
                        .service_name("counter")
                        .key("object")
                        .scope(None::<&str>);
                }
                4 => {
                    builder
                        .service_name("counter")
                        .key("object")
                        .scope(None::<&str>)
                        .handler(None::<&str>);
                }
                5 => {
                    builder
                        .service_name("counter")
                        .key("object")
                        .scope(None::<&str>)
                        .handler(None::<&str>)
                        .kind(EntryKind::StateMutation);
                }
                _ => {
                    builder
                        .service_name("counter")
                        .key("object")
                        .scope(None::<&str>)
                        .handler(None::<&str>)
                        .kind(EntryKind::StateMutation)
                        .partition_key(3337);
                }
            }
            assert!(virtual_object_key.starts_with(&prefix));
        }
        assert_eq!(prefix, virtual_object_key);

        let mut null_scope = Vec::new();
        VirtualObjectLoadKey::prefix(partition_id, &mut null_scope)
            .service_name("counter")
            .key("object")
            .scope(None::<&str>);
        let mut empty_scope = Vec::new();
        VirtualObjectLoadKey::prefix(partition_id, &mut empty_scope)
            .service_name("counter")
            .key("object")
            .scope(Some(""));
        assert_ne!(null_scope, empty_scope);
    }
}
