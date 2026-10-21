// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::codecs::{EntryKindCodec, ServiceNameCodec, StringCodec, U64Codec};
use crate::stats::macros::define_aggregated_stat;

use super::StageStatusGauge;

define_aggregated_stat!(
    /// Key for current virtual-object entry counts, including external state mutations.
    ///
    /// The payload orders `service_name -> scope -> key -> handler -> kind -> partition_key`
    /// within each physical partition. This supports service/scope prefix scans;
    /// NULL scope and handler values are distinct from empty strings. The trailing
    /// partition key preserves the entry's routing identity for custom queues.
    /// Workflow and unkeyed service entries do not contribute to this statistic.
    table: VirtualObjectStats,
    value: StageStatusGauge,
    key: VirtualObjectStatsKey(
        service_name: ServiceNameCodec => str,
        scope: Option<StringCodec> => str,
        key: StringCodec => str,
        handler: Option<StringCodec> => str,
        kind: EntryKindCodec,
        partition_key: U64Codec,
    ),
);

#[cfg(test)]
mod tests {
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::EntryKind;

    use crate::stats::Stat;

    use super::*;

    #[test]
    fn virtual_object_prefixes_match_complete_key_field_boundaries() {
        let partition_id = PartitionId::from(8);
        let mut virtual_object_key = Vec::new();
        VirtualObjectStats::encode_key(
            partition_id,
            VirtualObjectStatsKey::borrowed(
                "counter",
                None::<&str>,
                "object",
                None::<&str>,
                EntryKind::StateMutation,
                3337,
            ),
            &mut virtual_object_key,
        );
        let mut prefix = Vec::new();
        for fields in 0..=6 {
            prefix.clear();
            let builder = VirtualObjectStatsKey::prefix(partition_id, &mut prefix);
            match fields {
                0 => {}
                1 => {
                    builder.service_name("counter");
                }
                2 => {
                    builder.service_name("counter").scope(None::<&str>);
                }
                3 => {
                    builder
                        .service_name("counter")
                        .scope(None::<&str>)
                        .key("object");
                }
                4 => {
                    builder
                        .service_name("counter")
                        .scope(None::<&str>)
                        .key("object")
                        .handler(None::<&str>);
                }
                5 => {
                    builder
                        .service_name("counter")
                        .scope(None::<&str>)
                        .key("object")
                        .handler(None::<&str>)
                        .kind(EntryKind::StateMutation);
                }
                _ => {
                    builder
                        .service_name("counter")
                        .scope(None::<&str>)
                        .key("object")
                        .handler(None::<&str>)
                        .kind(EntryKind::StateMutation)
                        .partition_key(3337);
                }
            }
            assert!(virtual_object_key.starts_with(&prefix));
        }
        assert_eq!(prefix, virtual_object_key);

        let mut null_scope = Vec::new();
        VirtualObjectStatsKey::prefix(partition_id, &mut null_scope)
            .service_name("counter")
            .scope(None::<&str>);
        let mut empty_scope = Vec::new();
        VirtualObjectStatsKey::prefix(partition_id, &mut empty_scope)
            .service_name("counter")
            .scope(Some(""));
        assert_ne!(null_scope, empty_scope);
    }
}
