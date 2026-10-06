// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::stats::service_load::ServiceLoad;
use restate_types::ServiceName;
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

use crate::stats::macros::define_aggregated_stat;

use super::StageStatusGauge;

#[cfg(test)]
mod filter_tests;

// A gauge for the number of vqueue entries in a given stage/status and a service name.
define_aggregated_stat!(
    table: ServiceLoad,
    value: StageStatusGauge,
    key: ServiceLoadKey(
        service_name: ServiceName => str,
        handler: Option<ReString> => str,
        kind: EntryKind,
    ),
);

#[cfg(test)]
mod tests {
    use restate_types::sharding::PartitionId;
    use restate_util_string::ReString;

    use crate::keys::KeyDecoder;
    use crate::stats::Stat;

    use super::*;

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
            ServiceLoad::encode_key(
                PartitionId::from(8),
                ServiceLoadKey::borrowed(service_name, handler, EntryKind::Invocation),
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
        ServiceLoad::encode_key(
            PartitionId::from(8),
            ServiceLoadKey::borrowed(&service_name, Some("handler"), EntryKind::Invocation),
            &mut borrowed_service_name,
        );
        let mut borrowed_str = Vec::new();
        ServiceLoad::encode_key(
            PartitionId::from(8),
            ServiceLoadKey::borrowed("alpha", Some("handler"), EntryKind::Invocation),
            &mut borrowed_str,
        );
        let string = String::from("alpha");
        let handler_string = String::from("handler");
        let mut borrowed_string = Vec::new();
        ServiceLoad::encode_key(
            PartitionId::from(8),
            ServiceLoadKey::borrowed(&string, Some(&handler_string), EntryKind::Invocation),
            &mut borrowed_string,
        );
        let re_string = ReString::from("alpha");
        let handler_re_string = ReString::from("handler");
        let mut borrowed_re_string = Vec::new();
        ServiceLoad::encode_key(
            PartitionId::from(8),
            ServiceLoadKey::borrowed(&re_string, Some(&handler_re_string), EntryKind::Invocation),
            &mut borrowed_re_string,
        );
        let mut owned = Vec::new();
        ServiceLoad::encode_key(
            PartitionId::from(8),
            ServiceLoadKey {
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

        let (_, remaining) = KeyDecoder::new_stat(&owned).decode_prefix().unwrap();
        let decoder = remaining.into_decoder::<ServiceLoad, ServiceLoadKey>();
        let (service_name, decoder) = decoder.take_service_name().unwrap();
        assert_eq!(service_name.encoded(), crate::encoded_mem_cmp_str!("alpha"));
        assert_eq!(service_name.decode().unwrap().as_str(), "alpha");
        let (handler, decoder) = decoder.take_handler().unwrap();
        assert_eq!(handler.as_bytes()[0], 1);
        assert_eq!(
            &handler.as_bytes()[1..],
            crate::encoded_mem_cmp_str!("handler").as_bytes()
        );
        assert_eq!(handler.decode().unwrap().unwrap(), "handler");
        let kind = decoder.take_kind().unwrap();
        assert_eq!(
            kind.as_bytes(),
            EntryKind::Invocation.as_mem_cmp_str().as_bytes()
        );
        assert_eq!(kind.decode().unwrap(), EntryKind::Invocation);

        let mut borrowed_none = Vec::new();
        ServiceLoad::encode_key(
            PartitionId::from(8),
            ServiceLoadKey::borrowed("alpha", None::<&str>, EntryKind::Invocation),
            &mut borrowed_none,
        );
        let mut owned_none = Vec::new();
        ServiceLoad::encode_key(
            PartitionId::from(8),
            ServiceLoadKey {
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
        let partition_id = PartitionId::from(8);
        let mut service_key = Vec::new();
        ServiceLoad::encode_key(
            partition_id,
            ServiceLoadKey::borrowed("alpha", Some("handler"), EntryKind::Invocation),
            &mut service_key,
        );
        let mut prefix = Vec::new();
        for fields in 0..=3 {
            prefix.clear();
            let builder = ServiceLoadKey::prefix(partition_id, &mut prefix);
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
