// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Retry policy extraction utilities for the invoker implementation.

use restate_ingress_http::X_RESTATE_RETRY_POLICY;
use restate_invoker_api::InvokeInputJournal;
use restate_serde_util::patch_toml;
use restate_types::journal::{Entry as JournalEntry, InputEntry};
use restate_types::journal_v2::EntryMetadata;
use restate_types::retries::RetryPolicy;

pub(crate) fn extract_retry_policy_from_journal(
    journal: &InvokeInputJournal,
    default_retry_policy: &RetryPolicy,
) -> RetryPolicy {
    let journal_entries = match journal {
        InvokeInputJournal::CachedJournal(_, journal_entries) => journal_entries,
        _ => return default_retry_policy.clone(),
    };

    journal_entries
        .iter()
        .filter_map(|journal_entry| {
            match journal_entry {
                restate_invoker_api::invocation_reader::JournalEntry::JournalV1(plain_entry) => {
                    plain_entry.clone()
                        .deserialize_entry::<restate_service_protocol::codec::ProtobufRawEntryCodec>()
                        .ok()
                        .and_then(|entry| match entry {
                            JournalEntry::Input(input_entry) => Some(input_entry),
                            _ => None,
                        })
                }
                restate_invoker_api::invocation_reader::JournalEntry::JournalV2(stored_entry) => {
                    // Only process Input commands
                    if stored_entry.ty() != restate_types::journal_v2::EntryType::Command(restate_types::journal_v2::CommandType::Input) {
                        return None;
                    }

                    stored_entry.decode::<restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec, restate_types::journal_v2::command::InputCommand>()
                        .ok()
                        .map(|input_command| InputEntry {
                            headers: input_command.headers,
                            value: input_command.payload,
                        })
                }
            }
        })
        .find_map(|input_entry| {
            input_entry.headers.iter()
                .find(|header| header.name == X_RESTATE_RETRY_POLICY.as_str() && !header.value.is_empty())
                .map(|header| header.value.clone())
        })
        .map(|retry_policy_str| {
            retry_policy_str
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|line| {
                    line
                    .split_once('=')
                    .map_or_else(
                        || line.to_string(),
                        |(k, v)| {
                            let k = k.trim();
                            let v = v.trim();
                            match format!("{} = {}", k, v).parse::<toml::Table>() {
                                Ok(_) => format!("{} = {}", k, v),  // Valid TOML, use as-is
                                Err(_) => format!("{} = \"{}\"", k, v),  // Invalid, quote it
                            }
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        })
        .and_then(|retry_policy_str| {
            patch_toml(default_retry_policy, &retry_policy_str).ok()
        })
        .unwrap_or_else(|| default_retry_policy.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_extract_retry_policy_from_journal_no_cached_journal() {
        let default_policy = RetryPolicy::default();
        let journal = InvokeInputJournal::NoCachedJournal;

        let result = extract_retry_policy_from_journal(&journal, &default_policy);

        assert_eq!(result, default_policy);
    }

    #[test]
    fn test_function_exists_and_compiles() {
        // This test just ensures the function exists and compiles correctly
        let default_policy =
            RetryPolicy::exponential(Duration::from_millis(100), 2.0, Some(3), None);
        let journal = InvokeInputJournal::NoCachedJournal;
        let result = extract_retry_policy_from_journal(&journal, &default_policy);

        // For NoCachedJournal, should return the default policy
        assert_eq!(result, default_policy);
    }

    #[test]
    fn test_extract_retry_policy_from_journal_with_valid_header() {
        use bytes::Bytes;
        use restate_invoker_api::invocation_reader::JournalEntry;
        use restate_service_protocol::codec::ProtobufRawEntryCodec;
        use restate_types::invocation::Header;
        use restate_types::journal::raw::RawEntryCodec;

        // Create a default exponential retry policy
        let default_policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            Some(5),
            Some(Duration::from_secs(10)),
        );

        // Create an input entry with the x-restate-retry-policy header
        let input_entry = ProtobufRawEntryCodec::serialize_as_input_entry(
            vec![Header::new(
                X_RESTATE_RETRY_POLICY.as_str(),
                "initial-interval=250ms;max-interval=\"3s\"",
            )],
            Bytes::from("test payload"),
        );

        // Create journal metadata (simplified constructor)
        let journal_metadata = restate_invoker_api::JournalMetadata::new(
            1,
            restate_types::invocation::ServiceInvocationSpanContext::empty(),
            None,
            0, // InvocationEpoch is u32
            restate_types::time::MillisSinceEpoch::new(0),
        );

        // Create the journal with the input entry
        let journal = InvokeInputJournal::CachedJournal(
            journal_metadata,
            vec![JournalEntry::JournalV1(input_entry.erase_enrichment())],
        );

        let result = extract_retry_policy_from_journal(&journal, &default_policy);

        // Verify that the initial-interval was overridden while other parameters remain from default
        match result {
            RetryPolicy::Exponential {
                initial_interval,
                factor,
                max_attempts,
                max_interval,
            } => {
                // The header should override the initial-interval to 250ms
                assert_eq!(
                    initial_interval,
                    humantime::Duration::from(Duration::from_millis(250))
                );
                // Other parameters should remain from the default policy
                assert_eq!(factor, 2.0);
                assert_eq!(max_attempts, Some(std::num::NonZeroUsize::new(5).unwrap()));
                assert_eq!(
                    max_interval,
                    Some(humantime::Duration::from(Duration::from_secs(3)))
                );
            }
            _ => panic!("Expected Exponential policy, got {:?}", result),
        }
    }
}
