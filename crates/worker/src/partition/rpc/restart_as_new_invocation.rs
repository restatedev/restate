// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use opentelemetry::trace::Span;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::journal_table_v2::ReadOnlyJournalTable;
use restate_types::identifiers::{InvocationId, InvocationUuid, WithPartitionKey};
use restate_types::invocation;
use restate_types::invocation::{
    InvocationRequestHeader, InvocationRetention, ServiceInvocation, ServiceType, SpanRelation,
};
use restate_types::journal_v2::{CommandType, EntryMetadata, EntryType, InputCommand};
use restate_types::net::partition_processor::RestartAsNewInvocationRpcResponse;
use restate_types::service_protocol::ServiceProtocolVersion;

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
}

impl<'a, Proposer: CommandProposer, Storage> RpcHandler<Request>
    for RpcContext<'a, Proposer, Storage>
where
    Proposer: CommandProposer,
    Storage: ReadOnlyInvocationStatusTable + ReadOnlyJournalTable,
{
    type Output = RestartAsNewInvocationRpcResponse;
    type Error = ();

    async fn handle(
        self,
        Request {
            request_id,
            invocation_id,
        }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        // -- Resolve completed invocation status and input command

        // Retrieve the completed invocation
        let completed_invocation = match self.storage.get_invocation_status(&invocation_id).await {
            Ok(InvocationStatus::Completed(completed_invocation)) => completed_invocation,
            Ok(InvocationStatus::Free) => {
                replier.send(RestartAsNewInvocationRpcResponse::NotFound);
                return Ok(());
            }
            Ok(InvocationStatus::Scheduled(_) | InvocationStatus::Inboxed(_)) => {
                replier.send(RestartAsNewInvocationRpcResponse::NotStarted);
                return Ok(());
            }
            Ok(_) => {
                replier.send(RestartAsNewInvocationRpcResponse::StillRunning);
                return Ok(());
            }
            Err(storage_error) => {
                replier.send_result(Err(PartitionProcessorRpcError::Internal(
                    storage_error.to_string(),
                )));
                return Ok(());
            }
        };

        // Check if there's any journal stored
        if completed_invocation.journal_metadata.length == 0 {
            replier.send(RestartAsNewInvocationRpcResponse::MissingInput);
            return Ok(());
        }

        // Check if journal v2 was used
        if completed_invocation
            .pinned_deployment
            .is_none_or(|pd| pd.service_protocol_version < ServiceProtocolVersion::V4)
        {
            replier.send(RestartAsNewInvocationRpcResponse::Unsupported);
            return Ok(());
        }

        // Check that is not a workflow
        if completed_invocation.invocation_target.service_ty() == ServiceType::Workflow {
            replier.send(RestartAsNewInvocationRpcResponse::Unsupported);
            return Ok(());
        }

        // Now retrieve the input command
        let find_result = async {
            for index in 0..completed_invocation.journal_metadata.length {
                // Use `?` to propagate storage errors, mapping them to our error type.
                let Some(entry) = self
                    .storage
                    .get_journal_entry(invocation_id, index)
                    .await
                    .map_err(|e| PartitionProcessorRpcError::Internal(e.to_string()))?
                else {
                    // `Ok(None)` from storage indicates the end of the journal, so we stop.
                    break;
                };

                if entry.ty() == EntryType::Command(CommandType::Input) {
                    // Found the entry. Decode it and immediately return the result from the block.
                    return entry
                        .decode::<ServiceProtocolV4Codec, InputCommand>()
                        .map(Some) // On success, wrap the command in Some to indicate it was found.
                        .map_err(|e| PartitionProcessorRpcError::Internal(e.to_string()));
                }
            }
            // The loop finished without finding the entry.
            Ok(None)
        }
        .await;
        let input_command = match find_result {
            // Success: found and decoded the command.
            Ok(Some(ic)) => ic,
            // Success: searched the whole journal, but found no matching entry.
            Ok(None) => {
                replier.send(RestartAsNewInvocationRpcResponse::MissingInput);
                return Ok(());
            }
            // Failure: a storage or decoding error occurred.
            Err(err) => {
                replier.send_result(Err(err));
                return Ok(());
            }
        };

        // --- We have both the old invocation status, and the input command. We're ready to rock!

        // New invocation id
        let new_invocation_id = InvocationId::from_parts(
            invocation_id.partition_key(),
            InvocationUuid::generate(&completed_invocation.invocation_target, None),
        );
        debug_assert_ne!(new_invocation_id, invocation_id);

        // Generate the tracing span
        let restart_as_new_span = restate_tracing_instrumentation::info_invocation_span!(
            relation = SpanRelation::Linked(
                completed_invocation
                    .journal_metadata
                    .span_context
                    .span_context()
                    .clone(),
            ),
            prefix = "restart-as-new",
            id = new_invocation_id,
            target = completed_invocation.invocation_target,
            tags = (restate.invocation.restart_as_new.original_invocation_id =
                invocation_id.to_string())
        );

        // We copy in invocation_request_header the things we care about
        let mut invocation_request_header = InvocationRequestHeader::initialize(
            new_invocation_id,
            completed_invocation.invocation_target,
        );
        invocation_request_header.headers = input_command.headers;
        invocation_request_header.with_retention(InvocationRetention {
            completion_retention: completed_invocation.completion_retention_duration,
            journal_retention: completed_invocation.journal_retention_duration,
        });
        invocation_request_header
            .with_related_span(SpanRelation::parent(restart_as_new_span.span_context()));

        // Final bundling of the service invocation
        let invocation_request =
            InvocationRequest::new(invocation_request_header, input_command.payload);
        let service_invocation = ServiceInvocation::from_request(
            invocation_request,
            // TODO (slinkydeveloper) in Restate 1.6 replace this with
            // invocation::Source::RestartAsNew(invocation_id)
            invocation::Source::ingress(request_id),
        );

        // Propose and done
        self.proposer
            .self_propose_and_respond_asynchronously(
                service_invocation.partition_key(),
                Command::Invoke(Box::new(service_invocation)),
                replier,
                RestartAsNewInvocationRpcResponse::Ok { new_invocation_id },
            )
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::rpc::MockCommandProposer;
    use assert2::let_assert;
    use futures::{FutureExt, Stream, stream};
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, JournalMetadata,
        PreFlightInvocationMetadata, ScheduledInvocation,
    };
    use restate_test_util::rand;
    use restate_test_util::rand::bytestring;
    use restate_types::identifiers::EntryIndex;
    use restate_types::invocation::{Header, InvocationTarget};
    use restate_types::journal_v2::raw::RawCommand;
    use restate_types::journal_v2::{CompletionId, Entry, NotificationId};
    use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
    use restate_types::time::MillisSinceEpoch;
    use rstest::rstest;
    use std::collections::{HashMap, HashSet};
    use std::future::ready;
    use test_log::test;

    struct MockStorage {
        expected_invocation_id: InvocationId,
        status: InvocationStatus,
        journal_entry: Option<StoredRawEntry>,
    }

    impl ReadOnlyJournalTable for MockStorage {
        fn get_journal_entry(
            &mut self,
            invocation_id: InvocationId,
            index: u32,
        ) -> impl Future<Output = restate_storage_api::Result<Option<StoredRawEntry>>> + Send
        {
            assert_eq!(self.expected_invocation_id, invocation_id);
            if index == 0 {
                ready(Ok(self.journal_entry.clone()))
            } else {
                ready(Ok(None))
            }
        }

        fn get_journal(
            &mut self,
            invocation_id: InvocationId,
            _: EntryIndex,
        ) -> restate_storage_api::Result<
            impl Stream<Item = restate_storage_api::Result<(EntryIndex, StoredRawEntry)>> + Send,
        > {
            assert_eq!(self.expected_invocation_id, invocation_id);
            Ok(stream::empty())
        }

        fn get_notifications_index(
            &mut self,
            invocation_id: InvocationId,
        ) -> impl Future<Output = restate_storage_api::Result<HashMap<NotificationId, EntryIndex>>> + Send
        {
            assert_eq!(self.expected_invocation_id, invocation_id);
            ready(Ok(Default::default()))
        }

        fn get_command_by_completion_id(
            &mut self,
            invocation_id: InvocationId,
            _: CompletionId,
        ) -> impl Future<Output = restate_storage_api::Result<Option<RawCommand>>> + Send {
            assert_eq!(self.expected_invocation_id, invocation_id);
            ready(Ok(None))
        }
    }

    impl ReadOnlyInvocationStatusTable for MockStorage {
        fn get_invocation_status(
            &mut self,
            _: &InvocationId,
        ) -> impl Future<Output = restate_storage_api::Result<InvocationStatus>> + Send {
            ready(Ok(self.status.clone()))
        }
    }

    #[test(restate_core::test)]
    async fn reply_when_appended() {
        let old_invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_virtual_object();
        let headers = vec![Header::new("key", "value")];
        let payload = rand::bytes();

        let mut proposer = MockCommandProposer::new();
        let invocation_target_clone = invocation_target.clone();
        let headers_clone = vec![Header::new("key", "value")];
        let payload_clone = payload.clone();
        proposer
            .expect_self_propose_and_respond_asynchronously::<PartitionProcessorRpcResponse>()
            .return_once_st(move |_, cmd, _, response| {
                let_assert!(Command::Invoke(service_invocation) = cmd);
                assert_that!(
                    response,
                    pat!(PartitionProcessorRpcResponse::RestartAsNewInvocation(pat!(
                        RestartAsNewInvocationRpcResponse::Ok {
                            new_invocation_id: eq(service_invocation.invocation_id)
                        }
                    )))
                );
                assert_that!(
                    service_invocation,
                    points_to(all!(
                        field!(ServiceInvocation.invocation_id, not(eq(old_invocation_id))),
                        field!(ServiceInvocation.argument, eq(payload_clone)),
                        field!(ServiceInvocation.headers, eq(headers_clone)),
                        field!(
                            ServiceInvocation.invocation_target,
                            eq(invocation_target_clone)
                        ),
                        field!(ServiceInvocation.response_sink, none()),
                        field!(ServiceInvocation.submit_notification_sink, none()),
                    ))
                );
                ready(()).boxed()
            });
        proposer
            .expect_handle_rpc_proposal_command::<PartitionProcessorRpcResponse>()
            .never();

        let mut storage = MockStorage {
            expected_invocation_id: old_invocation_id,
            status: InvocationStatus::Completed(CompletedInvocation {
                idempotency_key: Some(bytestring()),
                journal_metadata: JournalMetadata {
                    length: 10,
                    ..JournalMetadata::empty()
                },
                invocation_target: invocation_target.clone(),
                ..CompletedInvocation::mock_neo()
            }),
            journal_entry: Some(StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::now()),
                Entry::from(InputCommand {
                    headers: headers.clone(),
                    payload: payload.clone(),
                    name: Default::default(),
                })
                .encode::<ServiceProtocolV4Codec>(),
            )),
        };

        let (tx, _rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id: old_invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();
    }

    #[test(restate_core::test)]
    async fn reply_not_found_for_unknown_invocation() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockCommandProposer::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<PartitionProcessorRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<PartitionProcessorRpcResponse>()
            .never();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: Default::default(),
            journal_entry: None,
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::NotFound
            )
        );
    }

    #[test(restate_core::test)]
    async fn reply_missing_input() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockCommandProposer::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<PartitionProcessorRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<PartitionProcessorRpcResponse>()
            .never();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: InvocationStatus::Completed(CompletedInvocation {
                journal_metadata: JournalMetadata {
                    length: 0,
                    ..JournalMetadata::empty()
                },
                ..CompletedInvocation::mock_neo()
            }),
            journal_entry: None,
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::MissingInput
            )
        );
    }

    #[test(restate_core::test)]
    async fn reply_unsupported() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockCommandProposer::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<PartitionProcessorRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<PartitionProcessorRpcResponse>()
            .never();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: InvocationStatus::Completed(CompletedInvocation {
                journal_metadata: JournalMetadata {
                    length: 1,
                    ..JournalMetadata::empty()
                },
                invocation_target: InvocationTarget::mock_workflow(),
                ..CompletedInvocation::mock_neo()
            }),
            journal_entry: Some(StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::now()),
                Entry::from(InputCommand {
                    headers: vec![Header::new("key", "value")],
                    payload: rand::bytes(),
                    name: Default::default(),
                })
                .encode::<ServiceProtocolV4Codec>(),
            )),
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::Unsupported
            )
        );
    }

    #[rstest]
    #[restate_core::test]
    async fn reply_still_running(
        #[values(
            InvocationStatus::Suspended {
                metadata: InFlightInvocationMetadata::mock(),
                waiting_for_notifications: HashSet::new(),
            },
            InvocationStatus::Invoked(InFlightInvocationMetadata::mock())
        )]
        status: InvocationStatus,
    ) {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockCommandProposer::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<PartitionProcessorRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<PartitionProcessorRpcResponse>()
            .never();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status,
            journal_entry: None,
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::StillRunning
            )
        );
    }

    #[rstest]
    #[restate_core::test]
    async fn reply_not_started(
        #[values(
            InvocationStatus::Inboxed(InboxedInvocation {
                inbox_sequence_number: 0,
                metadata: PreFlightInvocationMetadata::mock(),
            }),
            InvocationStatus::Scheduled(ScheduledInvocation {
                metadata: PreFlightInvocationMetadata::mock(),
            })
        )]
        status: InvocationStatus,
    ) {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockCommandProposer::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<PartitionProcessorRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<PartitionProcessorRpcResponse>()
            .never();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status,
            journal_entry: None,
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::NotStarted
            )
        );
    }
}
