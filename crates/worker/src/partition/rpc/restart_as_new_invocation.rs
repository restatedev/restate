// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use assert2::let_assert;
use opentelemetry::trace::Span;
use restate_service_protocol::codec::ProtobufRawEntryCodec as OldProtocolEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::{InvocationStatus, ReadInvocationStatusTable};
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2;
use restate_types::identifiers::{EntryIndex, InvocationId, InvocationUuid, WithPartitionKey};
use restate_types::invocation::client::PatchDeploymentId;
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationMutationResponseSink, InvocationRequestHeader,
    InvocationRetention, RestartAsNewInvocationRequest, ServiceInvocation, ServiceType,
    SpanRelation,
};
use restate_types::journal as journal_v1;
use restate_types::journal_v2::{CommandMetadata, EntryMetadata, EntryType};
use restate_types::net::partition_processor::RestartAsNewInvocationRpcResponse;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::{invocation, journal_v2};

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
    pub(super) copy_prefix_up_to_index_included: EntryIndex,
    pub(super) patch_deployment_id: PatchDeploymentId,
}

macro_rules! bail {
    ($replier:expr, $err:expr) => {
        use RestartAsNewInvocationRpcResponse::*;

        $replier.send($err);
        return Ok(());
    };
}

impl<'a, TActuator: Actuator, TSchemas, TStorage> RpcHandler<Request>
    for RpcContext<'a, TActuator, TSchemas, TStorage>
where
    TActuator: Actuator,
    TSchemas: DeploymentResolver,
    TStorage: ReadInvocationStatusTable
        + journal_table_v2::ReadJournalTable
        + journal_table_v1::ReadJournalTable,
{
    type Output = RestartAsNewInvocationRpcResponse;
    type Error = ();

    async fn handle(
        self,
        Request {
            request_id,
            invocation_id,
            copy_prefix_up_to_index_included,
            patch_deployment_id,
        }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        // -- Resolve completed invocation status and input command

        // Retrieve the completed invocation
        let completed_invocation = match self.storage.get_invocation_status(&invocation_id).await {
            Ok(InvocationStatus::Completed(completed_invocation)) => completed_invocation,
            Ok(InvocationStatus::Free) => {
                bail!(replier, NotFound);
            }
            Ok(InvocationStatus::Scheduled(_) | InvocationStatus::Inboxed(_)) => {
                bail!(replier, NotStarted);
            }
            Ok(_) => {
                bail!(replier, StillRunning);
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
            bail!(replier, MissingInput);
        }

        // Check that is not a workflow
        if completed_invocation.invocation_target.service_ty() == ServiceType::Workflow {
            bail!(replier, Unsupported);
        }

        // If the invocation is using the old protocol version or no version is set at all.
        // We have a workaround here that manually creates the ServiceInvocation data structure reading the journal table v1
        let use_old_journal_workaround = match &completed_invocation.pinned_deployment {
            // If the pinned deployment is set, we know for sure which journal version will be used here.
            Some(pd) => pd.service_protocol_version < ServiceProtocolVersion::V4,
            None => {
                // If the pinned deployment is not set, we try to read input entry v2. If absent, then we go with the workaround.
                match journal_table_v2::ReadJournalTable::get_journal_entry(
                    self.storage,
                    invocation_id,
                    0,
                )
                .await
                {
                    Ok(opt_entry) => opt_entry.is_none(),
                    Err(storage_error) => {
                        replier.send_result(Err(PartitionProcessorRpcError::Internal(
                            storage_error.to_string(),
                        )));
                        return Ok(());
                    }
                }
            }
        };

        // New invocation id
        let new_invocation_id = InvocationId::from_parts(
            invocation_id.partition_key(),
            InvocationUuid::generate(&completed_invocation.invocation_target, None),
        );
        debug_assert_ne!(new_invocation_id, invocation_id);

        // TODO this piece of code can be completely removed, or disabled,
        //  when we start writing by default to journal table v2
        //  see https://github.com/restatedev/restate/issues/3184
        if use_old_journal_workaround {
            // Only copying the input entry works with this workaround!
            if copy_prefix_up_to_index_included > 0 {
                bail!(replier, Unsupported);
            }

            // Restarting an invocation that is using the journal v1 works by creating a new
            // invocation w/o preserving any of the existing journal. A new invocation will
            // automatically pick the latest deployment and therefore, we cannot keep the current
            // pinned deployment nor can we specify the new deployment id.
            if matches!(
                patch_deployment_id,
                PatchDeploymentId::KeepPinned | PatchDeploymentId::PinTo { .. }
            ) {
                bail!(replier, CannotPatchDeploymentId);
            }

            // Now retrieve the input command
            let find_result = async {
                let Some(journal_table_v1::JournalEntry::Entry(entry)) =
                    journal_table_v1::ReadJournalTable::get_journal_entry(
                        self.storage,
                        &invocation_id,
                        0,
                    )
                    .await
                    .map_err(|e| PartitionProcessorRpcError::Internal(e.to_string()))?
                else {
                    return Ok(None);
                };

                if entry.ty() != journal_v1::EntryType::Input {
                    return Ok(None);
                }

                let_assert!(
                    journal_v1::Entry::Input(journal_v1::InputEntry { value, headers }) = entry
                        .deserialize_entry_ref::<OldProtocolEntryCodec>()
                        .map_err(|e| PartitionProcessorRpcError::Internal(e.to_string()))?
                );
                Ok(Some((value, headers)))
            }
            .await;
            let (payload, headers) = match find_result {
                // Found and decoded the command.
                Ok(Some(ic)) => ic,
                // No matching entry.
                Ok(None) => {
                    bail!(replier, MissingInput);
                }
                // Failure: a storage or decoding error occurred.
                Err(err) => {
                    replier.send_result(Err(err));
                    return Ok(());
                }
            };

            // --- We have both the old invocation status, and the input command. We're ready to rock!

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
            invocation_request_header.headers = headers;
            invocation_request_header.with_retention(InvocationRetention {
                completion_retention: completed_invocation.completion_retention_duration,
                journal_retention: completed_invocation.journal_retention_duration,
            });
            invocation_request_header
                .with_related_span(SpanRelation::parent(restart_as_new_span.span_context()));

            // Final bundling of the service invocation
            let invocation_request = InvocationRequest::new(invocation_request_header, payload);
            let service_invocation = ServiceInvocation::from_request(
                invocation_request,
                invocation::Source::RestartAsNew(invocation_id),
            );

            // Propose the usual Invoke command
            let cmd = Command::Invoke(Box::new(service_invocation));

            // Propose and done
            self.proposer
                .self_propose_and_respond_asynchronously(
                    invocation_id.partition_key(),
                    cmd,
                    replier,
                    RestartAsNewInvocationRpcResponse::Ok { new_invocation_id },
                )
                .await;

            // All good
            return Ok(());
        }

        // For Restart from prefix, the PP will actually execute the operation,
        // but here we perform few checks anyway.

        let pinned_service_protocol_version = completed_invocation
            .pinned_deployment
            .as_ref()
            .map(|pd| pd.service_protocol_version);

        // Because of the changes to ctx.rand, you can restart from prefix different from 0 only if invocation >= protocol 6
        if copy_prefix_up_to_index_included > 0
            && pinned_service_protocol_version.is_none_or(|sp| sp < ServiceProtocolVersion::V6)
        {
            bail!(replier, Unsupported);
        }

        // Figure out the deployment id, validate the protocol version constraints.
        let new_deployment_id = match patch_deployment_id {
            PatchDeploymentId::KeepPinned => {
                // Not setting a new deployment id will automatically keep the old deployment id
                None
            }
            PatchDeploymentId::PinToLatest | PatchDeploymentId::PinTo { .. } => {
                // Retrieve the deployment
                let Some(deployment) = (match patch_deployment_id {
                    PatchDeploymentId::PinToLatest => {
                        self.schemas.resolve_latest_deployment_for_service(
                            completed_invocation.invocation_target.service_name(),
                        )
                    }
                    PatchDeploymentId::PinTo { id } => self.schemas.get_deployment(&id),
                    PatchDeploymentId::KeepPinned => {
                        unreachable!()
                    }
                }) else {
                    bail!(replier, DeploymentNotFound);
                };

                // Check the protocol constraints are respected.
                if let Some(pinned_service_protocol) = pinned_service_protocol_version
                    && !deployment
                        .supported_protocol_versions
                        .contains(&(pinned_service_protocol as i32))
                {
                    replier.send(
                        RestartAsNewInvocationRpcResponse::IncompatibleDeploymentId {
                            pinned_protocol_version: pinned_service_protocol as i32,
                            deployment_id: deployment.id,
                            supported_protocol_versions: deployment.supported_protocol_versions,
                        },
                    );
                    return Ok(());
                }
                Some(deployment.id)
            }
        };

        // Check that it is safe to copy this prefix of the journal,
        // that is each copied commands must have the related completions in the journal (prefix or suffix).
        //
        // Some examples:
        // * Valid because all commands in prefix have a completion
        // input -> sleep(comp_id 1) -> sleep_comp(1)
        //          ∟ copy_prefix_idx
        // * Invalid because sleep 1 misses completion
        // input -> sleep(comp_id 1) -> sleep(comp_id 2) -> sleep_comp(2)
        //                              ∟ copy_prefix_idx
        // * Valid because sleep 1 has completion and sleep 2 is in the suffix we don't copy over
        // input -> sleep(comp_id 1) -> sleep_comp(1) -> sleep(comp_id 2)
        //                              ∟ copy_prefix_idx
        for i in 1..(copy_prefix_up_to_index_included + 1) {
            match journal_table_v2::ReadJournalTable::get_journal_entry(
                self.storage,
                invocation_id,
                i,
            )
            .await
            {
                Ok(Some(entry)) if matches!(entry.ty(), EntryType::Command(_)) => {
                    // Check we got the notifications available in the journal
                    let cmd = match entry.decode::<ServiceProtocolV4Codec, journal_v2::Command>() {
                        Ok(cmd) => cmd,
                        Err(err) => {
                            replier.send_result(Err(PartitionProcessorRpcError::Internal(
                                err.to_string(),
                            )));
                            return Ok(());
                        }
                    };
                    for completion_id in cmd.related_completion_ids() {
                        if !self
                            .storage
                            .has_completion(invocation_id, completion_id)
                            .await
                            .is_ok_and(|b| b)
                        {
                            bail!(replier, JournalCopyRangeInvalid);
                        }
                    }
                }
                Ok(Some(_)) => {
                    continue;
                }
                Ok(None) => {
                    // Not sure what else to do here...
                    bail!(replier, JournalCopyRangeInvalid);
                }

                Err(err) => {
                    replier.send_result(Err(PartitionProcessorRpcError::Internal(err.to_string())));
                    return Ok(());
                }
            };
        }

        // Pass the ball to the state machine, the PP will reply to the RPC request.
        let cmd = Command::RestartAsNewInvocation(RestartAsNewInvocationRequest {
            invocation_id,
            new_invocation_id,
            copy_prefix_up_to_index_included,
            patch_deployment_id: new_deployment_id,
            response_sink: Some(InvocationMutationResponseSink::Ingress(
                IngressInvocationResponseSink { request_id },
            )),
        });
        self.proposer
            .handle_rpc_proposal_command(invocation_id.partition_key(), cmd, request_id, replier)
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::future::ready;

    use assert2::let_assert;
    use bytes::Bytes;
    use futures::{FutureExt, Stream, StreamExt, stream};
    use googletest::prelude::*;
    use rstest::rstest;
    use test_log::test;

    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, JournalMetadata,
        PreFlightInvocationMetadata, ScheduledInvocation,
    };
    use restate_test_util::rand;
    use restate_test_util::rand::bytestring;
    use restate_types::deployment::PinnedDeployment;
    use restate_types::identifiers::{DeploymentId, EntryIndex};
    use restate_types::invocation::{Header, InvocationTarget};
    use restate_types::journal::raw::RawEntryCodec;
    use restate_types::journal_v2::raw::RawCommand;
    use restate_types::journal_v2::{CompletionId, Entry, NotificationId};
    use restate_types::schema::deployment::Deployment;
    use restate_types::schema::deployment::test_util::MockDeploymentMetadataRegistry;
    use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
    use restate_types::time::MillisSinceEpoch;

    use super::*;

    use crate::partition::rpc::MockActuator;
    use journal_v2::{InputCommand, SleepCommand};
    use restate_storage_api::BudgetedReadError;

    struct MockStorage {
        expected_invocation_id: InvocationId,
        status: InvocationStatus,
        // Journal v2 entries
        entries: Vec<StoredRawEntry>,
        has_completion: bool,
        // For journal v1 workaround
        v1_input_payload: Option<Bytes>,
        v1_input_headers: Option<Vec<Header>>,
    }

    impl MockStorage {
        fn new_with_input_v1(
            expected_invocation_id: InvocationId,
            status: InvocationStatus,
            payload: Bytes,
            headers: Vec<Header>,
        ) -> Self {
            Self {
                expected_invocation_id,
                status,
                entries: vec![],
                has_completion: false,
                v1_input_payload: Some(payload),
                v1_input_headers: Some(headers),
            }
        }

        fn new_without_journal(
            expected_invocation_id: InvocationId,
            status: InvocationStatus,
        ) -> Self {
            Self {
                expected_invocation_id,
                status,
                entries: vec![],
                has_completion: false,
                v1_input_payload: None,
                v1_input_headers: None,
            }
        }

        fn new_with_journal_v2(
            expected_invocation_id: InvocationId,
            status: InvocationStatus,
            entries: Vec<Entry>,
            has_completion: bool,
        ) -> Self {
            Self {
                expected_invocation_id,
                status,
                entries: entries
                    .into_iter()
                    .map(|e| {
                        StoredRawEntry::new(
                            StoredRawEntryHeader::new(MillisSinceEpoch::now()),
                            e.encode::<ServiceProtocolV4Codec>(),
                        )
                    })
                    .collect(),
                has_completion,
                v1_input_payload: None,
                v1_input_headers: None,
            }
        }
    }

    // Implement journal v2 table for tests
    impl journal_table_v2::ReadJournalTable for MockStorage {
        fn get_journal_entry(
            &mut self,
            invocation_id: InvocationId,
            index: u32,
        ) -> impl Future<Output = restate_storage_api::Result<Option<StoredRawEntry>>> + Send
        {
            assert_eq!(self.expected_invocation_id, invocation_id);
            let res = self.entries.get(index as usize).cloned();
            ready(Ok(res))
        }

        fn get_journal(
            &self,
            invocation_id: InvocationId,
            length: EntryIndex,
        ) -> restate_storage_api::Result<
            impl Stream<Item = restate_storage_api::Result<(EntryIndex, StoredRawEntry)>> + Send,
        > {
            assert_eq!(self.expected_invocation_id, invocation_id);
            Ok(stream::iter(
                self.entries
                    .clone()
                    .into_iter()
                    .enumerate()
                    .map(|(idx, e)| Ok((idx as EntryIndex, e)))
                    .take(length as usize),
            ))
        }

        fn get_notifications_index(
            &mut self,
            _: InvocationId,
        ) -> impl Future<Output = restate_storage_api::Result<HashMap<NotificationId, EntryIndex>>> + Send
        {
            panic!("This should be unused");
            #[allow(unreachable_code)]
            std::future::ready(Ok(HashMap::new()))
        }

        fn get_command_by_completion_id(
            &mut self,
            _: InvocationId,
            _: CompletionId,
        ) -> impl Future<
            Output = restate_storage_api::Result<Option<(StoredRawEntryHeader, RawCommand)>>,
        > + Send {
            panic!("This should be unused");
            #[allow(unreachable_code)]
            std::future::ready(Ok(None))
        }

        fn has_completion(
            &mut self,
            invocation_id: InvocationId,
            _: CompletionId,
        ) -> impl Future<Output = restate_storage_api::Result<bool>> + Send {
            assert_eq!(self.expected_invocation_id, invocation_id);
            ready(Ok(self.has_completion))
        }

        fn get_journal_budgeted<'a>(
            &'a self,
            invocation_id: InvocationId,
            entry_index: EntryIndex,
            budget: &'a mut restate_memory::LocalMemoryPool,
        ) -> restate_storage_api::Result<
            impl Stream<
                Item = std::result::Result<
                    (EntryIndex, StoredRawEntry, restate_memory::LocalMemoryLease),
                    restate_storage_api::BudgetedReadError,
                >,
            > + Send
            + 'a,
        > {
            journal_table_v2::ReadJournalTable::get_journal(self, invocation_id, entry_index).map(
                |stream| {
                    stream.map(|result| {
                        result
                            .map(|(index, entry)| (index, entry, budget.empty_lease()))
                            .map_err(BudgetedReadError::from)
                    })
                },
            )
        }
    }

    // Implement journal v1 table as workaround source for tests
    impl journal_table_v1::ReadJournalTable for MockStorage {
        fn get_journal_entry(
            &mut self,
            invocation_id: &InvocationId,
            journal_index: u32,
        ) -> impl Future<
            Output = restate_storage_api::Result<Option<journal_table_v1::JournalEntry>>,
        > + Send {
            assert_eq!(&self.expected_invocation_id, invocation_id);
            if journal_index == 0
                && let (Some(payload), Some(headers)) =
                    (self.v1_input_payload.clone(), self.v1_input_headers.clone())
            {
                let enr = OldProtocolEntryCodec::serialize_as_input_entry(headers, payload);
                return ready(Ok(Some(journal_table_v1::JournalEntry::Entry(enr))));
            }
            ready(Ok(None))
        }

        fn get_journal<'a>(
            &'a self,
            invocation_id: &InvocationId,
            journal_length: EntryIndex,
        ) -> restate_storage_api::Result<
            impl Stream<
                Item = restate_storage_api::Result<(EntryIndex, journal_table_v1::JournalEntry)>,
            > + Send
            + 'a,
        > {
            assert_eq!(&self.expected_invocation_id, invocation_id);
            let items: Vec<(EntryIndex, journal_table_v1::JournalEntry)> = if journal_length > 0 {
                if let (Some(payload), Some(headers)) =
                    (self.v1_input_payload.clone(), self.v1_input_headers.clone())
                {
                    let enr = OldProtocolEntryCodec::serialize_as_input_entry(headers, payload);
                    vec![(0, journal_table_v1::JournalEntry::Entry(enr))]
                } else {
                    vec![]
                }
            } else {
                vec![]
            };
            Ok(stream::iter(items.into_iter().map(Ok)))
        }

        fn get_journal_budgeted<'a>(
            &'a self,
            invocation_id: &InvocationId,
            entry_index: EntryIndex,
            budget: &'a mut restate_memory::LocalMemoryPool,
        ) -> restate_storage_api::Result<
            impl Stream<
                Item = std::result::Result<
                    (
                        EntryIndex,
                        journal_table_v1::JournalEntry,
                        restate_memory::LocalMemoryLease,
                    ),
                    restate_storage_api::BudgetedReadError,
                >,
            > + Send
            + 'a,
        > {
            journal_table_v1::ReadJournalTable::get_journal(self, invocation_id, entry_index).map(
                |stream| {
                    stream.map(|result| {
                        result
                            .map(|(index, entry)| (index, entry, budget.empty_lease()))
                            .map_err(BudgetedReadError::from)
                    })
                },
            )
        }
    }

    impl ReadInvocationStatusTable for MockStorage {
        fn get_invocation_status(
            &mut self,
            _: &InvocationId,
        ) -> impl Future<Output = restate_storage_api::Result<InvocationStatus>> + Send {
            ready(Ok(self.status.clone()))
        }
    }

    #[test(restate_core::test)]
    async fn copy_prefix_zero_with_no_version_uses_service_invocation_command() {
        let old_invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_virtual_object();
        let headers = vec![Header::new("key", "value")];
        let payload = rand::bytes();

        let mut proposer = MockActuator::new();
        let invocation_target_clone = invocation_target.clone();
        let headers_clone = vec![Header::new("key", "value")];
        let payload_clone = payload.clone();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .return_once_st(move |_, cmd, _, response| {
                let_assert!(Command::Invoke(service_invocation) = cmd);
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
                assert_that!(
                    response,
                    pat!(RestartAsNewInvocationRpcResponse::Ok {
                        new_invocation_id: eq(service_invocation.invocation_id)
                    })
                );
                ready(()).boxed()
            });
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let mut storage = MockStorage::new_with_input_v1(
            old_invocation_id,
            InvocationStatus::Completed(CompletedInvocation {
                idempotency_key: Some(bytestring()),
                journal_metadata: JournalMetadata {
                    length: 10,
                    ..JournalMetadata::empty()
                },
                invocation_target: invocation_target.clone(),
                pinned_deployment: None,
                ..CompletedInvocation::mock_neo()
            }),
            payload.clone(),
            headers.clone(),
        );

        let (tx, _rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id: old_invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: Default::default(),
            },
            Replier::new(tx),
        )
        .await
        .unwrap();
    }

    #[test(restate_core::test)]
    async fn old_workaround_nonzero_prefix_is_unsupported() {
        let invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_virtual_object();

        let mut proposer = MockActuator::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        // Completed with no pinned deployment triggers v1 workaround
        let status = InvocationStatus::Completed(CompletedInvocation {
            journal_metadata: JournalMetadata {
                length: 2,
                ..JournalMetadata::empty()
            },
            invocation_target: invocation_target.clone(),
            pinned_deployment: None,
            ..CompletedInvocation::mock_neo()
        });
        let mut storage = MockStorage::new_with_input_v1(
            invocation_id,
            status,
            rand::bytes(),
            vec![Header::new("k", "v")],
        );

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 1,
                patch_deployment_id: Default::default(),
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

    #[test(restate_core::test)]
    async fn old_workaround_keep_pinned_is_rejected() {
        let invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_virtual_object();
        let payload = rand::bytes();
        let headers = vec![Header::new("k", "v")];

        let mut proposer = MockActuator::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let status = InvocationStatus::Completed(CompletedInvocation {
            journal_metadata: JournalMetadata {
                length: 1,
                ..JournalMetadata::empty()
            },
            invocation_target: invocation_target.clone(),
            pinned_deployment: None,
            ..CompletedInvocation::mock_neo()
        });
        let mut storage = MockStorage::new_with_input_v1(invocation_id, status, payload, headers);

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: PatchDeploymentId::KeepPinned,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::CannotPatchDeploymentId
            )
        );
    }

    #[test(restate_core::test)]
    async fn old_workaround_pin_to_is_rejected() {
        let invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_virtual_object();

        let mut proposer = MockActuator::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let status = InvocationStatus::Completed(CompletedInvocation {
            journal_metadata: JournalMetadata {
                length: 1,
                ..JournalMetadata::empty()
            },
            invocation_target: invocation_target.clone(),
            pinned_deployment: None,
            ..CompletedInvocation::mock_neo()
        });
        let mut storage = MockStorage::new_with_input_v1(
            invocation_id,
            status,
            rand::bytes(),
            vec![Header::new("k", "v")],
        );

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: PatchDeploymentId::PinTo {
                    id: DeploymentId::new(),
                },
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::CannotPatchDeploymentId
            )
        );
    }

    #[test(restate_core::test)]
    async fn old_workaround_missing_v1_input_is_reported() {
        let invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_virtual_object();

        let mut proposer = MockActuator::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        // Completed with journal length>0 but no v1 input present in storage
        let status = InvocationStatus::Completed(CompletedInvocation {
            journal_metadata: JournalMetadata {
                length: 3,
                ..JournalMetadata::empty()
            },
            invocation_target: invocation_target.clone(),
            pinned_deployment: None,
            ..CompletedInvocation::mock_neo()
        });
        let mut storage = MockStorage::new_without_journal(invocation_id, status);

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: PatchDeploymentId::PinToLatest,
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
    async fn copy_prefix_zero_with_pinned_new_version_uses_restart_as_new_command() {
        let invocation_id = InvocationId::mock_random();
        let target = InvocationTarget::mock_virtual_object();
        let completed = mock_completed_invocation(target.clone(), ServiceProtocolVersion::V5, 1);

        let mut storage = MockStorage::new_with_journal_v2(
            invocation_id,
            InvocationStatus::Completed(completed.clone()),
            vec![input_command()],
            true,
        );

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .return_once_st(move |_, cmd, _, _| {
                assert_that!(
                    cmd,
                    pat!(Command::RestartAsNewInvocation(pat!(
                        RestartAsNewInvocationRequest {
                            copy_prefix_up_to_index_included: eq(0),
                            patch_deployment_id: none()
                        }
                    )))
                );
                ready(()).boxed()
            });
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(
                &mut proposer,
                &MockDeploymentMetadataRegistry::default(),
                &mut storage,
            ),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: PatchDeploymentId::KeepPinned,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        rx.assert_not_received();
    }

    #[test(restate_core::test)]
    async fn copy_prefix_zero_without_pinned_uses_restart_as_new_command() {
        let invocation_id = InvocationId::mock_random();
        let target = InvocationTarget::mock_virtual_object();
        let completed = CompletedInvocation {
            invocation_target: target,
            journal_metadata: JournalMetadata {
                length: 1,
                ..JournalMetadata::empty()
            },
            pinned_deployment: None,
            ..CompletedInvocation::mock_neo()
        };

        let mut storage = MockStorage::new_with_journal_v2(
            invocation_id,
            InvocationStatus::Completed(completed.clone()),
            vec![input_command()],
            true,
        );

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .return_once_st(move |_, cmd, _, _| {
                assert_that!(
                    cmd,
                    pat!(Command::RestartAsNewInvocation(pat!(
                        RestartAsNewInvocationRequest {
                            copy_prefix_up_to_index_included: eq(0),
                            patch_deployment_id: none()
                        }
                    )))
                );
                ready(()).boxed()
            });
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(
                &mut proposer,
                &MockDeploymentMetadataRegistry::default(),
                &mut storage,
            ),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: PatchDeploymentId::KeepPinned,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        rx.assert_not_received();
    }

    #[test(restate_core::test)]
    async fn reply_not_found_for_unknown_invocation() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let mut storage = MockStorage::new_without_journal(invocation_id, Default::default());

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: Default::default(),
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
    async fn reply_unsupported() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let mut storage = MockStorage::new_with_input_v1(
            invocation_id,
            InvocationStatus::Completed(CompletedInvocation {
                journal_metadata: JournalMetadata {
                    length: 1,
                    ..JournalMetadata::empty()
                },
                invocation_target: InvocationTarget::mock_workflow(),
                ..CompletedInvocation::mock_neo()
            }),
            rand::bytes(),
            vec![Header::new("key", "value")],
        );

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: Default::default(),
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

        let mut proposer = MockActuator::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let mut storage = MockStorage::new_without_journal(invocation_id, status);

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: Default::default(),
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

        let mut proposer = MockActuator::new();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let mut storage = MockStorage::new_without_journal(invocation_id, status);

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 0,
                patch_deployment_id: Default::default(),
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

    fn mock_completed_invocation(
        target: InvocationTarget,
        protocol: ServiceProtocolVersion,
        journal_length: EntryIndex,
    ) -> CompletedInvocation {
        CompletedInvocation {
            invocation_target: target,
            journal_metadata: JournalMetadata {
                length: journal_length,
                ..JournalMetadata::empty()
            },
            pinned_deployment: Some(PinnedDeployment {
                deployment_id: DeploymentId::default(),
                service_protocol_version: protocol,
            }),
            ..CompletedInvocation::mock_neo()
        }
    }

    fn input_command() -> Entry {
        InputCommand {
            headers: vec![],
            payload: Default::default(),
            name: Default::default(),
        }
        .into()
    }

    fn sleep_command(completion_id: CompletionId) -> Entry {
        SleepCommand {
            wake_up_time: MillisSinceEpoch::now(),
            completion_id,
            name: Default::default(),
        }
        .into()
    }

    #[test(restate_core::test)]
    async fn with_valid_prefix_keep_pinned_deployment() {
        let invocation_id = InvocationId::mock_random();
        let target = InvocationTarget::mock_virtual_object();
        let completed = mock_completed_invocation(target.clone(), ServiceProtocolVersion::V6, 2);

        // Create a command at index 1 with a completion id; storage will report it as present
        let mut storage = MockStorage::new_with_journal_v2(
            invocation_id,
            InvocationStatus::Completed(completed.clone()),
            vec![input_command(), sleep_command(1)],
            /* has completion */ true,
        );

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .return_once_st(move |_, cmd, _, _| {
                assert_that!(
                    cmd,
                    pat!(Command::RestartAsNewInvocation(pat!(
                        RestartAsNewInvocationRequest {
                            copy_prefix_up_to_index_included: eq(1),
                            patch_deployment_id: none()
                        }
                    )))
                );
                ready(()).boxed()
            });
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(
                &mut proposer,
                &MockDeploymentMetadataRegistry::default(),
                &mut storage,
            ),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 1,
                patch_deployment_id: PatchDeploymentId::KeepPinned,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        rx.assert_not_received();
    }

    #[test(restate_core::test)]
    async fn with_valid_prefix_pin_to_latest() {
        let invocation_id = InvocationId::mock_random();
        let target = InvocationTarget::mock_virtual_object();
        let completed = mock_completed_invocation(target.clone(), ServiceProtocolVersion::V6, 2);
        let pinned = completed.pinned_deployment.clone().unwrap();

        // Deployment registry with a compatible latest deployment
        let mut registry = MockDeploymentMetadataRegistry::default();
        let mut deployment = Deployment::mock();
        deployment.supported_protocol_versions =
            (pinned.service_protocol_version as i32)..=(pinned.service_protocol_version as i32 + 2);
        let latest_id = deployment.id;
        registry.mock_deployment(deployment.clone());
        registry.mock_latest_service(target.service_name(), latest_id);

        let mut storage = MockStorage::new_with_journal_v2(
            invocation_id,
            InvocationStatus::Completed(completed.clone()),
            vec![input_command(), sleep_command(1)],
            /* has completion */ true,
        );

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .return_once_st(move |_, cmd, _, _| {
                assert_that!(
                    cmd,
                    pat!(Command::RestartAsNewInvocation(pat!(
                        RestartAsNewInvocationRequest {
                            copy_prefix_up_to_index_included: eq(1),
                            patch_deployment_id: some(eq(latest_id))
                        }
                    )))
                );
                ready(()).boxed()
            });
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &registry, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 1,
                patch_deployment_id: PatchDeploymentId::PinToLatest,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        rx.assert_not_received();
    }

    #[test(restate_core::test)]
    async fn with_invalid_resume_deployment() {
        let invocation_id = InvocationId::mock_random();
        let target = InvocationTarget::mock_virtual_object();
        let completed = mock_completed_invocation(target.clone(), ServiceProtocolVersion::V6, 2);
        let pinned = completed.pinned_deployment.clone().unwrap();

        // Registry returns a deployment that does NOT support pinned protocol
        let mut registry = MockDeploymentMetadataRegistry::default();
        let mut deployment = Deployment::mock();
        deployment.supported_protocol_versions = 1..=2; // incompatible
        let id = deployment.id;
        registry.mock_deployment(deployment);

        let mut storage = MockStorage::new_with_journal_v2(
            invocation_id,
            InvocationStatus::Completed(completed.clone()),
            vec![input_command(), sleep_command(1)],
            true,
        );

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();
        proposer
            .expect_self_propose_and_respond_asynchronously::<RestartAsNewInvocationRpcResponse>()
            .never();

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &registry, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 1,
                patch_deployment_id: PatchDeploymentId::PinTo { id },
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::IncompatibleDeploymentId {
                    pinned_protocol_version: pinned.service_protocol_version as i32,
                    deployment_id: id,
                    supported_protocol_versions: 1..=2,
                }
            )
        );
    }

    #[test(restate_core::test)]
    async fn pin_to_unknown_deployment() {
        let invocation_id = InvocationId::mock_random();
        let target = InvocationTarget::mock_virtual_object();
        let completed = mock_completed_invocation(target.clone(), ServiceProtocolVersion::V6, 2);

        let registry = MockDeploymentMetadataRegistry::default();
        let some_id = DeploymentId::default();

        let mut storage = MockStorage::new_with_journal_v2(
            invocation_id,
            InvocationStatus::Completed(completed.clone()),
            vec![input_command(), sleep_command(1)],
            true,
        );

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &registry, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 1,
                patch_deployment_id: PatchDeploymentId::PinTo { id: some_id },
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::DeploymentNotFound
            )
        );
    }

    #[test(restate_core::test)]
    async fn prefix_journal_copy_range_invalid_missing_completion() {
        let invocation_id = InvocationId::mock_random();
        let target = InvocationTarget::mock_virtual_object();
        let completed = mock_completed_invocation(target.clone(), ServiceProtocolVersion::V6, 2);

        let mut storage = MockStorage::new_with_journal_v2(
            invocation_id,
            InvocationStatus::Completed(completed.clone()),
            vec![input_command(), sleep_command(1)],
            /* has completion */ false,
        );

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(
                &mut proposer,
                &MockDeploymentMetadataRegistry::default(),
                &mut storage,
            ),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 1,
                patch_deployment_id: PatchDeploymentId::KeepPinned,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::RestartAsNewInvocation(
                RestartAsNewInvocationRpcResponse::JournalCopyRangeInvalid
            )
        );
    }

    #[test(restate_core::test)]
    async fn prefix_unsupported_when_protocol_less_than_6() {
        let invocation_id = InvocationId::mock_random();
        let target = InvocationTarget::mock_virtual_object();
        let completed = mock_completed_invocation(target.clone(), ServiceProtocolVersion::V4, 2);

        let mut storage =
            MockStorage::new_without_journal(invocation_id, InvocationStatus::Completed(completed));

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<RestartAsNewInvocationRpcResponse>()
            .never();

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(
                &mut proposer,
                &MockDeploymentMetadataRegistry::default(),
                &mut storage,
            ),
            Request {
                request_id: Default::default(),
                invocation_id,
                copy_prefix_up_to_index_included: 1,
                patch_deployment_id: PatchDeploymentId::KeepPinned,
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
}
