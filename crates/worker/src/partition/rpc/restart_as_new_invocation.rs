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

        // Attach the new invocation as linked to the old one via span context
        invocation_request_header.with_related_span(SpanRelation::Linked(
            completed_invocation
                .journal_metadata
                .span_context
                .span_context()
                .clone(),
        ));

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
