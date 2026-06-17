// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::RoughTimestamp;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, ReadInvocationStatusTable,
    WriteInvocationStatusTable,
};
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_types::identifiers::{DeploymentId, InvocationId};
use restate_types::invocation::InvocationMutationResponseSink;
use restate_types::invocation::client::{PatchDeploymentId, ResumeInvocationResponse};
use restate_types::schema::deployment::DeploymentResolver;
use tracing::trace;

use crate::partition::state_machine::lifecycle::ResumeInvocationCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnManualResumeCommand {
    pub invocation_id: InvocationId,
    /// The unresolved deployment patch (VQueue path only), resolved against `ctx.schema` here in
    /// the apply path so the decision uses the invocation status as of this log position. `None`
    /// means the non-VQueue path / a pre-1.7.0 node already resolved it into
    /// `update_pinned_deployment_id`.
    pub update_deployment_id: Option<PatchDeploymentId>,
    /// Already-resolved deployment id from the non-VQueue path (or a pre-1.7.0 node). When present
    /// we honor it directly instead of resolving `update_deployment_id`.
    pub update_pinned_deployment_id: Option<DeploymentId>,
    /// When the rescheduled VQueue entry should run; `None` defaults to its `created_at`.
    pub run_at: Option<RoughTimestamp>,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

/// Resolves and validates the pinned-deployment patch to apply on resume. Shared by the VQueue
/// apply path (here) and the non-VQueue resume RPC handler.
///
/// Returns `Ok(Some(id))` to repin, `Ok(None)` to leave the pinned deployment unchanged, or
/// `Err(response)` with the client-facing error to forward when the patch cannot be applied.
pub(crate) fn resolve_pinned_deployment<R: DeploymentResolver>(
    update_deployment_id: Option<&PatchDeploymentId>,
    already_resolved_deployment_id: Option<DeploymentId>,
    metadata: &InFlightInvocationMetadata,
    schema: Option<&R>,
) -> Result<Option<DeploymentId>, ResumeInvocationResponse> {
    // The non-VQueue path (or a pre-1.7.0 node) carries an already-resolved id; honor it directly.
    if let Some(id) = already_resolved_deployment_id {
        return Ok(Some(id));
    }

    // No unresolved patch to apply (or an explicit `KeepPinned`): leave the pinned deployment as is.
    let Some(update_deployment_id) = update_deployment_id else {
        return Ok(None);
    };
    if update_deployment_id == &PatchDeploymentId::KeepPinned {
        return Ok(None);
    }

    let Some(schema) = schema else {
        // Without a schema we cannot resolve a deployment.
        return Err(ResumeInvocationResponse::DeploymentNotFound);
    };

    match metadata.pinned_deployment.as_ref() {
        // Pinning to latest while nothing is pinned yet is a no-op (matches the old RPC behavior).
        None if matches!(update_deployment_id, PatchDeploymentId::PinToLatest) => Ok(None),
        None => Err(ResumeInvocationResponse::CannotChangeDeploymentId),
        Some(pinned_deployment) => {
            let resolved = match update_deployment_id {
                PatchDeploymentId::PinToLatest => schema.resolve_latest_deployment_for_service(
                    metadata.invocation_target.service_name(),
                ),
                PatchDeploymentId::PinTo { id } => schema.get_deployment(id),
                PatchDeploymentId::KeepPinned => unreachable!("handled above"),
            };

            let Some(deployment) = resolved else {
                return Err(ResumeInvocationResponse::DeploymentNotFound);
            };

            if !deployment
                .supported_protocol_versions
                .contains(&(pinned_deployment.service_protocol_version as i32))
            {
                return Err(ResumeInvocationResponse::IncompatibleDeploymentId {
                    pinned_protocol_version: pinned_deployment.service_protocol_version as i32,
                    deployment_id: deployment.id,
                    supported_protocol_versions: deployment.supported_protocol_versions,
                });
            }
            Ok(Some(deployment.id))
        }
    }
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnManualResumeCommand
where
    S: ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteVQueueTable
        + WriteLockTable
        + ReadVQueueTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnManualResumeCommand {
            invocation_id,
            update_deployment_id,
            update_pinned_deployment_id,
            run_at,
            response_sink,
        } = self;
        match ctx.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Invoked(mut metadata) => {
                // For VQueue invocations the resume RPC proposes this command instead of poking the
                // invoker. A backing-off attempt is parked in the VQueue inbox with a future
                // run_at; reschedule it so the scheduler retries it immediately (the VQueue
                // analogue of InputCommand::RetryNow).
                //
                // A non-VQueue invocation reaches this arm only if it was Suspended/Paused at RPC
                // time (those propose the command) but has since become Invoked -- i.e. it is
                // already resumed, so there is nothing left to do.
                if metadata.vqueue_id.is_some() {
                    // Resolve the (optional) deployment patch against the current status; a serial
                    // pause+resume may land here as Invoked, so we honor the intent rather than
                    // dropping it.
                    let resolved = match resolve_pinned_deployment(
                        update_deployment_id.as_ref(),
                        update_pinned_deployment_id,
                        &metadata,
                        ctx.schema.as_ref(),
                    ) {
                        Ok(resolved) => resolved,
                        Err(response) => {
                            ctx.reply_to_resume_invocation(response_sink, response);
                            return Ok(());
                        }
                    };

                    // Pull a backing-off attempt forward. The returned flag tells us whether the
                    // entry was actually waiting (Inbox/Suspended/Paused); a running attempt is a
                    // no-op.
                    let was_waiting = ctx
                        .vqueue_reschedule_invocation(&invocation_id, run_at, resolved)
                        .await?;

                    if let Some(new_pinned_deployment_id) = resolved {
                        // A repin only makes sense for a waiting (backing-off) entry. If the entry
                        // is running its attempt, repinning would change the deployment mid-flight
                        // -- which the API/legacy path rejects -- so reject it here too instead of
                        // silently repinning.
                        if !was_waiting {
                            ctx.reply_to_resume_invocation(
                                response_sink,
                                ResumeInvocationResponse::CannotChangeDeploymentId,
                            );
                            return Ok(());
                        }

                        // Apply the repin so the next retry uses it, then persist.
                        if let Some(pinned) = &mut metadata.pinned_deployment {
                            trace!(
                                "Updating pinned deployment from {} to {} when resuming",
                                pinned.deployment_id, new_pinned_deployment_id
                            );
                            pinned.deployment_id = new_pinned_deployment_id;
                            ctx.storage.put_invocation_status(
                                &invocation_id,
                                &InvocationStatus::Invoked(metadata),
                            )?;
                        }
                    }
                }
                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::Ok);
            }
            mut is @ InvocationStatus::Suspended { .. } | mut is @ InvocationStatus::Paused(_) => {
                // Resolve + validate the deployment patch here (rather than at RPC time) so the
                // decision uses the status as of this log position.
                let resolved = match resolve_pinned_deployment(
                    update_deployment_id.as_ref(),
                    update_pinned_deployment_id,
                    is.get_invocation_metadata().unwrap(),
                    ctx.schema.as_ref(),
                ) {
                    Ok(resolved) => resolved,
                    Err(response) => {
                        ctx.reply_to_resume_invocation(response_sink, response);
                        return Ok(());
                    }
                };

                if let Some(new_pinned_deployment_id) = resolved
                    && let Some(invocation_pinned_deploment) =
                        &mut is.get_invocation_metadata_mut().unwrap().pinned_deployment
                {
                    trace!(
                        "Updating pinned deployment from {} to {} when resuming",
                        invocation_pinned_deploment.deployment_id, new_pinned_deployment_id
                    );
                    invocation_pinned_deploment.deployment_id = new_pinned_deployment_id;
                }

                // Resume
                ResumeInvocationCommand {
                    invocation_id,
                    invocation_status: &mut is,
                }
                .apply(ctx)
                .await?;

                // Update invocation status
                ctx.storage.put_invocation_status(&invocation_id, &is)?;

                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::Ok);
            }
            InvocationStatus::Scheduled(_) | InvocationStatus::Inboxed(_) => {
                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::NotStarted);
            }
            InvocationStatus::Completed(_) => {
                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::Completed);
            }
            InvocationStatus::Free => {
                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::NotFound);
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::Action;
    use crate::partition::state_machine::tests::fixtures::{
        invoker_entry_effect, invoker_suspended,
    };
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use crate::partition::types::InvokerEffectKind;
    use googletest::prelude::{all, assert_that, contains, eq, pat};
    use restate_storage_api::invocation_status_table::{
        InvocationStatusDiscriminants, ReadInvocationStatusTable,
    };
    use restate_types::deployment::PinnedDeployment;
    use restate_types::identifiers::PartitionProcessorRpcRequestId;
    use restate_types::invocation::{
        IngressInvocationResponseSink, InvocationTarget, ResumeInvocationRequest,
    };
    use restate_types::journal_v2::{NotificationId, SleepCommand};
    use restate_types::schema::deployment::Deployment;
    use restate_types::schema::deployment::test_util::MockDeploymentMetadataRegistry;
    use restate_types::service_protocol::ServiceProtocolVersion;
    use restate_wal_protocol::v2::{Command, commands};
    use restate_worker_api::invoker::Effect;
    use std::time::{Duration, SystemTime};

    fn mock_metadata(
        pinned: Option<ServiceProtocolVersion>,
        invocation_target: InvocationTarget,
    ) -> InFlightInvocationMetadata {
        InFlightInvocationMetadata {
            invocation_target,
            pinned_deployment: pinned
                .map(|version| PinnedDeployment::new(DeploymentId::new(), version)),
            ..InFlightInvocationMetadata::mock()
        }
    }

    #[test]
    fn resolve_no_patch_keep_pinned_and_legacy_field() {
        let meta = mock_metadata(
            Some(ServiceProtocolVersion::V5),
            InvocationTarget::mock_service(),
        );

        // No unresolved patch (VQueue path with nothing to change) is a no-op, even without a schema.
        assert_eq!(
            resolve_pinned_deployment(None, None, &meta, None::<&MockDeploymentMetadataRegistry>),
            Ok(None)
        );

        // An explicit KeepPinned is likewise a no-op.
        assert_eq!(
            resolve_pinned_deployment(
                Some(&PatchDeploymentId::KeepPinned),
                None,
                &meta,
                None::<&MockDeploymentMetadataRegistry>,
            ),
            Ok(None)
        );

        // An already-resolved id (non-VQueue / pre-1.7.0 path) is honored directly without resolving.
        let resolved = DeploymentId::new();
        assert_eq!(
            resolve_pinned_deployment(
                Some(&PatchDeploymentId::PinToLatest),
                Some(resolved),
                &meta,
                None::<&MockDeploymentMetadataRegistry>,
            ),
            Ok(Some(resolved))
        );
    }

    #[test]
    fn resolve_compatible_repins_via_pin_to_and_latest() {
        let invocation_target = InvocationTarget::mock_service();
        let mut dep = Deployment::mock();
        dep.supported_protocol_versions = 1..=4;
        let dep_id = dep.id;

        let mut schemas = MockDeploymentMetadataRegistry::default();
        schemas.mock_deployment(dep.clone());
        schemas.mock_latest_service(invocation_target.service_name(), dep.id);

        let meta = mock_metadata(Some(ServiceProtocolVersion::V4), invocation_target);

        assert_eq!(
            resolve_pinned_deployment(
                Some(&PatchDeploymentId::PinTo { id: dep_id }),
                None,
                &meta,
                Some(&schemas),
            ),
            Ok(Some(dep_id))
        );
        assert_eq!(
            resolve_pinned_deployment(
                Some(&PatchDeploymentId::PinToLatest),
                None,
                &meta,
                Some(&schemas),
            ),
            Ok(Some(dep_id))
        );
    }

    #[test]
    fn resolve_incompatible_and_unknown_and_unpinned() {
        let invocation_target = InvocationTarget::mock_service();
        let mut dep = Deployment::mock();
        dep.supported_protocol_versions = 1..=4; // incompatible with V5
        let dep_id = dep.id;
        let mut schemas = MockDeploymentMetadataRegistry::default();
        schemas.mock_deployment(dep);

        let pinned_version = ServiceProtocolVersion::V5;
        let meta = mock_metadata(Some(pinned_version), invocation_target);

        // Incompatible protocol version.
        assert_eq!(
            resolve_pinned_deployment(
                Some(&PatchDeploymentId::PinTo { id: dep_id }),
                None,
                &meta,
                Some(&schemas),
            ),
            Err(ResumeInvocationResponse::IncompatibleDeploymentId {
                pinned_protocol_version: pinned_version as i32,
                deployment_id: dep_id,
                supported_protocol_versions: 1..=4,
            })
        );

        // Unknown deployment id.
        assert_eq!(
            resolve_pinned_deployment(
                Some(&PatchDeploymentId::PinTo {
                    id: DeploymentId::new()
                }),
                None,
                &meta,
                Some(&schemas),
            ),
            Err(ResumeInvocationResponse::DeploymentNotFound)
        );

        // Nothing pinned: PinToLatest is a no-op, PinTo cannot change.
        let unpinned = mock_metadata(None, InvocationTarget::mock_service());
        assert_eq!(
            resolve_pinned_deployment(
                Some(&PatchDeploymentId::PinToLatest),
                None,
                &unpinned,
                Some(&schemas),
            ),
            Ok(None)
        );
        assert_eq!(
            resolve_pinned_deployment(
                Some(&PatchDeploymentId::PinTo {
                    id: DeploymentId::new()
                }),
                None,
                &unpinned,
                Some(&schemas),
            ),
            Err(ResumeInvocationResponse::CannotChangeDeploymentId)
        );
    }

    #[restate_core::test]
    async fn pause_then_resume() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        test_env
            .modify_invocation_status(invocation_id, |invocation_status| {
                // Mock the invocation to be paused
                *invocation_status = InvocationStatus::Paused(
                    invocation_status
                        .get_invocation_metadata_mut()
                        .unwrap()
                        .clone(),
                )
            })
            .await;

        // Now on manual resume, we should resume the suspended invocation
        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(commands::ResumeInvocationCommand::test_envelope(
                ResumeInvocationRequest {
                    invocation_id,
                    update_deployment_id: None,
                    update_pinned_deployment_id: None,
                    run_at: None,
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink { request_id },
                    )),
                },
            ))
            .await;
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id(invocation_id)),
                contains(pat!(Action::ForwardResumeInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(ResumeInvocationResponse::Ok)
                }))
            )
        );
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked)
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn pause_then_resume_update_pinned_deployment() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        let initial_deployment_id = DeploymentId::new();
        // Pin deployment
        let _ = test_env
            .apply(commands::InvokerEffectCommand::test_envelope(Effect {
                invocation_id,
                kind: InvokerEffectKind::PinnedDeployment(PinnedDeployment {
                    deployment_id: initial_deployment_id,
                    service_protocol_version: ServiceProtocolVersion::V5,
                }),
            }))
            .await;
        // Mock paused
        test_env
            .modify_invocation_status(invocation_id, |invocation_status| {
                // Mock the invocation to be paused
                *invocation_status = InvocationStatus::Paused(
                    invocation_status
                        .get_invocation_metadata_mut()
                        .unwrap()
                        .clone(),
                )
            })
            .await;

        // Now on manual resume, we should resume the suspended invocation
        let request_id = PartitionProcessorRpcRequestId::new();
        let new_deployment_id = DeploymentId::new();
        let actions = test_env
            .apply(commands::ResumeInvocationCommand::test_envelope(
                ResumeInvocationRequest {
                    invocation_id,
                    // Exercises the deprecated read-compat path: an already-resolved id is honored
                    // directly (no schema resolution).
                    update_deployment_id: None,
                    update_pinned_deployment_id: Some(new_deployment_id),
                    run_at: None,
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink { request_id },
                    )),
                },
            ))
            .await;
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id(invocation_id)),
                contains(pat!(Action::ForwardResumeInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(ResumeInvocationResponse::Ok)
                }))
            )
        );
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            all!(
                matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked),
                matchers::storage::pinned_deployment_id_eq(new_deployment_id)
            )
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn sleep_then_suspend_then_manual_resume() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        // Let's suspend the invocation
        let completion_id = 1;
        let _ = test_env
            .apply_multiple([
                invoker_entry_effect(
                    invocation_id,
                    SleepCommand {
                        wake_up_time: (SystemTime::now() + Duration::from_secs(60)).into(),
                        name: Default::default(),
                        completion_id,
                    },
                ),
                invoker_suspended(invocation_id, NotificationId::for_completion(completion_id)),
            ])
            .await;

        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Suspended)
        );

        // Now on manual resume, we should resume the suspended invocation
        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(commands::ResumeInvocationCommand::test_envelope(
                ResumeInvocationRequest {
                    invocation_id,
                    update_deployment_id: None,
                    update_pinned_deployment_id: None,
                    run_at: None,
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink { request_id },
                    )),
                },
            ))
            .await;
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id(invocation_id)),
                contains(pat!(Action::ForwardResumeInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(ResumeInvocationResponse::Ok)
                }))
            )
        );
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked)
        );

        test_env.shutdown().await;
    }
}
