// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::CodedError;
use restate_ingress_dispatcher::{
    IngressDispatcherOutput, Service as IngressDispatcherService,
    ServiceError as IngressDispatcherServiceError,
};
use restate_ingress_grpc::HyperServerIngress;
use restate_schema_impl::Schemas;
use tokio::select;
use tokio::sync::mpsc;

type ExternalClientIngress = HyperServerIngress<Schemas>;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum IngressIntegrationError {
    #[error(transparent)]
    #[code(unknown)]
    Dispatcher(#[from] IngressDispatcherServiceError),
    #[error(transparent)]
    Ingress(
        #[from]
        #[code]
        restate_ingress_grpc::IngressServerError,
    ),
}

pub(super) struct ExternalClientIngressRunner {
    dispatcher_service: IngressDispatcherService,
    external_client_ingress: ExternalClientIngress,
    sender: mpsc::Sender<IngressDispatcherOutput>,
}

impl ExternalClientIngressRunner {
    pub(super) fn new(
        dispatcher_service: IngressDispatcherService,
        external_client_ingress: ExternalClientIngress,
        sender: mpsc::Sender<IngressDispatcherOutput>,
    ) -> Self {
        Self {
            dispatcher_service,
            external_client_ingress,
            sender,
        }
    }

    pub(super) async fn run(
        self,
        shutdown_watch: drain::Watch,
    ) -> Result<(), IngressIntegrationError> {
        let ExternalClientIngressRunner {
            dispatcher_service,
            external_client_ingress,
            sender,
        } = self;

        select! {
            result = dispatcher_service.run(sender, shutdown_watch.clone()) => result?,
            result = external_client_ingress.run(shutdown_watch) => result?,
        }

        Ok(())
    }
}
