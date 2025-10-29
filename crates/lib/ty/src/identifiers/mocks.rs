// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytestring::ByteString;
use rand::Rng;
use rand::distr::{Alphanumeric, SampleString};

use super::invocation::InvocationUuid;
use super::{IdempotencyId, InvocationId};
use crate::invocation::InvocationTarget;
use crate::partitions::PartitionKey;

impl InvocationUuid {
    pub fn mock_generate(invocation_target: &InvocationTarget) -> Self {
        InvocationUuid::generate(invocation_target, None)
    }

    pub fn mock_random() -> Self {
        InvocationUuid::mock_generate(&InvocationTarget::mock_service())
    }
}

impl InvocationId {
    pub fn mock_generate(invocation_target: &InvocationTarget) -> Self {
        InvocationId::generate(invocation_target, None)
    }

    pub fn mock_random() -> Self {
        Self::from_parts(
            rand::rng().sample::<PartitionKey, _>(rand::distr::StandardUniform),
            InvocationUuid::mock_random(),
        )
    }
}

impl IdempotencyId {
    pub const fn unkeyed(
        partition_key: PartitionKey,
        service_name: &'static str,
        service_handler: &'static str,
        idempotency_key: &'static str,
    ) -> Self {
        Self {
            service_name: ByteString::from_static(service_name),
            service_key: None,
            service_handler: ByteString::from_static(service_handler),
            idempotency_key: ByteString::from_static(idempotency_key),
            partition_key,
        }
    }

    pub fn mock_random() -> Self {
        Self::new(
            Alphanumeric.sample_string(&mut rand::rng(), 8).into(),
            Some(Alphanumeric.sample_string(&mut rand::rng(), 16).into()),
            Alphanumeric.sample_string(&mut rand::rng(), 8).into(),
            Alphanumeric.sample_string(&mut rand::rng(), 8).into(),
        )
    }
}
