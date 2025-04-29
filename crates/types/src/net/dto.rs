// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;

use restate_encoding::NetSerde;

/// A trait for converting from internal types and their DTO (Data Transfer Object)
/// representations used for storage or wire transmission.
///
/// Types that need to be serialized or sent over the network should implement this trait,
/// defining how they convert to and from a `Target` type, which is expected to implement
/// [`bilrost::Message`]
///
/// For types that already implement [`bilrost::Message`], a blanket implementation is provided,
/// allowing them to be stored or transferred directly without transformation.
pub trait IntoBilrostDto {
    type Target: bilrost::Message + NetSerde;
    fn into_dto(self) -> Self::Target;
}

/// A trait for converting from DTOs (Data Transfer Object) used mainly for storage or wire transmission, and
/// their internal types used for in memory manipulation
///
/// Types that need to be serialized or sent over the network should implement this trait,
/// defining how they convert to and from a `Target` type, which is expected to implement
/// [`bilrost::OwnedMessage`]
///
/// For types that already implement [`bilrost::Message`], a blanket implementation is provided,
/// allowing them to be stored or transferred directly without transformation.
///
pub trait FromBilrostDto: Sized {
    type Target: bilrost::OwnedMessage + NetSerde;
    type Error: std::error::Error + Send + Sync + 'static;

    fn from_dto(value: Self::Target) -> Result<Self, Self::Error>;
}

impl<T> IntoBilrostDto for T
where
    T: bilrost::Message + NetSerde,
{
    type Target = T;

    fn into_dto(self) -> Self::Target {
        self
    }
}

impl<T> FromBilrostDto for T
where
    T: bilrost::OwnedMessage + NetSerde,
{
    type Target = T;
    type Error = Infallible;

    fn from_dto(value: Self::Target) -> Result<Self, Self::Error> {
        Ok(value)
    }
}
