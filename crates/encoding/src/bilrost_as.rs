// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Adaptor trait used by the `BilrostAs` derive macro. The macro will
/// create an instance of the adaptor to be able to convert from the original
/// type to the `As` type.
///
/// Any [`bilrost::Message`] that implements `From<T> for As`
/// and `From<As> for T` will automatically implement the Adaptor trait.
/// Hence is possible to do something like
///
/// ```ignore
/// #[derive(Default, Clone, BilrostAs)]
/// #[bilrost_as(Wire)]
/// struct Inner{
///
/// }
///
/// #[derive(bilrost::Message)]
/// struct Wire{
/// }
///
/// // impl From<Inner> for Wire{ .. }
/// // impl From<Wire> for Inner{ .. }
/// ```
pub trait BilrostAsAdaptor<'a, Source> {
    fn create(value: &'a Source) -> Self;
    fn into_inner(self) -> Result<Source, bilrost::DecodeError>;
}

impl<'a, Target, Source> BilrostAsAdaptor<'a, Source> for Target
where
    Target: bilrost::Message,
    Source: 'static,
    Target: From<&'a Source>,
    Source: From<Target>,
{
    fn create(value: &'a Source) -> Self {
        Target::from(value)
    }

    fn into_inner(self) -> Result<Source, bilrost::DecodeError> {
        Ok(Source::from(self))
    }
}
