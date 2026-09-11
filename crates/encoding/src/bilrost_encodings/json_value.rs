// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::encoding::{EmptyState, ForOverwrite, Proxiable};
use serde_json::Value;

use crate::RestateEncoding;

struct JsonValueTag;

impl ForOverwrite<RestateEncoding, Value> for () {
    fn for_overwrite() -> Value {
        ::core::default::Default::default()
    }
}

impl EmptyState<RestateEncoding, Value> for () {
    fn is_empty(val: &Value) -> bool {
        *val == Value::Null
    }

    fn clear(val: &mut Value) {
        *val = Value::Null;
    }
}

impl Proxiable<JsonValueTag> for Value
where
    Value: ::std::fmt::Display + ::std::str::FromStr,
{
    type Proxy = ::std::string::String;

    fn encode_proxy(&self) -> ::std::string::String {
        self.to_string()
    }

    fn decode_proxy(
        &mut self,
        proxy: ::std::string::String,
    ) -> ::core::result::Result<(), ::bilrost::DecodeErrorKind> {
        *self = proxy
            .parse::<Self>()
            .map_err(|_| ::bilrost::DecodeErrorKind::InvalidValue)?;
        Ok(())
    }
}

bilrost::delegate_proxied_encoding!(
    use encoding (bilrost::encoding::General)
    to encode proxied type (Value)
    using proxy tag (JsonValueTag)
    with encoding (RestateEncoding)
);
