// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use bilrost::{
    DecodeErrorKind,
    encoding::{EmptyState, ForOverwrite, Proxiable},
};
use bytes::Bytes;
use http::{HeaderName, HeaderValue, Uri};

use super::RestateEncoding;

struct HeaderTag;

impl Proxiable<HeaderTag> for HeaderName {
    type Proxy = String;

    fn encode_proxy(&self) -> Self::Proxy {
        self.to_string()
    }

    fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
        *self = HeaderName::from_str(&proxy).map_err(|_| DecodeErrorKind::InvalidValue)?;

        Ok(())
    }
}

impl ForOverwrite<RestateEncoding, HeaderName> for () {
    fn for_overwrite() -> HeaderName
    where
        HeaderValue: Sized,
    {
        HeaderName::from_static("")
    }
}

impl EmptyState<RestateEncoding, HeaderName> for () {
    fn clear(val: &mut HeaderName) {
        *val = HeaderName::from_static("")
    }

    fn is_empty(val: &HeaderName) -> bool {
        val.as_str().is_empty()
    }
}

bilrost::delegate_proxied_encoding!(
    use encoding (bilrost::encoding::General)
    to encode proxied type (HeaderName)
    using proxy tag (HeaderTag)
    with encoding (RestateEncoding)
);

#[derive(Default, bilrost::Message)]
struct HeaderValueProxy {
    #[bilrost(tag = 1)]
    is_sensitive: bool,
    #[bilrost(tag = 2)]
    data: Bytes,
}

impl Proxiable<HeaderTag> for HeaderValue {
    type Proxy = HeaderValueProxy;

    fn encode_proxy(&self) -> Self::Proxy {
        HeaderValueProxy {
            is_sensitive: self.is_sensitive(),
            data: Bytes::copy_from_slice(self.as_bytes()),
        }
    }

    fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
        *self = HeaderValue::from_bytes(&proxy.data).map_err(|_| DecodeErrorKind::InvalidValue)?;
        self.set_sensitive(proxy.is_sensitive);

        Ok(())
    }
}

impl ForOverwrite<RestateEncoding, HeaderValue> for () {
    fn for_overwrite() -> HeaderValue
    where
        HeaderValue: Sized,
    {
        HeaderValue::from_static("")
    }
}

impl EmptyState<RestateEncoding, HeaderValue> for () {
    fn clear(val: &mut HeaderValue) {
        *val = HeaderValue::from_static("")
    }

    fn is_empty(val: &HeaderValue) -> bool {
        val.is_empty()
    }
}

bilrost::delegate_proxied_encoding!(
    use encoding (bilrost::encoding::General)
    to encode proxied type (HeaderValue)
    using proxy tag (HeaderTag)
    with encoding (RestateEncoding)
);

struct VersionTag;

#[derive(PartialEq, Eq, Clone, Copy, bilrost::Enumeration)]
enum HttpVersion {
    // default is http1.1
    Http11 = 0,
    Http09 = 1,
    Http10 = 2,
    Http2 = 3,
    Http3 = 4,
}

impl Proxiable<VersionTag> for http::Version {
    type Proxy = HttpVersion;

    fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
        match proxy {
            HttpVersion::Http09 => *self = http::Version::HTTP_09,
            HttpVersion::Http10 => *self = http::Version::HTTP_10,
            HttpVersion::Http11 => *self = http::Version::HTTP_11,
            HttpVersion::Http2 => *self = http::Version::HTTP_2,
            HttpVersion::Http3 => *self = http::Version::HTTP_3,
        }
        Ok(())
    }

    fn encode_proxy(&self) -> Self::Proxy {
        match *self {
            http::Version::HTTP_09 => HttpVersion::Http09,
            http::Version::HTTP_10 => HttpVersion::Http10,
            http::Version::HTTP_11 => HttpVersion::Http11,
            http::Version::HTTP_2 => HttpVersion::Http2,
            http::Version::HTTP_3 => HttpVersion::Http3,
            _ => {
                // http4 is out already! what year is this?
                unreachable!("unknown http version")
            }
        }
    }
}

impl ForOverwrite<RestateEncoding, http::Version> for () {
    fn for_overwrite() -> http::Version
    where
        HeaderValue: Sized,
    {
        http::Version::HTTP_11
    }
}

impl EmptyState<RestateEncoding, http::Version> for () {
    fn clear(val: &mut http::Version) {
        *val = http::Version::HTTP_11
    }

    fn is_empty(val: &http::Version) -> bool {
        val == &http::Version::HTTP_11
    }
}

bilrost::delegate_proxied_encoding!(
    use encoding (bilrost::encoding::General)
    to encode proxied type (http::Version)
    using proxy tag (VersionTag)
    with encoding (RestateEncoding)
);

struct UriTag;

impl ForOverwrite<RestateEncoding, Uri> for () {
    fn for_overwrite() -> Uri {
        Uri::default()
    }
}

impl EmptyState<RestateEncoding, Uri> for () {
    fn is_empty(val: &Uri) -> bool {
        val == &Uri::default()
    }

    fn clear(val: &mut Uri) {
        *val = Uri::default();
    }
}

impl Proxiable<UriTag> for Uri {
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
    to encode proxied type (Uri)
    using proxy tag (UriTag)
    with encoding (RestateEncoding)
);
