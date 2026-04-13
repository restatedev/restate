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

use restate_util_string::{
    OwnedStringLike, ReString, RestrictedValue, RestrictedValueError, StringLike,
};

/// Error type for limit key parsing failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("invalid limit-key component: {0}")]
    InvalidComponent(#[from] RestrictedValueError),
    #[error("too many path components in the given limit-key (max 2)")]
    TooManyComponents,
}

/// A key that specifies 1, 2, or 3 levels of the hierarchy.
///
/// The depth of the key determines which counters are affected:
/// - `L1(level1)` - Only the Level 1 counter
/// - `L2(level1, level2)` - Level 1 and Level 2 counters
///
/// # Examples
///
/// ```
/// use std::str::FromStr;
/// use restate_limiter::LimitKey;
/// use restate_util_string::RestrictedValue;
///
/// // Create keys from validated components
/// let key: LimitKey<String> = "workflow/tenant1".parse().unwrap();
/// assert_eq!(key.depth(), 2);
/// ```
#[derive(Clone, Debug, Default, Hash, Eq, PartialEq)]
pub enum LimitKey<S: StringLike> {
    #[default]
    None,
    /// Level 1 key (Scope only).
    ///
    /// Acquiring with this key only consumes from the L1 counter.
    L1(RestrictedValue<S>),

    /// Level 2 key (Level 1 + Level 2).
    ///
    /// Acquiring with this key consumes from L1 and L2 counters.
    L2(RestrictedValue<S>, RestrictedValue<S>),
}

impl<S: StringLike> LimitKey<S> {
    pub fn to_cheap_cloneable(&self) -> LimitKey<ReString> {
        match self {
            Self::None => LimitKey::None,
            Self::L1(s) => LimitKey::L1(s.to_cheap_cloneable()),
            Self::L2(s1, s2) => LimitKey::L2(s1.to_cheap_cloneable(), s2.to_cheap_cloneable()),
        }
    }
    /// Create a Level 1 key.
    #[inline]
    pub const fn l1(level1: RestrictedValue<S>) -> Self {
        Self::L1(level1)
    }

    /// Create a Level 2 key (Level 1 + Level 2).
    #[inline]
    pub const fn l2(level1: RestrictedValue<S>, level2: RestrictedValue<S>) -> Self {
        Self::L2(level1, level2)
    }

    /// Returns the depth of this key
    #[inline]
    pub fn depth(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::L1(_) => 1,
            Self::L2(_, _) => 2,
        }
    }

    /// Returns true if this key is empty (no components).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.depth() == 0
    }

    /// Returns a reference to the Level 1 component (always present).
    #[inline]
    pub fn level1(&self) -> Option<&RestrictedValue<S>> {
        match self {
            Self::None => None,
            Self::L1(l1) | Self::L2(l1, _) => Some(l1),
        }
    }

    /// Returns a reference to the Level 2 component, if present.
    #[inline]
    pub fn level2(&self) -> Option<&RestrictedValue<S>> {
        match self {
            Self::None => None,
            Self::L1(_) => None,
            Self::L2(_, l2) => Some(l2),
        }
    }
}

impl<S: StringLike> std::fmt::Display for LimitKey<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, ""),
            Self::L1(l1) => write!(f, "{l1}"),
            Self::L2(l1, l2) => write!(f, "{l1}/{l2}"),
        }
    }
}

/// Parse a limit key from a string
pub fn parse_limit_key<S: OwnedStringLike>(s: &str) -> Result<LimitKey<S>, ParseError> {
    // Split the pattern into components
    let mut next_idx = 0;
    let mut parts: [Option<RestrictedValue<S>>; 2] = [None, None];
    for part in s.split_terminator('/') {
        if next_idx >= 2 {
            return Err(ParseError::TooManyComponents);
        }

        let part = RestrictedValue::from_str(part)?;
        parts[next_idx] = Some(part);
        next_idx += 1;
    }

    match next_idx {
        0 => Ok(LimitKey::None),
        1 => Ok(LimitKey::L1(parts[0].take().unwrap())),
        2 => Ok(LimitKey::L2(
            parts[0].take().unwrap(),
            parts[1].take().unwrap(),
        )),
        _ => Err(ParseError::TooManyComponents),
    }
}

impl<S: OwnedStringLike> FromStr for LimitKey<S> {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_limit_key(s)
    }
}

#[cfg(feature = "bilrost")]
mod bilrost_encodings {
    use std::borrow::Cow;

    use super::LimitKey;

    use bilrost::encoding::{General, Proxiable};
    use bilrost::{DecodeErrorKind, delegate_proxied_encoding, for_overwrite_via_default};

    use restate_util_string::{OwnedStringLike, ReString, RestrictedValue, StringLike, ToReString};

    struct ReStringTag;

    trait OwnedLimitKeyComponent: OwnedStringLike + ToReString + Default {}

    impl OwnedLimitKeyComponent for String {}
    impl OwnedLimitKeyComponent for ReString {}

    for_overwrite_via_default!(LimitKey<S>,
        with generics (S),
        with where clause (S: StringLike + Default)
    );

    impl<S: StringLike + Default> bilrost::encoding::EmptyState<(), LimitKey<S>> for () {
        fn is_empty(val: &LimitKey<S>) -> bool {
            matches!(*val, LimitKey::None)
        }

        fn clear(val: &mut LimitKey<S>) {
            *val = LimitKey::None;
        }
    }

    impl<S: OwnedLimitKeyComponent> Proxiable<ReStringTag> for LimitKey<S> {
        type Proxy = ReString;

        fn encode_proxy(&self) -> ReString {
            self.to_restring()
        }

        fn decode_proxy(&mut self, proxy: ReString) -> Result<(), DecodeErrorKind> {
            *self = proxy
                .parse::<Self>()
                .map_err(|_| ::bilrost::DecodeErrorKind::InvalidValue)?;
            Ok(())
        }
    }

    impl<'a> Proxiable<ReStringTag> for LimitKey<&'a str> {
        type Proxy = Cow<'a, str>;

        fn encode_proxy(&self) -> Cow<'a, str> {
            match self {
                Self::None => Cow::Borrowed(""),
                Self::L1(level1) => Cow::Borrowed(*level1.as_ref()),
                Self::L2(level1, level2) => Cow::Owned(format!("{level1}/{level2}")),
            }
        }

        fn decode_proxy(&mut self, proxy: Cow<'a, str>) -> Result<(), DecodeErrorKind> {
            let proxy = match proxy {
                Cow::Borrowed(value) => value,
                Cow::Owned(_) => return Err(DecodeErrorKind::InvalidValue),
            };

            let mut next_idx = 0;
            let mut parts: [Option<RestrictedValue<&'a str>>; 2] = [None, None];

            for part in proxy.split_terminator('/') {
                if next_idx >= 2 {
                    return Err(DecodeErrorKind::InvalidValue);
                }

                let part = RestrictedValue::new(part).map_err(|_| DecodeErrorKind::InvalidValue)?;
                parts[next_idx] = Some(part);
                next_idx += 1;
            }

            *self = match next_idx {
                0 => LimitKey::None,
                1 => LimitKey::L1(parts[0].take().expect("index 0 must be present")),
                2 => LimitKey::L2(
                    parts[0].take().expect("index 0 must be present"),
                    parts[1].take().expect("index 1 must be present"),
                ),
                _ => return Err(DecodeErrorKind::InvalidValue),
            };

            Ok(())
        }
    }

    // We use a specialized trait OwnedLimitKeyComponent to avoid trait impl conflicts
    // with the borrowed version.
    delegate_proxied_encoding!(
        use encoding (General)
        to encode proxied type (LimitKey<S>)
        using proxy tag (ReStringTag)
        with general encodings with generics (S: OwnedLimitKeyComponent)
    );

    // specialized encoding to allow borrowed decoding of LimitKey<&'a str>
    delegate_proxied_encoding!(
        use encoding (General)
        to encode proxied type (LimitKey<&'a str>)
        using proxy tag (ReStringTag)
        with encoding (bilrost::encoding::GeneralGeneric<P>)
        with generics ('a, const P: u8)
    );

    #[cfg(test)]
    #[test]
    fn bilrost_limit_key() {
        #[derive(bilrost::Message)]
        struct LimitKeyMessage {
            #[bilrost(1)]
            value: LimitKey<ReString>,
        }

        for input in ["", "hello", "hello/value"] {
            use bilrost::{Message, OwnedMessage};

            let src = LimitKeyMessage {
                value: input.parse().unwrap(),
            };
            let encoded = src.encode_to_bytes();
            let decoded = LimitKeyMessage::decode(encoded).unwrap();
            assert_eq!(decoded.value.to_string(), input);
        }
    }

    #[cfg(test)]
    #[test]
    fn bilrost_limit_key_borrowed_decode() {
        #[derive(bilrost::Message)]
        struct OwnedLimitKeyMessage {
            #[bilrost(1)]
            value: LimitKey<ReString>,
        }

        #[derive(bilrost::Message)]
        struct BorrowedLimitKeyMessage<'a> {
            #[bilrost(1)]
            value: LimitKey<&'a str>,
        }

        for input in ["", "hello", "hello/value"] {
            use bilrost::{BorrowedMessage, Message};

            let src = OwnedLimitKeyMessage {
                value: input.parse().unwrap(),
            };
            let encoded = src.encode_to_bytes();
            let decoded = BorrowedLimitKeyMessage::decode_borrowed(&encoded).unwrap();
            assert_eq!(decoded.value.to_string(), input);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_limit_key() {
        let k: LimitKey<String> = "".parse().unwrap();
        assert_eq!(k.depth(), 0);
        assert!(matches!(k, LimitKey::None));

        let err = "/".parse::<LimitKey<String>>().unwrap_err();
        assert!(matches!(
            err,
            ParseError::InvalidComponent(RestrictedValueError::Empty),
        ));

        let k: LimitKey<String> = "kimo".parse().unwrap();
        assert_eq!(k.depth(), 1);
        assert_eq!(k, LimitKey::L1("kimo".parse().unwrap()));
        assert_eq!(k.to_string(), "kimo");

        let k: LimitKey<String> = "kimo/".parse().unwrap();
        assert_eq!(k.depth(), 1);
        assert_eq!(k, LimitKey::L1("kimo".parse().unwrap()));
        assert_eq!(k.to_string(), "kimo");

        let k: LimitKey<String> = "kimo/antimo".parse().unwrap();
        assert_eq!(k.depth(), 2);
        assert_eq!(
            k,
            LimitKey::L2("kimo".parse().unwrap(), "antimo".parse().unwrap()),
        );
        assert_eq!(k.to_string(), "kimo/antimo");

        let k: LimitKey<String> = "kimo/antimo/".parse().unwrap();
        assert_eq!(k.depth(), 2);
        assert_eq!(
            k,
            LimitKey::L2("kimo".parse().unwrap(), "antimo".parse().unwrap())
        );

        assert_eq!(k.to_string(), "kimo/antimo");

        let err = "kimo/antimo//".parse::<LimitKey<String>>().unwrap_err();
        assert!(matches!(err, ParseError::TooManyComponents));
    }
}
