// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains a collection of matchers

pub use googletest::matchers::*;

#[cfg(feature = "prost")]
pub use prost::protobuf_decoded;
#[cfg(feature = "prost")]
mod prost {
    use googletest::matcher::{Matcher, MatcherResult};
    use prost::bytes;
    use std::fmt::Debug;
    use std::marker::PhantomData;

    struct ProtobufDecodeMatcher<InnerMatcher, B>(InnerMatcher, PhantomData<B>);

    impl<
        T: prost::Message + Default,
        B: bytes::Buf + Clone + Debug,
        InnerMatcher: Matcher<ActualT = T>,
    > Matcher for ProtobufDecodeMatcher<InnerMatcher, B>
    {
        type ActualT = B;

        fn matches(&self, actual: &Self::ActualT) -> MatcherResult {
            let mut buf = actual.clone();
            if let Ok(msg) = T::decode(&mut buf) {
                self.0.matches(&msg)
            } else {
                MatcherResult::NoMatch
            }
        }

        fn describe(&self, matcher_result: MatcherResult) -> String {
            match matcher_result {
                MatcherResult::Match => {
                    format!(
                        "can be decoded from protobuf which {:?}",
                        self.0.describe(MatcherResult::Match)
                    )
                }
                MatcherResult::NoMatch => "cannot be decoded from protobuf".to_string(),
            }
        }
    }

    // Decode Bytes as protobuf
    pub fn protobuf_decoded<T: prost::Message + Default, B: bytes::Buf + Clone + Debug>(
        inner: impl Matcher<ActualT = T>,
    ) -> impl Matcher<ActualT = B> {
        ProtobufDecodeMatcher(inner, Default::default())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use googletest::{assert_that, matchers::eq};
        use prost::Message;
        use prost_types::Timestamp;

        #[test]
        fn timestamp() {
            let expected = Timestamp::date(2023, 9, 21).unwrap();

            assert_that!(
                bytes::Bytes::from(expected.encode_to_vec()),
                protobuf_decoded(eq(expected))
            );
            assert_that!(
                expected.encode_to_vec().as_slice(),
                protobuf_decoded(eq(expected))
            );
        }
    }
}
