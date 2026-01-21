// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// This macro generates const declaration of [`codederror::Code`] for each provided code name. E.g.:
///
/// ```rust,ignore
/// const RT0001_DESCRIPTION: Option<&'static str> = Some(include_str!("error_codes/RT0001.md"));
/// pub const RT0001: Code = Code::new("RT0001", "For more details, look at the docs with https://restate.dev/doc/errors/RT0001", RT0001_DESCRIPTION);
/// ```
///
/// If the feature `include_doc` is enabled, this will load the code description in the binaries,
/// otherwise errors won't have an hardcoded description.
macro_rules! declare_restate_error_codes {
    ($($code:ident),* $(,)?) => {
        $(
            declare_restate_error_codes!(@declare_doc $code);
            declare_restate_error_codes!(@declare_code $code);
        )*
    };
    (@declare_doc $code:ident) => { paste::paste! {
        #[cfg(feature="include_doc")]
        const [< $code _DESCRIPTION >]: Option<&'static str> = Some(include_str!(concat!("error_codes/", stringify!($code), ".md")));
        #[cfg(not(feature="include_doc"))]
        const [< $code _DESCRIPTION >]: Option<&'static str> = None;
    }};
    (@declare_code $code:ident) => {
        pub const $code: codederror::Code = codederror::Code::new(
            stringify!($code),
            None,
            paste::paste! { [< $code _DESCRIPTION >] }
        );
    };
}
