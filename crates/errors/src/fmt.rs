// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module provides macros to print Restate's [`CodedError`] using [`tracing`] log macros,
//! and printing the description, rendering it as markdown, whenever is possible.
//!
//! Example usage (same applies to all the macros in this module):
//! ```rust
//! use restate_errors::error_it;
//!
//! # fn print(error: impl codederror::CodedError) {
//! // Just print the error without additional info
//! error_it!(error);
//!
//! // Add a message when printing the error
//! error_it!(error, "Error happened while trying to do something");
//!
//! // Add a format string and some format arguments when printing the error
//! error_it!(error, "Error happened while trying to do something with this number: '{}'", 1);
//! # }
//! ```

use codederror::Code;
use std::fmt;

/// Check module documentation for more details.
#[macro_export]
macro_rules! info_it {
    ($err:expr) => {
        {
            #[allow(unused_imports)]
            use ::codederror::CodedError;

            let err = &$err;
            let code = $crate::fmt::RestateCode::from_code(err.code());

            tracing::info!(error = %err, restate.error.code = ?code);
        }
    };
    ($err:expr, $($field:tt)*) => {
        {
            #[allow(unused_imports)]
            use ::codederror::CodedError;

            let err = &$err;
            let code = $crate::fmt::RestateCode::from_code(err.code());

            tracing::info!(error = %err, restate.error.code = ?code, $($field)*);
        }
    };
}

/// Check module documentation for more details.
#[macro_export]
macro_rules! warn_it {
     ($err:expr) => {
        {
            #[allow(unused_imports)]
            use ::codederror::CodedError;

            let err = &$err;
            let code = $crate::fmt::RestateCode::from_code(err.code());

            tracing::warn!(error = %err, restate.error.code = ?code);
        }
    };
    ($err:expr, $($field:tt)*) => {
        {
            #[allow(unused_imports)]
            use ::codederror::CodedError;

            let err = &$err;
            let code = $crate::fmt::RestateCode::from_code(err.code());

            tracing::warn!(error = %err, restate.error.code = ?code, $($field)*);
        }
    };
}

/// Check module documentation for more details.
#[macro_export]
macro_rules! error_it {
    ($err:expr) => {
        {
            #[allow(unused_imports)]
            use ::codederror::CodedError;

            let err = &$err;
            let code = $crate::fmt::RestateCode::from_code(err.code());

            tracing::error!(error = %err, restate.error.code = ?code);
        }
    };
    ($err:expr, $($field:tt)*) => {
        {
            #[allow(unused_imports)]
            use ::codederror::CodedError;

            let err = &$err;
            let code = $crate::fmt::RestateCode::from_code(err.code());

            tracing::error!(error = %err, restate.error.code = ?code, $($field)*);
        }
    };
}

pub struct RestateCode(Option<&'static Code>);

impl RestateCode {
    pub fn from_code(code: Option<&'static Code>) -> Self {
        RestateCode(code)
    }
}

#[cfg(feature = "include_doc")]
impl RestateCode {
    fn fmt_alternate(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => {
                write!(f, "No error description")
            }
            Some(code) => match code.description() {
                None => {
                    // We should never end up here,
                    // as we enforce all restate error codes to have a description thanks to the macro in helper.rs
                    write!(f, "{}", &code.code().to_string())
                }
                Some(description) => {
                    write!(
                        f,
                        "{}",
                        description
                            .trim_start_matches(&format!("## {}", code.code()))
                            .trim()
                    )
                }
            },
        }
    }
}

#[cfg(not(feature = "include_doc"))]
impl RestateCode {
    fn fmt_alternate(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The error code is already included in the error message
        Ok(())
    }
}

impl fmt::Debug for RestateCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            self.fmt_alternate(f)
        } else {
            match self.0 {
                None => write!(f, "None"),
                Some(code) => write!(f, "{}", code.code()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use codederror::CodedError;
    use test_log::test;

    use crate::RT0001;

    #[derive(thiserror::Error, CodedError, Debug)]
    #[code(RT0001)]
    #[error("my error")]
    pub struct MyError;

    #[test]
    fn test_printing_error() {
        let error = MyError {};
        error_it!(error);
        error_it!(&error, "My error message {}", 1);
    }

    #[test]
    fn test_printing_warn() {
        let error = MyError {};
        warn_it!(error);
        warn_it!(&error, "My error message {}", 1);
    }

    #[test]
    fn test_printing_info() {
        let error = MyError {};
        info_it!(error);
        info_it!(&error, "My error message {}", 1);
    }
}
