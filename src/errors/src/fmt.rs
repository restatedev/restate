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

use codederror::{Code, CodedError};
use std::fmt;

/// Check module documentation for more details.
#[macro_export]
macro_rules! info_it {
    ($err:expr) => {
        tracing::info!(error = tracing::field::display(codederror::CodedError::decorate(&$err)), restate.error.code = ?$crate::fmt::RestateCode::from(&$err));
    };
    ($err:expr, $($field:tt)*) => {
        tracing::info!(error = tracing::field::display(codederror::CodedError::decorate(&$err)), restate.error.code = ?$crate::fmt::RestateCode::from(&$err), $($field)*);
    };
}

/// Check module documentation for more details.
#[macro_export]
macro_rules! warn_it {
    ($err:expr) => {
        tracing::warn!(error = tracing::field::display(codederror::CodedError::decorate(&$err)), restate.error.code = ?$crate::fmt::RestateCode::from(&$err));
    };
    ($err:expr, $($field:tt)*) => {
        tracing::warn!(error = tracing::field::display(codederror::CodedError::decorate(&$err)), restate.error.code = ?$crate::fmt::RestateCode::from(&$err), $($field)*);
    };
}

/// Check module documentation for more details.
#[macro_export]
macro_rules! error_it {
    ($err:expr) => {
        tracing::error!(error = tracing::field::display(codederror::CodedError::decorate(&$err)), restate.error.code = ?$crate::fmt::RestateCode::from(&$err));
    };
    ($err:expr, $($field:tt)*) => {
        tracing::error!(error = tracing::field::display(codederror::CodedError::decorate(&$err)), restate.error.code = ?$crate::fmt::RestateCode::from(&$err), $($field)*);
    };
}

pub struct RestateCode(Option<Code>);

impl<T> From<&T> for RestateCode
where
    T: CodedError,
{
    fn from(value: &T) -> Self {
        RestateCode(value.code().cloned())
    }
}

#[cfg(feature = "include_doc")]
impl RestateCode {
    fn fmt_alternate(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => {
                write!(f, "{}", termimad::term_text("## No error description"))
            }
            Some(code) => match code.description() {
                None => {
                    // We should never end up here,
                    // as we enforce all restate error codes to have a description thanks to the macro in helper.rs
                    write!(f, "{}", termimad::term_text(&format!("## {}", code.code())))
                }
                Some(description) => {
                    write!(f, "{}", termimad::term_text(description))
                }
            },
        }
    }
}

#[cfg(not(feature = "include_doc"))]
impl RestateCode {
    fn fmt_alternate(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "No error description"),
            Some(code) => write!(f, "{}", code.code()),
        }
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
    use super::*;

    use crate::RT0001;
    use restate_test_utils::test;

    #[derive(thiserror::Error, CodedError, Debug)]
    #[code(RT0001)]
    #[error("my error")]
    pub struct MyError;

    #[test]
    fn test_printing_error() {
        let error = MyError {};
        error_it!(error);
        error_it!(error, "My error message {}", 1);
    }

    #[test]
    fn test_printing_warn() {
        let error = MyError {};
        warn_it!(error);
        warn_it!(error, "My error message {}", 1);
    }

    #[test]
    fn test_printing_info() {
        let error = MyError {};
        info_it!(error);
        info_it!(error, "My error message {}", 1);
    }
}
