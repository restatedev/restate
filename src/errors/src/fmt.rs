//! This module provides macros to print Restate's [`CodedError`] using [`tracing`] log macros,
//! and printing the description, rendering it as markdown, whenever is possible.
//!
//! Example usage (same applies to all the macros in this module):
//! ```rust
//! use errors::error_it;
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

use codederror::CodedError;

/// Check module documentation for more details.
#[macro_export]
macro_rules! info_it {
    ($err:expr) => {
        tracing::info!(error = tracing::field::display($err.decorate()));
        $crate::fmt::CodedErrorExt::print_description_as_markdown(&$err);
    };
    ($err:expr, $($field:tt)*) => {
        tracing::info!(error = tracing::field::display($err.decorate()), $($field)*);
        $crate::fmt::CodedErrorExt::print_description_as_markdown(&$err);
    };
}

/// Check module documentation for more details.
#[macro_export]
macro_rules! warn_it {
    ($err:expr) => {
        tracing::warn!(error = tracing::field::display($err.decorate()));
        $crate::fmt::CodedErrorExt::print_description_as_markdown(&$err);
    };
    ($err:expr, $($field:tt)*) => {
        tracing::warn!(error = tracing::field::display($err.decorate()), $($field)*);
        $crate::fmt::CodedErrorExt::print_description_as_markdown(&$err);
    };
}

/// Check module documentation for more details.
#[macro_export]
macro_rules! error_it {
    ($err:expr) => {
        tracing::error!(error = tracing::field::display($err.decorate()));
        $crate::fmt::CodedErrorExt::print_description_as_markdown(&$err);
    };
    ($err:expr, $($field:tt)*) => {
        tracing::error!(error = tracing::field::display($err.decorate()), $($field)*);
        $crate::fmt::CodedErrorExt::print_description_as_markdown(&$err);
    };
}

#[doc(hidden)]
pub trait CodedErrorExt {
    fn print_description_as_markdown(&self);
}

#[cfg(feature = "include_doc")]
impl<CE> CodedErrorExt for CE
where
    CE: CodedError,
{
    fn print_description_as_markdown(&self) {
        if let Some(description) = self.code().and_then(codederror::Code::description) {
            println!("{}", termimad::term_text(description))
        }
    }
}

#[cfg(not(feature = "include_doc"))]
impl<CE> CodedErrorExt for CE
where
    CE: CodedError,
{
    fn print_description_as_markdown(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::RT0001;
    use test_utils::test;

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
