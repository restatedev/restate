//! This library extends [`thiserror`] to decorate errors with two additional metadata:
//! error code and hints to fix it. These can be useful in projects where errors are indexed by a code,
//! for example as the [Rust compiler errors](https://doc.rust-lang.org/error-index.html).
//!
//! # Example
//!
//! ```rust
//! use thiserror::Error;
//! use codederror::CodedError;
//!
//! // To derive CodedError you need to derive thiserror::Error and fmt::Debug
//! #[derive(Error, CodedError, Debug)]
//! #[hint("Please contact the developers!")]
//! pub enum Error {
//!     #[code(1)]
//!     #[hint("Fix the config")]
//!     #[error("configuration error: {0}")]
//!     Configuration(String),
//!
//!     #[error("user {user_id} error: {source}")]
//!     User {
//!         user_id: u64,
//!         #[source]
//!         #[code] // Take code from OtherError
//!         #[hint] // Add hints from OtherError to hints of Error
//!         source: OtherError,
//!     },
//!
//!     #[error(transparent)]
//!     #[code(unknown)] // No code available for this variant
//!     Other(#[from] GenericError),
//! }
//!
//! #[derive(Error, CodedError, Debug)]
//! #[code(2)]
//! #[error("some other error")]
//! #[hint("Turn off and on")]
//! pub struct OtherError {}
//!
//! pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;
//!
//! # fn main() {
//! let e = Error::User {user_id: 1, source: OtherError {}};
//!
//! println!("{}", e);
//! // This will print:
//! // user 1 error: some other error
//!
//! println!("{}", e.decorate());
//! // This will print:
//! // [0002] user 1 error: some other error
//! //
//! // Hints:
//! // * Turn off and on
//! // * Please contact the developers!
//! //
//! // Error code 0002
//!
//! println!("{:#?}", e.decorate());
//! // This will print:
//! // CodedError {
//! //     code: "Error code 0002",
//! //     inner: user 1 error: some other error,
//! //     hints: [
//! //         "Turn off and on",
//! //         "Please contact the developers!",
//! //     ],
//! // }
//! # }
//!
//! ```
//!
//! # Formatting options
//!
//! You can modify the code formatting options for the crate you're implementing by setting the following
//! environment variables in your [`config.toml`](https://doc.rust-lang.org/nightly/cargo/reference/config.html#env):
//!
//! ```toml
//! [env]
//! CODEDERROR_PADDING = "4"
//! # Available variables:
//! # - code: numeric padded value
//! CODEDERROR_CODE_FORMAT = "APP-{code}"
//! # Available variables:
//! # - code: numeric padded value
//! # - code_str: code formatted using CODEDERROR_CODE_FORMAT
//! CODEDERROR_HELP_FORMAT = "For more details, look at the docs with http://mydocs.com/{code_str}"
//! ```
//!
//! In case of the previous example, the display output becomes:
//!
//! ```text
//! [APP-0002] user 1 error: some other error
//!
//!  Hints:
//!  * Turn off and on
//!  * Please contact the developers!
//!
//!  For more details, look at the docs with http://mydocs.com/APP-0002
//! ```
//!
//! This crate uses [`strfmt`] to format strings, make sure you use the correct format pattern.
//!
//! [`thiserror`]: https://docs.rs/thiserror
//! [`strfmt`]: https://docs.rs/strfmt

use std::fmt;
use std::fmt::{format, Debug, Display};
use std::ops::Deref;

pub use codederror_impl::*;

/// This trait defines a coded error, that is an error with code and hints metadata.
///
/// This trait provides the method [`decorate`](`CodedError::decorate`),
/// which returns a struct implementing [`fmt::Display`], printing error message, cause, code and hints.
pub trait CodedError: std::error::Error + Sized {
    #[doc(hidden)]
    fn code(&self) -> Option<Code>;
    #[doc(hidden)]
    fn fmt_hints(&self, f: &mut HintFormatter<'_, '_>) -> fmt::Result;

    /// Create a [`DecoratedError`] from a reference of this error.
    fn decorate(&self) -> DecoratedError<'_, Self> {
        DecoratedError {
            this: DecoratedErrorInner::Ref(self),
        }
    }

    /// Convert this error into a [`DecoratedError`].
    fn into_decorated(self) -> DecoratedError<'static, Self> {
        DecoratedError {
            this: DecoratedErrorInner::Owned(self),
        }
    }
}

/// This struct implements [`Display`] and [`Debug`] for a [CodedError],
/// adding code and hints to the error message.
///
/// Sample:
/// ```rust
/// # fn print(e: impl codederror::CodedError) {
/// println!("{}", e.decorate());
/// // [RT-0001] configuration error: this config error happened
/// //
/// // Hints:
/// // * Fix the config
/// // * Please contact the developers!
/// //
/// // For more details, please open http://mydocs.com/RT-0001
/// # }
/// ```
pub struct DecoratedError<'a, T> {
    this: DecoratedErrorInner<'a, T>,
}

enum DecoratedErrorInner<'a, T> {
    Ref(&'a T),
    Owned(T),
}

impl<'a, T: 'a> Deref for DecoratedErrorInner<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            DecoratedErrorInner::Ref(r) => r,
            DecoratedErrorInner::Owned(t) => t,
        }
    }
}

impl<T: CodedError> Display for DecoratedError<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.this.code() {
            write!(f, "[{}] ", code.code_str)?;
        }
        write!(f, "{}", self.this.deref())?;
        let mut hint_formatter = HintFormatter::FanOut {
            first: true,
            inner: f,
        };
        self.this.fmt_hints(&mut hint_formatter)?;

        if let Some(code) = self.this.code() {
            write!(f, "\n\n{}", code.help_str)?;
        }

        Ok(())
    }
}

impl<T: CodedError> Debug for DecoratedError<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut collected_hints = vec![];

        let mut hint_formatter = HintFormatter::Collecting(&mut collected_hints);
        self.this.fmt_hints(&mut hint_formatter)?;

        f.debug_struct("CodedError")
            .field("code", &self.this.code())
            .field("inner", &format_args!("{}", self.this.deref()))
            .field("hints", &collected_hints)
            .finish()
    }
}

impl<T: CodedError> std::error::Error for DecoratedError<'_, T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.this.source()
    }

    #[cfg(feature = "backtrace")]
    fn backtrace(&self) -> Option<&std::backtrace::Backtrace> {
        self.this.backtrace()
    }
}

#[doc(hidden)]
pub enum HintFormatter<'a, 'b> {
    FanOut {
        first: bool,
        inner: &'a mut fmt::Formatter<'b>,
    },
    Collecting(&'a mut Vec<String>),
}

impl<'a, 'b> HintFormatter<'a, 'b> {
    pub fn write_fmt_next(&mut self, args: fmt::Arguments<'_>) -> fmt::Result {
        match self {
            HintFormatter::FanOut { first, inner } => {
                if *first {
                    write!((*inner), "\n\nHints:")?;
                    *first = false;
                }
                write!((*inner), "\n* ")?;
                (*inner).write_fmt(args)
            }
            HintFormatter::Collecting(v) => {
                (*v).push(format(args));
                Ok(())
            }
        }
    }
}

#[doc(hidden)]
pub struct Code {
    pub value: u32,
    pub code_str: &'static str,
    pub help_str: &'static str,
}

impl Debug for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.help_str)
    }
}

impl Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.help_str)
    }
}
