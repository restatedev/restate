// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This library extends [`thiserror`] to decorate errors with additional metadata:
//! error code, help string and long error description.
//!
//! These can be useful in projects where errors are indexed by a code, for example as the
//! [Rust compiler errors](https://doc.rust-lang.org/error-index.html).
//!
//! # Example
//!
//! ```rust
//! use thiserror::Error;
//! use codederror::{error_code, CodedError, Code};
//!
//! // Define error codes
//! pub const E0001: Code = error_code!(
//!     "E0001",
//!     help="Look E0001 in the documentation"
//! );
//! pub const E0002: Code = error_code!(
//!     "E0002",
//!     help="Ask developers about error code 0002",
//!     description="Don't know much about it to be fair"
//! );
//!
//! // To derive CodedError you need to derive thiserror::Error and fmt::Debug
//! #[derive(Error, CodedError, Debug)]
//! pub enum Error {
//!     #[code(E0001)]
//!     #[error("configuration error: {0}")]
//!     Configuration(String),
//!
//!     #[error("user {user_id} error: {source}")]
//!     User {
//!         user_id: u64,
//!         #[source]
//!         #[code] // Take code from OtherError
//!         source: OtherError,
//!     },
//!
//!     #[error(transparent)]
//!     #[code(unknown)] // No code available for this variant
//!     Other(#[from] BoxError),
//! }
//!
//! #[derive(Error, CodedError, Debug)]
//! #[code(E0002)]
//! #[error("some other error")]
//! pub struct OtherError {}
//!
//! # pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
//!
//! # fn main() {
//! let e = Error::User {user_id: 1, source: OtherError {}};
//!
//! // The Display trait is implemented by thiserror
//! println!("{}", e);
//! // user 1 error: some other error
//!
//! // Using decorate will enable printing code and the help message
//! println!("{}", e.decorate());
//! // [E0002] user 1 error: some other error
//! //
//! // Look E0002 in the documentation
//!
//! // Printing in alternate mode will print the description as well
//! println!("{:#}", e.decorate());
//! // [E0002] user 1 error: some other error
//! //
//! // Don't know much about it to be fair
//! //
//! // Look E0002 in the documentation
//!
//! // You can also use the Debug format
//! println!("{:#?}", e.decorate());
//! // CodedError {
//! //     code: "E002",
//! //     inner: user 1 error: some other error,
//! // }
//! # }
//!
//! ```
//!
//! [`thiserror`]: https://docs.rs/thiserror

use std::fmt;
use std::fmt::{Debug, Display};
use std::ops::Deref;

pub use codederror_derive::*;

/// This macro provides constructors for error codes, to be used in the [`CodedError`] derive macro.
/// See the module documentation for more details.
///
/// ```rust
/// use codederror::{Code, error_code};
///
/// // Define an error code
/// pub const E0001: Code = error_code!("E0001");
///
/// // Define an error code with an help string
/// pub const E0002: Code = error_code!(
///     "E0002",
///     help="Look E0002 in the documentation."
/// );
///
/// // Define an error code with an help string and a long description
/// pub const E0003: Code = error_code!(
///     "E0003",
///     help="Ask developers about error code E0003.",
///     description="Don't know much about it to be fair, but I think by changing the configuration entry 'stop_E0003: true' you won't encounter this issue anymore."
///  );
/// ```
#[macro_export]
macro_rules! error_code {
    ($code:literal) => {
        Code::new($code, None, None)
    };
    ($code:literal, help=$help:literal) => {
        Code::new($code, Some($help), None)
    };
    ($code:literal, description=$description:literal) => {
        Code::new($code, None, Some($description))
    };
    ($code:literal, help=$help:literal, description=$description:literal) => {
        Code::new($code, Some($help), Some($description))
    };
}

/// This trait defines a coded error, that is an error with an error code.
///
/// This trait provides the method [`decorate`](`CodedError::decorate`),
/// which returns a struct implementing [`fmt::Display`], printing error message, cause and code.
pub trait CodedError: std::error::Error + Sized {
    #[doc(hidden)]
    fn code(&self) -> Option<&'static Code>;

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

    fn into_boxed(self) -> BoxedCodedError
    where
        Self: Send + Sync + 'static,
    {
        BoxedCodedError {
            code: self.code(),
            inner: Box::new(self),
        }
    }
}

/// This struct implements [`Display`] and [`Debug`] for a [CodedError],
/// adding the code and help string to the error message.
///
/// Sample:
/// ```rust
/// # fn print(e: impl codederror::CodedError) {
/// println!("{}", e.decorate());
/// // [E0002] user 1 error: some other error
/// //
/// // Look E0002 in the documentation
///
/// // Printing in alternate mode will print the description as well
/// println!("{:#}", e.decorate());
/// // [E0002] user 1 error: some other error
/// //
/// // Don't know much about it to be fair
/// //
/// // Look E0002 in the documentation
///
/// // You can also use the Debug format
/// println!("{:#?}", e.decorate());
/// // CodedError {
/// //     code: "E002",
/// //     inner: user 1 error: some other error,
/// // }
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
            write!(f, "[{}] ", code.code)?;
        }
        write!(f, "{}", self.this.deref())?;
        if f.alternate()
            && let Some(description) = self.this.code().and_then(|c| c.description)
        {
            write!(f, "\n\n{description}")?;
        }

        if let Some(help) = self.this.code().and_then(|c| c.help) {
            if f.alternate() {
                // Add new lines after description
                write!(f, "\n\n{help}")?;
            } else {
                write!(f, ". {help}")?;
            }
        }

        Ok(())
    }
}

impl<T: CodedError> Debug for DecoratedError<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodedError")
            .field("code", &self.this.code())
            .field("inner", &format_args!("{}", self.this.deref()))
            .finish()
    }
}

impl<T: CodedError> std::error::Error for DecoratedError<'_, T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.this.source()
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Code {
    code: &'static str,
    help: Option<&'static str>,
    description: Option<&'static str>,
}

impl Code {
    pub const fn new(
        code: &'static str,
        help: Option<&'static str>,
        description: Option<&'static str>,
    ) -> Self {
        Self {
            code,
            help,
            description,
        }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub const fn help(&self) -> Option<&'static str> {
        self.help
    }

    pub const fn description(&self) -> Option<&'static str> {
        self.description
    }
}

impl Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code)
    }
}

/// Boxed coded error
pub struct BoxedCodedError {
    code: Option<&'static Code>,
    inner: Box<dyn std::error::Error + Send + Sync>,
}

impl CodedError for BoxedCodedError {
    fn code(&self) -> Option<&'static Code> {
        self.code
    }

    fn into_boxed(self) -> BoxedCodedError
    where
        Self: Send + Sync + 'static,
    {
        self
    }
}

impl Display for BoxedCodedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.code() {
            write!(f, "[{}] ", code.code)?;
        }
        write!(f, "{}", self.inner)?;
        if f.alternate()
            && let Some(description) = self.code().and_then(|c| c.description)
        {
            write!(f, "\n\n{description}")?;
        }

        if let Some(help) = self.code().and_then(|c| c.help) {
            if f.alternate() {
                // Add new lines after description
                write!(f, "\n\n{help}")?;
            } else {
                write!(f, ". {help}")?;
            }
        }

        Ok(())
    }
}

impl Debug for BoxedCodedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodedError")
            .field("code", &self.code())
            .field("inner", &format_args!("{}", self.inner))
            .finish()
    }
}

impl std::error::Error for BoxedCodedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}
