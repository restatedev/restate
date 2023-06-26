//! Useful test utilities for Restate crates. Import them with:
//!
//! ```rust
//! use restate_test_util::{assert, test, assert_vec, assert_eq, assert_ne};
//! ```
//!
//! Note: You cannot import them with a glob import, as the compiler won't be able to distinguish our imports and the stdlib imports.

/// Macro for assert Vec properties. Examples:
///
/// ```rust
/// use restate_test_util::assert_vec;
///
/// let v: Vec<String> = vec![];
/// assert_vec!(v => is_empty);
/// ```
///
/// ```rust
/// use restate_test_util::assert_vec;
///
/// let mut v = vec![];
/// v.push(1_u16);
/// assert_vec!(v => len == 1);
/// assert_vec!(v => contains_once |el: &u16| *el == 1_u16);
/// assert_vec!(v => contains 1 |el: &u16| *el == 1_u16);
/// ```
///
#[macro_export]
macro_rules! assert_vec {
    ($vec:expr => is_empty) => {
        $crate::assert_vec!($vec => len == 0);
    };
    ($vec:expr => len == $n:literal) => {
        assert_eq!($vec.len(), $n);
    };
    ($vec:expr => contains $n:literal $closure:expr) => {
        let count = $vec.iter().filter(|e| $closure(*e)).count();
        if count == 0 {
            panic!("Cannot find in {} an item respecting the closure", stringify!($vec));
        }
        if count != $n {
            panic!("Found in {} more than {} item respecting the closure", stringify!($vec), stringify!($n));
        }
    };
    ($vec:expr => contains_once $closure:expr) => {
        $crate::assert_vec!($vec => contains 1 $closure);
    };
}

/// Macro to assert no message is received.
///
/// Note: this check is based on a timeout, hence based on the context it might not be enough to use
/// this assert to check the behaviour of the producers.
#[macro_export]
macro_rules! assert_no_recv {
    ($ch:expr) => {
         assert!(let Err(_) = tokio::time::timeout(std::time::Duration::from_millis(100), $ch.recv()).await);
    };
}

// A couple of useful re-exports
pub use assert2::{assert, check, let_assert};
pub use pretty_assertions::{assert_eq, assert_ne};
pub use test_log::test;
