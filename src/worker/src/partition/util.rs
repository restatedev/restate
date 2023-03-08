/// Unwraps the inner value of a given enum variant.
///
/// # Panics
/// If the enum variant does not match the given enum variant, it panics.
///
/// # Example
///
/// ```
/// use worker::enum_inner;
/// enum Enum {
///     A(u64),
///     B(String),
/// }
///
/// let variant = Enum::A(42);
///
/// let inner = enum_inner!(variant, Enum::A);
/// assert_eq!(inner, 42);
/// ```
///
/// ## Expansion
///
/// The given example will expand to:
///
/// ```no_run
/// enum Enum {
///     A(u64),
///     B(String),
/// }
///
/// let variant = Enum::A(42);
///
/// let inner = match variant {
///     Enum::A(inner) => inner,
///     _ => panic!()
/// };
/// ```
#[macro_export]
macro_rules! enum_inner {
    ($ty:expr, $variant:path) => {
        match $ty {
            $variant(inner) => inner,
            _ => panic!("Unexpected enum type"),
        }
    };
}
