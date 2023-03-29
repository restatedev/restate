/// This macro generates const declaration of Code for each defined code.
/// If the feature `include_doc` is enabled, this will load the code description in the binaries.
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
            Some(concat!("For more details, look at the docs with https://restate.dev/doc/errors/", stringify!($code))),
            paste::paste! { [< $code _DESCRIPTION >] }
        );
    };
}
