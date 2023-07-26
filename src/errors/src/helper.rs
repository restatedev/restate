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
            Some(concat!("For more details, look at the docs with https://docs.restate.dev/errors#", stringify!($code))),
            paste::paste! { [< $code _DESCRIPTION >] }
        );
    };
}
