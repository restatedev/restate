pub struct DisplayFromStr;

#[macro_export]
macro_rules! bilrost_as_display_from_str {
    ($ty:ty) => {
        impl ::bilrost::encoding::ForOverwrite<(), $ty> for () {
            fn for_overwrite() -> $ty {
                ::core::default::Default::default()
            }
        }

        impl ::bilrost::encoding::EmptyState<(), $ty> for () {
            fn is_empty(val: &$ty) -> bool {
                *val == ::core::default::Default::default()
            }

            fn clear(val: &mut $ty) {
                *val = ::core::default::Default::default();
            }
        }

        impl ::bilrost::encoding::Proxiable<$crate::bilrost_encodings::display_from_str::DisplayFromStr> for $ty
        where
            $ty: ::std::fmt::Display + ::std::str::FromStr,
        {
            type Proxy = ::std::string::String;

            fn new_proxy() -> ::std::string::String {
                ::std::string::String::new()
            }

            fn encode_proxy(&self) -> ::std::string::String {
                self.to_string()
            }

            fn decode_proxy(&mut self, proxy: ::std::string::String) -> ::core::result::Result<(), ::bilrost::DecodeErrorKind> {
                *self = proxy
                    .parse::<Self>()
                    .map_err(|_| ::bilrost::DecodeErrorKind::InvalidValue)?;
                Ok(())
            }
        }

        ::bilrost::delegate_proxied_encoding!(
            use encoding (::bilrost::encoding::General)
            to encode proxied type ($ty)
            using proxy tag ($crate::bilrost_encodings::display_from_str::DisplayFromStr)
            with general encodings
        );
    }
}
