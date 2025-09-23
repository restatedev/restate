// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helper macros for restate-types crate.

// Helper macro to generate serialization primitives back and forth for id types with well-defined prefixes.
macro_rules! prefixed_ids {
    (
    $(#[$m:meta])*
    $type_vis:vis enum $typename:ident {
        $(
            $(#[$variant_meta:meta])*
            $variant:ident($variant_prefix:literal),
        )+
    }
    ) => {
        #[allow(clippy::all)]
        $(#[$m])*
        $type_vis enum $typename {
            $(
                $(#[$variant_meta])*
                $variant,
            )+
        }

        impl $typename {
            #[doc = "The prefix string for this identifier"]
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $(
                        $typename::$variant => $variant_prefix,
                    )+
                }
            }
            pub fn iter() -> ::core::slice::Iter<'static, $typename> {
                static VARIANTS: &'static [$typename] = &[
                    $(
                        $typename::$variant,
                    )+
                ];
                VARIANTS.iter()
            }
        }


        #[automatically_derived]
        impl ::core::str::FromStr for $typename {
            type Err = crate::errors::IdDecodeError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $(
                        $variant_prefix => Ok($typename::$variant),
                    )+
                    _ => Err(crate::errors::IdDecodeError::UnrecognizedType(value.to_string())),
                }
            }
        }

    };
}

pub(crate) use prefixed_ids;
