use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned, ToTokens};
use std::collections::HashMap;
use strfmt::strfmt;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, LitInt, LitStr, Member, Result, Visibility};

use crate::ast::{Enum, Field, Input, Struct};
use crate::attr::Code;
use crate::generics::InferredBounds;
use crate::Config;

pub fn derive(config: Config, node: &DeriveInput) -> Result<TokenStream> {
    let input = Input::from_syn(node)?;
    input.validate()?;
    Ok(match input {
        Input::Struct(input) => impl_struct(config, input),
        Input::Enum(input) => impl_enum(config, input),
    })
}

fn impl_struct(config: Config, input: Struct) -> TokenStream {
    let ty = &input.ident;
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();
    let mut coded_error_inferred_bounds = InferredBounds::new();

    let code_body = if let Some(code) = input.attrs.code {
        impl_code(&config, &code)
    } else {
        // Delegate code to field
        let code_field = input.code_field().unwrap();
        if code_field.contains_generic {
            coded_error_inferred_bounds.insert(code_field.ty, quote!(codederror::CodedError));
        }
        let member = &code_field.member;
        quote! { self.#member.code() }
    };
    let code_method = quote! {
        fn code(&self) -> std::option::Option<codederror::Code> {
            #code_body
        }
    };

    let mut fmt_hints_body = quote! {};
    // Print the hint from the field first, as it might be more useful because it's more specialized
    if let Some(hint_field) = input.hint_field() {
        if hint_field.contains_generic {
            coded_error_inferred_bounds.insert(hint_field.ty, quote!(codederror::CodedError));
        }
        let member = &hint_field.member;
        fmt_hints_body = quote! {
            #fmt_hints_body
            self.#member.fmt_hints(__formatter)?;
        }
    }
    for display in input.attrs.hints {
        fmt_hints_body = quote! {
            #fmt_hints_body
            __formatter.write_fmt_next(format_args!(#display))?;
        }
    }
    fmt_hints_body = quote! {
        #fmt_hints_body
        std::result::Result::Ok(())
    };
    let fmt_hints_method = quote! {
        fn fmt_hints(&self, __formatter: &mut codederror::HintFormatter<'_, '_>) -> std::fmt::Result {
            #fmt_hints_body
        }
    };

    let coded_error_trait = spanned_coded_error_trait(input.original);
    let coded_error_where_clause = coded_error_inferred_bounds.augment_where_clause(input.generics);
    quote! {
        #[allow(unused_variables)]
        impl #impl_generics #coded_error_trait for #ty #ty_generics #coded_error_where_clause {
            #code_method
            #fmt_hints_method
        }
    }
}

fn impl_enum(config: Config, input: Enum) -> TokenStream {
    let ty = &input.ident;
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();
    let mut coded_error_inferred_bounds = InferredBounds::new();

    // Generate code()
    let root_code = input.attrs.code.map(|code| impl_code(&config, &code));
    let code_method_arms = input.variants.iter().map(|variant| {
        let ident = &variant.ident;
        let pat = fields_pat(&variant.fields);
        if let Some(code) = variant.attrs.code {
            let code = impl_code(&config, &code);
            quote! {
                #ty::#ident #pat => #code,
            }
        } else if let Some(code_field) = variant.code_field() {
            if code_field.contains_generic {
                coded_error_inferred_bounds.insert(code_field.ty, quote!(codederror::CodedError));
            }
            let member = member_identifier(code_field);
            quote! {
                #ty::#ident #pat => #member.code(),
            }
        } else {
            let code = root_code.clone().unwrap();
            quote! {
                #ty::#ident #pat => #code,
            }
        }
    });
    let code_method = quote! {
        fn code(&self) -> std::option::Option<codederror::Code> {
           match self {
                #(#code_method_arms)*
            }
        }
    };

    // Generate fmt_hints()
    let root_hints = input.attrs.hints;
    let fmt_hint_method_arms = input.variants.iter().map(|variant| {
        let ident = &variant.ident;
        let pat = fields_pat(&variant.fields);

        let mut fmt_hints_body = quote!();
        if let Some(hint_field) = variant.hint_field() {
            if hint_field.contains_generic {
                coded_error_inferred_bounds.insert(hint_field.ty, quote!(codederror::CodedError));
            }
            let member = member_identifier(hint_field);
            fmt_hints_body = quote! {
                #fmt_hints_body
                #member.fmt_hints(__formatter)?;
            };
        }

        // Append variant and enum hints
        let mut hints = variant.attrs.hints.clone();
        hints.extend_from_slice(&root_hints);
        for display in hints {
            fmt_hints_body = quote! {
                #fmt_hints_body
                __formatter.write_fmt_next(format_args!(#display))?;
            }
        }

        quote! {
            #ty::#ident #pat => {
                #fmt_hints_body
                std::result::Result::Ok(())
            },
        }
    });
    let fmt_hints_method = quote! {
        fn fmt_hints(&self, __formatter: &mut codederror::HintFormatter<'_, '_>) -> std::fmt::Result {
           match self {
                #(#fmt_hint_method_arms)*
            }
        }
    };

    let coded_error_trait = spanned_coded_error_trait(input.original);
    let coded_error_where_clause = coded_error_inferred_bounds.augment_where_clause(input.generics);
    quote! {
        #[allow(unused_variables)]
        impl #impl_generics #coded_error_trait for #ty #ty_generics #coded_error_where_clause {
            #code_method
            #fmt_hints_method
        }
    }
}

fn fields_pat(fields: &[Field]) -> TokenStream {
    let mut members = fields.iter().map(|field| &field.member).peekable();
    match members.peek() {
        Some(Member::Named(_)) => quote!({ #(#members),* }),
        Some(Member::Unnamed(_)) => {
            let vars = members.map(|member| match member {
                Member::Unnamed(member) => format_ident!("_{}", member),
                Member::Named(_) => unreachable!(),
            });
            quote!((#(#vars),*))
        }
        None => quote!({}),
    }
}

fn member_identifier(field: &Field) -> TokenStream {
    match &field.member {
        Member::Named(n) => n.into_token_stream(),
        Member::Unnamed(i) => {
            proc_macro2::Ident::new(&format!("_{}", i.index), i.span()).into_token_stream()
        }
    }
}

fn spanned_coded_error_trait(input: &DeriveInput) -> TokenStream {
    let vis_span = match &input.vis {
        Visibility::Public(vis) => Some(vis.pub_token.span()),
        Visibility::Crate(vis) => Some(vis.crate_token.span()),
        Visibility::Restricted(vis) => Some(vis.pub_token.span()),
        Visibility::Inherited => None,
    };
    let data_span = match &input.data {
        Data::Struct(data) => data.struct_token.span(),
        Data::Enum(data) => data.enum_token.span(),
        Data::Union(data) => data.union_token.span(),
    };
    let first_span = vis_span.unwrap_or(data_span);
    let last_span = input.ident.span();
    let path = quote_spanned!(first_span=> codederror::);
    let error = quote_spanned!(last_span=> CodedError);
    quote!(#path #error)
}

fn impl_code(config: &Config, code: &Code) -> TokenStream {
    if let Some(code_value) = code.value {
        let code_padded = format!("{:0padding$}", code_value, padding = config.padding);
        let mut vars = HashMap::new();
        vars.insert("code".to_string(), code_padded);

        let value_lit = LitInt::new(&code_value.to_string(), code.original.span());
        let code_str = strfmt(config.code_format, &vars).unwrap();
        let code_str_lit = LitStr::new(&code_str, code.original.span());

        vars.insert("code_str".to_string(), code_str);
        let help_str_lit = LitStr::new(
            &strfmt(config.code_help_format, &vars).unwrap(),
            code.original.span(),
        );
        quote! {
            std::option::Option::Some(
                codederror::Code {
                    value: #value_lit,
                    code_str: #code_str_lit,
                    help_str: #help_str_lit,
                }
            )
        }
    } else {
        quote! {
            std::option::Option::None
        }
    }
}
