use crate::model::{
    RequestArgument, RequestResponseMethodDefinition,
};
use convert_case::Case::UpperCamel;
use convert_case::Casing;
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::{FnArg, Signature, Type, Visibility};

pub fn generate_struct_impl(
    vis: Visibility,
    struct_name: Ident,
    methods: Vec<RequestResponseMethodDefinition>,
) -> TokenStream {
    let input_message_ident = input_message_ident(&struct_name);
    let output_message_ident = output_message_ident(&struct_name);
    let request_response_sender_ident =
        quote!(futures_util::channel_interface::RequestResponseSender);

    let struct_def = quote!(
        #[derive(Clone)]
        #vis struct #struct_name(#request_response_sender_ident<#input_message_ident, #output_message_ident>);
    );

    let mut methods_impl = Vec::with_capacity(methods.len());
    for method_def in methods {
        let sig = method_def.sig;
        let variant_ident = variant_ident(&sig.ident);
        let input_message_args: Vec<_> = method_def.arguments.iter().map(|req| req.name.clone()).collect();

        methods_impl.push(
            quote!(
                pub #sig {
                    let input_msg = #input_message_ident::#variant_ident{#(#input_message_args),*};
                    let res = #request_response_sender_ident::send_and_receive(&self.0, input_msg).await;
                    match res {
                        Ok(#output_message_ident::#variant_ident(res)) => res,
                        Err(closed_error) => return Err(closed_error.into()),
                        _ => panic!("Received unexpected variant of #output_message_ident")
                    }
                }
            )
        );
    }

    let impl_struct = quote!(
        impl #struct_name {
           pub fn new(sender: impl Into<#request_response_sender_ident<#input_message_ident, #output_message_ident>>) -> Self {
                Self(sender.into())
            }

            #(#methods_impl)*
        }
    );

    quote!(
        #struct_def
        #impl_struct
    )
}

pub fn generate_input_message_enum(
    struct_name: Ident,
    methods: Vec<(Ident, Vec<RequestArgument>)>,
) -> TokenStream {
    let input_message_ident = input_message_ident(&struct_name);

    let mut variants = Vec::with_capacity(methods.len());
    for (method_ident, request_args) in methods {
        let variant_ident = variant_ident(&method_ident);
        let variant_fields: Vec<_> = request_args
            .iter()
            .map(|req| {
                let name = &req.name;
                let ty = &req.ty;
                quote!(#name: #ty)
            })
            .collect();
        variants.push(quote!(#variant_ident{#(#variant_fields),*}));
    }

    quote!(
        #[derive(Debug)]
        pub enum #input_message_ident {
            #(#variants),*
        }
    )
}

pub fn generate_output_message_enum(
    struct_name: Ident,
    methods: Vec<(Ident, Box<Type>)>,
) -> TokenStream {
    let output_message_ident = output_message_ident(&struct_name);

    let mut variants = Vec::with_capacity(methods.len());
    for (method_ident, return_type) in methods {
        let variant_ident = variant_ident(&method_ident);
        variants.push(quote!(#variant_ident(#return_type)));
    }

    quote!(
        #[derive(Debug)]
        pub enum #output_message_ident {
            #(#variants),*
        }
    )
}

fn variant_ident(method_name_ident: &Ident) -> Ident {
    let upper_camel_method_name = method_name_ident.to_string().to_case(UpperCamel);
    format_ident!("{}", upper_camel_method_name)
}

fn input_message_ident(struct_name: &Ident) -> Ident {
    format_ident!("{}Input", struct_name)
}

fn output_message_ident(struct_name: &Ident) -> Ident {
    format_ident!("{}Output", struct_name)
}
