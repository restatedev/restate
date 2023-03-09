mod generate;
mod model;

use generate::{generate_input_message_enum, generate_output_message_enum, generate_struct_impl};
use model::RequestResponseChannelInterfaceDefinition;
use quote::quote;

#[proc_macro]
pub fn request_response_channel_interface(
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    do_request_response_channel_interface(input.into()).into()
}

fn do_request_response_channel_interface(
    input: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let item: RequestResponseChannelInterfaceDefinition = match syn::parse2(input) {
        Ok(channel_interface_definition) => channel_interface_definition,
        Err(err) => {
            return err.to_compile_error();
        }
    };
    let struct_impl = generate_struct_impl(item.vis, item.name.clone(), item.methods.clone());
    let input_message_enum = generate_input_message_enum(
        item.name.clone(),
        item.methods
            .iter()
            .cloned()
            .map(|method| (method.sig.ident, method.arguments))
            .collect(),
    );
    let output_message_enum = generate_output_message_enum(
        item.name.clone(),
        item.methods
            .iter()
            .cloned()
            .map(|method| (method.sig.ident, method.return_type))
            .collect(),
    );
    quote!(
        #struct_impl
        #input_message_enum
        #output_message_enum
    )
}
