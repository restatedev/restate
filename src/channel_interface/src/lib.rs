mod model;
mod generate;

use quote::quote;
use generate::{generate_input_message_enum, generate_output_message_enum, generate_struct_impl};
use model::ChannelInterfaceDefinition;

#[proc_macro]
pub fn channel_interface(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    do_channel_interface(input.into()).into()
}

fn do_channel_interface(input: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let item: ChannelInterfaceDefinition = match syn::parse2(input) {
        Ok(channel_interface_definition) => channel_interface_definition,
        Err(err) => {
            return err.to_compile_error();
        }
    };
    let struct_impl = generate_struct_impl(&item);
    let input_message_enum = generate_input_message_enum(&item);
    let output_message_enum = generate_output_message_enum(&item);
    quote!(
        use futures_util::

        #struct_impl
        #input_message_enum
        #output_message_enum
    )
}

