use once_cell::sync::Lazy;
use prost_reflect::DescriptorPool;

pub mod greeter {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]
    include!(concat!(env!("OUT_DIR"), "/greeter.rs"));
}

pub mod test {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

pub static DESCRIPTOR_POOL: Lazy<DescriptorPool> = Lazy::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set_test.bin")).as_ref(),
    )
    .expect("The built-in descriptor pool should be valid")
});

pub const GREETER_SERVICE_NAME: &str = "greeter.Greeter";
pub const ANOTHER_GREETER_SERVICE_NAME: &str = "greeter.AnotherGreeter";
