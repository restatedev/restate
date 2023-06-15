use super::Schemas;

use prost_reflect::FileDescriptor;

// TODO perhaps rework this interface to hide the prost_reflect types.
//  We should try to not leak prost_reflect types as they can be problematic to implement
//  the consistent schema view in future.
pub trait ProtoSymbolResolver {
    fn list_services(&self) -> &[&str];

    fn get_file_descriptor_by_symbol_name(&self, symbol: &str) -> Option<FileDescriptor>;

    fn get_file_descriptor(&self, file_name: &str) -> Option<FileDescriptor>;
}

impl ProtoSymbolResolver for Schemas {
    fn list_services(&self) -> &[&str] {
        todo!()
    }

    fn get_file_descriptor_by_symbol_name(&self, symbol: &str) -> Option<FileDescriptor> {
        todo!()
    }

    fn get_file_descriptor(&self, file_name: &str) -> Option<FileDescriptor> {
        todo!()
    }
}
