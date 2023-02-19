use std::error::Error;

pub type GenericError = Box<dyn Error + Send + Sync + 'static>;
