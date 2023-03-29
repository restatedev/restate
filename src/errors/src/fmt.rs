use codederror::CodedError;

#[macro_export]
macro_rules! info_it {
    ($err:expr) => {
        tracing::info!(error = tracing::field::display($err.decorate()));
        $err.print_description_as_markdown();
    };
    ($err:expr, $($field:tt)*) => {
        tracing::info!(error = tracing::field::display($err.decorate()), $($field)*);
        $err.print_description_as_markdown();
    };
}

#[macro_export]
macro_rules! warn_it {
    ($err:expr) => {
        tracing::warn!(error = tracing::field::display($err.decorate()));
        $err.print_description_as_markdown();
    };
    ($err:expr, $($field:tt)*) => {
        tracing::warn!(error = tracing::field::display($err.decorate()), $($field)*);
        $err.print_description_as_markdown();
    };
}

#[macro_export]
macro_rules! error_it {
    ($err:expr) => {
        tracing::error!(error = tracing::field::display($err.decorate()));
        $err.print_description_as_markdown();
    };
    ($err:expr, $($field:tt)*) => {
        tracing::error!(error = tracing::field::display($err.decorate()), $($field)*);
        $err.print_description_as_markdown();
    };
}

trait CodedErrorExt {
    fn print_description_as_markdown(&self);
}

#[cfg(feature = "include_doc")]
impl<CE> CodedErrorExt for CE
where
    CE: CodedError,
{
    fn print_description_as_markdown(&self) {
        if let Some(description) = self.code().and_then(codederror::Code::description) {
            println!("{}", termimad::term_text(description))
        }
    }
}

#[cfg(not(feature = "include_doc"))]
impl<CE> CodedErrorExt for CE
where
    CE: CodedError,
{
    fn print_description_as_markdown(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::RT0001;
    use test_utils::test;

    #[derive(thiserror::Error, CodedError, Debug)]
    #[code(RT0001)]
    #[error("my error")]
    pub struct MyError;

    #[test]
    fn test_printing_error() {
        let error = MyError {};
        error_it!(error);
        error_it!(error, "My error message {}", 1);
    }

    #[test]
    fn test_printing_warn() {
        let error = MyError {};
        warn_it!(error);
        warn_it!(error, "My error message {}", 1);
    }

    #[test]
    fn test_printing_info() {
        let error = MyError {};
        info_it!(error);
        info_it!(error, "My error message {}", 1);
    }
}
