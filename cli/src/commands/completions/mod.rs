use crate::CliApp;
use restate_cli_util::{completion_commands, completions::CompletionProvider};

// Use the completion_commands macro to generate standard completion structures
completion_commands!(CliApp);
impl CompletionProvider for CliApp {
    // for cases when the package name differs from the binary name
    fn default_binary_name() -> Option<String> {
        Some("restate".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use restate_cli_util::completions::CompletionProvider;

    #[test]
    fn test_completion_provider() {
        let binary_name = CliApp::completion_binary_name();
        assert_eq!(binary_name, "restate");
    }
}
