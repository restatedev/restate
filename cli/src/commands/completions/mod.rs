use crate::CliApp;
use clap::CommandFactory;

// Use the completion_commands macro to generate standard completion structures
restate_cli_util::completion_commands!(CliApp);

// Implement CompletionProvider for CliApp
impl restate_cli_util::completions::CompletionProvider for CliApp {
    fn completion_binary_name() -> String {
        Self::command()
            .get_bin_name()
            .unwrap_or("restate")
            .to_string()
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
