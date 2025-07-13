use clap_complete::Shell;

pub struct ShellConfig {
    pub completion_path: &'static str,
    pub file_extension: &'static str,
    pub post_install_message: &'static str,
}

impl ShellConfig {
    pub const BASH: Self = Self {
        completion_path: ".local/share/bash-completion/completions",
        file_extension: "",
        post_install_message: "You may need to restart your shell or source your ~/.bashrc",
    };

    pub const ZSH: Self = Self {
        completion_path: ".local/share/zsh/site-functions",
        file_extension: "",
        post_install_message: "Completions should be available immediately in new zsh sessions",
    };

    pub const FISH: Self = Self {
        completion_path: ".config/fish/completions",
        file_extension: ".fish",
        post_install_message: "Completions should be available immediately in new fish sessions",
    };

    pub fn for_shell(shell: Shell) -> Option<&'static Self> {
        match shell {
            Shell::Bash => Some(&Self::BASH),
            Shell::Zsh => Some(&Self::ZSH),
            Shell::Fish => Some(&Self::FISH),
            _ => None,
        }
    }

    pub fn file_name(&self, binary_name: &str, shell: Shell) -> String {
        match shell {
            Shell::Zsh => format!("_{}{}", binary_name, self.file_extension),
            _ => format!("{}{}", binary_name, self.file_extension),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bash_config() {
        let config = ShellConfig::for_shell(Shell::Bash).unwrap();
        assert_eq!(config.completion_path, ".local/share/bash-completion/completions");
        assert_eq!(config.file_extension, "");
        assert_eq!(config.file_name("restatectl", Shell::Bash), "restatectl");
    }

    #[test]
    fn test_zsh_config() {
        let config = ShellConfig::for_shell(Shell::Zsh).unwrap();
        assert_eq!(config.completion_path, ".local/share/zsh/site-functions");
        assert_eq!(config.file_extension, "");
        assert_eq!(config.file_name("restatectl", Shell::Zsh), "_restatectl");
    }

    #[test]
    fn test_fish_config() {
        let config = ShellConfig::for_shell(Shell::Fish).unwrap();
        assert_eq!(config.completion_path, ".config/fish/completions");
        assert_eq!(config.file_extension, ".fish");
        assert_eq!(config.file_name("restatectl", Shell::Fish), "restatectl.fish");
    }

    #[test]
    fn test_unsupported_shell() {
        assert!(ShellConfig::for_shell(Shell::Elvish).is_none());
    }
}