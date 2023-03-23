pub struct Config {
    pub padding: usize,
    pub code_format: &'static str,
    pub code_help_format: &'static str,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            padding: option_env!("CODEDERROR_PADDING")
                .map(|s| s.parse().expect("CODEDERROR_PADDING should be usize"))
                .unwrap_or(4),
            code_format: option_env!("CODEDERROR_CODE_FORMAT").unwrap_or("{code}"),
            code_help_format: option_env!("CODEDERROR_HELP_FORMAT")
                .unwrap_or("Error code {code_str}"),
        }
    }
}
