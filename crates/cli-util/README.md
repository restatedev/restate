# Restate CLI Utilities

A framework for building consistent, user-friendly command-line interfaces. This crate provides styling primitives, output macros, and utilities that ensure a cohesive look and feel across all Restate CLI tools.

## Quick Start

```rust
use restate_cli_util::{
    CliContext, CommonOpts,
    c_println, c_success, c_error, c_warn, c_tip, c_title,
    c_indentln, c_indent_table,
};
use restate_cli_util::ui::console::{Icon, Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use comfy_table::Table;

// Initialize the CLI context (typically done once at startup)
let opts = CommonOpts::default();
CliContext::new(opts).set_as_global();

// Print styled output
c_success!("Operation completed successfully!");
c_error!("Something went wrong: {}", error_message);
c_warn!("This action is destructive!");
c_tip!("You can use --force to skip confirmation");

// Create styled tables
let mut table = Table::new_styled();
table.add_kv_row("Name:", "my-service");
table.add_kv_row("Status:", Styled(Style::Success, "running"));
c_println!("{}", table);
```

## CLI Style Guide

This section documents the visual conventions and patterns used across Restate CLI tools.

### Output Channels

| Channel | Use Case | Example |
|---------|----------|---------|
| `stdout` | Primary output, data for piping | Command results, tables, JSON |
| `stderr` | Errors, warnings, progress, tips | Error messages, confirmations |

**Rule:** Data that users might pipe to other tools goes to stdout. Everything else goes to stderr.

### Color Semantics

| Style | Color | Usage |
|-------|-------|-------|
| `Style::Success` | Green | Positive outcomes, additions, running states |
| `Style::Danger` | Red + Bold | Errors, removals, critical warnings |
| `Style::Warn` | Magenta | Caution, updates, draining states |
| `Style::Info` | Bright + Bold | Important values (IDs, keys), emphasis |
| `Style::Notice` | Italic | Secondary information, tips |
| `Style::Normal` | Default | Regular text |

### Standard Icons

| Icon | Constant | Fallback | Usage |
|------|----------|----------|-------|
| `SUCCESS_ICON` | "[OK]:" | Success messages |
| `ERR_ICON` | "[ERR]:" | Error messages |
| `WARN_ICON` | "[WARNING]:" | Warning boxes |
| `TIP_ICON` | "[TIP]:" | Helpful hints |
| `GLOBE_ICON` | "[GLOBE]:" | Public/external resources |
| `LOCK_ICON` | "[LOCK]:" | Private/protected resources |

**Fallback behavior:** Icons automatically fall back to text when colors are disabled, ensuring accessibility in non-color terminals.

### Output Patterns

#### Success/Error Messages
```rust
c_success!("Deployment '{}' registered successfully!", deployment_id);
c_error!("Failed to connect to admin service: {}", error);
```

#### Warnings (Boxed, Prominent)
```rust
c_warn!(
    "This will remove all data for service '{}'. \
    This action cannot be undone!"
    service_name
);
```

#### Tips (Subtle, Helpful)
```rust
c_tip!(
    "Use 'restate services describe {}' to see handler details",
    service_name
);
```

#### Section Titles
```rust
c_title!("📜", "Service Information");
c_title!("🔌", "Handlers");
```

#### Indented Output
```rust
c_indentln!(1, "- {}", Styled(Style::Success, &service.name));
c_indentln!(2, "Type: {:?}", service.ty);
c_indent_table!(2, handlers_table);
```

### Table Patterns

#### Key-Value Tables (Detail Views)
```rust
let mut table = Table::new_styled();
table.add_kv_row("Name:", &service.name);
table.add_kv_row("Status:", Styled(Style::Success, "running"));
table.add_kv_row_if(
    || deployment.is_some(),
    "Deployment:",
    || deployment.unwrap().id,
);
c_println!("{}", table);
```

#### List Tables (Collection Views)
```rust
let mut table = Table::new_styled();
table.set_styled_header(vec!["NAME", "TYPE", "STATUS"]);
for service in services {
    table.add_row(vec![&service.name, &service.ty, &service.status]);
}
c_println!("{}", table);
```

#### Diff Tables (Comparisons)
```rust
// Use green for additions, red for removals
table.add_row(vec![
    Cell::new("++").fg(Color::Green),
    Cell::new(&new_handler.name).fg(Color::Green),
]);
table.add_row(vec![
    Cell::new("--").fg(Color::Red),
    Cell::new(&removed_handler.name).fg(Color::Red),
]);
```

### Time and Duration Display

```rust
use restate_cli_util::ui::{
    timestamp_as_human_duration,
    duration_to_human_precise,
    duration_to_human_rough,
};
use chrono_humanize::Tense;

// Precise: "5 seconds and 78 ms"
let precise = duration_to_human_precise(duration, Tense::Present);

// Rough: "about 5 minutes ago"  
let rough = duration_to_human_rough(duration, Tense::Past);

// Timestamp respects --time-format flag
let formatted = timestamp_as_human_duration(timestamp, Tense::Past);
```

### Interactive Prompts

```rust
use restate_cli_util::ui::console::{confirm, confirm_or_exit, choose, input};

// Yes/No confirmation (auto-confirms with --yes or in CI)
if confirm("Do you want to proceed?") {
    // User confirmed
}

// Confirmation that exits on "no"
confirm_or_exit("This will delete all data. Continue?")?;

// Selection menu
let options = ["Option A", "Option B", "Option C"];
let selected = choose("Select an option:", &options)?;

// Text input with default
let name = input("Enter name:", "default-name".to_string())?;
```

### Watch Mode

For commands that support continuous refresh:

```rust
use restate_cli_util::ui::watcher::Watch;

#[derive(clap::Args)]
pub struct MyCommand {
    #[clap(flatten)]
    watch: Watch,
}

pub async fn run(opts: &MyCommand) -> Result<()> {
    opts.watch.run(|| async {
        // Your command logic here
        // Output will be refreshed periodically
        Ok(())
    }).await
}
```

## Module Overview

### `ui::console` - Output Macros and Types

Core output primitives that handle broken pipes gracefully (unlike `println!`).

| Macro | Output | Purpose |
|-------|--------|---------|
| `c_println!` | stdout | Print line |
| `c_print!` | stdout | Print (no newline) |
| `c_eprintln!` | stderr | Print line to stderr |
| `c_success!` | stdout | Success message with icon |
| `c_error!` | stderr | Error message with icon |
| `c_warn!` | stderr | Warning box (yellow, bold) |
| `c_tip!` | stderr | Tip box (italic, dim) |
| `c_title!` | stdout | Section title with underline |
| `c_indentln!` | stdout | Indented line |
| `c_indent_table!` | stdout | Indented table |

### `ui::stylesheet` - Visual Constants

Defines the visual language: icons, styles, and table formatting.

### `context` - Global Configuration

`CliContext` manages global settings:
- Color detection (NO_COLOR, TERM, CLICOLOR_FORCE, TTY detection)
- Table style (compact vs. borders)
- Time format (human, ISO-8601, RFC-2822)
- Auto-confirmation (--yes flag, CI environment)
- Network timeouts

### `completions` - Shell Completions

Utilities for generating and installing shell completions (Bash, Zsh, Fish, PowerShell).

### `watcher` - Continuous Refresh

The `Watch` struct enables `-w` flag for commands that benefit from live updates.

## Color Detection Logic

Colors are enabled when all conditions are met:
1. `NO_COLOR` environment variable is unset or set to "0"
2. `TERM` is not "dumb"
3. stdout is a TTY

Override with `CLICOLOR_FORCE=1` to force colors even in pipes.

## Best Practices

1. **Use semantic styles** - Don't hardcode colors; use `Style::Success`, `Style::Danger`, etc.

2. **Provide fallbacks** - Icons automatically fall back to text; ensure your output is readable without colors.

3. **Respect user preferences** - The framework handles `NO_COLOR`, `--yes`, and `--time-format` automatically.

4. **Separate data from decoration** - Put pipeable data on stdout, everything else on stderr.

5. **Be concise** - CLI output should be scannable; use tables and indentation to organize information.

6. **Confirm destructive actions** - Use `confirm()` or `c_warn!()` before irreversible operations.

## Global Options

These options are available to all commands via `CommonOpts`:

| Option | Description |
|--------|-------------|
| `-v`, `-vv`, `-vvv` | Increase verbosity (logging) |
| `-y`, `--yes` | Auto-confirm prompts |
| `--table-style` | `compact` (default) or `borders` |
| `--time-format` | `human` (default), `iso8601`, or `rfc2822` |
| `--connect-timeout` | Connection timeout in ms |
| `--request-timeout` | Request timeout in ms |
