# CLI: Retry editor on JSON parse error in `restate state edit`

## Improvement

### What Changed

`restate state edit` now re-opens the editor when the edited JSON fails to parse, instead of discarding your work.

### Impact on Users

- Interactive use: you'll see the parse error and a prompt to re-open the editor
- Non-interactive use (`--yes` / CI): behavior is unchanged — the command fails immediately on invalid JSON
