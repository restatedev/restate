# Release Notes Process

This directory contains release notes for Restate, organized to track changes between releases.

## Structure

```
release-notes/
├── README.md          # This file
├── v1.6.0.md          # Consolidated release notes for v1.6.0
├── v1.7.0.md          # (future releases follow the same pattern)
└── unreleased/        # Release notes for changes not yet released
    └── *.md           # Individual release note files
```

## Adding Release Notes

When making a significant change that affects users, create a release note file in the `unreleased/` directory:

1. **Create a new file** in `unreleased/` with a descriptive name:
   - Format: `<issue-number>-<short-description>.md`
   - Example: `3961-change-abort-timeout-default.md`

2. **Structure your release note** with the following sections:
   ```markdown
   # Release Notes for Issue #<number>: <Title>

   ## Behavioral Change / New Feature / Bug Fix / Breaking Change

   ### What Changed
   Brief description of what changed

   ### Why This Matters
   Explain the impact and reasoning

   ### Impact on Users
   - How this affects existing deployments
   - How this affects new deployments
   - Any migration considerations

   ### Migration Guidance
   Steps users should take if needed, including:
   - Configuration changes
   - Code changes
   - Upgrade considerations

   ### Related Issues
   - Issue #XXX: Description
   ```

3. **Commit the release note** with your changes:
   ```bash
   git add release-notes/unreleased/<your-file>.md
   git commit -m "Add release notes for <change>"
   ```

## Release Process

When creating a new release:

1. **Review all unreleased notes**: Check `unreleased/` for all pending release notes

2. **Create a consolidated release notes file** (`v<version>.md`) with the following structure:

   ```markdown
   # Restate v<version> Release Notes

   ## Highlights
   - 3-5 bullet points summarizing the most important changes

   ## Table of Contents
   (Links to all sections below)

   ## Breaking Changes
   (Items sorted by impact, most impactful first)

   ## Deprecations

   ## New Features
   (Items sorted by impact, most impactful first)

   ## Improvements
   ### Behavioral Changes
   ### Observability
   ### Stability and Security
   ### CLI Improvements
   ### Helm Chart

   ## Bug Fixes
   (Grouped by area: Kafka, Invocations, Memory, CLI, UI, Metadata, etc.)
   ```

3. **Consolidation guidelines**:
   - Sort items by impact within each category (most impactful first)
   - Preserve all migration guidance, configuration examples, and code snippets
   - Keep all related issue/PR links
   - For items spanning multiple categories (e.g., both breaking change and new feature), place in the primary category with full context

4. **Delete the individual unreleased files** after consolidation:
   ```bash
   rm release-notes/unreleased/*.md
   ```

5. **Use the consolidated notes** to prepare:
   - GitHub release description
   - Documentation updates
   - Blog post content (if applicable)

## Guidelines

### When to Write a Release Note

Write a release note for:
- **Breaking changes**: Any change that requires user action or breaks existing functionality
- **Behavioral changes**: Changes to defaults, timeouts, or system behavior
- **New features**: User-facing features or capabilities
- **Important bug fixes**: Fixes that significantly impact reliability or security
- **Deprecations**: Features or APIs being deprecated

### When NOT to Write a Release Note

Skip release notes for:
- Internal refactoring with no user impact
- Test changes
- Documentation-only changes (unless significant)
- Minor dependency updates
- Build system changes

### Writing Style

- **Be clear and concise**: Users should quickly understand the change
- **Focus on impact**: Explain what users need to know and do
- **Provide examples**: Include configuration snippets or code examples
- **Link to documentation**: Reference detailed docs when available
- **Be honest about breaking changes**: Don't hide backwards-incompatible changes

## Examples

### Breaking Change Example
```markdown
# Release Notes for Issue #1234: Remove deprecated API endpoint

## Breaking Change

### What Changed
The deprecated `/v1/old-endpoint` API has been removed.

### Impact on Users
- Applications using `/v1/old-endpoint` will receive 404 errors
- The replacement `/v2/new-endpoint` has been available since v1.5.0

### Migration Guidance
Replace all calls to `/v1/old-endpoint` with `/v2/new-endpoint`:

\```bash
# Old
curl http://localhost:8080/v1/old-endpoint

# New
curl http://localhost:8080/v2/new-endpoint
\```

See [migration guide](https://docs.restate.dev/migrate/v2) for details.
```

### Behavioral Change Example
```markdown
# Release Notes for Issue #3961: Change abort timeout default

## Behavioral Change

### What Changed
The default `abort-timeout` has been increased from 1 minute to 10 minutes.

### Impact on Users
- New deployments: Use 10-minute default automatically
- Existing deployments: Will adopt new default on upgrade
- Services now have more time for graceful shutdown

### Migration Guidance
To keep the previous 1-minute timeout:

\```toml
[worker.invoker]
abort-timeout = "1m"
\```
```
