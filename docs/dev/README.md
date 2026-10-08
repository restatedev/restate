# Developer documentation

This directory contains documentation relevant for Restate developers.

## Table of contents

[development-guidelines.md](development-guidelines.md) contains guidelines for the development in this repository.

[local-development.md](local-development.md) contains recommendations for how to set up the local development environment.

[rust-guidelines.md](rust-guidelines.md) contains guidelines for coding Rust.

[ordered-key-filtering.md](ordered-key-filtering.md) explains the table markers, filter schemas,
physical key macros, and scan execution using service-load statistics as an example.

[journey-of-stat-query.md](journey-of-stat-query.md) follows a stat query from SQL planning
through local/remote dispatch and RocksDB iteration to the result stream, including review findings.

[debug.md](debug.md) contains some tips for debugging the runtime.

[release.md](release.md) contains explanation for how to release the runtime.

[release-testing.md](release-testing.md) contains the release testing checklist and process for testing before a major/minor release.

[bilrost-migration-guidelines](bilrost-migration-guidelines.md) guidelines on how to create bilrost messages and how to migrate current serde messages to bilrost without breaking compatibility
