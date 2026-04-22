# restate-platform

Foundation crate for the Restate project. Provides key type aliases, traits, and
essential types that the rest of the codebase can safely and reliably depend upon.

This crate sits at the bottom of the dependency graph and has minimal
dependencies of its own, making it suitable for use by any crate in the
workspace without pulling in heavy transitive dependencies.

## What belongs here

- Common traits (error handling, networking, memory estimation)
- Lightweight type aliases and marker types
- Re-exports that unify scattered definitions under a single import path

## Prelude

`restate_platform::prelude::*` provides a curated set of the most commonly used
types and traits, intended as a single convenient import for crates across the
project.

## What does NOT belong here

- Concrete implementations or business logic
- Types with heavy dependencies (serialization frameworks, async runtimes, storage engines)
- Anything that would cause circular dependencies
