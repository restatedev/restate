# Compatibility fixtures

Bytes written by released Restate versions. Every later version must keep decoding them, so a
test that fails on one of these files means a compatibility break, not a stale fixture.

## Rules

1. Never edit or regenerate an existing fixture. Add a new file instead.
2. Name a fixture after the release that wrote it, for example `nodes_config/v1.8.1.flexbuffers`.
3. Generate fixtures from the release tag, not from a development branch.
4. Tests assert the decoded content, not the bytes, so harmless encoder changes do not fail them.

## Adding a fixture for a release

Check out the release tag and run the generator, which refuses to overwrite an existing file:

```sh
RESTATE_FIXTURE_PATH=$PWD/crates/types/tests/fixtures/nodes_config/v1.9.0.flexbuffers \
  cargo nextest run -p restate-types --run-ignored only -E 'test(write_nodes_config_fixture)'
```

Copy the file into this directory on the main branch, and add assertions for its content to
`decodes_nodes_config_fixtures` in `crates/types/src/nodes_config.rs`. Every file in
`nodes_config/` is decoded by that test automatically.

## Provenance

| Fixture | Written by | How |
| --- | --- | --- |
| `nodes_config/v1.8.1.flexbuffers` | v1.8.1 tag, commit `20efb1b83a39` | That release has no generator, so the generator from `crates/types/src/nodes_config.rs` was added to an export of the tag, without the address lists that v1.8.1 does not have. |
