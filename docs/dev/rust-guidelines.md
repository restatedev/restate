# Rust guidelines

This document contains a collection of guidelines for developing Rust.

All the guidelines available in https://rust-lang.github.io/api-guidelines/checklist.html apply as well.

## Modules, naming, visibility and re-exports

* Avoid repeating the module name in struct/traits, either use module qualified names or re-exports. For example, given you need to name the error struct overarching the module `runtime::dispatcher`, don't name it `DispatcherError`, just name it `Error` and, when using it in another crate, either refer to it as `dispatcher::Error` or import it with alias `use dispatcher::Error as DispatcherError` (former is preferred)
* Avoid exposing struct fields, but rather provide getters/setters
* Carefully consider when making a trait/struct `pub` or `pub(crate)`.
* The `mod.rs` file should not contain any logic, but only re-exports, constants and eventually type definitions (without logic, sometimes referred as POROs) and traits

## Event loops design

When designing event loops, certain patterns should be followed:

* Don't expose channels, but rather structs/interfaces for interacting with the underlying channels. This is helpful for mocking and for hiding certain implementation details of the channel interaction. Make sure these structs/interfaces are cheap to `Clone`.
* The event loop should own the channels, rather than getting them as input. For example, you should not inject the channels as input of the `new` function like this:

```rust
let (tx, rx) = ...

let event_loop = EventLoop::new(rx);
let other_component = OtherComponent::new(tx);
```

But you should rather do this:

```rust
let event_loop = EventLoop::new(); // new internally creates (tx, rx)
let other_component= OtherComponent::new(event_loop.create_communication_struct());
```


## Error design

* Use `thiserror` whenever possible to implement errors.
