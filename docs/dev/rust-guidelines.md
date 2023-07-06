# Rust guidelines

This document contains a collection of guidelines for developing Rust.

All the guidelines available in https://rust-lang.github.io/api-guidelines/checklist.html apply as well.

## Modules, naming, visibility and re-exports

* Avoid repeating the module name in struct/traits definitions. When consuming, use module qualified names or re-exports. 
  For example, the error struct representing an `invoker` error should be defined as `Error`, not `InvokerError`. 
  When consuming it, it should be referred as `invoker::Error` or imported as `use invoker::Error as InvokerError`.
* Avoid defining the struct field's visibility as `pub` or even `pub(crate)`, but rather use getters/setters. There are two notable exceptions to this rule:
  * When a type needs to be deconstructed, to take ownership of the single fields, which would be rather heavy to `Clone`. In these cases, either make the field `pub` or provide a [`into_inner(self)` deconstructor](https://users.rust-lang.org/t/explanation-of-into-inner/13872) method. 
  * When defining [Passive data structures](https://en.wikipedia.org/wiki/Passive_data_structure), such as Restate domain types in `restate_types` crate. In these cases, it is fine to declare the field as `pub` for ease to use.
* The crate entrypoint (`lib.rs`) file should contain `pub` (re-)exports to the crate public types.

### `restate-types`

The `restate-types` module contains the Restate "core" domain types that should be shared among all/most of Restate components,
such as IDs, etc. It should not contain types belonging to specific components interfaces.

As a rule of thumb, when defining a new component's interface, define its types as close as possible to the interface itself and,
if multiple interfaces needs the same type, move it to `restate-types` afterward.

### Util modules

When implementing a utility method/type to interact with 3rd party library, that should be shared by different Restate components, 
place it in a util module that groups all the utilities to interact with that specific library, e.g. `restate_serde_util`. 
Alternatively, you could also place it in ad-hoc module.

## Restate component design

Each Restate internal "component" is usually composed by the following building blocks:

* An `Options` type, a serdeable struct defining the different configuration options. 
  The `Options` struct should derive `serde::{Serialize, Deserialize}` in order to read/write it from user's configuration file, 
  and `schemars::JsonSchema` in order to generate the configuration schema. The `Options` type has a method `build()` that returns the `Service`.
* A `Service` struct, that is a struct implementing the component logic. 
  The `Service` type has a method `run(self, {...}, drain: drain::Watch)` executing the event loop, where the `drain` is used to listen on the shutdown signal.
* A `Handle` trait, used to interact with the `Service`. 
  Usually the implementation of this trait, under the hood, contains a channel to send messages to the `Service` event loop. 
  `Handle` instances should be provided by the `Service`, for example with methods such as `Service::handle(&self) -> Handle`. 
  `Handle` implementations should be cheap to `Clone`.
* One or more `Error` types, implemented with `thiserror` and possibly defining error codes with `CodedError`.

When possible, split the component between `_api` and `_impl` crate, where `_api` should contain all the necessary types to interact with the component without executing it (e.g. `Handle` and `Error` types)

### Event loops

When designing event loops, certain patterns should be followed:

* Don't expose channels themselves, but always go through the `Handle` or similar structs/interfaces for interacting with the underlying channels. This is helpful for mocking and for hiding certain implementation details of the channel interaction. Make sure these structs/interfaces are cheap to `Clone`.
* The event loop should own the channels, rather than getting them as input. For example, you should not inject the channels as input of the `new` function like this:

```rust
let (tx, rx) = ...

let service = Service::new(rx);
let other_service = OtherService::new(tx);
```

But you should rather do this:

```rust
let service = Service::new(); // new internally creates (tx, rx)
let other_service = OtherService::new(service.handle());
```

### Asynchronous communication

When developing components which communicate via channels (message passing) with each other, keep in mind that message ordering can only be maintained when using a single channel.
As soon as there are two different channels between these components, the order in which messages are consumed from them can in the general case not be guaranteed.
The result can be unforeseen race conditions.

## Error design

* Use `thiserror` whenever possible to implement errors containing different "error kind" variants.
* Don't use `Box<dyn std::error::Error>` but rather use `anyhow::Error` for generic errors/error type erasure.
* `anyhow::Error` should be used only when it is irrelevant for the consumer of the fallible method to distinguish the error type. 
  It is fine to use `anyhow::Error` as error source when defining a variant of your error type using `thiserror`, in order to hide the implementation details of the fallible method.   
* When defining error types, use `CodedError` to document how to troubleshoot the error. These documentation pages are automatically embedded in the Restate documentation. Check `restate_errors` for more details.
