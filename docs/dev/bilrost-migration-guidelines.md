# Bilrost best practice
Over the course of integrating bilrost into our project—particularly for network serialization and storage encoding—we’ve developed a few guidelines to streamline its usage. Our primary objective has been to adopt bilrost in a way that preserves compatibility with existing serde-based structures. In some cases, our existing types were already compatible and could derive bilrost::Message directly with minimal changes.

To avoid the overhead of maintaining parallel DTOs, we recommend designing new structures to be bilrost-friendly from the start. This approach simplifies serialization logic and reduces redundancy as the project evolves.

## Supported types

Luckily bilrost supports a wide range of built-in types and 3rd party types as well (for example `ByteString`, `Bytes` , and more) **but** it lacks support for something native like `u128` (same as protobuf btw). This makes it a bit tedious to support something like `Ulid` and all `Ulid` derived IDs.

To work around this I had to create my own custom `U128` which internally stores itss value as a `(u64, u64)` but of course I need to create a DTO for every structure that uses `Ulid` driven IDs.



<aside>
💡

> *Rule #1*: Avoid using u128 🤷 for network/storage types, If you must, use our `::restate_encoding::U128` type.

> *Note*: All Ulid based types in restate repo already can be used in bilrost Messages with no issues. They leverage on `U128` and `BilrostAs` to serialize itself
</aside>

## Enums

`bilrost` makes a clear distinction between a `numeric` enum (represented as an integer value) , and `fat` enums. This was the most annoying thing to work with due to the following

### Numeric (slim) Enums

```rust
// Before
enum MySlimEnum {
	FirstVariant,
	SecondVariant
}
```

```rust
// After
#[derive(
    PartialEq,
    Eq,
    bilrost::Enumeration
)]
enum MySlimEnum {
    Unknown = 0,
    FirstVariant = 1,
    SecondVariant = 2,
}
```

- Enum variants must be assigned to tags, either by the `=` sign or by using the `#[bilrost(<tag>)]` annotation on each attribute
- Enum must have an empty state of value `zero` If this not possible then the enum has to be used as an option field

```rust
#[derive(PartialEq, Eq, bilrost::Enumeration)]
enum MySlimEnum {
    #[bilrost(1)]
    FirstVariant,
    #[bilrost(2)]
    SecondVariant,
}

#[derive(bilrost::Message)]
struct MyStruct {
    // This works because although the enum has NO empty state (0)
    // the field here is optional
    slim: Option<MySlimEnum>,
}
```

```rust
#[derive(PartialEq, Eq, bilrost::Enumeration)]
enum MySlimEnum {
    #[bilrost(0)]
    Unknown,
    #[bilrost(1)]
    FirstVariant,
    #[bilrost(2)]
    SecondVariant,
}

#[derive(bilrost::Message)]
struct MyStruct {
    // This will also work because the Enum now has EmptyState (Unknown)
    slim: MySlimEnum,
}
```

```rust
#[derive(PartialEq, Eq, bilrost::Enumeration)]
enum MySlimEnum {
    #[bilrost(1)]
    FirstVariant,
    #[bilrost(2)]
    SecondVariant,
}

#[derive(bilrost::Message)]
struct MyStruct {
    // This will not compile! (required field but no empty state)
    slim: MySlimEnum,
}
```

### Fat Enums

This works completely different from above. It’s mainly because fat enums translate to a `Oneof` field (similar to protobuf `oneof`) this then enforces the following rules.

- You also need to have an `EmptyState` an empty variant that carries no data, otherwise you need your field to also become `Option` (that’s where the similarity with numeric enums ends)
- Your enum must derive `bilrost::Oneof`
- Each variant must carry **EXACTLY** one value. Only **ONE** empty enum variant can show up at the top of the enum to work as the “empty state” AND it must be `untagged`

```rust
// Derives Oneof
#[derive(bilrost::Oneof)]
enum MyFatEnum {
    // the only place where the empty state can appear
    // and it's the only variant that is not associated with
    // a value
    Unknown,
    // It will be clear why I am starting from 2 here later in this document
    // and not from 1
    //
    // Also there is exactly one value (in this cause a u64)
    #[bilrost(2)]
    FirstVariant(u64),
    #[bilrost(3)]
    // this also carries a single value of type tuple(u64, u64)
    SecondVariant((u64, u64)),
    // this also works
    #[bilrost(4)]
    ThirdVariant {
        name: String,
    },
    /// This one will not work
    // FourthVariant(u64, u64)
    /// Neither this
    // FifthVariant{value: u64, name: String}
}

```

To use this inside your struct you need to do the following

```rust
#[derive(bilrost::Message)]
struct MyStruct {
    #[bilrost(1)]
    id: String,
    #[bilrost(oneof(2, 3, 4))]
    fat: MyFatEnum,
}
```

You notice the following:

When using your `MyFatEnum` type, you need to make sure you use the `#[bilrost(oneof(<all possible tags>))]` annotations as shown in the example.

The tags **MUST** be unique inside your container message. This is why the enum variants were starting from `2` and not the logical `1` (since in my container type `MyStruct` 1 was already taken)

This is mainly because the enum variant is “flattened” inside the container message.

You can see how this becomes tedious task when you try to reuse your enum in another structure since now your enum tags are fixed at `2, 3, and 4` it’s now impossible to extend a struct that already has `2` assigned

```rust
// NOT POSSIBLE
struct MyOtherStructure{
	#[bilrost(1)]
	id: String,
	#[bilrost(2)]
	value: SomeMessage
	// This is not possible now because 2 is alread
	// taken by the above field.
	#[bilrost(oneof(2, 3, 4)]
	fat: MyFatEnum,
}
```

To work around this issue, your fat enums must be wrapped. in it’s own mesage

```rust

#[derive(bilrost::Message)]
struct MyFatEnumWrapper(#[bilrost(oneof(2,3,4))] MyFatEnum);

// then use it normally as a sub-message
struct MyStruct {
     #[bilrost(1)]
    id: String,
    #[bilrost(2)]
    fat: MyFatEnumWrapper,
}

struct MyOtherStructure{
	#[bilrost(1)]
	id: String,
	#[bilrost(2)]
	value: SomeMessage
	// This is now possible because MyFatEnumWrapper
	// is just it's own sub-message
	#[bilrost(3)]
	fat: MyFatEnumWrapper,
}
```

<aside>
💡

> *Rule #2*: If your Fat enums are reusable, they need to be wrapped in a special wrapper that can be used in all container messages.

</aside>

## Backward compatibility

When migrating some structures/messages to Bilrost an extra care must be taken to avoid breaking compatibility with V1. This means the serde serialisation can not be changes (flexbuffer/serde) this forces us most of the time to stick with the current available structure.

The problem arise when we face a type that is not serialisable with bilrost. For example (non-exhaustive list):

- An enum that has incompatible variants

    ```rust
    pub enum InputContentType {
        /// `*/*`
        #[default]
        Any,
        /// `<MIME_type>/*`
        MimeType(ByteString),
        /// `<MIME_type>/<MIME_subtype>`
        MimeTypeAndSubtype(ByteString, ByteString),
    }
    ```

- Foreign types that are not serialisable with bilrost (for example `humantime::Duration`

To work around this usually we have 2 solutions.

- For types that is not compatible, usually a `BilrostAs` macro can become handy to fix this issue.
    - Usage [examples](https://github.com/restatedev/restate/blob/main/crates/encoding/tests/bilrost.rs)
- For foreign types, we normally create a `NewType` that wraps the foreign type, then use `BilrostAs`
    - [Example](https://github.com/restatedev/restate/pull/3212/files#diff-c88bdf6293027da569d1e0b6d02a108f116197d27258b0bb4b107bbdc82472b3R163)  that creates a new type for `RangeInclusive<Idx>`