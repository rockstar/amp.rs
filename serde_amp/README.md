serde_amp
==

![build-and-check](https://github.com/rockstar/amp.rs/actions/workflows/build-and-check.yml/badge.svg) ![crates.io](https://img.shields.io/crates/v/serde_amp.svg) ![docs.rs](https://docs.rs/serde_amp/badge.svg)

[serde](https://serde.rs) support for [Asynchronous Messaging Protocol](https://amp-protocol.net/)
(AMP), the key/value remoting protocol from Twisted.

AMP packets ("boxes") are flat collections of byte keys and byte values. This
crate converts a Rust struct or map into an `AmpBox`, and back. The box type
comes from the [amp-protocol](https://crates.io/crates/amp-protocol) crate and
is re-exported here; it knows how to put itself on the wire, and the
protocol's request and response types carry a box of arguments or results.

Usage
--

```rust
use serde::{Deserialize, Serialize};
use serde_amp::AmpBox;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Sum {
    a: i64,
    b: i64,
    note: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sum = Sum { a: 13, b: 81, note: None };

    // A struct becomes a box: one key/value pair per field, with each value
    // in AMP's text form. A `None` field is left out entirely.
    let amp_box = serde_amp::to_box(&sum)?;
    assert_eq!(amp_box.get("a"), Some(&b"13"[..]));
    assert_eq!(amp_box.get("note"), None);

    // The box goes on the wire and comes back off it.
    let bytes = amp_box.encode();
    let (amp_box, _consumed) = AmpBox::decode(&bytes)?;

    // And a box becomes a struct. An absent key reads as `None`.
    let decoded: Sum = serde_amp::from_box(&amp_box)?;
    assert_eq!(decoded, sum);
    Ok(())
}
```

Type mapping
--

The top-level value must be a struct or a map, since a box is a collection of
pairs. Each field holds one AMP value:

| AMP type          | Rust type                                                         |
|-------------------|-------------------------------------------------------------------|
| Integer           | any integer type, as decimal text                                 |
| Unicode           | `String`, `&str`, `char`, as UTF-8                                |
| String (bytes)    | `&[u8]` when reading; `serde_bytes::ByteBuf` or `Bytes` to write  |
| Boolean           | `bool`, as `True` or `False`                                      |
| Float             | `f32`, `f64`, including `inf`, `-inf`, and `NaN`                  |
| Decimal           | a `String`, or a decimal type that serializes as text             |
| DateTime          | a `String` in Twisted's form, `1969-08-15T12:00:00.000000+00:00`  |
| ListOf            | `Vec`, array, or tuple of any of the above                        |
| AmpList           | `Vec` of structs or maps                                          |
| optional argument | `Option<T>`; `None` omits the key, and an absent key reads `None` |
| no results        | `()` or a unit struct, as an empty box                            |

A unit enum variant is written as its name, which suits a command argument
that takes one of a fixed set of strings. Keys that a struct does not name are
ignored when reading.

Text and bytes are borrowed from the box when reading, not copied, so a struct
may hold `&str`, `&[u8]`, or `Cow` fields and then lives no longer than the
box. This holds inside an `AmpList` too. Owned fields such as `String` work as
they always have.

Note that a plain `Vec<u8>` is a `ListOf(Integer)`, because that is how serde
sees it. Use `serde_bytes` or a custom `Serialize` for an AMP `String`.

Not representable, and reported as an error: a struct or map as a field (AMP
has no nesting except through `AmpList`), a unit value as a field, an enum
variant that carries data, and `None` inside a sequence.

A type that contains itself, such as a tree node holding a `Vec` of nodes,
reads up to 32 levels deep. Past that, reading fails with `Error::TooDeep`
rather than letting a peer overflow the stack with a deeply nested value.

Limitations
--

**`#[serde(flatten)]` reads only string fields.** Writing a flattened struct
works for any field types. Reading one works only when every flattened field
is a `String` or `Option<String>`. Serde implements `flatten` by buffering the
unclaimed pairs without their types, and an AMP value carries no type of its
own, so every buffered value is handed to the inner struct as a string. An
integer or boolean field behind `flatten` fails with serde's own
`invalid type: string "13", expected u32`. If you were reaching for `flatten`
to add the protocol's `_command` or `_ask` keys, use the request and response
types in `amp-protocol` instead, which carry your arguments as a box.

License
--

serde_amp is licensed under the MIT license ([LICENSE-MIT](LICENSE-MIT) or
<http://opensource.org/licenses/MIT>).
