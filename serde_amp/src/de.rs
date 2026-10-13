use amp_protocol::AmpBox;
use serde::de::{self, Deserialize, DeserializeSeed, Visitor};

use crate::Error;

/// Deserializes a value of type `T` from a box.
///
/// `T` must be a struct or a map, or a newtype or `Option` wrapping one. Each
/// key/value pair becomes one field. A field holding an `Option` reads as
/// `None` when its key is absent. Keys that `T` does not name are ignored,
/// and `()` or a unit struct reads from any box, ignoring every key.
/// The crate README lists how every AMP type maps to a Rust type.
///
/// String and byte values are borrowed from the box rather than copied, so
/// `T` may hold `&str`, `&[u8]`, or `Cow` fields, and the result cannot
/// outlive `amp_box`. Numbers, booleans, and other parsed values are copied
/// as they are parsed. An owned type works as before.
///
/// A `#[serde(flatten)]` field can only hold `String` and `Option<String>`
/// fields. Serde buffers flattened values without their types, and AMP
/// values carry no type of their own, so every buffered value arrives as a
/// string. An integer or boolean field behind `flatten` fails with serde's
/// "invalid type: string" error.
pub fn from_box<'a, T>(amp_box: &'a AmpBox) -> Result<T, Error>
where
    T: Deserialize<'a>,
{
    T::deserialize(BoxDeserializer { amp_box })
}

/// Deserializes the top-level value, which must be a struct or map.
struct BoxDeserializer<'de> {
    amp_box: &'de AmpBox,
}

macro_rules! not_a_box {
    ($($method:ident),* $(,)?) => {
        $(fn $method<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
            Err(Error::NotABox)
        })*
    };
}

impl<'de> de::Deserializer<'de> for BoxDeserializer<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_map(BoxAccess {
            iter: self.amp_box.iter(),
            pair: None,
        })
    }

    serde::forward_to_deserialize_any! { map struct ignored_any }

    not_a_box! {
        deserialize_bool,
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_i128,
        deserialize_u8,
        deserialize_u16,
        deserialize_u32,
        deserialize_u64,
        deserialize_u128,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_seq,
        deserialize_identifier,
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Error> {
        Err(Error::NotABox)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Error> {
        Err(Error::NotABox)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Error> {
        Err(Error::NotABox)
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

/// Walks a box's pairs as a map. The pairs borrow from the input for the
/// whole deserialization, whether they come from an [`AmpBox`] or straight
/// from the bytes of an `AmpList` element.
struct BoxAccess<'de, I> {
    iter: I,
    /// The key most recently handed out and its value, until the value is
    /// read.
    pair: Option<(&'de [u8], &'de [u8])>,
}

impl<'de, I> de::MapAccess<'de> for BoxAccess<'de, I>
where
    I: ExactSizeIterator<Item = (&'de [u8], &'de [u8])>,
{
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Error> {
        match self.iter.next() {
            Some((key, value)) => {
                self.pair = Some((key, value));
                seed.deserialize(ValueDeserializer { bytes: key }).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Error> {
        let (key, value) = self
            .pair
            .take()
            .ok_or_else(|| de::Error::custom("map value requested before its key"))?;
        seed.deserialize(ValueDeserializer { bytes: value })
            .map_err(|err| match err {
                Error::InvalidValue(expected) => Error::InvalidValueForKey {
                    key: String::from_utf8_lossy(key).into_owned(),
                    expected,
                    value: value.to_vec(),
                },
                other => other,
            })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.iter.len())
    }
}

/// Deserializes one value's bytes, or one key's. Text and bytes are handed to
/// the visitor borrowed, so a `&'de str` field points into the input.
struct ValueDeserializer<'de> {
    bytes: &'de [u8],
}

impl<'de> ValueDeserializer<'de> {
    fn text(&self, expected: &'static str) -> Result<&'de str, Error> {
        std::str::from_utf8(self.bytes).map_err(|_| Error::InvalidValue(expected))
    }

    fn parse<T: std::str::FromStr>(&self, expected: &'static str) -> Result<T, Error> {
        self.text(expected)?
            .parse()
            .map_err(|_| Error::InvalidValue(expected))
    }
}

macro_rules! parse_number {
    ($($method:ident => $visit:ident($expected:literal)),* $(,)?) => {
        $(fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
            visitor.$visit(self.parse($expected)?)
        })*
    };
}

impl<'de> de::Deserializer<'de> for ValueDeserializer<'de> {
    type Error = Error;

    /// The wire carries no type information, so an untyped read yields the
    /// text if there is valid UTF-8 and the raw bytes otherwise.
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match std::str::from_utf8(self.bytes) {
            Ok(text) => visitor.visit_borrowed_str(text),
            Err(_) => visitor.visit_borrowed_bytes(self.bytes),
        }
    }

    parse_number! {
        deserialize_i8 => visit_i8("an integer"),
        deserialize_i16 => visit_i16("an integer"),
        deserialize_i32 => visit_i32("an integer"),
        deserialize_i64 => visit_i64("an integer"),
        deserialize_i128 => visit_i128("an integer"),
        deserialize_u8 => visit_u8("an unsigned integer"),
        deserialize_u16 => visit_u16("an unsigned integer"),
        deserialize_u32 => visit_u32("an unsigned integer"),
        deserialize_u64 => visit_u64("an unsigned integer"),
        deserialize_u128 => visit_u128("an unsigned integer"),
        deserialize_f32 => visit_f32("a number"),
        deserialize_f64 => visit_f64("a number"),
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.bytes {
            b"True" => visitor.visit_bool(true),
            b"False" => visitor.visit_bool(false),
            _ => Err(Error::InvalidValue("a boolean, `True` or `False`")),
        }
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let expected = "a single character";
        let mut chars = self.text(expected)?.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => visitor.visit_char(c),
            _ => Err(Error::InvalidValue(expected)),
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_borrowed_str(self.text("a UTF-8 string")?)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_borrowed_bytes(self.bytes)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_bytes(visitor)
    }

    /// A key that is present always holds a value; absence was handled by the
    /// struct's field visitor never seeing the key.
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::Unsupported("a unit value"))
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Error> {
        Err(Error::Unsupported("a unit struct"))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_seq(SeqAccess {
            bytes: self.bytes,
            pos: 0,
        })
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::Unsupported("a nested struct or map"))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Error> {
        Err(Error::Unsupported("a nested struct or map"))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_str(visitor)
    }

    /// The value is already framed, so there is nothing to skip over.
    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

/// A unit enum variant, spelled as its name.
impl<'de> de::EnumAccess<'de> for ValueDeserializer<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self), Error> {
        let bytes = self.bytes;
        let variant = seed.deserialize(self)?;
        Ok((variant, ValueDeserializer { bytes }))
    }
}

impl<'de> de::VariantAccess<'de> for ValueDeserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Error> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, _seed: T) -> Result<T::Value, Error> {
        Err(Error::Unsupported("an enum variant carrying data"))
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::Unsupported("an enum variant carrying data"))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Error> {
        Err(Error::Unsupported("an enum variant carrying data"))
    }
}

/// Walks the elements of a `ListOf` or `AmpList` value.
struct SeqAccess<'de> {
    bytes: &'de [u8],
    pos: usize,
}

impl<'de> de::SeqAccess<'de> for SeqAccess<'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        if self.pos >= self.bytes.len() {
            return Ok(None);
        }
        let mut consumed = 0;
        let element = seed.deserialize(ElementDeserializer {
            bytes: &self.bytes[self.pos..],
            consumed: &mut consumed,
        })?;
        if consumed == 0 {
            return Err(de::Error::custom("sequence element consumed no input"));
        }
        self.pos += consumed;
        Ok(Some(element))
    }
}

/// One element of a sequence. The wire does not say whether the sequence is
/// a `ListOf` (length-prefixed scalars) or an `AmpList` (boxes), so the
/// element's own type decides: a struct or map reads a box, anything else
/// reads a length-prefixed value. Reports how many bytes it used through
/// `consumed`.
struct ElementDeserializer<'de, 'c> {
    bytes: &'de [u8],
    consumed: &'c mut usize,
}

impl<'de, 'c> ElementDeserializer<'de, 'c> {
    fn scalar(self) -> Result<ValueDeserializer<'de>, Error> {
        let expected = "a length-prefixed list element";
        let len = match self.bytes.get(..2) {
            Some(&[high, low]) => u16::from_be_bytes([high, low]) as usize,
            _ => return Err(Error::InvalidValue(expected)),
        };
        let bytes = self
            .bytes
            .get(2..2 + len)
            .ok_or(Error::InvalidValue(expected))?;
        *self.consumed = 2 + len;
        Ok(ValueDeserializer { bytes })
    }

    /// Reads the element as a box, borrowing its pairs from the input rather
    /// than building an `AmpBox`, so the element's fields can borrow too.
    fn amp_box<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let (pairs, consumed) = AmpBox::decode_borrowed(self.bytes).map_err(Error::Protocol)?;
        *self.consumed = consumed;
        visitor.visit_map(BoxAccess {
            iter: pairs.into_iter(),
            pair: None,
        })
    }
}

macro_rules! scalar_element {
    ($($method:ident),* $(,)?) => {
        $(fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
            self.scalar()?.$method(visitor)
        })*
    };
}

impl<'de, 'c> de::Deserializer<'de> for ElementDeserializer<'de, 'c> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::Unsupported("a list element of unknown type"))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::Unsupported("a list element of unknown type"))
    }

    scalar_element! {
        deserialize_bool,
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_i128,
        deserialize_u8,
        deserialize_u16,
        deserialize_u32,
        deserialize_u64,
        deserialize_u128,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_option,
        deserialize_unit,
        deserialize_seq,
        deserialize_identifier,
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.scalar()?.deserialize_unit_struct(name, visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Error> {
        self.scalar()?.deserialize_tuple(len, visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.scalar()?.deserialize_tuple_struct(name, len, visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.scalar()?.deserialize_enum(name, variants, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.amp_box(visitor)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        self.amp_box(visitor)
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;
    use std::collections::{BTreeMap, HashMap};

    use serde::Deserialize;

    use super::*;

    /// A struct with one field, so a scalar can be checked through the only
    /// public entry point.
    #[derive(Deserialize, Debug, PartialEq)]
    struct One<T> {
        v: T,
    }

    fn value<T: de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
        let mut amp_box = AmpBox::new();
        amp_box.insert("v", bytes).unwrap();
        from_box::<One<T>>(&amp_box).map(|one| one.v)
    }

    #[test]
    fn integers() {
        assert_eq!(value::<u8>(b"255").unwrap(), 255);
        assert_eq!(value::<i8>(b"-15").unwrap(), -15);
        assert_eq!(value::<u64>(b"2965537").unwrap(), 2_965_537);
        assert_eq!(value::<i128>(b"-1").unwrap(), -1);
        assert!(matches!(
            value::<u8>(b"300"),
            Err(Error::InvalidValueForKey { .. })
        ));
        assert!(matches!(
            value::<u8>(b"-1"),
            Err(Error::InvalidValueForKey { .. })
        ));
        assert!(matches!(
            value::<i32>(b"xy"),
            Err(Error::InvalidValueForKey { .. })
        ));
        assert!(matches!(
            value::<i32>(b""),
            Err(Error::InvalidValueForKey { .. })
        ));
        assert!(matches!(
            value::<i32>(b"\xff"),
            Err(Error::InvalidValueForKey { .. })
        ));
    }

    /// A value that fails to parse reports the key it sat under, what was
    /// expected, and the bytes found, so a client can tell which argument
    /// was wrong.
    #[test]
    fn invalid_values_name_their_key() {
        let err = value::<i32>(b"xy").unwrap_err();
        assert!(matches!(
            &err,
            Error::InvalidValueForKey {
                key,
                expected: "an integer",
                value,
            } if key == "v" && value == b"xy"
        ));
        assert_eq!(
            err.to_string(),
            "key \"v\": expected an integer, got \"xy\""
        );

        // Bytes that are not UTF-8 are shown lossily, and a long value is
        // cut short.
        assert_eq!(
            value::<i32>(b"\xff").unwrap_err().to_string(),
            "key \"v\": expected an integer, got \"\u{fffd}\""
        );
        let long = "x".repeat(100);
        assert_eq!(
            value::<i32>(long.as_bytes()).unwrap_err().to_string(),
            format!(
                "key \"v\": expected an integer, got {:?}...",
                "x".repeat(64)
            )
        );

        // An element inside a list of boxes reports its own key, not the
        // key of the list.
        #[derive(Deserialize, Debug)]
        struct Item {
            #[allow(dead_code)]
            inner: u8,
        }
        assert!(matches!(
            value::<Vec<Item>>(b"\x00\x05inner\x00\x02xy\x00\x00"),
            Err(Error::InvalidValueForKey { key, .. }) if key == "inner"
        ));

        // A map key has no key of its own.
        let mut amp_box = AmpBox::new();
        amp_box.insert("x", "1").unwrap();
        assert!(matches!(
            from_box::<BTreeMap<u32, String>>(&amp_box),
            Err(Error::InvalidValue("an unsigned integer"))
        ));
    }

    #[test]
    fn floats_as_twisted_writes_them() {
        assert_eq!(value::<f64>(b"12.9").unwrap(), 12.9);
        assert_eq!(value::<f32>(b"12.9").unwrap(), 12.9);
        assert_eq!(value::<f64>(b"10.0").unwrap(), 10.0);
        assert_eq!(value::<f64>(b"10.").unwrap(), 10.0);
        assert_eq!(value::<f64>(b"1e+300").unwrap(), 1e300);
        assert_eq!(value::<f64>(b"inf").unwrap(), f64::INFINITY);
        assert_eq!(value::<f64>(b"-inf").unwrap(), f64::NEG_INFINITY);
        assert!(value::<f64>(b"nan").unwrap().is_nan());
        assert!(matches!(
            value::<f64>(b"ten"),
            Err(Error::InvalidValueForKey { .. })
        ));
    }

    #[test]
    fn booleans() {
        assert!(value::<bool>(b"True").unwrap());
        assert!(!value::<bool>(b"False").unwrap());
        assert!(matches!(
            value::<bool>(b"true"),
            Err(Error::InvalidValueForKey { .. })
        ));
    }

    #[test]
    fn text() {
        assert_eq!(
            value::<String>("h\u{e9}llo".as_bytes()).unwrap(),
            "h\u{e9}llo"
        );
        assert_eq!(value::<char>("\u{e9}".as_bytes()).unwrap(), '\u{e9}');
        assert!(matches!(
            value::<String>(b"\xff"),
            Err(Error::InvalidValueForKey { .. })
        ));
        assert!(matches!(
            value::<char>(b"ab"),
            Err(Error::InvalidValueForKey { .. })
        ));
        assert!(matches!(
            value::<char>(b""),
            Err(Error::InvalidValueForKey { .. })
        ));
    }

    #[test]
    fn bytes_are_raw() {
        #[derive(Debug, PartialEq)]
        struct Raw(Vec<u8>);
        impl<'de> Deserialize<'de> for Raw {
            fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl<'de> Visitor<'de> for V {
                    type Value = Raw;
                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        f.write_str("bytes")
                    }
                    fn visit_bytes<E>(self, v: &[u8]) -> Result<Raw, E> {
                        Ok(Raw(v.to_vec()))
                    }
                }
                d.deserialize_bytes(V)
            }
        }
        assert_eq!(value::<Raw>(b"\x00\xff").unwrap(), Raw(vec![0, 255]));
    }

    /// Text and bytes come out of the box without a copy: the references
    /// point into the box's own storage, and a `Cow` stays borrowed.
    #[test]
    fn text_and_bytes_are_borrowed_from_the_box() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Borrowed<'a> {
            s: &'a str,
            b: &'a [u8],
            #[serde(borrow)]
            c: Cow<'a, str>,
        }
        let mut amp_box = AmpBox::new();
        amp_box.insert("s", "hello").unwrap();
        amp_box.insert("b", b"\x00\xff".to_vec()).unwrap();
        amp_box.insert("c", "cow").unwrap();
        let borrowed: Borrowed = from_box(&amp_box).unwrap();
        assert_eq!(borrowed.s, "hello");
        assert_eq!(borrowed.b, b"\x00\xff");
        assert!(matches!(borrowed.c, Cow::Borrowed("cow")));
        assert_eq!(borrowed.s.as_ptr(), amp_box.get("s").unwrap().as_ptr());
        assert_eq!(borrowed.b.as_ptr(), amp_box.get("b").unwrap().as_ptr());

        assert!(matches!(
            from_box::<Borrowed>(&{
                let mut amp_box = amp_box.clone();
                amp_box.insert("s", b"\xff".to_vec()).unwrap();
                amp_box
            }),
            Err(Error::InvalidValueForKey { .. })
        ));
    }

    /// An `AmpList` element is read straight out of the parent value's
    /// bytes, so its fields borrow from the box like top-level fields do.
    #[test]
    fn list_elements_borrow_from_the_box() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Item<'a> {
            name: &'a str,
            #[serde(borrow)]
            raw: Cow<'a, [u8]>,
        }
        #[derive(Deserialize, Debug, PartialEq)]
        struct List<'a> {
            #[serde(borrow)]
            v: Vec<Item<'a>>,
        }
        let mut amp_box = AmpBox::new();
        let list = b"\x00\x04name\x00\x01a\x00\x03raw\x00\x01\xff\x00\x00\x00\x04name\x00\x02bc\x00\x03raw\x00\x00\x00\x00";
        amp_box.insert("v", list.to_vec()).unwrap();
        let list: List = from_box(&amp_box).unwrap();
        assert_eq!(list.v.len(), 2);
        assert_eq!(list.v[0].name, "a");
        assert!(matches!(&list.v[0].raw, Cow::Borrowed(b) if *b == b"\xff"));
        assert_eq!(list.v[1].name, "bc");
        assert_eq!(list.v[1].raw, Cow::Borrowed(&b""[..]));
        let value = amp_box.get("v").unwrap();
        let bc = value.windows(2).position(|w| w == b"bc").unwrap();
        assert_eq!(list.v[1].name.as_ptr(), value[bc..].as_ptr());
    }

    /// A key repeated inside a list element keeps its last value, as a
    /// repeated key in a top-level box does.
    #[test]
    fn list_element_with_repeated_key_keeps_the_last_value() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Item {
            k: u8,
        }
        assert_eq!(
            value::<Vec<Item>>(b"\x00\x01k\x00\x011\x00\x01k\x00\x012\x00\x00").unwrap(),
            vec![Item { k: 2 }]
        );
    }

    #[test]
    fn options_absent_and_present() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct S {
            a: Option<u8>,
            b: Option<u8>,
        }
        let mut amp_box = AmpBox::new();
        amp_box.insert("b", "2").unwrap();
        assert_eq!(
            from_box::<S>(&amp_box).unwrap(),
            S {
                a: None,
                b: Some(2)
            }
        );
    }

    #[test]
    fn unit_variants_by_name() {
        #[derive(Deserialize, Debug, PartialEq)]
        enum Mode {
            Fast,
            #[serde(rename = "slow")]
            Slow,
        }
        assert_eq!(value::<Mode>(b"Fast").unwrap(), Mode::Fast);
        assert_eq!(value::<Mode>(b"slow").unwrap(), Mode::Slow);
        assert!(matches!(value::<Mode>(b"Medium"), Err(Error::Message(_))));

        #[derive(Deserialize, Debug)]
        enum Data {
            #[allow(dead_code)]
            Newtype(u8),
        }
        assert!(matches!(
            value::<Data>(b"Newtype"),
            Err(Error::Unsupported("an enum variant carrying data"))
        ));
    }

    #[test]
    fn newtype_structs_are_transparent() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Id(u32);
        assert_eq!(value::<Id>(b"7").unwrap(), Id(7));

        #[derive(Deserialize, Debug, PartialEq)]
        struct Wrapper(One<u8>);
        let mut amp_box = AmpBox::new();
        amp_box.insert("v", "1").unwrap();
        assert_eq!(
            from_box::<Wrapper>(&amp_box).unwrap(),
            Wrapper(One { v: 1 })
        );
        assert_eq!(
            from_box::<Option<One<u8>>>(&amp_box).unwrap(),
            Some(One { v: 1 })
        );
    }

    #[test]
    fn list_of_scalars_as_twisted_writes_them() {
        assert_eq!(
            value::<Vec<u32>>(b"\x00\x0210\x00\x0211").unwrap(),
            vec![10, 11]
        );
        assert_eq!(value::<Vec<u32>>(b"").unwrap(), Vec::<u32>::new());
        assert_eq!(
            value::<Vec<String>>(b"\x00\x00\x00\x01x").unwrap(),
            vec!["".to_string(), "x".to_string()]
        );
        assert_eq!(value::<[u8; 2]>(b"\x00\x011\x00\x012").unwrap(), [1, 2]);
        assert_eq!(
            value::<(u8, String)>(b"\x00\x011\x00\x01a").unwrap(),
            (1, "a".to_string())
        );
        assert_eq!(
            value::<Vec<Vec<u8>>>(b"\x00\x03\x00\x011\x00\x06\x00\x012\x00\x013").unwrap(),
            vec![vec![1], vec![2, 3]]
        );
        assert_eq!(
            value::<Vec<Option<u8>>>(b"\x00\x011").unwrap(),
            vec![Some(1)]
        );
    }

    #[test]
    fn list_of_structs_as_twisted_writes_them() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Item {
            inner: u8,
        }
        assert_eq!(
            value::<Vec<Item>>(b"\x00\x05inner\x00\x011\x00\x00\x00\x05inner\x00\x012\x00\x00")
                .unwrap(),
            vec![Item { inner: 1 }, Item { inner: 2 }]
        );
        assert_eq!(value::<Vec<Item>>(b"").unwrap(), Vec::<Item>::new());

        let maps = value::<Vec<BTreeMap<String, String>>>(b"\x00\x01k\x00\x01v\x00\x00").unwrap();
        assert_eq!(maps.len(), 1);
        assert_eq!(maps[0]["k"], "v");
    }

    #[test]
    fn malformed_lists_are_errors_not_panics() {
        assert!(matches!(
            value::<Vec<u8>>(b"\x00"),
            Err(Error::InvalidValueForKey { .. })
        ));
        assert!(matches!(
            value::<Vec<u8>>(b"\x00\x051"),
            Err(Error::InvalidValueForKey { .. })
        ));
        #[derive(Deserialize, Debug)]
        struct Item {
            #[allow(dead_code)]
            inner: u8,
        }
        assert!(matches!(
            value::<Vec<Item>>(b"\x00\x05inner\x00\x011"),
            Err(Error::Protocol(amp_protocol::Error::Incomplete))
        ));
    }

    #[test]
    fn maps() {
        let mut amp_box = AmpBox::new();
        amp_box.insert("a", "1").unwrap();
        amp_box.insert("b", "2").unwrap();
        let map: HashMap<String, u8> = from_box(&amp_box).unwrap();
        assert_eq!(map["a"], 1);
        assert_eq!(map["b"], 2);
        let mut amp_box = AmpBox::new();
        amp_box.insert("7", "x").unwrap();
        let map: BTreeMap<u32, String> = from_box(&amp_box).unwrap();
        assert_eq!(map[&7], "x");
    }

    #[test]
    fn unknown_keys_are_ignored_and_missing_keys_are_errors() {
        let mut amp_box = AmpBox::new();
        amp_box.insert("other", b"\xff".to_vec()).unwrap();
        amp_box.insert("v", "1").unwrap();
        assert_eq!(from_box::<One<u8>>(&amp_box).unwrap(), One { v: 1 });
        assert!(matches!(
            from_box::<One<u8>>(&AmpBox::new()),
            Err(Error::Message(message)) if message.contains("missing field")
        ));
    }

    #[test]
    fn top_level_must_be_a_struct_or_map() {
        let mut amp_box = AmpBox::new();
        amp_box.insert("v", "1").unwrap();
        assert!(matches!(from_box::<u8>(&amp_box), Err(Error::NotABox)));
        assert!(matches!(from_box::<String>(&amp_box), Err(Error::NotABox)));
        assert!(matches!(from_box::<Vec<u8>>(&amp_box), Err(Error::NotABox)));
    }

    #[test]
    fn unit_reads_from_any_box() {
        let mut amp_box = AmpBox::new();
        amp_box.insert("v", "1").unwrap();
        from_box::<()>(&amp_box).unwrap();
        from_box::<()>(&AmpBox::new()).unwrap();
        #[derive(Deserialize, Debug, PartialEq)]
        struct Unit;
        assert_eq!(from_box::<Unit>(&amp_box).unwrap(), Unit);
    }

    #[test]
    fn flatten_reads_string_fields_only() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Strings {
            s: String,
            t: Option<String>,
        }
        #[derive(Deserialize, Debug, PartialEq)]
        struct Outer {
            a: u8,
            #[serde(flatten)]
            inner: Strings,
        }
        let mut amp_box = AmpBox::new();
        amp_box.insert("a", "1").unwrap();
        amp_box.insert("s", "x").unwrap();
        assert_eq!(
            from_box::<Outer>(&amp_box).unwrap(),
            Outer {
                a: 1,
                inner: Strings {
                    s: "x".to_string(),
                    t: None
                }
            }
        );

        // The same box fails as soon as a flattened field is not a string,
        // because serde buffers flattened values without their types.
        #[derive(Deserialize, Debug)]
        struct Number {
            #[allow(dead_code)]
            s: u32,
        }
        #[derive(Deserialize, Debug)]
        struct OuterNumber {
            #[serde(flatten)]
            #[allow(dead_code)]
            inner: Number,
        }
        amp_box.insert("s", "13").unwrap();
        assert!(matches!(
            from_box::<OuterNumber>(&amp_box),
            Err(Error::Message(message)) if message == "invalid type: string \"13\", expected u32"
        ));

        #[derive(Deserialize, Debug)]
        struct Flag {
            #[allow(dead_code)]
            s: bool,
        }
        #[derive(Deserialize, Debug)]
        struct OuterFlag {
            #[serde(flatten)]
            #[allow(dead_code)]
            inner: Flag,
        }
        amp_box.insert("s", "True").unwrap();
        assert!(matches!(
            from_box::<OuterFlag>(&amp_box),
            Err(Error::Message(message)) if message.starts_with("invalid type: string")
        ));
    }

    #[test]
    fn nested_struct_as_a_field_is_unsupported() {
        #[derive(Deserialize, Debug)]
        struct Outer {
            #[allow(dead_code)]
            inner: One<u8>,
        }
        let mut amp_box = AmpBox::new();
        amp_box.insert("inner", "").unwrap();
        assert!(matches!(
            from_box::<Outer>(&amp_box),
            Err(Error::Unsupported("a nested struct or map"))
        ));
    }
}
