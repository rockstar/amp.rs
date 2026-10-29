use amp_protocol::AmpBox;
use serde::ser::{self, Impossible, Serialize};

use crate::Error;

/// Serializes `value` into a box.
///
/// `value` must be a struct or a map, or a newtype or `Some` wrapping one.
/// Each field becomes one key/value pair. A field holding `None` is omitted.
/// `()` and a unit struct become an empty box.
/// A field holding a sequence of scalars becomes a `ListOf`, and a sequence
/// of structs or maps becomes an `AmpList`. The crate README lists how every
/// Rust type maps to an AMP type.
///
/// # Errors
///
/// Fails with [`Error::NotABox`] if `value` is not a struct or map, or a
/// wrapper around one, and with [`Error::Unsupported`] if a field has a type
/// AMP cannot carry, such as a nested struct or an enum variant carrying
/// data. A key or value too long for the wire fails with
/// [`Error::Protocol`], and anything `value`'s own `Serialize`
/// implementation rejects with [`Error::Message`].
pub fn to_box<T>(value: &T) -> Result<AmpBox, Error>
where
    T: Serialize + ?Sized,
{
    value.serialize(BoxSerializer)
}

/// What one field serializes to.
enum Value {
    /// `None`: the field is left out of the box.
    Absent,
    /// A scalar or a sequence, already encoded as the value's bytes.
    Scalar(Vec<u8>),
    /// A struct or map. Only valid as an element of a sequence (an `AmpList`).
    Box(AmpBox),
}

/// Serializes the top-level value, which must be a struct or map.
struct BoxSerializer;

macro_rules! not_a_box {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(fn $method(self, _value: $ty) -> Result<AmpBox, Error> {
            Err(Error::NotABox)
        })*
    };
}

impl ser::Serializer for BoxSerializer {
    type Ok = AmpBox;
    type Error = Error;
    type SerializeSeq = Impossible<AmpBox, Error>;
    type SerializeTuple = Impossible<AmpBox, Error>;
    type SerializeTupleStruct = Impossible<AmpBox, Error>;
    type SerializeTupleVariant = Impossible<AmpBox, Error>;
    type SerializeMap = BoxBuilder;
    type SerializeStruct = BoxBuilder;
    type SerializeStructVariant = Impossible<AmpBox, Error>;

    not_a_box! {
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8]),
    }

    fn serialize_none(self) -> Result<AmpBox, Error> {
        Err(Error::NotABox)
    }

    fn serialize_some<T>(self, value: &T) -> Result<AmpBox, Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<AmpBox, Error> {
        Ok(AmpBox::new())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<AmpBox, Error> {
        Ok(AmpBox::new())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<AmpBox, Error> {
        Err(Error::NotABox)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<AmpBox, Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<AmpBox, Error>
    where
        T: Serialize + ?Sized,
    {
        Err(Error::NotABox)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(Error::NotABox)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Error> {
        Err(Error::NotABox)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        Err(Error::NotABox)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(Error::NotABox)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Ok(BoxBuilder::default())
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        Ok(BoxBuilder::default())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(Error::NotABox)
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

/// Collects the fields of a struct or the entries of a map into a box.
#[derive(Default)]
struct BoxBuilder {
    amp_box: AmpBox,
    /// A map key waiting for its value.
    key: Option<Vec<u8>>,
}

impl BoxBuilder {
    fn field(&mut self, key: Vec<u8>, value: Value) -> Result<(), Error> {
        match value {
            Value::Absent => Ok(()),
            Value::Scalar(bytes) => {
                let _ = self.amp_box.insert(key, bytes).map_err(Error::Protocol)?;
                Ok(())
            }
            Value::Box(_) => Err(Error::Unsupported("a nested struct or map")),
        }
    }
}

impl ser::SerializeStruct for BoxBuilder {
    type Ok = AmpBox;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        let value = value.serialize(ValueSerializer)?;
        self.field(key.into(), value)
    }

    fn end(self) -> Result<AmpBox, Error> {
        Ok(self.amp_box)
    }
}

impl ser::SerializeMap for BoxBuilder {
    type Ok = AmpBox;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        match key.serialize(ValueSerializer)? {
            Value::Scalar(bytes) => {
                self.key = Some(bytes);
                Ok(())
            }
            Value::Absent | Value::Box(_) => Err(Error::Unsupported(
                "a map key that is not a string, number, or bytes",
            )),
        }
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        let key = self
            .key
            .take()
            .ok_or_else(|| ser::Error::custom("map value serialized before its key"))?;
        let value = value.serialize(ValueSerializer)?;
        self.field(key, value)
    }

    fn end(self) -> Result<AmpBox, Error> {
        Ok(self.amp_box)
    }
}

/// Serializes one field's value.
struct ValueSerializer;

macro_rules! scalar_as_text {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(fn $method(self, value: $ty) -> Result<Value, Error> {
            Ok(Value::Scalar(value.to_string().into_bytes()))
        })*
    };
}

impl ser::Serializer for ValueSerializer {
    type Ok = Value;
    type Error = Error;
    type SerializeSeq = SeqBuilder;
    type SerializeTuple = SeqBuilder;
    type SerializeTupleStruct = SeqBuilder;
    type SerializeTupleVariant = Impossible<Value, Error>;
    type SerializeMap = NestedBoxBuilder;
    type SerializeStruct = NestedBoxBuilder;
    type SerializeStructVariant = Impossible<Value, Error>;

    scalar_as_text! {
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
    }

    fn serialize_bool(self, value: bool) -> Result<Value, Error> {
        let text: &[u8] = if value { b"True" } else { b"False" };
        Ok(Value::Scalar(text.to_vec()))
    }

    fn serialize_str(self, value: &str) -> Result<Value, Error> {
        Ok(Value::Scalar(value.as_bytes().to_vec()))
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Value, Error> {
        Ok(Value::Scalar(value.to_vec()))
    }

    fn serialize_none(self) -> Result<Value, Error> {
        Ok(Value::Absent)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Value, Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Value, Error> {
        Err(Error::Unsupported("a unit value"))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Value, Error> {
        Err(Error::Unsupported("a unit struct"))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<Value, Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<Value, Error>
    where
        T: Serialize + ?Sized,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Value, Error>
    where
        T: Serialize + ?Sized,
    {
        Err(Error::Unsupported("an enum variant carrying data"))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Ok(SeqBuilder::default())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Error> {
        Ok(SeqBuilder::default())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        Ok(SeqBuilder::default())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(Error::Unsupported("an enum variant carrying data"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Ok(NestedBoxBuilder::default())
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        Ok(NestedBoxBuilder::default())
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(Error::Unsupported("an enum variant carrying data"))
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

/// Which compound type a sequence is being encoded as.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SeqKind {
    /// Scalars, each with a two-byte length prefix.
    ListOf,
    /// Boxes, each with its own terminator.
    AmpList,
}

/// Encodes the elements of a sequence into one value.
#[derive(Default)]
struct SeqBuilder {
    out: Vec<u8>,
    kind: Option<SeqKind>,
}

impl SeqBuilder {
    fn element(&mut self, value: Value) -> Result<(), Error> {
        match value {
            Value::Absent => Err(Error::Unsupported("`None` inside a sequence")),
            Value::Scalar(bytes) => {
                self.set_kind(SeqKind::ListOf)?;
                let len = u16::try_from(bytes.len()).map_err(|_| {
                    Error::Protocol(amp_protocol::Error::ValueTooLong { len: bytes.len() })
                })?;
                self.out.extend_from_slice(&len.to_be_bytes());
                self.out.extend_from_slice(&bytes);
                Ok(())
            }
            Value::Box(amp_box) => {
                self.set_kind(SeqKind::AmpList)?;
                self.out.extend_from_slice(&amp_box.encode());
                Ok(())
            }
        }
    }

    fn set_kind(&mut self, kind: SeqKind) -> Result<(), Error> {
        match self.kind {
            None => {
                self.kind = Some(kind);
                Ok(())
            }
            Some(existing) if existing == kind => Ok(()),
            Some(_) => Err(Error::Unsupported(
                "a sequence mixing scalars with structs or maps",
            )),
        }
    }
}

impl ser::SerializeSeq for SeqBuilder {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        self.element(value.serialize(ValueSerializer)?)
    }

    fn end(self) -> Result<Value, Error> {
        Ok(Value::Scalar(self.out))
    }
}

impl ser::SerializeTuple for SeqBuilder {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        self.element(value.serialize(ValueSerializer)?)
    }

    fn end(self) -> Result<Value, Error> {
        Ok(Value::Scalar(self.out))
    }
}

impl ser::SerializeTupleStruct for SeqBuilder {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        self.element(value.serialize(ValueSerializer)?)
    }

    fn end(self) -> Result<Value, Error> {
        Ok(Value::Scalar(self.out))
    }
}

/// A struct or map appearing as a value, which is only valid inside a
/// sequence. The builder produces a box; the caller decides whether that is
/// allowed where it appeared.
#[derive(Default)]
struct NestedBoxBuilder(BoxBuilder);

impl ser::SerializeStruct for NestedBoxBuilder {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        ser::SerializeStruct::serialize_field(&mut self.0, key, value)
    }

    fn end(self) -> Result<Value, Error> {
        Ok(Value::Box(self.0.amp_box))
    }
}

impl ser::SerializeMap for NestedBoxBuilder {
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        ser::SerializeMap::serialize_key(&mut self.0, key)
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: Serialize + ?Sized,
    {
        ser::SerializeMap::serialize_value(&mut self.0, value)
    }

    fn end(self) -> Result<Value, Error> {
        Ok(Value::Box(self.0.amp_box))
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use serde::Serialize;

    use super::*;

    /// A struct with one field, so a scalar can be checked through the only
    /// public entry point.
    #[derive(Serialize)]
    struct One<T> {
        v: T,
    }

    fn value<T: Serialize>(v: T) -> Vec<u8> {
        let amp_box = to_box(&One { v }).unwrap();
        assert_eq!(amp_box.len(), 1);
        amp_box.get("v").unwrap().to_vec()
    }

    #[test]
    fn integers_are_decimal_text() {
        assert_eq!(value(10_u8), b"10");
        assert_eq!(value(-20_i8), b"-20");
        assert_eq!(value(u64::MAX), u64::MAX.to_string().as_bytes());
        assert_eq!(value(i128::MIN), i128::MIN.to_string().as_bytes());
    }

    #[test]
    fn floats_are_text_python_accepts() {
        assert_eq!(value(1.5_f32), b"1.5");
        assert_eq!(value(10.0_f64), b"10");
        assert_eq!(value(f64::INFINITY), b"inf");
        assert_eq!(value(f64::NEG_INFINITY), b"-inf");
        assert_eq!(value(f64::NAN), b"NaN");
    }

    #[test]
    fn booleans_are_python_spelling() {
        assert_eq!(value(true), b"True");
        assert_eq!(value(false), b"False");
    }

    #[test]
    fn text_is_utf8() {
        assert_eq!(value("h\u{e9}llo"), "h\u{e9}llo".as_bytes());
        assert_eq!(value('X'), b"X");
        assert_eq!(value(String::from("s")), b"s");
    }

    #[test]
    fn bytes_are_raw() {
        struct Raw(&'static [u8]);
        impl Serialize for Raw {
            fn serialize<S: ser::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_bytes(self.0)
            }
        }
        assert_eq!(value(Raw(b"\x00\xff")), b"\x00\xff");
    }

    #[test]
    fn none_omits_the_key_and_some_is_transparent() {
        #[derive(Serialize)]
        struct S {
            a: Option<u8>,
            b: Option<u8>,
        }
        let amp_box = to_box(&S {
            a: None,
            b: Some(2),
        })
        .unwrap();
        assert_eq!(amp_box.get("a"), None);
        assert_eq!(amp_box.get("b"), Some(&b"2"[..]));
        assert_eq!(amp_box.len(), 1);
    }

    #[test]
    fn unit_variants_are_their_name() {
        #[derive(Serialize)]
        enum Mode {
            Fast,
            #[serde(rename = "slow")]
            Slow,
        }
        assert_eq!(value(Mode::Fast), b"Fast");
        assert_eq!(value(Mode::Slow), b"slow");
    }

    #[test]
    fn newtype_structs_are_transparent() {
        #[derive(Serialize)]
        struct Id(u32);
        #[derive(Serialize)]
        struct Inner {
            v: u8,
        }
        #[derive(Serialize)]
        struct Wrapper(Inner);
        assert_eq!(value(Id(7)), b"7");

        let amp_box = to_box(&Wrapper(Inner { v: 1 })).unwrap();
        assert_eq!(amp_box.get("v"), Some(&b"1"[..]));
    }

    #[test]
    fn list_of_scalars_matches_twisted() {
        // Twisted: ListOf(Integer()) for [10, 11]
        assert_eq!(value(vec![10, 11]), b"\x00\x0210\x00\x0211");
        assert_eq!(value(Vec::<u8>::new()), b"");
        assert_eq!(value([1_u8, 2]), b"\x00\x011\x00\x012");
        assert_eq!(value((1_u8, "a")), b"\x00\x011\x00\x01a");
        assert_eq!(value(vec!["", "x"]), b"\x00\x00\x00\x01x");
    }

    #[test]
    fn list_of_structs_matches_twisted() {
        // Twisted: AmpList([(b"inner", Integer())]) for [{inner: 1}, {inner: 2}]
        #[derive(Serialize)]
        struct Item {
            inner: u8,
        }
        assert_eq!(
            value(vec![Item { inner: 1 }, Item { inner: 2 }]),
            b"\x00\x05inner\x00\x011\x00\x00\x00\x05inner\x00\x012\x00\x00"
        );
        assert_eq!(value(Vec::<Item>::new()), b"");
    }

    #[test]
    fn nested_lists_are_lists_of_lists() {
        assert_eq!(
            value(vec![vec![1_u8], vec![2_u8, 3]]),
            b"\x00\x03\x00\x011\x00\x06\x00\x012\x00\x013"
        );
    }

    #[test]
    fn maps_with_scalar_keys() {
        let mut map = BTreeMap::new();
        let _ = map.insert("b", 2_u8);
        let _ = map.insert("a", 1_u8);
        let amp_box = to_box(&map).unwrap();
        assert_eq!(
            amp_box.iter().collect::<Vec<_>>(),
            vec![(&b"a"[..], &b"1"[..]), (&b"b"[..], &b"2"[..])]
        );

        let mut map = BTreeMap::new();
        let _ = map.insert(1_u32, "x");
        assert_eq!(to_box(&map).unwrap().get("1"), Some(&b"x"[..]));
    }

    #[test]
    fn struct_fields_keep_declaration_order() {
        #[derive(Serialize)]
        struct S {
            z: u8,
            a: u8,
        }
        let amp_box = to_box(&S { z: 1, a: 2 }).unwrap();
        assert_eq!(
            amp_box.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            vec![&b"z"[..], &b"a"[..]]
        );
    }

    #[test]
    fn flatten_writes_any_field_type() {
        // Serializing through `flatten` has no type problem; only reading
        // does. See the matching deserializer test.
        #[derive(Serialize)]
        struct Inner {
            n: u32,
            flag: bool,
        }
        #[derive(Serialize)]
        struct Outer {
            a: u8,
            #[serde(flatten)]
            inner: Inner,
        }
        let amp_box = to_box(&Outer {
            a: 1,
            inner: Inner { n: 13, flag: true },
        })
        .unwrap();
        assert_eq!(
            amp_box.iter().collect::<Vec<_>>(),
            vec![
                (&b"a"[..], &b"1"[..]),
                (&b"n"[..], &b"13"[..]),
                (&b"flag"[..], &b"True"[..]),
            ]
        );
    }

    #[test]
    fn top_level_must_be_a_struct_or_map() {
        assert!(matches!(to_box(&1_u8), Err(Error::NotABox)));
        assert!(matches!(to_box(&1_i128), Err(Error::NotABox)));
        assert!(matches!(to_box(&1_u128), Err(Error::NotABox)));
        assert!(matches!(to_box("s"), Err(Error::NotABox)));
        assert!(matches!(to_box(&vec![1_u8]), Err(Error::NotABox)));
        assert!(matches!(to_box(&Option::<u8>::None), Err(Error::NotABox)));
    }

    #[test]
    fn unit_is_an_empty_box() {
        #[derive(Serialize)]
        struct Unit;
        assert!(to_box(&()).unwrap().is_empty());
        assert!(to_box(&Unit).unwrap().is_empty());
    }

    #[test]
    fn nested_struct_as_a_field_is_unsupported() {
        #[derive(Serialize)]
        struct Inner {
            v: u8,
        }
        #[derive(Serialize)]
        struct Outer {
            inner: Inner,
        }
        assert!(matches!(
            to_box(&Outer {
                inner: Inner { v: 1 }
            }),
            Err(Error::Unsupported("a nested struct or map"))
        ));
    }

    #[test]
    fn unrepresentable_values_are_errors() {
        #[derive(Serialize)]
        enum Data {
            Newtype(u8),
            Tuple(u8, u8),
            Struct { v: u8 },
        }
        for data in [Data::Newtype(1), Data::Tuple(1, 2), Data::Struct { v: 1 }] {
            assert!(matches!(
                to_box(&One { v: data }),
                Err(Error::Unsupported("an enum variant carrying data"))
            ));
        }
        assert!(matches!(
            to_box(&One { v: () }),
            Err(Error::Unsupported("a unit value"))
        ));
        assert!(matches!(
            to_box(&One {
                v: vec![Some(1_u8), None]
            }),
            Err(Error::Unsupported("`None` inside a sequence"))
        ));
    }

    #[test]
    fn length_limits_come_from_the_protocol() {
        let long = "x".repeat(65_536);
        assert!(matches!(
            to_box(&One { v: long.as_str() }),
            Err(Error::Protocol(amp_protocol::Error::ValueTooLong {
                len: 65_536
            }))
        ));
        let mut map = BTreeMap::new();
        let _ = map.insert("", 1_u8);
        assert!(matches!(
            to_box(&map),
            Err(Error::Protocol(amp_protocol::Error::EmptyKey))
        ));
        assert!(matches!(
            to_box(&One {
                v: vec![long.as_str()]
            }),
            Err(Error::Protocol(amp_protocol::Error::ValueTooLong {
                len: 65_536
            }))
        ));
    }
}
