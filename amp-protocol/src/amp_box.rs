use std::collections::HashMap;

use crate::{Error, MAX_KEY_LENGTH, MAX_VALUE_LENGTH};

/// One AMP packet: an ordered collection of key/value pairs, each a byte
/// string.
///
/// Keys are unique within a box, between 1 and 255 bytes long, and values are
/// at most 65,535 bytes long. Both limits are enforced when a pair is
/// inserted, so a box that exists can always be encoded. Pairs keep the order
/// they were inserted in, which is the order they are written to the wire.
///
/// On the wire, each pair is a two-byte big-endian key length, the key, a
/// two-byte big-endian value length, and the value. A key length of zero ends
/// the box.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AmpBox {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl AmpBox {
    /// Creates an empty box.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a box from key/value pairs, in order.
    pub(crate) fn from_pairs<I, K, V>(pairs: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let mut amp_box = Self::new();
        for (key, value) in pairs {
            amp_box.insert(key, value)?;
        }
        Ok(amp_box)
    }

    /// Inserts a pair, replacing the value of an existing key in place.
    ///
    /// Returns the replaced value, if any. Fails if the key is empty or
    /// longer than 255 bytes, or the value is longer than 65,535 bytes, in
    /// which case the box is unchanged.
    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<Option<Vec<u8>>, Error>
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let key = key.into();
        let value = value.into();
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if key.len() > MAX_KEY_LENGTH {
            return Err(Error::KeyTooLong { len: key.len() });
        }
        if value.len() > MAX_VALUE_LENGTH {
            return Err(Error::ValueTooLong { len: value.len() });
        }
        Ok(self.insert_unchecked(key, value))
    }

    fn insert_unchecked(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some((_, existing)) => Some(std::mem::replace(existing, value)),
            None => {
                self.entries.push((key, value));
                None
            }
        }
    }

    /// Returns the value stored under `key`.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[u8]> {
        let key = key.as_ref();
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_slice())
    }

    /// Whether the box holds a pair under `key`.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let key = key.as_ref();
        self.entries.iter().any(|(k, _)| k == key)
    }

    /// Removes `key` and returns its value, if it was present.
    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) -> Option<Vec<u8>> {
        let key = key.as_ref();
        let index = self.entries.iter().position(|(k, _)| k == key)?;
        Some(self.entries.remove(index).1)
    }

    /// The number of pairs in the box.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the box has no pairs.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates over the pairs in insertion order.
    pub fn iter(&self) -> Iter<'_> {
        Iter(self.entries.iter())
    }

    /// The number of bytes [`encode`](Self::encode) will produce.
    pub(crate) fn encoded_len(&self) -> usize {
        self.entries
            .iter()
            .map(|(k, v)| 2 + k.len() + 2 + v.len())
            .sum::<usize>()
            + 2
    }

    /// Encodes the box, including its terminator.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        for (key, value) in &self.entries {
            out.extend_from_slice(&(key.len() as u16).to_be_bytes());
            out.extend_from_slice(key);
            out.extend_from_slice(&(value.len() as u16).to_be_bytes());
            out.extend_from_slice(value);
        }
        out.extend_from_slice(&[0, 0]);
        out
    }

    /// Decodes one box from the front of `bytes`.
    ///
    /// Returns the box and the number of bytes it occupied, so a caller
    /// framing a stream can advance past it. Bytes after the box are left
    /// alone. Returns [`Error::Incomplete`] if `bytes` ends before the
    /// terminator, and [`Error::KeyTooLong`] if a key's length prefix exceeds
    /// 255. A key that repeats within one box keeps the last value, matching
    /// the reference implementation.
    ///
    /// This puts no limit on the number of pairs. Each decoded pair costs two
    /// heap allocations and a few dozen bytes of bookkeeping, while on the
    /// wire a pair can be as short as five bytes, so a box of many tiny pairs
    /// decodes to many times its own size. That is fine for bytes you trust,
    /// or that the `tokio` feature's `AmpCodec` has already framed, since it
    /// counts pairs before anything is decoded. For bytes from an untrusted
    /// peer that you frame yourself, use
    /// [`decode_bounded`](Self::decode_bounded) instead.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), Error> {
        Self::decode_bounded(bytes, usize::MAX)
    }

    /// Decodes one box from the front of `bytes`, refusing one with more than
    /// `max_pairs` pairs.
    ///
    /// Behaves as [`decode`](Self::decode) does, except that a box fails with
    /// [`Error::TooManyPairs`] as soon as the start of pair `max_pairs + 1` is
    /// read, before anything is allocated for it. A repeated key counts each
    /// time it appears, so the limit bounds the work of decoding, not just
    /// the size of the result. Set `max_pairs` to the most pairs the peer
    /// could legitimately send in one box, counting the reserved keys
    /// (`_command` and `_ask` on a request, `_answer` on an answer, and
    /// `_error`, `_error_code`, and `_error_description` on an error).
    pub fn decode_bounded(bytes: &[u8], max_pairs: usize) -> Result<(Self, usize), Error> {
        let (pairs, consumed) = decode_pairs(bytes, max_pairs)?;
        let entries = pairs
            .into_iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect();
        Ok((AmpBox { entries }, consumed))
    }

    /// Decodes one box from the front of `bytes` without copying its contents.
    ///
    /// Returns the box's pairs as slices into `bytes`, in wire order, and the
    /// number of bytes the box occupied. The pairs are exactly those
    /// [`decode`](Self::decode) would hold, including a repeated key keeping
    /// its last value, and the errors are the same. Use this when the pairs
    /// are read once and the box itself is not needed.
    ///
    /// Like [`decode`](Self::decode), this puts no limit on the number of
    /// pairs. It copies nothing, but still holds a few dozen bytes per pair,
    /// so the caveat there applies: frame untrusted bytes with `AmpCodec` or
    /// cap their length tightly.
    // A pair of slices is the whole point of this function, and a named
    // alias for it would be one more public type to learn.
    #[expect(clippy::type_complexity)]
    pub fn decode_borrowed(bytes: &[u8]) -> Result<(Vec<(&[u8], &[u8])>, usize), Error> {
        decode_pairs(bytes, usize::MAX)
    }
}

/// Decodes the pairs of the box at the front of `bytes` as slices into it,
/// failing with [`Error::TooManyPairs`] at the start of pair `max_pairs + 1`.
/// The checks happen in the order `scan_frame` makes them, so a box the
/// codec would reject fails here with the same error.
// See `AmpBox::decode_borrowed` for why the type is spelled out.
#[expect(clippy::type_complexity)]
fn decode_pairs(bytes: &[u8], max_pairs: usize) -> Result<(Vec<(&[u8], &[u8])>, usize), Error> {
    let mut pairs: Vec<(&[u8], &[u8])> = Vec::new();
    let mut keys = Keys::new();
    let mut pos = 0;
    // Counts every pair on the wire, where `pairs` holds only distinct keys.
    let mut seen = 0;
    loop {
        let key_len = read_u16(bytes, pos)?;
        pos += 2;
        if key_len == 0 {
            return Ok((pairs, pos));
        }
        if key_len > MAX_KEY_LENGTH {
            return Err(Error::KeyTooLong { len: key_len });
        }
        if seen == max_pairs {
            return Err(Error::TooManyPairs { limit: max_pairs });
        }
        seen += 1;
        let key = read_slice(bytes, pos, key_len)?;
        pos += key_len;

        let value_len = read_u16(bytes, pos)?;
        pos += 2;
        let value = read_slice(bytes, pos, value_len)?;
        pos += value_len;

        match keys.position(key) {
            Some(index) => pairs[index].1 = value,
            None => {
                keys.push(key, pairs.len());
                pairs.push((key, value));
            }
        }
    }
}

/// The keys seen so far while decoding a box, and where each one sits in the
/// entries, so a repeated key can overwrite its value in place.
///
/// Boxes are usually a handful of pairs, for which a linear scan over key
/// slices held on the stack beats hashing and costs no allocation. Once a box
/// outgrows that, a hash index takes over so a large box costs one lookup per
/// pair instead of a scan per pair.
// The linear variant is a few hundred bytes larger than the map. It lives on
// the stack for the length of one decode, and keeping it there is the point.
// Slices are half the width on wasm32, so there the gap stays under the
// lint's threshold and the `expect` would itself warn as unfulfilled.
#[cfg_attr(not(target_family = "wasm"), expect(clippy::large_enum_variant))]
enum Keys<'a> {
    Linear {
        keys: [&'a [u8]; LINEAR_SCAN_LIMIT],
        len: usize,
    },
    Indexed(HashMap<&'a [u8], usize>),
}

/// The most keys worth scanning linearly before building a hash index.
const LINEAR_SCAN_LIMIT: usize = 16;

impl<'a> Keys<'a> {
    fn new() -> Self {
        Keys::Linear {
            keys: [&[]; LINEAR_SCAN_LIMIT],
            len: 0,
        }
    }

    /// The index of the entry holding `key`, if it has been seen before.
    fn position(&self, key: &[u8]) -> Option<usize> {
        match self {
            Keys::Linear { keys, len } => keys[..*len].iter().position(|k| *k == key),
            Keys::Indexed(index) => index.get(key).copied(),
        }
    }

    /// Records that `key` is held by the entry at `index`. Keys are pushed in
    /// entry order, so in the linear form the position is the index.
    fn push(&mut self, key: &'a [u8], index: usize) {
        match self {
            Keys::Linear { keys, len } if *len < LINEAR_SCAN_LIMIT => {
                keys[*len] = key;
                *len += 1;
            }
            Keys::Linear { keys, .. } => {
                let mut map = HashMap::with_capacity(LINEAR_SCAN_LIMIT * 2);
                map.extend(keys.iter().copied().zip(0..));
                map.insert(key, index);
                *self = Keys::Indexed(map);
            }
            Keys::Indexed(map) => {
                map.insert(key, index);
            }
        }
    }
}

/// Where a scan of the box at the front of a buffer stopped.
#[cfg(feature = "tokio")]
pub(crate) enum Frame {
    /// The whole box, terminator included, is present and this long.
    Complete(usize),
    /// The box runs past the end of the buffer. This much of it has been
    /// scanned, so a later scan can resume there once more arrives.
    Incomplete(Scanned),
}

/// How much of an unfinished box a scan has covered: a prefix of whole pairs.
#[cfg(feature = "tokio")]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Scanned {
    /// The length of that prefix in bytes.
    pub(crate) bytes: usize,
    /// The number of pairs in it.
    pub(crate) pairs: usize,
}

/// Walks the pairs of the box at the front of `bytes` without decoding or
/// copying them, to find where it ends. Starts at `from`: nothing, or the
/// position a previous [`Frame::Incomplete`] reported for the same buffer.
///
/// Fails with [`Error::KeyTooLong`] as [`AmpBox::decode`] would, so a
/// malformed box is rejected as soon as the bad length prefix arrives, and
/// with [`Error::TooManyPairs`] as soon as the start of pair `max_pairs + 1`
/// does. Counting pairs here, before anything is allocated for them, is what
/// keeps a box of millions of tiny pairs from costing many times its size.
#[cfg(feature = "tokio")]
pub(crate) fn scan_frame(bytes: &[u8], from: Scanned, max_pairs: usize) -> Result<Frame, Error> {
    let mut pos = from.bytes;
    let mut pairs = from.pairs;
    loop {
        let start = pos;
        let Ok(key_len) = read_u16(bytes, pos) else {
            return Ok(Frame::Incomplete(Scanned {
                bytes: start,
                pairs,
            }));
        };
        pos += 2;
        if key_len == 0 {
            return Ok(Frame::Complete(pos));
        }
        if key_len > MAX_KEY_LENGTH {
            return Err(Error::KeyTooLong { len: key_len });
        }
        if pairs == max_pairs {
            return Err(Error::TooManyPairs { limit: max_pairs });
        }
        pos += key_len;
        let Ok(value_len) = read_u16(bytes, pos) else {
            return Ok(Frame::Incomplete(Scanned {
                bytes: start,
                pairs,
            }));
        };
        pos += 2 + value_len;
        if pos > bytes.len() {
            return Ok(Frame::Incomplete(Scanned {
                bytes: start,
                pairs,
            }));
        }
        pairs += 1;
    }
}

fn read_u16(bytes: &[u8], pos: usize) -> Result<usize, Error> {
    match bytes.get(pos..pos + 2) {
        Some(&[high, low]) => Ok(u16::from_be_bytes([high, low]) as usize),
        _ => Err(Error::Incomplete),
    }
}

fn read_slice(bytes: &[u8], pos: usize, len: usize) -> Result<&[u8], Error> {
    bytes.get(pos..pos + len).ok_or(Error::Incomplete)
}

impl IntoIterator for AmpBox {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = std::vec::IntoIter<(Vec<u8>, Vec<u8>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<'a> IntoIterator for &'a AmpBox {
    type Item = (&'a [u8], &'a [u8]);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterates over the pairs of an [`AmpBox`] in insertion order, as borrowed
/// key and value slices.
///
/// Returned by [`AmpBox::iter`] and by iterating over `&AmpBox`.
#[derive(Clone, Debug)]
pub struct Iter<'a>(std::slice::Iter<'a, (Vec<u8>, Vec<u8>)>);

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for Iter<'_> {}

#[cfg(test)]
mod test {
    use super::*;

    fn sum_request() -> AmpBox {
        AmpBox::from_pairs([("_ask", "1"), ("_command", "Sum"), ("a", "13"), ("b", "81")]).unwrap()
    }

    const SUM_REQUEST: &[u8] =
        b"\x00\x04_ask\x00\x011\x00\x08_command\x00\x03Sum\x00\x01a\x00\x0213\x00\x01b\x00\x0281\x00\x00";

    #[test]
    fn insert_replaces_in_place() {
        let mut amp_box = AmpBox::new();
        assert_eq!(amp_box.insert("a", "1").unwrap(), None);
        assert_eq!(amp_box.insert("b", "2").unwrap(), None);
        assert_eq!(amp_box.insert("a", "3").unwrap(), Some(b"1".to_vec()));
        assert_eq!(amp_box.len(), 2);
        assert_eq!(
            amp_box.iter().collect::<Vec<_>>(),
            vec![(&b"a"[..], &b"3"[..]), (&b"b"[..], &b"2"[..])]
        );
    }

    #[test]
    fn get_contains_remove() {
        let mut amp_box = sum_request();
        assert_eq!(amp_box.get("a"), Some(&b"13"[..]));
        assert_eq!(amp_box.get(b"b"), Some(&b"81"[..]));
        assert_eq!(amp_box.get("missing"), None);
        assert!(amp_box.contains_key("_ask"));
        assert!(amp_box.contains_key(b"b"));
        assert!(!amp_box.contains_key("missing"));
        assert_eq!(amp_box.remove("_ask"), Some(b"1".to_vec()));
        assert_eq!(amp_box.remove("_ask"), None);
        assert_eq!(amp_box.get("_ask"), None);
        assert!(!amp_box.contains_key("_ask"));
        assert_eq!(amp_box.len(), 3);
    }

    #[test]
    fn borrowed_iteration_matches_iter() {
        let amp_box = sum_request();
        let mut seen = Vec::new();
        for (key, value) in &amp_box {
            seen.push((key, value));
        }
        assert_eq!(seen, amp_box.iter().collect::<Vec<_>>());
        assert_eq!(seen.len(), 4);
        assert_eq!(amp_box.iter().len(), 4);
        // The box is still usable after borrowed iteration.
        assert_eq!(amp_box.get("a"), Some(&b"13"[..]));
    }

    #[test]
    fn rejects_empty_key() {
        let mut amp_box = AmpBox::new();
        assert!(matches!(amp_box.insert("", "x"), Err(Error::EmptyKey)));
        assert!(amp_box.is_empty());
    }

    #[test]
    fn enforces_key_length_limit() {
        let mut amp_box = AmpBox::new();
        amp_box.insert(vec![b'k'; MAX_KEY_LENGTH], "").unwrap();
        assert!(matches!(
            amp_box.insert(vec![b'k'; MAX_KEY_LENGTH + 1], ""),
            Err(Error::KeyTooLong { len: 256 })
        ));
    }

    #[test]
    fn enforces_value_length_limit() {
        let mut amp_box = AmpBox::new();
        amp_box.insert("k", vec![0; MAX_VALUE_LENGTH]).unwrap();
        assert!(matches!(
            amp_box.insert("k", vec![0; MAX_VALUE_LENGTH + 1]),
            Err(Error::ValueTooLong { len: 65536 })
        ));
        // The failed insert left the old value alone.
        assert_eq!(amp_box.get("k").unwrap().len(), MAX_VALUE_LENGTH);
    }

    #[test]
    fn encodes_reference_request() {
        assert_eq!(sum_request().encode(), SUM_REQUEST);
        assert_eq!(sum_request().encoded_len(), SUM_REQUEST.len());
    }

    #[test]
    fn encodes_empty_box_as_terminator() {
        assert_eq!(AmpBox::new().encode(), b"\x00\x00");
    }

    #[test]
    fn encodes_empty_value() {
        let amp_box = AmpBox::from_pairs([("k", "")]).unwrap();
        assert_eq!(amp_box.encode(), b"\x00\x01k\x00\x00\x00\x00");
    }

    #[test]
    fn encodes_largest_lengths() {
        let amp_box =
            AmpBox::from_pairs([(vec![b'k'; MAX_KEY_LENGTH], vec![b'v'; MAX_VALUE_LENGTH])])
                .unwrap();
        let bytes = amp_box.encode();
        assert_eq!(&bytes[..2], &[0x00, 0xFF]);
        assert_eq!(
            &bytes[2 + MAX_KEY_LENGTH..4 + MAX_KEY_LENGTH],
            &[0xFF, 0xFF]
        );
        assert_eq!(AmpBox::decode(&bytes).unwrap(), (amp_box, bytes.len()));
    }

    #[test]
    fn decodes_reference_request() {
        let (amp_box, consumed) = AmpBox::decode(SUM_REQUEST).unwrap();
        assert_eq!(amp_box, sum_request());
        assert_eq!(consumed, SUM_REQUEST.len());
    }

    #[test]
    fn decode_reports_consumed_and_leaves_the_rest() {
        let mut stream = SUM_REQUEST.to_vec();
        stream.extend_from_slice(b"\x00\x07_answer\x00\x011\x00\x05total\x00\x0294\x00\x00");
        let (first, consumed) = AmpBox::decode(&stream).unwrap();
        assert_eq!(first, sum_request());
        let (second, consumed_second) = AmpBox::decode(&stream[consumed..]).unwrap();
        assert_eq!(second.get("total"), Some(&b"94"[..]));
        assert_eq!(consumed + consumed_second, stream.len());
    }

    #[test]
    fn decode_is_incomplete_at_every_truncation() {
        for len in 0..SUM_REQUEST.len() {
            assert!(
                matches!(AmpBox::decode(&SUM_REQUEST[..len]), Err(Error::Incomplete)),
                "truncated to {len} bytes"
            );
        }
    }

    #[test]
    fn decode_rejects_key_longer_than_255() {
        let bytes = b"\x01\x00";
        assert!(matches!(
            AmpBox::decode(bytes),
            Err(Error::KeyTooLong { len: 256 })
        ));
    }

    #[test]
    fn decode_empty_box() {
        let (amp_box, consumed) = AmpBox::decode(b"\x00\x00rest").unwrap();
        assert!(amp_box.is_empty());
        assert_eq!(consumed, 2);
    }

    #[test]
    fn decode_keeps_last_duplicate() {
        let (amp_box, _) = AmpBox::decode(b"\x00\x01k\x00\x011\x00\x01k\x00\x012\x00\x00").unwrap();
        assert_eq!(amp_box.len(), 1);
        assert_eq!(amp_box.get("k"), Some(&b"2"[..]));
    }

    #[test]
    fn decode_bounded_accepts_up_to_the_limit() {
        let (amp_box, consumed) = AmpBox::decode_bounded(SUM_REQUEST, 4).unwrap();
        assert_eq!(amp_box, sum_request());
        assert_eq!(consumed, SUM_REQUEST.len());
        let (empty, consumed) = AmpBox::decode_bounded(b"\x00\x00", 0).unwrap();
        assert!(empty.is_empty());
        assert_eq!(consumed, 2);
    }

    #[test]
    fn decode_bounded_rejects_one_pair_too_many() {
        assert!(matches!(
            AmpBox::decode_bounded(SUM_REQUEST, 3),
            Err(Error::TooManyPairs { limit: 3 })
        ));
        assert!(matches!(
            AmpBox::decode_bounded(b"\x00\x01k\x00\x00", 0),
            Err(Error::TooManyPairs { limit: 0 })
        ));
    }

    /// The limit is checked as soon as the length prefix of one pair too many
    /// is read, so it fires before the box is complete and before a bad key
    /// in that pair is seen, but after a bad length prefix is.
    #[test]
    fn decode_bounded_rejects_at_the_start_of_the_extra_pair() {
        let mut bytes = pair("a", "1");
        bytes.extend([0x00, 0x05]);
        assert!(matches!(
            AmpBox::decode_bounded(&bytes, 1),
            Err(Error::TooManyPairs { limit: 1 })
        ));
        assert!(matches!(
            AmpBox::decode_bounded(&bytes, 2),
            Err(Error::Incomplete)
        ));
        let mut bytes = pair("a", "1");
        bytes.extend([0x01, 0x00]);
        assert!(matches!(
            AmpBox::decode_bounded(&bytes, 1),
            Err(Error::KeyTooLong { len: 256 })
        ));
    }

    /// A repeated key is one entry in the box but two pairs on the wire, and
    /// it is the wire that costs work, so it counts twice.
    #[test]
    fn decode_bounded_counts_a_repeated_key_each_time() {
        let mut bytes = pair("k", "1");
        bytes.extend(pair("k", "2"));
        bytes.extend([0, 0]);
        assert!(matches!(
            AmpBox::decode_bounded(&bytes, 1),
            Err(Error::TooManyPairs { limit: 1 })
        ));
        let (amp_box, _) = AmpBox::decode_bounded(&bytes, 2).unwrap();
        assert_eq!(amp_box.len(), 1);
        assert_eq!(amp_box.get("k"), Some(&b"2"[..]));
    }

    #[test]
    fn decode_borrowed_matches_decode() {
        let input = b"\x00\x01k\x00\x011\x00\x01j\x00\x02ab\x00\x01k\x00\x012\x00\x00tail";
        let (pairs, consumed) = AmpBox::decode_borrowed(input).unwrap();
        assert_eq!(pairs, vec![(&b"k"[..], &b"2"[..]), (&b"j"[..], &b"ab"[..])]);
        assert_eq!(consumed, input.len() - 4);
        let (amp_box, owned_consumed) = AmpBox::decode(input).unwrap();
        assert_eq!(owned_consumed, consumed);
        assert_eq!(amp_box.iter().collect::<Vec<_>>(), pairs);
        assert!(matches!(
            AmpBox::decode_borrowed(&input[..consumed - 1]),
            Err(Error::Incomplete)
        ));
    }

    /// Encodes one pair by hand, since `AmpBox` will not hold a duplicate.
    fn pair(key: &str, value: &str) -> Vec<u8> {
        let mut out = (key.len() as u16).to_be_bytes().to_vec();
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(&(value.len() as u16).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
        out
    }

    /// A box with more keys than the linear scan covers switches to the hash
    /// index part way through. Duplicates of keys seen before and after the
    /// switch must both overwrite the earlier value in its original position.
    #[test]
    fn decode_keeps_last_duplicate_past_the_linear_scan_limit() {
        let keys = LINEAR_SCAN_LIMIT * 3;
        let last = format!("k{}", keys - 1);
        let switch = format!("k{LINEAR_SCAN_LIMIT}");
        let mut bytes = Vec::new();
        for i in 0..keys {
            bytes.extend(pair(&format!("k{i}"), "old"));
        }
        bytes.extend(pair("k0", "first"));
        bytes.extend(pair(&switch, "at switch"));
        bytes.extend(pair(&last, "last"));
        bytes.extend([0, 0]);

        let (amp_box, consumed) = AmpBox::decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(amp_box.len(), keys);
        let order: Vec<_> = amp_box.iter().map(|(k, _)| k).collect();
        let expected: Vec<_> = (0..keys).map(|i| format!("k{i}")).collect();
        assert_eq!(
            order,
            expected.iter().map(String::as_bytes).collect::<Vec<_>>()
        );
        assert_eq!(amp_box.get("k0"), Some(&b"first"[..]));
        assert_eq!(amp_box.get("k1"), Some(&b"old"[..]));
        assert_eq!(amp_box.get(&switch), Some(&b"at switch"[..]));
        assert_eq!(amp_box.get(&last), Some(&b"last"[..]));
    }

    #[test]
    fn decode_accepts_non_utf8() {
        let (amp_box, _) = AmpBox::decode(b"\x00\x02\xff\xfe\x00\x02\x00\x01\x00\x00").unwrap();
        assert_eq!(amp_box.get(b"\xff\xfe"), Some(&b"\x00\x01"[..]));
    }

    /// The pair boundaries of `SUM_REQUEST`, excluding the terminator. The
    /// boundary at index `i` follows `i` whole pairs.
    #[cfg(feature = "tokio")]
    const SUM_REQUEST_PAIRS: [usize; 5] = [0, 9, 24, 31, 38];

    /// The scan position after the first `pairs` pairs of `SUM_REQUEST`.
    #[cfg(feature = "tokio")]
    fn scanned(pairs: usize) -> Scanned {
        Scanned {
            bytes: SUM_REQUEST_PAIRS[pairs],
            pairs,
        }
    }

    #[cfg(feature = "tokio")]
    const UNLIMITED: usize = usize::MAX;

    #[cfg(feature = "tokio")]
    #[test]
    fn scan_frame_finds_the_end_of_the_box() {
        assert!(matches!(
            scan_frame(SUM_REQUEST, scanned(0), UNLIMITED),
            Ok(Frame::Complete(40))
        ));
        assert!(matches!(
            scan_frame(b"\x00\x00rest", scanned(0), UNLIMITED),
            Ok(Frame::Complete(2))
        ));
        let mut stream = SUM_REQUEST.to_vec();
        stream.extend_from_slice(SUM_REQUEST);
        assert!(matches!(
            scan_frame(&stream, scanned(0), UNLIMITED),
            Ok(Frame::Complete(40))
        ));
    }

    /// At every truncation the scan stops at the start of the pair that is
    /// cut off, with the count of pairs before it, and resuming from any
    /// earlier boundary agrees with a scan from the start.
    #[cfg(feature = "tokio")]
    #[test]
    fn scan_frame_stops_at_the_last_whole_pair() {
        for len in 0..SUM_REQUEST.len() {
            let bytes = &SUM_REQUEST[..len];
            let whole = SUM_REQUEST_PAIRS
                .into_iter()
                .rposition(|b| b <= len)
                .unwrap();
            for from in 0..=whole {
                assert!(
                    matches!(
                        scan_frame(bytes, scanned(from), UNLIMITED),
                        Ok(Frame::Incomplete(at)) if at == scanned(whole)
                    ),
                    "truncated to {len} bytes, scanning from {from} pairs"
                );
            }
        }
        assert!(matches!(
            scan_frame(SUM_REQUEST, scanned(4), UNLIMITED),
            Ok(Frame::Complete(40))
        ));
    }

    /// The limit is on pairs seen, so a box at the limit passes and one over
    /// it fails as soon as the extra pair's key length is in, whether the
    /// scan started from the beginning or resumed partway through.
    #[cfg(feature = "tokio")]
    #[test]
    fn scan_frame_rejects_a_box_over_the_pair_limit() {
        assert!(matches!(
            scan_frame(SUM_REQUEST, scanned(0), 4),
            Ok(Frame::Complete(40))
        ));
        let cut = &SUM_REQUEST[..SUM_REQUEST_PAIRS[3] + 2];
        assert!(matches!(
            scan_frame(cut, scanned(0), 3),
            Err(Error::TooManyPairs { limit: 3 })
        ));
        assert!(matches!(
            scan_frame(cut, scanned(2), 3),
            Err(Error::TooManyPairs { limit: 3 })
        ));
        assert!(matches!(
            scan_frame(&cut[..cut.len() - 1], scanned(2), 3),
            Ok(Frame::Incomplete(at)) if at == scanned(3)
        ));
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn scan_frame_rejects_key_longer_than_255() {
        let bytes = b"\x00\x01k\x00\x01v\x01\x00";
        assert!(matches!(
            scan_frame(bytes, Scanned::default(), UNLIMITED),
            Err(Error::KeyTooLong { len: 256 })
        ));
        assert!(matches!(
            scan_frame(bytes, Scanned { bytes: 6, pairs: 1 }, UNLIMITED),
            Err(Error::KeyTooLong { len: 256 })
        ));
    }

    #[test]
    fn round_trips() {
        let amp_box = AmpBox::from_pairs([
            ("empty", b"".to_vec()),
            ("binary", vec![0, 1, 2, 255]),
            ("text", b"h\xc3\xa9llo".to_vec()),
        ])
        .unwrap();
        let bytes = amp_box.encode();
        assert_eq!(AmpBox::decode(&bytes).unwrap(), (amp_box, bytes.len()));
    }
}
