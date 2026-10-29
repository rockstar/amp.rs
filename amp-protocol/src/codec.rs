//! Framing for boxes on a byte stream, via [`tokio_util::codec`].
//!
//! Enabled by the `tokio` feature.

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::amp_box::{Frame, Scanned, scan_frame};
use crate::{AmpBox, Error};

/// Splits a byte stream into boxes and writes boxes back out.
///
/// Decoding yields an [`AmpBox`] per packet, and encoding writes one.
///
/// ```
/// use amp_protocol::codec::AmpCodec;
/// use tokio_util::codec::Framed;
/// # fn framed<S: tokio::io::AsyncRead + tokio::io::AsyncWrite>(stream: S) {
/// let framed = Framed::new(stream, AmpCodec::new());
/// # }
/// ```
///
/// A box is read into memory in full before it is decoded, so the codec
/// holds a box to two limits rather than buffer without bound:
///
/// * Its encoding may be at most the maximum frame length, 1 MiB by default.
///   A longer box fails with [`Error::FrameTooLong`].
/// * It may hold at most the maximum number of pairs, 1,024 by default. A box
///   with more fails with [`Error::TooManyPairs`]. A decoded pair costs far
///   more than the few bytes it can take on the wire, so without this limit
///   a frame of tiny pairs would cost many times its length to decode.
///
/// Both are checked as the box arrives, so an offending box is rejected as
/// soon as it passes a limit and is never decoded. [`new`](AmpCodec::new)
/// uses the defaults, [`with_max_frame_length`](AmpCodec::with_max_frame_length)
/// sets the first, and [`with_limits`](AmpCodec::with_limits) sets both.
///
/// While a box is still arriving, the codec remembers how much of it has
/// already been checked, so each call scans only the new bytes and the box is
/// decoded exactly once, when it is complete. That makes a codec specific to
/// one stream. A clone keeps the limits but not that position, so it is
/// ready for a fresh stream: clone one codec per connection.
///
/// [`Framed`]: tokio_util::codec::Framed
#[derive(Debug)]
pub struct AmpCodec {
    max_frame_length: usize,
    max_pairs: usize,
    /// How much of the box at the front of the buffer has been scanned and
    /// found to be whole pairs, while its terminator has not arrived yet.
    scanned: Scanned,
}

/// The frame length [`AmpCodec::new`] allows: 1 MiB. A value is at most
/// 65,535 bytes, so that is room for a box of sixteen full-size values,
/// which is beyond any sensible packet, while a peer that never finishes a
/// box costs at most this much to buffer.
const DEFAULT_MAX_FRAME_LENGTH: usize = 1024 * 1024;

/// The number of pairs [`AmpCodec::new`] allows. A box has a pair per
/// argument of a command plus a few the protocol reserves, so this too is far
/// beyond any sensible packet.
const DEFAULT_MAX_PAIRS: usize = 1024;

impl AmpCodec {
    /// Creates a codec with the default limits: a frame length of 1 MiB and
    /// 1,024 pairs.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a codec that rejects a box whose encoding is longer than
    /// `max_frame_length` bytes, counting the key and value length prefixes
    /// and the two-byte terminator, with the default limit of 1,024 pairs.
    /// Decoding such a box fails with [`Error::FrameTooLong`], as soon as
    /// that many bytes of it have arrived without a terminator.
    ///
    /// Set this to the largest box the peer could legitimately send. Each
    /// value is at most 65,535 bytes and each key at most 255, so the length
    /// is bounded by the number of pairs a box carries.
    #[must_use]
    pub fn with_max_frame_length(max_frame_length: usize) -> Self {
        Self::with_limits(max_frame_length, DEFAULT_MAX_PAIRS)
    }

    /// Creates a codec with both limits set: a box longer than
    /// `max_frame_length` bytes fails with [`Error::FrameTooLong`], as
    /// [`with_max_frame_length`](AmpCodec::with_max_frame_length) describes,
    /// and one with more than `max_pairs` pairs fails with
    /// [`Error::TooManyPairs`], as soon as the start of one pair too many has
    /// arrived.
    ///
    /// Set `max_pairs` to the most pairs the peer could legitimately send in
    /// one box, counting the reserved keys (`_command` and `_ask` on a
    /// request, `_answer` on an answer, and `_error`, `_error_code`, and
    /// `_error_description` on an error). A repeated key counts each time it
    /// appears.
    #[must_use]
    pub fn with_limits(max_frame_length: usize, max_pairs: usize) -> Self {
        AmpCodec {
            max_frame_length,
            max_pairs,
            scanned: Scanned::default(),
        }
    }
}

impl Default for AmpCodec {
    fn default() -> Self {
        Self::with_limits(DEFAULT_MAX_FRAME_LENGTH, DEFAULT_MAX_PAIRS)
    }
}

/// A clone is a codec for another stream with the same limits. The scan
/// position is an offset into one buffer and means nothing in any other, so
/// it is not carried over.
impl Clone for AmpCodec {
    fn clone(&self) -> Self {
        Self::with_limits(self.max_frame_length, self.max_pairs)
    }
}

impl Decoder for AmpCodec {
    type Item = AmpBox;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<AmpBox>, Error> {
        // The scan finds the end of the box and counts its pairs without
        // allocating for them, so both limits are checked before anything is
        // decoded. It resumes from where the last call stopped, so a box
        // that trickles in is walked once in full and decoded once, when it
        // is complete.
        match scan_frame(src, self.scanned, self.max_pairs) {
            Ok(Frame::Complete(len)) => {
                self.scanned = Scanned::default();
                if len > self.max_frame_length {
                    return Err(Error::FrameTooLong {
                        limit: self.max_frame_length,
                    });
                }
                // The scan proved the box ends at `len`, so this decode sees
                // exactly one complete box and cannot report it incomplete.
                let (amp_box, consumed) = AmpBox::decode(&src[..len])?;
                src.advance(consumed);
                Ok(Some(amp_box))
            }
            Ok(Frame::Incomplete(scanned)) => {
                // Every byte in `src` belongs to this box, since the ones
                // before it were consumed. Once there are more than the
                // limit, this box can never be accepted, so stop buffering it.
                if src.len() > self.max_frame_length {
                    self.scanned = Scanned::default();
                    return Err(Error::FrameTooLong {
                        limit: self.max_frame_length,
                    });
                }
                self.scanned = scanned;
                Ok(None)
            }
            Err(err) => {
                self.scanned = Scanned::default();
                Err(err)
            }
        }
    }
}

impl Encoder<AmpBox> for AmpCodec {
    type Error = Error;

    // `AmpBox` caps keys at 255 bytes and values at 65,535, so both lengths
    // fit their two-byte prefix.
    #[expect(clippy::cast_possible_truncation)]
    fn encode(&mut self, item: AmpBox, dst: &mut BytesMut) -> Result<(), Error> {
        dst.reserve(item.encoded_len());
        for (key, value) in &item {
            dst.put_u16(key.len() as u16);
            dst.put_slice(key);
            dst.put_u16(value.len() as u16);
            dst.put_slice(value);
        }
        dst.put_u16(0);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const SUM_REQUEST: &[u8] =
        b"\x00\x04_ask\x00\x011\x00\x08_command\x00\x03Sum\x00\x01a\x00\x0213\x00\x01b\x00\x0281\x00\x00";

    /// One encoded pair, four bytes long, with no terminator after it.
    const PAIR: &[u8] = b"\x00\x01k\x00\x01v";

    #[test]
    fn decodes_boxes_as_bytes_arrive() {
        let mut codec = AmpCodec::new();
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&SUM_REQUEST[..5]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 5, "an incomplete box is left in the buffer");

        buf.extend_from_slice(&SUM_REQUEST[5..]);
        buf.extend_from_slice(&SUM_REQUEST[..3]);
        let amp_box = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(amp_box.get("_command"), Some(&b"Sum"[..]));
        assert_eq!(buf.len(), 3, "the next box's bytes are kept");

        buf.extend_from_slice(&SUM_REQUEST[3..]);
        assert!(codec.decode(&mut buf).unwrap().is_some());
        assert!(buf.is_empty());
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_surfaces_malformed_input() {
        let mut codec = AmpCodec::new();
        let mut buf = BytesMut::from(&b"\x01\x00"[..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(Error::KeyTooLong { len: 256 })
        ));
    }

    #[test]
    fn decode_eof_with_partial_box_is_an_error() {
        let mut codec = AmpCodec::new();
        let mut buf = BytesMut::from(&SUM_REQUEST[..5]);
        assert!(matches!(codec.decode_eof(&mut buf), Err(Error::Io(_))));
    }

    #[test]
    fn encodes_box() {
        let mut codec = AmpCodec::new();
        let mut buf = BytesMut::new();
        let (amp_box, _) = AmpBox::decode(SUM_REQUEST).unwrap();
        codec.encode(amp_box, &mut buf).unwrap();
        assert_eq!(&buf[..], SUM_REQUEST);
    }

    #[test]
    fn default_limits_are_1_mib_and_1024_pairs() {
        for codec in [AmpCodec::new(), AmpCodec::default()] {
            assert_eq!(codec.max_frame_length, 1024 * 1024);
            assert_eq!(codec.max_pairs, 1024);
        }
        let codec = AmpCodec::with_max_frame_length(1024);
        assert_eq!(codec.max_frame_length, 1024);
        assert_eq!(codec.max_pairs, 1024);
    }

    /// The attack: a frame of the smallest possible pairs, each with a
    /// unique key, decodes to many times its own size. The codec must count
    /// the pairs and refuse the box before it decodes any of them.
    #[test]
    fn complete_box_over_the_pair_limit_is_rejected() {
        let mut bytes = Vec::new();
        for i in 0..4u8 {
            bytes.extend_from_slice(&[0, 1, b'a' + i, 0, 0]);
        }
        bytes.extend_from_slice(&[0, 0]);

        let mut codec = AmpCodec::with_limits(1024, 3);
        let mut buf = BytesMut::from(&bytes[..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(Error::TooManyPairs { limit: 3 })
        ));
        assert_eq!(buf.len(), bytes.len(), "the box is left where it was");
        assert_eq!(codec.scanned, Scanned::default());

        let mut codec = AmpCodec::with_limits(1024, 4);
        let amp_box = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(amp_box.len(), 4, "a box at the limit is accepted");
        assert!(buf.is_empty());
    }

    /// A box that trickles in is refused as soon as one pair too many starts
    /// to arrive, with no terminator in sight. The pair count carries over
    /// from one call to the next like the byte position does.
    #[test]
    fn unterminated_box_over_the_pair_limit_is_rejected() {
        let mut codec = AmpCodec::with_limits(1024, 2);
        let mut buf = BytesMut::new();
        for _ in 0..2 {
            buf.extend_from_slice(PAIR);
            assert!(codec.decode(&mut buf).unwrap().is_none());
        }
        assert_eq!(
            codec.scanned,
            Scanned {
                bytes: 2 * PAIR.len(),
                pairs: 2
            }
        );

        buf.extend_from_slice(&PAIR[..2]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(Error::TooManyPairs { limit: 2 })
        ));
        assert_eq!(codec.scanned, Scanned::default());
    }

    /// Repeated keys collapse into one entry when decoded, but each one is
    /// a pair on the wire and costs work to decode, so each one counts.
    #[test]
    fn repeated_keys_count_as_separate_pairs() {
        let mut codec = AmpCodec::with_limits(1024, 2);
        let mut buf = BytesMut::new();
        for _ in 0..3 {
            buf.extend_from_slice(PAIR);
        }
        buf.extend_from_slice(b"\x00\x00");
        assert!(matches!(
            codec.decode(&mut buf),
            Err(Error::TooManyPairs { limit: 2 })
        ));
    }

    /// The attack: pairs keep arriving and the terminator never does. The
    /// codec must give up once the buffer passes the limit instead of
    /// asking for more.
    #[test]
    fn unterminated_box_past_the_limit_is_rejected() {
        let limit = PAIR.len() * 3;
        let mut codec = AmpCodec::with_max_frame_length(limit);
        let mut buf = BytesMut::new();

        for _ in 0..3 {
            buf.extend_from_slice(PAIR);
            assert!(codec.decode(&mut buf).unwrap().is_none());
        }
        assert_eq!(buf.len(), limit, "exactly the limit is still incomplete");

        buf.extend_from_slice(&PAIR[..1]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(Error::FrameTooLong { limit: l }) if l == limit
        ));
    }

    /// A box that arrives whole is held to the same limit as one that
    /// trickles in.
    #[test]
    fn complete_box_past_the_limit_is_rejected() {
        let mut codec = AmpCodec::with_max_frame_length(SUM_REQUEST.len() - 1);
        let mut buf = BytesMut::from(SUM_REQUEST);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(Error::FrameTooLong { .. })
        ));
    }

    #[test]
    fn box_at_the_limit_is_accepted() {
        let mut codec = AmpCodec::with_max_frame_length(SUM_REQUEST.len());
        let mut buf = BytesMut::from(SUM_REQUEST);
        assert!(codec.decode(&mut buf).unwrap().is_some());
        assert!(buf.is_empty());
    }

    /// A large box that trickles in is checked incrementally: each call
    /// scans only from the pair boundary where the last one stopped, and the
    /// box is decoded once, when it is complete.
    #[test]
    fn chunked_box_is_scanned_incrementally() {
        const PAIR_LEN: usize = 2 + 4 + 2 + 100;
        let mut amp_box = AmpBox::new();
        for i in 0..1000u32 {
            let _ = amp_box
                .insert(
                    format!("k{i:03}"),
                    vec![u8::try_from(i % 256).unwrap(); 100],
                )
                .unwrap();
        }
        let bytes = amp_box.encode();

        let mut codec = AmpCodec::new();
        let mut buf = BytesMut::new();
        let mut decoded = None;
        // A chunk size that never lines up with a pair.
        for chunk in bytes.chunks(97) {
            buf.extend_from_slice(chunk);
            if let Some(amp_box) = codec.decode(&mut buf).unwrap() {
                decoded = Some(amp_box);
            } else {
                assert_eq!(
                    codec.scanned.bytes % PAIR_LEN,
                    0,
                    "scan stops at a pair boundary"
                );
                assert!(
                    buf.len() - codec.scanned.bytes < PAIR_LEN,
                    "scan covers every whole pair"
                );
            }
        }
        assert_eq!(decoded.unwrap(), amp_box);
        assert!(buf.is_empty());
        assert_eq!(codec.scanned, Scanned::default());
    }

    /// The scan position belongs to one box. It is dropped once that box is
    /// decoded or rejected, so the next box starts from its first byte.
    #[test]
    fn scan_position_is_reset_after_a_box_or_an_error() {
        let mut codec = AmpCodec::new();
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&SUM_REQUEST[..12]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert_eq!(codec.scanned, Scanned { bytes: 9, pairs: 1 });
        buf.extend_from_slice(&SUM_REQUEST[12..]);
        assert!(codec.decode(&mut buf).unwrap().is_some());
        assert_eq!(codec.scanned, Scanned::default());

        buf.extend_from_slice(PAIR);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert_eq!(
            codec.scanned,
            Scanned {
                bytes: PAIR.len(),
                pairs: 1
            }
        );
        buf.extend_from_slice(b"\x01\x00");
        assert!(matches!(
            codec.decode(&mut buf),
            Err(Error::KeyTooLong { len: 256 })
        ));
        assert_eq!(codec.scanned, Scanned::default());
    }

    /// A codec cloned while a box is still arriving starts the new stream
    /// from its first byte, instead of from where the old one left off.
    #[test]
    fn clone_starts_from_the_beginning_of_a_new_stream() {
        let mut codec = AmpCodec::with_max_frame_length(1024);
        let mut buf = BytesMut::from(&SUM_REQUEST[..30]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert_eq!(
            codec.scanned,
            Scanned {
                bytes: 24,
                pairs: 2
            }
        );

        let mut clone = codec.clone();
        assert_eq!(clone.scanned, Scanned::default());
        assert_eq!(clone.max_frame_length, 1024);
        assert_eq!(clone.max_pairs, DEFAULT_MAX_PAIRS);

        let mut fresh = BytesMut::from(PAIR);
        fresh.extend_from_slice(b"\x00\x00");
        let amp_box = clone.decode(&mut fresh).unwrap().unwrap();
        assert_eq!(amp_box.get("k"), Some(&b"v"[..]));
        assert!(fresh.is_empty());

        buf.extend_from_slice(&SUM_REQUEST[30..]);
        assert!(
            codec.decode(&mut buf).unwrap().is_some(),
            "the original is unaffected"
        );
    }

    /// The limit is per box, not per buffer: several boxes that together
    /// exceed it still decode one by one.
    #[test]
    fn limit_applies_to_each_box_not_the_buffer() {
        let mut codec = AmpCodec::with_max_frame_length(SUM_REQUEST.len());
        let mut buf = BytesMut::new();
        for _ in 0..3 {
            buf.extend_from_slice(SUM_REQUEST);
        }
        for _ in 0..3 {
            assert!(codec.decode(&mut buf).unwrap().is_some());
        }
        assert!(buf.is_empty());
    }
}
