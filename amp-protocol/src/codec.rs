//! Framing for boxes on a byte stream, via [`tokio_util::codec`].
//!
//! Enabled by the `tokio` feature.

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::amp_box::{Frame, scan_frame};
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
/// refuses one longer than its maximum frame length with
/// [`Error::FrameTooLong`] rather than buffer without bound. The default
/// limit is 16 MiB; [`with_max_frame_length`](AmpCodec::with_max_frame_length)
/// sets another.
///
/// While a box is still arriving, the codec remembers how much of it has
/// already been checked, so each call scans only the new bytes and the box is
/// decoded exactly once, when it is complete. That makes a codec specific to
/// one stream: use a fresh one per connection, as [`Framed`] does.
///
/// [`Framed`]: tokio_util::codec::Framed
#[derive(Clone, Copy, Debug)]
pub struct AmpCodec {
    max_frame_length: usize,
    /// How many bytes at the front of the buffer have been scanned and found
    /// to be whole pairs of a box whose terminator has not arrived yet.
    scanned: usize,
}

/// The frame length [`AmpCodec::new`] allows: 16 MiB, room for a box of a
/// few hundred full-size values, which is far beyond any sensible packet.
const DEFAULT_MAX_FRAME_LENGTH: usize = 16 * 1024 * 1024;

impl AmpCodec {
    /// Creates a codec with the default maximum frame length of 16 MiB.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a codec that rejects a box whose encoding is longer than
    /// `max_frame_length` bytes, counting the key and value length prefixes
    /// and the two-byte terminator. Decoding such a box fails with
    /// [`Error::FrameTooLong`], as soon as that many bytes of it have
    /// arrived without a terminator.
    ///
    /// Set this to the largest box the peer could legitimately send. Each
    /// value is at most 65,535 bytes and each key at most 255, so the length
    /// is bounded by the number of pairs a box carries.
    pub fn with_max_frame_length(max_frame_length: usize) -> Self {
        AmpCodec {
            max_frame_length,
            scanned: 0,
        }
    }
}

impl Default for AmpCodec {
    fn default() -> Self {
        Self::with_max_frame_length(DEFAULT_MAX_FRAME_LENGTH)
    }
}

impl Decoder for AmpCodec {
    type Item = AmpBox;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<AmpBox>, Error> {
        if self.scanned == 0 {
            // Most boxes arrive whole, so decode in one pass when nothing of
            // this box has been seen before. If it turns out to be incomplete
            // the wasted work is bounded by the bytes already here, and later
            // calls scan from where this one stopped instead of retrying, so
            // a box that trickles in is still decoded only once in full.
            match AmpBox::decode(src) {
                Ok((amp_box, consumed)) => return self.accept(src, amp_box, consumed),
                Err(Error::Incomplete) => {}
                Err(err) => return Err(err),
            }
        }
        match scan_frame(src, self.scanned) {
            Ok(Frame::Complete(len)) => {
                self.scanned = 0;
                if len > self.max_frame_length {
                    return Err(Error::FrameTooLong {
                        limit: self.max_frame_length,
                    });
                }
                // The scan proved the box ends at `len`, so this decode sees
                // exactly one complete box and cannot report it incomplete.
                let (amp_box, consumed) = AmpBox::decode(&src[..len])?;
                self.accept(src, amp_box, consumed)
            }
            Ok(Frame::Incomplete(scanned)) => {
                // Every byte in `src` belongs to this box, since the ones
                // before it were consumed. Once there are more than the
                // limit, this box can never be accepted, so stop buffering it.
                if src.len() > self.max_frame_length {
                    self.scanned = 0;
                    return Err(Error::FrameTooLong {
                        limit: self.max_frame_length,
                    });
                }
                self.scanned = scanned;
                Ok(None)
            }
            Err(err) => {
                self.scanned = 0;
                Err(err)
            }
        }
    }
}

impl AmpCodec {
    /// Hands over a box decoded from the front of `src`, consuming its bytes,
    /// unless it is longer than the limit.
    fn accept(
        &mut self,
        src: &mut BytesMut,
        amp_box: AmpBox,
        consumed: usize,
    ) -> Result<Option<AmpBox>, Error> {
        self.scanned = 0;
        if consumed > self.max_frame_length {
            return Err(Error::FrameTooLong {
                limit: self.max_frame_length,
            });
        }
        src.advance(consumed);
        Ok(Some(amp_box))
    }
}

impl Encoder<AmpBox> for AmpCodec {
    type Error = Error;

    fn encode(&mut self, item: AmpBox, dst: &mut BytesMut) -> Result<(), Error> {
        dst.reserve(item.encoded_len());
        for (key, value) in item.iter() {
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
    fn default_limit_is_16_mib() {
        assert_eq!(AmpCodec::new().max_frame_length, 16 * 1024 * 1024);
        assert_eq!(AmpCodec::default().max_frame_length, 16 * 1024 * 1024);
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
            amp_box
                .insert(format!("k{i:03}"), vec![i as u8; 100])
                .unwrap();
        }
        let bytes = amp_box.encode();

        let mut codec = AmpCodec::new();
        let mut buf = BytesMut::new();
        let mut decoded = None;
        // A chunk size that never lines up with a pair.
        for chunk in bytes.chunks(97) {
            buf.extend_from_slice(chunk);
            match codec.decode(&mut buf).unwrap() {
                Some(amp_box) => decoded = Some(amp_box),
                None => {
                    assert_eq!(codec.scanned % PAIR_LEN, 0, "scan stops at a pair boundary");
                    assert!(
                        buf.len() - codec.scanned < PAIR_LEN,
                        "scan covers every whole pair"
                    );
                }
            }
        }
        assert_eq!(decoded.unwrap(), amp_box);
        assert!(buf.is_empty());
        assert_eq!(codec.scanned, 0);
    }

    /// The scan position belongs to one box. It is dropped once that box is
    /// decoded or rejected, so the next box starts from its first byte.
    #[test]
    fn scan_position_is_reset_after_a_box_or_an_error() {
        let mut codec = AmpCodec::new();
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&SUM_REQUEST[..12]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert_eq!(codec.scanned, 9, "the first pair is 9 bytes");
        buf.extend_from_slice(&SUM_REQUEST[12..]);
        assert!(codec.decode(&mut buf).unwrap().is_some());
        assert_eq!(codec.scanned, 0);

        buf.extend_from_slice(PAIR);
        assert!(codec.decode(&mut buf).unwrap().is_none());
        assert_eq!(codec.scanned, PAIR.len());
        buf.extend_from_slice(b"\x01\x00");
        assert!(matches!(
            codec.decode(&mut buf),
            Err(Error::KeyTooLong { len: 256 })
        ));
        assert_eq!(codec.scanned, 0);
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
