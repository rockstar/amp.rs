//! What a connection has to say about failures the peer does not hear about
//! in full. Each function is an event with the `tracing` feature, and
//! nothing without it, so the callers need no `cfg`.
//!
//! Everything the peer chose, and every error text, is logged as its Debug
//! form: quoted, with control characters and escape bytes written out. A
//! command name with a newline in it would otherwise forge a second log
//! line, and one with terminal escapes would restyle whoever tails the log.
//! Each is also cut short: a command or an ask can be 64 KiB, and Debug
//! escaping can quadruple it, so an uncapped field would let one request
//! write a quarter megabyte of log, at line rate.

#[cfg(feature = "tracing")]
mod with_tracing {
    use crate::Error;

    /// How much of a command name or an ask a log line shows. The same
    /// limit serde_amp puts on a value in its error text.
    const PEER_LIMIT: usize = 64;

    /// How much of an error text a log line shows. Longer than a peer
    /// field, since the text is mostly this side's own and is what the
    /// operator reads the line for; it embeds at most one peer value.
    const ERROR_LIMIT: usize = 256;

    /// `text` as a log field: quoted and escaped, lossily from UTF-8, and
    /// cut to `limit` characters with `...` after the closing quote when
    /// it was longer.
    fn field(text: &[u8], limit: usize) -> String {
        let text = String::from_utf8_lossy(text);
        let mut chars = text.chars();
        let shown: String = chars.by_ref().take(limit).collect();
        if chars.next().is_some() {
            format!("{shown:?}...")
        } else {
            format!("{shown:?}")
        }
    }

    fn command(command: &[u8]) -> String {
        field(command, PEER_LIMIT)
    }

    /// The ask as a log field: the peer's own text, or `-` for a request
    /// that wanted no reply.
    fn ask(ask: Option<&[u8]>) -> String {
        ask.map_or_else(|| String::from("\"-\""), |ask| field(ask, PEER_LIMIT))
    }

    fn error(err: &dyn std::fmt::Display) -> String {
        field(err.to_string().as_bytes(), ERROR_LIMIT)
    }

    /// A command whose service returned `err`. A command error is the
    /// handler's own answer to the peer, so it is only of interest when
    /// debugging; anything else is a failure the peer was told nothing
    /// about, or told only that its request was malformed.
    pub(crate) fn command_failed(command: &[u8], ask: Option<&[u8]>, err: &Error) {
        let command = self::command(command);
        let ask = self::ask(ask);
        let error = self::error(err);
        match err {
            Error::Command { .. } => {
                tracing::debug!(%command, %ask, %error, "command failed")
            }
            _ => tracing::warn!(%command, %ask, %error, "command failed"),
        }
    }

    /// A command whose handler panicked, with `reason`.
    pub(crate) fn command_panicked(command: &[u8], ask: Option<&[u8]>, reason: &str) {
        let command = self::command(command);
        let ask = self::ask(ask);
        tracing::error!(%command, %ask, "{}", reason.escape_debug());
    }

    /// An error from `accept` that the accept loop carries on past.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn accept_skipped(err: &std::io::Error) {
        let error = self::error(err);
        tracing::warn!(%error, "could not accept a connection");
    }

    /// An error that ended one of `serve`'s connections. A timeout is the
    /// idle limit doing its job on a peer that kept a connection open and
    /// quiet, which is routine, so it is only of interest when debugging;
    /// anything else is a failure.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn connection_failed(err: &Error) {
        let error = self::error(err);
        match err {
            Error::Io(io) if io.kind() == std::io::ErrorKind::TimedOut => {
                tracing::debug!(%error, "connection timed out")
            }
            _ => tracing::warn!(%error, "connection failed"),
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn a_short_field_is_quoted_whole() {
            assert_eq!(field(b"Sum", PEER_LIMIT), r#""Sum""#);
            assert_eq!(ask(None), r#""-""#);
            assert_eq!(ask(Some(b"7")), r#""7""#);
        }

        #[test]
        fn a_long_field_is_cut() {
            let long = vec![b'a'; 65_535];
            let shown = field(&long, PEER_LIMIT);
            assert_eq!(shown, format!("{:?}...", "a".repeat(PEER_LIMIT)));
            assert_eq!(ask(Some(&long)), shown);
        }

        #[test]
        fn the_cut_counts_characters_not_escaped_bytes() {
            // Each byte escapes to six characters; the limit still bounds
            // what is shown to a constant number of input characters.
            let noisy = vec![0x1b; 65_535];
            let shown = field(&noisy, PEER_LIMIT);
            assert_eq!(shown.len(), PEER_LIMIT * 6 + 5, "{shown}");
            assert!(shown.ends_with(r#"\u{1b}"..."#), "{shown}");
        }
    }
}

#[cfg(not(feature = "tracing"))]
mod with_tracing {
    use crate::Error;

    pub(crate) fn command_failed(_: &[u8], _: Option<&[u8]>, _: &Error) {}
    pub(crate) fn command_panicked(_: &[u8], _: Option<&[u8]>, _: &str) {}
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn accept_skipped(_: &std::io::Error) {}
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn connection_failed(_: &Error) {}
}

pub(crate) use with_tracing::*;
