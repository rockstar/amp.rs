//! What a connection has to say about failures the peer does not hear about
//! in full. Each function is an event with the `tracing` feature, and
//! nothing without it, so the callers need no `cfg`.

#[cfg(feature = "tracing")]
mod with_tracing {
    use std::borrow::Cow;

    use crate::Error;

    /// The ask as a log field: the peer's own text, or `-` for a request
    /// that wanted no reply.
    fn ask(ask: Option<&[u8]>) -> Cow<'_, str> {
        ask.map_or(Cow::Borrowed("-"), String::from_utf8_lossy)
    }

    /// A command whose service returned `err`. A command error is the
    /// handler's own answer to the peer, so it is only of interest when
    /// debugging; anything else is a failure the peer was told nothing
    /// about, or told only that its request was malformed.
    pub(crate) fn command_failed(command: &[u8], ask: Option<&[u8]>, err: &Error) {
        let command = String::from_utf8_lossy(command);
        let ask = self::ask(ask);
        match err {
            Error::Command { .. } => {
                tracing::debug!(%command, %ask, error = %err, "command failed")
            }
            _ => tracing::warn!(%command, %ask, error = %err, "command failed"),
        }
    }

    /// A command whose handler panicked, with `reason`.
    pub(crate) fn command_panicked(command: &[u8], ask: Option<&[u8]>, reason: &str) {
        let command = String::from_utf8_lossy(command);
        let ask = self::ask(ask);
        tracing::error!(%command, %ask, "{reason}");
    }

    /// An error from `accept` that the accept loop carries on past.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn accept_skipped(err: &std::io::Error) {
        tracing::warn!(error = %err, "could not accept a connection");
    }

    /// An error that ended one of `serve`'s connections.
    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn connection_failed(err: &Error) {
        tracing::warn!(error = %err, "connection failed");
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
