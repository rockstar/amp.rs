use crate::{
    AmpBox, Error, KEY_ANSWER, KEY_ASK, KEY_COMMAND, KEY_ERROR, KEY_ERROR_CODE,
    KEY_ERROR_DESCRIPTION, RESERVED_KEYS, is_reserved_key,
};

/// A request to run a command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// The name of the command to run.
    pub command: Vec<u8>,
    /// The identifier a response must echo back. `None` for a request that
    /// wants no response.
    pub ask: Option<Vec<u8>>,
    /// The command's arguments. Must not use any reserved key.
    pub arguments: AmpBox,
}

impl Request {
    /// Creates a request with no `_ask`, so no response is expected.
    pub fn new<C: Into<Vec<u8>>>(command: C, arguments: AmpBox) -> Self {
        Self {
            command: command.into(),
            ask: None,
            arguments,
        }
    }

    /// Sets the identifier a response must echo back.
    pub fn with_ask<A: Into<Vec<u8>>>(mut self, ask: A) -> Self {
        self.ask = Some(ask.into());
        self
    }

    /// Splits a box into a request.
    ///
    /// Fails with [`Error::MissingKey`] if the box has no `_command`.
    /// Everything other than `_command` and `_ask` becomes an argument,
    /// except that any other reserved key is dropped, as the reference
    /// implementation ignores it.
    pub fn from_box(mut amp_box: AmpBox) -> Result<Self, Error> {
        let command = amp_box
            .remove(KEY_COMMAND)
            .ok_or(Error::MissingKey(KEY_COMMAND))?;
        let ask = amp_box.remove(KEY_ASK);
        strip_reserved(&mut amp_box);
        Ok(Self {
            command,
            ask,
            arguments: amp_box,
        })
    }

    /// Assembles the box this request is sent as.
    ///
    /// Fails with [`Error::ReservedKey`] if an argument uses a reserved key,
    /// or with a length error if `command` or `ask` is too long.
    pub fn into_box(self) -> Result<AmpBox, Error> {
        let mut amp_box = reject_reserved(self.arguments)?;
        amp_box.insert(KEY_COMMAND, self.command)?;
        if let Some(ask) = self.ask {
            amp_box.insert(KEY_ASK, ask)?;
        }
        Ok(amp_box)
    }
}

/// A successful response to a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    /// The `_ask` value of the request being answered.
    pub answer: Vec<u8>,
    /// The command's results. Must not use any reserved key.
    pub results: AmpBox,
}

impl Answer {
    /// Creates an answer to the request identified by `answer`.
    pub fn new<A: Into<Vec<u8>>>(answer: A, results: AmpBox) -> Self {
        Self {
            answer: answer.into(),
            results,
        }
    }

    /// Splits a box into an answer.
    ///
    /// Fails with [`Error::MissingKey`] if the box has no `_answer`.
    /// Everything else becomes a result, except that any other reserved key
    /// is dropped, as the reference implementation ignores it.
    pub fn from_box(mut amp_box: AmpBox) -> Result<Self, Error> {
        let answer = amp_box
            .remove(KEY_ANSWER)
            .ok_or(Error::MissingKey(KEY_ANSWER))?;
        strip_reserved(&mut amp_box);
        Ok(Self {
            answer,
            results: amp_box,
        })
    }

    /// Assembles the box this answer is sent as.
    ///
    /// Fails with [`Error::ReservedKey`] if a result uses a reserved key, or
    /// with a length error if `answer` is too long.
    pub fn into_box(self) -> Result<AmpBox, Error> {
        let mut amp_box = reject_reserved(self.results)?;
        amp_box.insert(KEY_ANSWER, self.answer)?;
        Ok(amp_box)
    }
}

/// A failed response to a request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorResponse {
    /// The `_ask` value of the request that failed.
    pub error: Vec<u8>,
    /// The error code, such as [`ErrorResponse::UNHANDLED`] or
    /// [`ErrorResponse::UNKNOWN`].
    pub code: Vec<u8>,
    /// A human-readable description of the failure.
    pub description: Vec<u8>,
}

impl ErrorResponse {
    /// The error code for a request naming a command the receiver does not
    /// implement.
    pub const UNHANDLED: &'static str = "UNHANDLED";
    /// The error code for a command that failed with an error that has no
    /// code of its own. Any other code may be used; the convention is
    /// all-caps `SNAKE_CASE`.
    pub const UNKNOWN: &'static str = "UNKNOWN";

    /// Creates an error response to the request identified by `error`.
    pub fn new<E, C, D>(error: E, code: C, description: D) -> Self
    where
        E: Into<Vec<u8>>,
        C: Into<Vec<u8>>,
        D: Into<Vec<u8>>,
    {
        Self {
            error: error.into(),
            code: code.into(),
            description: description.into(),
        }
    }

    /// Splits a box into an error response.
    ///
    /// Fails with [`Error::MissingKey`] if the box has no `_error` or no
    /// `_error_code`. A missing `_error_description` is read as empty, and
    /// any other keys are ignored.
    pub fn from_box(mut amp_box: AmpBox) -> Result<Self, Error> {
        let error = amp_box
            .remove(KEY_ERROR)
            .ok_or(Error::MissingKey(KEY_ERROR))?;
        let code = amp_box
            .remove(KEY_ERROR_CODE)
            .ok_or(Error::MissingKey(KEY_ERROR_CODE))?;
        let description = amp_box.remove(KEY_ERROR_DESCRIPTION).unwrap_or_default();
        Ok(Self {
            error,
            code,
            description,
        })
    }

    /// Assembles the box this error response is sent as.
    ///
    /// Fails with a length error if any field is too long.
    pub fn into_box(self) -> Result<AmpBox, Error> {
        AmpBox::from_pairs([
            (KEY_ERROR, self.error),
            (KEY_ERROR_CODE, self.code),
            (KEY_ERROR_DESCRIPTION, self.description),
        ])
    }
}

/// Any box a peer may send, classified by the reserved keys it carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// A box carrying `_command`.
    Request(Request),
    /// A box carrying `_answer`.
    Answer(Answer),
    /// A box carrying `_error`.
    Error(ErrorResponse),
}

impl Message {
    /// Classifies a box.
    ///
    /// `_answer` takes precedence over `_error`, which takes precedence over
    /// `_command`, as in the reference implementation. A box with none of the
    /// three fails with [`Error::UnrecognizedBox`].
    pub fn from_box(amp_box: AmpBox) -> Result<Self, Error> {
        if amp_box.contains_key(KEY_ANSWER) {
            Answer::from_box(amp_box).map(Message::Answer)
        } else if amp_box.contains_key(KEY_ERROR) {
            ErrorResponse::from_box(amp_box).map(Message::Error)
        } else if amp_box.contains_key(KEY_COMMAND) {
            Request::from_box(amp_box).map(Message::Request)
        } else {
            Err(Error::UnrecognizedBox)
        }
    }

    /// Assembles the box this message is sent as.
    pub fn into_box(self) -> Result<AmpBox, Error> {
        match self {
            Message::Request(request) => request.into_box(),
            Message::Answer(answer) => answer.into_box(),
            Message::Error(error) => error.into_box(),
        }
    }
}

fn reject_reserved(amp_box: AmpBox) -> Result<AmpBox, Error> {
    let reserved = amp_box
        .iter()
        .map(|(key, _)| key)
        .find(|key| is_reserved_key(key))
        .map(<[u8]>::to_vec);
    match reserved {
        Some(key) => Err(Error::ReservedKey(key)),
        None => Ok(amp_box),
    }
}

/// Removes every reserved key, so a box read off the wire never hands one to
/// the caller as an argument or result.
fn strip_reserved(amp_box: &mut AmpBox) {
    for key in RESERVED_KEYS {
        amp_box.remove(key);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn args() -> AmpBox {
        AmpBox::from_pairs([("a", "13"), ("b", "81")]).unwrap()
    }

    #[test]
    fn request_round_trip() {
        let request = Request::new("Sum", args()).with_ask("1");
        let amp_box = request.clone().into_box().unwrap();
        assert_eq!(
            amp_box.iter().collect::<Vec<_>>(),
            vec![
                (&b"a"[..], &b"13"[..]),
                (&b"b"[..], &b"81"[..]),
                (&b"_command"[..], &b"Sum"[..]),
                (&b"_ask"[..], &b"1"[..]),
            ]
        );
        assert_eq!(
            Message::from_box(amp_box).unwrap(),
            Message::Request(request)
        );
    }

    #[test]
    fn request_without_ask() {
        let request = Request::new("Log", args());
        let amp_box = request.clone().into_box().unwrap();
        assert_eq!(amp_box.get(KEY_ASK), None);
        assert_eq!(Request::from_box(amp_box).unwrap(), request);
    }

    #[test]
    fn request_requires_command() {
        assert!(matches!(
            Request::from_box(args()),
            Err(Error::MissingKey(KEY_COMMAND))
        ));
    }

    #[test]
    fn request_rejects_reserved_argument() {
        let mut arguments = args();
        arguments.insert("_answer", "1").unwrap();
        assert!(matches!(
            Request::new("Sum", arguments).into_box(),
            Err(Error::ReservedKey(key)) if key == b"_answer"
        ));
    }

    #[test]
    fn request_from_box_strips_reserved_keys() {
        let amp_box = AmpBox::from_pairs([
            ("_command", "Sum"),
            ("_ask", "1"),
            ("_error_code", "UNKNOWN"),
            ("a", "1"),
        ])
        .unwrap();
        let request = Request::from_box(amp_box).unwrap();
        assert_eq!(request.command, b"Sum");
        assert_eq!(request.ask.as_deref(), Some(&b"1"[..]));
        assert_eq!(request.arguments, AmpBox::from_pairs([("a", "1")]).unwrap());
        let mut pairs: Vec<_> = request
            .into_box()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            [
                (b"_ask".to_vec(), b"1".to_vec()),
                (b"_command".to_vec(), b"Sum".to_vec()),
                (b"a".to_vec(), b"1".to_vec()),
            ]
        );
    }

    #[test]
    fn answer_from_box_strips_reserved_keys() {
        let amp_box = AmpBox::from_pairs([
            ("_answer", "1"),
            ("_command", "Sum"),
            ("_error_description", "nope"),
            ("total", "94"),
        ])
        .unwrap();
        let answer = Answer::from_box(amp_box).unwrap();
        assert_eq!(answer.answer, b"1");
        assert_eq!(
            answer.results,
            AmpBox::from_pairs([("total", "94")]).unwrap()
        );
        let mut pairs: Vec<_> = answer
            .into_box()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            [
                (b"_answer".to_vec(), b"1".to_vec()),
                (b"total".to_vec(), b"94".to_vec()),
            ]
        );
    }

    #[test]
    fn answer_round_trip() {
        let answer = Answer::new("1", AmpBox::from_pairs([("total", "94")]).unwrap());
        let amp_box = answer.clone().into_box().unwrap();
        assert_eq!(
            amp_box.encode(),
            b"\x00\x05total\x00\x0294\x00\x07_answer\x00\x011\x00\x00"
        );
        assert_eq!(Message::from_box(amp_box).unwrap(), Message::Answer(answer));
    }

    #[test]
    fn answer_rejects_reserved_result() {
        let results = AmpBox::from_pairs([("_command", "x")]).unwrap();
        assert!(matches!(
            Answer::new("1", results).into_box(),
            Err(Error::ReservedKey(_))
        ));
    }

    #[test]
    fn error_round_trip() {
        let error = ErrorResponse::new("1", ErrorResponse::UNKNOWN, "it broke");
        let amp_box = error.clone().into_box().unwrap();
        assert_eq!(
            amp_box.encode(),
            b"\x00\x06_error\x00\x011\x00\x0b_error_code\x00\x07UNKNOWN\x00\x12_error_description\x00\x08it broke\x00\x00"
        );
        assert_eq!(Message::from_box(amp_box).unwrap(), Message::Error(error));
    }

    #[test]
    fn error_description_defaults_to_empty() {
        let amp_box = AmpBox::from_pairs([("_error", "1"), ("_error_code", "UNKNOWN")]).unwrap();
        let error = ErrorResponse::from_box(amp_box).unwrap();
        assert_eq!(error.description, b"");
    }

    #[test]
    fn error_requires_code() {
        let amp_box = AmpBox::from_pairs([("_error", "1")]).unwrap();
        assert!(matches!(
            ErrorResponse::from_box(amp_box),
            Err(Error::MissingKey(KEY_ERROR_CODE))
        ));
    }

    #[test]
    fn classification_precedence() {
        let amp_box =
            AmpBox::from_pairs([("_command", "x"), ("_error", "1"), ("_answer", "1")]).unwrap();
        assert!(matches!(
            Message::from_box(amp_box).unwrap(),
            Message::Answer(_)
        ));
        let amp_box = AmpBox::from_pairs([
            ("_command", "x"),
            ("_error", "1"),
            ("_error_code", "UNKNOWN"),
        ])
        .unwrap();
        assert!(matches!(
            Message::from_box(amp_box).unwrap(),
            Message::Error(_)
        ));
    }

    #[test]
    fn unrecognized_box() {
        assert!(matches!(
            Message::from_box(args()),
            Err(Error::UnrecognizedBox)
        ));
        assert!(matches!(
            Message::from_box(AmpBox::new()),
            Err(Error::UnrecognizedBox)
        ));
    }

    #[test]
    fn reserved_keys() {
        for key in RESERVED_KEYS {
            assert!(is_reserved_key(key.as_bytes()));
        }
        assert!(!is_reserved_key(b"_other"));
        assert!(!is_reserved_key(b"ask"));
    }
}
