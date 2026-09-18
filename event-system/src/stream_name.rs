use std::borrow::Cow;

const VALID_SPECIAL_CHARACTERS: &[char] = &['.', '-', ':', '_'];

/// A validated stream name.
///
/// A stream name must be:
/// - non empty
/// - contain only ASCII alphanumeric characters and/or the special characters `-`, `.`, `:`, `_`
///
/// The names "." and ".." are reserved and **not permitted**.
///
/// # Examples
///
///
/// ```
/// use agave_event_system::{stream_name, stream_name::StreamName};
///
/// const NAME: StreamName = stream_name!("slot.events:v1");
///
/// assert_eq!(NAME.as_str(), "slot.events:v1");
/// ```
///
/// Validate an owned name at runtime, returning an error for invalid characters:
///
/// ```
/// # use agave_event_system::stream_name::{StreamName, StreamNameValidationError};
///
/// let version = 1;
/// let name = StreamName::try_new(format!("slot.events:v{version}")).unwrap();
/// assert_eq!(name.as_str(), "slot.events:v1");
/// ```
///
/// ```
/// # use agave_event_system::stream_name::{StreamName, StreamNameValidationError};
///
/// // fails due to `/` character
/// assert_eq!(
///     StreamName::try_new(String::from("slot/events")),
///     Err(StreamNameValidationError::InvalidCharacter),
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamName(Cow<'static, str>);

impl StreamName {
    /// Creates a stream name if validation passes.
    ///
    /// # Errors
    ///
    /// Returns [`StreamNameValidationError`] if the name
    /// fails validation.
    pub fn try_new(name: impl Into<Cow<'static, str>>) -> Result<Self, StreamNameValidationError> {
        let name = name.into();
        validate_stream_name(&name)?;

        Ok(Self(name))
    }

    /// Returns the stream name as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

/// Creates a [`StreamName`] validated at compile time.
///
/// Use [`StreamName::try_new`] for names determined at runtime.
///
/// # Examples
///
/// ```
/// use agave_event_system::stream_name;
///
/// let name = stream_name!("solana_core.replay_stage");
/// assert_eq!(name.as_str(), "solana_core.replay_stage");
/// ```
///
/// ```compile_fail
/// use agave_event_system::stream_name;
///
/// let name = stream_name!("bad/name");
/// ```
#[macro_export]
macro_rules! stream_name {
    ($name:expr $(,)?) => {
        const { $crate::__private::stream_name($name) }
    };
}

pub(crate) mod macro_support {
    use super::{Cow, StreamName, validate_stream_name};

    pub const fn stream_name(name: &'static str) -> StreamName {
        match validate_stream_name(name) {
            Ok(()) => StreamName(Cow::Borrowed(name)),
            Err(_) => panic!("invalid stream name"),
        }
    }
}

const fn validate_stream_name(stream_name: &str) -> Result<(), StreamNameValidationError> {
    let stream_name_bytes = stream_name.as_bytes();
    match stream_name_bytes {
        [] => return Err(StreamNameValidationError::Empty),
        [b'.'] | [b'.', b'.'] => return Err(StreamNameValidationError::ReservedDirectoryName),
        _ => {}
    }
    let mut bytes = stream_name_bytes;
    while let [byte, rest @ ..] = bytes {
        if !(byte.is_ascii_alphanumeric() || is_valid_special_character(*byte)) {
            return Err(StreamNameValidationError::InvalidCharacter);
        }
        bytes = rest;
    }

    Ok(())
}

const fn is_valid_special_character(byte: u8) -> bool {
    let mut chars = VALID_SPECIAL_CHARACTERS;
    while let [ch, rest @ ..] = chars {
        if byte as char == *ch {
            return true;
        }
        chars = rest;
    }
    false
}

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum StreamNameValidationError {
    #[error("stream name must not be empty")]
    Empty,
    #[error("stream name must not be `.` or `..`")]
    ReservedDirectoryName,
    #[error("validation of stream name failed due to a disallowed character in the name.")]
    InvalidCharacter,
}
