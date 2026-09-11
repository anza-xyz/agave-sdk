pub use wincode::ReadError;
use {
    crate::backend,
    std::{marker::PhantomData, path::PathBuf},
    wincode::Deserialize,
    wincode_dynamic::{Decoder, Fields, RootSchema},
};

/// A [`StreamExplorer`] listens to a given directory for event streams that are created
/// by [`EventSystem::create_stream`](crate::EventSystem::create_stream) in the same
/// directory.
///
/// To subscribe to a stream, simply use the [`StreamExplorer::available_streams] API
/// which yields an iterator over streams that can be subscribed to.
pub struct StreamExplorer(backend::StreamExplorer);

impl StreamExplorer {
    pub fn new(event_system_directory: PathBuf) -> Self {
        Self(backend::StreamExplorer::new(event_system_directory))
    }

    /// Iterates over streams that are available to be subscribed to.
    ///
    /// A stream can be subscribed to with [`AvailableStream::try_connect_dynamic`] or
    /// [`AvailableStream::try_connect_typed`].
    pub fn available_streams(&self) -> impl Iterator<Item = AvailableStream> + '_ {
        self.0.available_streams().map(AvailableStream)
    }
}

/// Marker for dynamically reflecting over stream messages.
/// See [`StreamSubscriber`] for details on subscriber modes.
pub struct Dynamic;

/// Marker for decoding stream messages into a statically typed `T`.
/// See [`StreamSubscriber`] for details on subscriber modes.
pub struct Typed<T> {
    _marker: PhantomData<T>,
}

/// A subscriber to a specific event stream. This
/// subscriber can be created with either [`Dynamic`] mode
/// which lets users dynamically reflect over messages on the stream,
/// or the [`Typed<T>`] mode which returns the T directly if the user
/// knows what T is at compile time.
pub struct StreamSubscriber<Mode> {
    backend: backend::StreamSubscriber,
    mode: PhantomData<Mode>,
}

impl<Mode> StreamSubscriber<Mode> {
    /// The name of the stream the subscriber is listening on.
    pub fn stream_name(&self) -> &str {
        self.backend.stream_name()
    }
    /// The name of the type that is sent on the stream.
    pub fn type_name(&self) -> &str {
        self.backend.type_name()
    }

    /// Returns a message if there is any unseen message in the stream.
    pub fn try_recv(&mut self) -> Result<StreamMessage<'_, Mode>, TryRecvError> {
        self.backend.try_recv().map(StreamMessage::new)
    }

    fn new(backend: backend::StreamSubscriber) -> Self {
        Self {
            backend,
            mode: PhantomData,
        }
    }
}

/// A message received from a stream with [`StreamSubscriber::try_recv`].
pub struct StreamMessage<'a, Mode> {
    backend: backend::StreamMessage<'a>,
    mode: PhantomData<Mode>,
}

impl<'a, Mode> StreamMessage<'a, Mode> {
    /// Metadata of the producer lane that this message was published on.
    pub fn lane_metadata(&self) -> ProducerMetadata<'_> {
        ProducerMetadata(self.backend.lane_metadata())
    }

    fn new(backend: backend::StreamMessage<'a>) -> Self {
        Self {
            backend,
            mode: PhantomData,
        }
    }
}

impl<T> StreamMessage<'_, Typed<T>>
where
    T: for<'de> Deserialize<'de, Dst = T>,
{
    pub fn decode(&self) -> Result<T, ReadError> {
        // An error should never happen here since the schema of the stream was
        // validated against `T` when subscribing to the stream in
        // [`AvailableStream::try_connect_typed`].
        wincode::deserialize(self.backend.payload())
    }
}

impl<'a> StreamMessage<'a, Dynamic> {
    /// Decodes the message into a [`DecodedMessage`], whose fields can be
    /// reflected over.
    pub fn decode<'de>(&'de self) -> Result<DecodedMessage<'a, 'de>, ReadError> {
        Ok(match Decoder::new(self.backend.schema()) {
            Decoder::Struct(schema_decoder) => DecodedMessage::Struct {
                fields: schema_decoder.fields(self.backend.payload()),
            },
            Decoder::Enum(enum_decoder) => {
                let variant_decoder = enum_decoder.decode_variant(self.backend.payload())?;
                DecodedMessage::Enum {
                    variant_name: variant_decoder.variant_name(),
                    fields: variant_decoder.fields(),
                }
            }
        })
    }
}

/// Metadata of the [`Producer`](crate::producer::Producer) lane that a [`StreamMessage`] was published on.
#[derive(Clone, Copy, Debug)]
pub struct ProducerMetadata<'a>(backend::ProducerMetadata<'a>);

impl ProducerMetadata<'_> {
    /// The lane of the producer that emitted this event.
    pub fn lane(&self) -> usize {
        self.0.lane()
    }

    /// The thread id of the producer that emitted the event.
    pub fn thread_id(&self) -> u64 {
        self.0.thread_id()
    }

    /// The number of events the producer could not publish on this lane because
    /// subscribers did not consume them fast enough.
    pub fn rejected_items(&self) -> u64 {
        self.0.rejected_items()
    }
}

/// A discovered stream that can be connected to.
pub struct AvailableStream(backend::AvailableStream);

impl AvailableStream {
    /// The name of the available stream.
    pub fn stream_name(&self) -> &str {
        self.0.stream_name()
    }

    /// The name of the type that is sent on the stream.
    pub fn type_name(&self) -> &str {
        self.0.type_name()
    }

    /// Attempts to connect to the stream with dynamic decoding of the messages.
    ///
    /// This method should be used over [`try_connect_typed`](Self::try_connect_typed) when you don't know what
    /// concrete rust type is sent on the stream.
    pub fn try_connect_dynamic(self) -> Result<StreamSubscriber<Dynamic>, TryConnectError> {
        self.0.try_connect().map(StreamSubscriber::<Dynamic>::new)
    }

    /// Attempts to connect to the stream with static decoding of the messages into a concrete type `T`.
    ///
    /// If you don't know at compile time what type the stream contains you should instead use
    /// [`try_connect_dynamic`](Self::try_connect_dynamic).
    #[allow(clippy::result_large_err)]
    pub fn try_connect_typed<T: wincode_dynamic::SchemaDynamic>(
        self,
    ) -> Result<StreamSubscriber<Typed<T>>, TryConnectTypedError> {
        let expected_schema = T::schema();
        let actual_schema = self.0.stream_schema();

        if expected_schema != *actual_schema {
            return Err(TryConnectTypedError::SchemaMismatch {
                expected: expected_schema,
                actual: actual_schema.clone(),
            });
        }

        self.0
            .try_connect()
            .map(StreamSubscriber::<Typed<T>>::new)
            .map_err(TryConnectTypedError::Connection)
    }
}

/// Errors that can arise when trying to receive an event on a specific event stream
/// through a subscriber.
#[derive(thiserror::Error, Debug)]
pub enum TryRecvError {
    #[error("the stream has no new message")]
    Empty,
}

#[derive(Debug, thiserror::Error)]
pub enum TryConnectTypedError {
    #[error("stream schema does not match the requested type")]
    SchemaMismatch {
        expected: RootSchema,
        actual: RootSchema,
    },

    #[error(transparent)]
    Connection(#[from] TryConnectError),
}

#[derive(Debug, thiserror::Error)]
pub enum TryConnectError {}

/// A decoded dynamically typed message. Messages can either be an enum or a struct.
pub enum DecodedMessage<'a, 'de> {
    Struct {
        fields: Fields<'a, 'de, &'de [u8]>,
    },
    Enum {
        fields: Fields<'a, 'de, &'de [u8]>,
        variant_name: &'a str,
    },
}
