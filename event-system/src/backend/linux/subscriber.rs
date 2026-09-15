use {
    super::{QUEUE_FILE_NAME_PREFIX, REQUIRED_SEALS, SCHEMA_FILE_NAME, STREAMS_DIRECTORY_NAME},
    crate::subscriber::{RecvTimeoutError, TryConnectError, TryRecvError},
    nix::{
        dir::Dir,
        fcntl::{OFlag, openat},
        sys::stat::Mode,
    },
    shaq::broadcast::{Broadcast, LaneMetadata, SliceReadGuard, UnknownType},
    std::{
        fs::File,
        io::{self, Read},
        os::fd::AsRawFd,
        path::PathBuf,
        time::Duration,
    },
    wincode_dynamic::RootSchema,
};

#[derive(Debug)]
pub(crate) struct StreamSubscriber {
    slice_consumer: shaq::broadcast::SliceConsumer,
    stream_name: String,
    schema: RootSchema,
}

impl StreamSubscriber {
    /// The name of the stream the subscriber is listening on.
    pub(crate) fn stream_name(&self) -> &str {
        &self.stream_name
    }
    /// The name of the type that is sent on the stream.
    pub(crate) fn type_name(&self) -> &str {
        self.schema.name()
    }

    /// Returns a message if there is any unseen message in the stream.
    pub(crate) fn try_recv(&mut self) -> Result<StreamMessage<'_>, TryRecvError> {
        let read_guard = self.slice_consumer.try_read().ok_or(TryRecvError::Empty)?;

        Ok(StreamMessage {
            schema: &self.schema,
            read_guard,
        })
    }

    /// Blocks until an unseen event arrives on the stream or timeout duration elapses.
    pub(crate) fn try_recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<StreamMessage<'_>, RecvTimeoutError> {
        let read_guard = self
            .slice_consumer
            .read_timeout(timeout)
            .map_err(|_| RecvTimeoutError::Timeout)?;

        Ok(StreamMessage {
            schema: &self.schema,
            read_guard,
        })
    }
}

#[derive(Debug)]
pub(crate) struct StreamMessage<'a> {
    schema: &'a RootSchema,
    read_guard: SliceReadGuard<'a>,
}

impl<'a> StreamMessage<'a> {
    pub(crate) fn schema(&self) -> &'a RootSchema {
        self.schema
    }

    pub(crate) fn payload(&self) -> &[u8] {
        self.read_guard.as_slice()
    }

    pub(crate) fn producer_metadata(&self) -> ProducerMetadata<'_> {
        ProducerMetadata(self.read_guard.lane_metadata())
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProducerMetadata<'a>(LaneMetadata<'a>);

impl ProducerMetadata<'_> {
    pub(crate) fn lane(&self) -> usize {
        self.0.lane()
    }

    pub(crate) fn thread_id(&self) -> u64 {
        self.0.producer_id().get()
    }

    pub(crate) fn rejected_items(&self) -> u64 {
        self.0.rejected_items()
    }
}

#[derive(Debug)]
pub(crate) struct StreamExplorer {
    event_system_directory: PathBuf,
}

impl StreamExplorer {
    pub fn new(event_system_directory: PathBuf) -> Self {
        Self {
            event_system_directory,
        }
    }

    /// Yields an iterator of [`AvailableStream`]s that can be used to subscribed to streams with
    /// [`AvailableStream::try_connect`] which returns a [`StreamSubscriber`] on success.
    pub(crate) fn available_streams(&self) -> impl Iterator<Item = AvailableStream> + '_ {
        let streams_directory = self.event_system_directory.join(STREAMS_DIRECTORY_NAME);

        let read_streams_directory = std::fs::read_dir(streams_directory)
            // returns an empty iterator if `read_dir` errors. This can happen
            // if the subscriber is launched _before_ the producer side has created
            // the event system.
            .into_iter()
            .flatten();

        read_streams_directory
            .filter_map(Result::ok)
            .filter_map(|entry| AvailableStream::try_new(entry).ok())
    }
}

#[derive(Debug)]
pub(crate) struct AvailableStream {
    stream_name: String,
    schema: RootSchema,
    broadcast_handle: Broadcast<UnknownType>,
}

impl AvailableStream {
    pub(crate) fn try_connect(self) -> Result<StreamSubscriber, TryConnectError> {
        // SAFETY:
        // The producer is always sending byte arrays which satisfies `SliceConsumer's full-byte initialization requirement.
        let slice_consumer_result = unsafe { self.broadcast_handle.slice_consumer() };

        let Ok(slice_consumer) = slice_consumer_result else {
            return Err(TryConnectError::SubscriberSlotsExhausted);
        };

        Ok(StreamSubscriber {
            slice_consumer,
            stream_name: self.stream_name,
            schema: self.schema,
        })
    }

    pub(crate) fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub(crate) fn type_name(&self) -> &str {
        self.schema.name()
    }

    pub(crate) fn stream_schema(&self) -> &RootSchema {
        &self.schema
    }

    /// Creates an [`AvailableStream`] if the given stream directory passes validation.
    fn try_new(stream_directory: std::fs::DirEntry) -> Result<Self, CreateAvailableStreamError> {
        let stream_name = stream_directory
            .file_name()
            .into_string()
            .map_err(|_| CreateAvailableStreamError::InvalidStreamName)?;

        // Keep an open file descriptor for the stream directory and use it for all lookups.
        //
        // This prevents a race in comparison to resolving with pathname:
        // - Read stream A schema.
        // - A is dropped and B is published at the same path.
        // - Open B's queue, which passes its identifier check.
        // - A's schema is paired with B's queue.
        //
        // This lookup technique works because each new stream, even name reuse, causes producers
        // to first create a new directory object. Existing directories are never mutated.
        let mut stream_directory = Dir::open(
            &stream_directory.path(),
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY,
            Mode::empty(),
        )
        .map_err(io::Error::from)?;

        let mut schema_file = File::from(
            openat(
                &stream_directory,
                SCHEMA_FILE_NAME,
                OFlag::O_RDONLY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(io::Error::from)?,
        );

        let queue_file_entry = stream_directory
            .iter()
            .filter_map(Result::ok)
            .find(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_ok_and(|name| name.starts_with(QUEUE_FILE_NAME_PREFIX))
            })
            .ok_or(CreateAvailableStreamError::QueueFileIsNotPublished)?;

        let expected_broadcast_identifier = queue_file_entry
            .file_name()
            .to_str()
            .ok()
            .and_then(|name| name.strip_prefix(QUEUE_FILE_NAME_PREFIX))
            .and_then(|identifier| identifier.parse::<u64>().ok())
            .ok_or(CreateAvailableStreamError::InvalidQueueIdentifier)?;

        let queue_file = File::from(
            openat(
                &stream_directory,
                queue_file_entry.file_name(),
                OFlag::O_RDWR | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(io::Error::from)?,
        );
        let queue_file_fd = queue_file.as_raw_fd();

        // SAFETY: queue_file_fd is a valid file descriptor and F_GET_SEALS takes no argument.
        let queue_file_seals = unsafe { libc::fcntl(queue_file_fd, libc::F_GET_SEALS) };
        let queue_file_is_not_sealed = (queue_file_seals & REQUIRED_SEALS) != REQUIRED_SEALS;
        let seal_check_failed = queue_file_seals == -1;

        if seal_check_failed || queue_file_is_not_sealed {
            return Err(CreateAvailableStreamError::QueueIsNotSealed);
        }

        // SAFETY:
        // - file is a live broadcast queue, and checked above to be sealed against resizing.
        // - the payload, Event::QueueCell guarantees fully byte initialization.
        // - Event::QueueCell can always be decoded as bytes.
        let broadcast_handle = unsafe { Broadcast::join_untyped(&queue_file) }?;

        let actual_broadcast_identifier = broadcast_handle.queue_identifier();

        // The producer's descriptor number may be reused for another queue
        // before opening the queue's /proc symlink above.
        if expected_broadcast_identifier != actual_broadcast_identifier {
            return Err(CreateAvailableStreamError::QueueIdentifierMismatch {
                expected: expected_broadcast_identifier,
                actual: actual_broadcast_identifier,
            });
        }

        let mut encoded_schema = Vec::new();
        schema_file.read_to_end(&mut encoded_schema)?;
        let schema: RootSchema = wincode::deserialize(&encoded_schema)?;

        Ok(Self {
            stream_name,
            schema,
            broadcast_handle,
        })
    }
}

#[derive(thiserror::Error, Debug)]
enum CreateAvailableStreamError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("queue identifier mismatch: expected {expected}, found {actual}")]
    QueueIdentifierMismatch { expected: u64, actual: u64 },

    // these errors should only happen if producer implementation is incorrect
    // or the user manually tampered with the event directory filesystem
    //
    #[error("failed to deserialize the stream schema")]
    SchemaDeserializationFailed(#[from] wincode::ReadError),
    #[error("the stream directory must have a UTF-8 encoded file name")]
    InvalidStreamName,
    #[error("the queue file name must contain a valid u64 identifier")]
    InvalidQueueIdentifier,
    #[error("the queue file is not properly sealed against resizing")]
    QueueIsNotSealed,
    #[error("the stream contains no queue file")]
    QueueFileIsNotPublished,
    #[error("failed to create a handle to the broadcast")]
    JoiningBroadcastFailed(#[from] shaq::error::Error),
}
