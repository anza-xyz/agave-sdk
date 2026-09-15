use {
    super::{QUEUE_FILE_NAME_PREFIX, REQUIRED_SEALS, SCHEMA_FILE_NAME, STREAMS_DIRECTORY_NAME},
    crate::subscriber::{TryConnectError, TryRecvError},
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

        let schema = read_schema(&stream_directory)?;
        let broadcast_handle = open_queue(&mut stream_directory)?;

        Ok(Self {
            stream_name,
            schema,
            broadcast_handle,
        })
    }
}

fn open_queue(
    stream_directory: &mut Dir,
) -> Result<Broadcast<UnknownType>, CreateAvailableStreamError> {
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
            &*stream_directory,
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

    Ok(broadcast_handle)
}

fn read_schema(stream_directory: &Dir) -> Result<RootSchema, CreateAvailableStreamError> {
    let mut schema_file = File::from(
        openat(
            stream_directory,
            SCHEMA_FILE_NAME,
            OFlag::O_RDONLY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );

    let mut encoded_schema = Vec::new();
    schema_file.read_to_end(&mut encoded_schema)?;
    let schema: RootSchema = wincode::deserialize(&encoded_schema)?;
    Ok(schema)
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

#[cfg(test)]
mod tests {
    use {
        super::{
            CreateAvailableStreamError, Dir, Mode, OFlag, QUEUE_FILE_NAME_PREFIX, REQUIRED_SEALS,
            STREAMS_DIRECTORY_NAME, open_queue,
        },
        crate::{EventSystem, ProducerFactory, StreamConfig, event},
        nix::{
            fcntl::{FcntlArg, SealFlag, fcntl},
            sys::memfd::{MFdFlags, memfd_create},
        },
        rstest::rstest,
        std::{
            assert_matches,
            fs::File,
            os::{fd::AsRawFd, unix::fs::symlink},
            path::PathBuf,
        },
        tempfile::TempDir,
    };

    #[event]
    struct TestEvent {
        value: u64,
    }

    struct TestStream {
        path: PathBuf,
        queue_path: PathBuf,
        _producer_factory: ProducerFactory<TestEvent>,
        _directory: TempDir,
    }

    impl TestStream {
        fn new() -> Self {
            let directory = TempDir::new().unwrap();
            let event_system = EventSystem::new(directory.path()).unwrap();
            let producer_factory = event_system
                .create_stream::<TestEvent>(
                    "test-stream",
                    StreamConfig {
                        capacity: 2,
                        producer_slots: 1,
                        consumer_slots: 1,
                    },
                )
                .unwrap();
            let path = directory
                .path()
                .join(STREAMS_DIRECTORY_NAME)
                .join("test-stream");
            let queue_path = std::fs::read_dir(&path)
                .unwrap()
                .map(Result::unwrap)
                .find(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .unwrap()
                        .starts_with(QUEUE_FILE_NAME_PREFIX)
                })
                .unwrap()
                .path();
            Self {
                path,
                queue_path,
                _producer_factory: producer_factory,
                _directory: directory,
            }
        }

        fn open_directory(&self) -> Dir {
            Dir::open(
                &self.path,
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY,
                Mode::empty(),
            )
            .unwrap()
        }
    }

    #[rstest]
    #[case::empty("queue-")]
    #[case::non_numeric("queue-invalid")]
    #[case::overflow("queue-18446744073709551616")]
    fn open_queue_rejects_invalid_identifier(#[case] name: &str) {
        let stream = TestStream::new();
        let mut stream_directory = stream.open_directory();

        std::fs::rename(&stream.queue_path, stream.path.join(name)).unwrap();

        assert_matches!(
            open_queue(&mut stream_directory),
            Err(CreateAvailableStreamError::InvalidQueueIdentifier)
        );
    }

    #[test]
    fn open_queue_rejects_mismatched_identifier() {
        let stream = TestStream::new();
        let mut stream_directory = stream.open_directory();
        let actual_identifier = open_queue(&mut stream_directory)
            .unwrap()
            .queue_identifier();
        let published_identifier = actual_identifier + 1;

        std::fs::rename(
            &stream.queue_path,
            stream
                .path
                .join(format!("{QUEUE_FILE_NAME_PREFIX}{published_identifier}")),
        )
        .unwrap();

        assert_matches!(
            open_queue(&mut stream_directory),
            Err(CreateAvailableStreamError::QueueIdentifierMismatch { expected, actual })
                if expected == published_identifier && actual == actual_identifier
        );
    }

    #[rstest]
    #[case::unsealed(0)]
    #[case::missing_shrink(REQUIRED_SEALS & !libc::F_SEAL_SHRINK)]
    #[case::missing_grow(REQUIRED_SEALS & !libc::F_SEAL_GROW)]
    #[case::missing_seal(REQUIRED_SEALS & !libc::F_SEAL_SEAL)]
    fn open_queue_rejects_missing_seals(#[case] seals: libc::c_int) {
        const TEST_QUEUE_IDENTIFIER: u16 = 42;

        let queue_file = File::from(
            memfd_create(
                "test-queue",
                MFdFlags::MFD_CLOEXEC | MFdFlags::MFD_ALLOW_SEALING,
            )
            .unwrap(),
        );
        let seal_flags = SealFlag::from_bits(seals).unwrap();
        fcntl(&queue_file, FcntlArg::F_ADD_SEALS(seal_flags)).unwrap();

        let stream_directory = TempDir::new().unwrap();

        let proc_fd_path = format!("/proc/self/fd/{}", queue_file.as_raw_fd());
        let stream_queue_directory_path = stream_directory
            .path()
            .join(format!("{QUEUE_FILE_NAME_PREFIX}{TEST_QUEUE_IDENTIFIER}"));

        symlink(proc_fd_path, stream_queue_directory_path).unwrap();

        let mut stream_directory = Dir::open(
            stream_directory.path(),
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY,
            Mode::empty(),
        )
        .unwrap();

        assert_matches!(
            open_queue(&mut stream_directory),
            Err(CreateAvailableStreamError::QueueIsNotSealed)
        );
    }
}
