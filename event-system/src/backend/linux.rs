use {
    crate::{
        Event,
        cache_padded::CachePadded,
        event_system::{
            CreateEventSystemError, CreateStreamError, EventQueueError as PublicEventQueueError,
            StreamConfig,
        },
        stream_name::StreamName,
        stream_policy::{StreamPolicy, StreamRule},
    },
    shaq::broadcast::{Broadcast, BroadcastConfig, Producer},
    std::{
        fs::{File, OpenOptions, create_dir, create_dir_all, remove_dir, remove_dir_all},
        io::{self, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::fs::symlink,
        },
        path::{Path, PathBuf},
        sync::{
            Arc, Mutex, RwLock,
            atomic::{AtomicU64, Ordering},
        },
    },
    stream_policy::{PolicyControlledStream, StreamPolicyManager},
};
pub(crate) use {
    publisher::Publisher,
    subscriber::{
        AvailableStream, LaneMetadata, LanesMetadata, StreamExplorer, StreamMessage, Subscriber,
    },
};

#[path = "linux/publisher.rs"]
mod publisher;
#[path = "linux/stream_policy.rs"]
mod stream_policy;
#[path = "linux/subscriber.rs"]
mod subscriber;

// Layout of the event-system directory:
//
// event-system-directory/
// ├── tmp/
// │   └── transaction-events/
// │       ├── queue-<id>
// │       └── schema
// └── event-streams/
//     ├── shred-events/
//     │   ├── queue-<id>
//     │   └── schema
//     └── slot-events/
//         ├── queue-<id>
//         └── schema
//
const QUEUE_FILE_NAME_PREFIX: &str = "queue-";
const SCHEMA_FILE_NAME: &str = "schema";

const STAGING_DIRECTORY_NAME: &str = "tmp";
const STREAMS_DIRECTORY_NAME: &str = "event-streams";

// Seals required by shaq's safety contract to prevent the file from resizing.
const REQUIRED_SEALS: libc::c_int = libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;
const ANONYMOUS_FILE_NAME: *const libc::c_char = c"agave-event-stream".as_ptr();

pub(crate) type EventQueueError = shaq::error::Error;

#[derive(Debug, Clone)]
pub(crate) struct EventSystem {
    event_system_directory: Arc<EventSystemDirectory>,
    stream_policy_manager: Arc<Mutex<StreamPolicyManager>>,
}

impl EventSystem {
    /// Creates an event system directory in the given path, `event_system_directory`.
    ///
    /// ### Note:
    /// - If the directory path already exists, it must be empty.
    /// - This functions creates the given directory and any missing parents.
    /// - The given path is canonicalized.
    /// - The directory is emptied once this [`EventSystem`], its clones, and
    ///   all of its streams are dropped. The directory itself is left in place.
    pub(crate) fn new(
        event_system_directory: impl AsRef<Path>,
    ) -> Result<Self, CreateEventSystemError> {
        let event_system_directory = EventSystemDirectory::create(event_system_directory.as_ref())?;

        Ok(Self {
            event_system_directory: Arc::new(event_system_directory),
            stream_policy_manager: Arc::new(Mutex::new(StreamPolicyManager::default())),
        })
    }

    /// Creates a [`PublisherFactory`] for the given stream name and config.
    ///
    /// The stream is not published if the current [`StreamPolicy`] of this event
    /// system has this stream name disabled.
    pub(crate) fn create_stream<E: Event>(
        &self,
        stream_name: StreamName,
        stream_config: StreamConfig,
    ) -> Result<PublisherFactory<E>, CreateStreamError> {
        let stream = Arc::new(EventStream::new(
            self.event_system_directory.clone(),
            stream_name,
            stream_config,
        ));

        self.stream_policy_manager
            .lock()
            .unwrap()
            .register_new_stream(stream.clone())?;

        Ok(PublisherFactory { stream })
    }

    pub(crate) fn set_stream_policy(&self, new_stream_policy: StreamPolicy) {
        self.stream_policy_manager
            .lock()
            .unwrap()
            .set_stream_policy(new_stream_policy);
    }
}

/// Creates a queue backed by a sealed file.
/// Returns both the [`Broadcast`] and its backing [`File`].
fn create_sealed_queue<E: Event>(
    stream_config: StreamConfig,
    queue_identifier: u64,
) -> Result<(Broadcast<E::QueueCell>, File), CreateStreamError> {
    let broadcast_config = BroadcastConfig {
        capacity: stream_config.capacity,
        producer_slots: stream_config.publisher_slots,
        consumer_slots: stream_config.subscriber_slots,
    };

    let check_libc_result = |result: i32| {
        if result == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    };

    // SAFETY: ANONYMOUS_FILE_NAME points to a valid static C string.
    let queue_fd = unsafe {
        libc::memfd_create(
            ANONYMOUS_FILE_NAME,
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    check_libc_result(queue_fd)?;

    // SAFETY: memfd_create returned a new owned file descriptor.
    let queue_file = unsafe { File::from_raw_fd(queue_fd) };

    // SAFETY:
    // - memfd_create returned a new anonymous file, so this call uniquely
    //   initializes it.
    // - the file is sealed against resizing below.
    // - E::QueueCell guarantees Broadcast::create's T type requirements.
    let broadcast = unsafe {
        Broadcast::create_with_identifier(&queue_file, broadcast_config, queue_identifier)
    }
    .map_err(|error| CreateStreamError::Queue(PublicEventQueueError(error)))?;

    // SAFETY: queue_file owns a valid descriptor and F_ADD_SEALS accepts this bitmask.
    let seal_result = unsafe { libc::fcntl(queue_fd, libc::F_ADD_SEALS, REQUIRED_SEALS) };
    check_libc_result(seal_result)?;

    Ok((broadcast, queue_file))
}

pub(crate) struct PublisherFactory<E: Event> {
    stream: Arc<EventStream<E>>,
}

impl<E: Event> PublisherFactory<E> {
    pub(crate) fn try_create_publisher(&self) -> Option<Publisher<E>> {
        let mut stream_state = self.stream.state.write().unwrap();
        stream_state.remaining_publisher_slots =
            stream_state.remaining_publisher_slots.checked_sub(1)?;
        let producer = stream_state.create_producer();
        let queue_generation = self.stream.queue_generation.load(Ordering::Relaxed);
        drop(stream_state);

        Some(Publisher::new(
            self.stream.clone(),
            queue_generation,
            producer,
        ))
    }
}

impl<E: Event> Clone for PublisherFactory<E> {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}

impl<E: Event> std::fmt::Debug for PublisherFactory<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublisherFactory")
            .field("stream_name", &self.stream.stream_name)
            .finish_non_exhaustive()
    }
}

/// An event stream whose queue only exists while the stream policy enables it.
///
/// Each time the stream is enabled, a new queue is created and published. Each
/// time it is disabled, the queue is unpublished. Publishers notice either change
/// at their next publish, where they drop their producer on the previous queue and
/// create one on the current queue.
///
/// Publishers swap producers themselves so that publishing never takes a lock. The
/// cost is that a publisher keeps the previous queue mapped until its next publish.
struct EventStream<E: Event> {
    /// Incremented each time the queue is created or dropped, while `state` is locked.
    ///
    /// Publishers load it on every publish, so it is kept on its own cache line.
    queue_generation: CachePadded<AtomicU64>,
    stream_name: StreamName,
    stream_config: StreamConfig,
    state: RwLock<StreamState<E>>,
    /// Declared after `state`, so the queue's directory is removed before the
    /// event-system directory layout.
    event_system_directory: Arc<EventSystemDirectory>,
}

impl<E: Event> EventStream<E> {
    /// Creates a disabled stream, which has no queue until a stream rule enables it.
    fn new(
        event_system_directory: Arc<EventSystemDirectory>,
        stream_name: StreamName,
        stream_config: StreamConfig,
    ) -> Self {
        Self {
            queue_generation: CachePadded::new(AtomicU64::new(0)),
            stream_name,
            stream_config,
            state: RwLock::new(StreamState {
                queue: None,
                remaining_publisher_slots: stream_config.publisher_slots,
            }),
            event_system_directory,
        }
    }
}

impl<E: Event> PolicyControlledStream for EventStream<E> {
    fn stream_name(&self) -> &StreamName {
        &self.stream_name
    }

    fn apply_stream_rule(&self, stream_rule: StreamRule) -> Result<(), CreateStreamError> {
        let mut state = self.state.write().unwrap();
        match (stream_rule, &state.queue) {
            (StreamRule::On, None) => state.queue = Some(StreamQueue::create(self)?),
            (StreamRule::Off, Some(_)) => state.queue = None,
            // the stream already follows the rule
            (StreamRule::On, Some(_)) | (StreamRule::Off, None) => return Ok(()),
        }

        self.queue_generation.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct StreamState<E: Event> {
    /// The stream's queue, which is `None` while the stream is disabled.
    queue: Option<StreamQueue<E>>,
    /// Number of publishers that can still be created for the stream, as its
    /// publisher slots are a lifetime budget.
    remaining_publisher_slots: usize,
}

impl<E: Event> StreamState<E> {
    /// Creates a producer on the queue, or returns `None` while the stream is disabled.
    fn create_producer(&self) -> Option<Producer<E::QueueCell>> {
        self.queue.as_ref()?.broadcast.producer().ok()
    }
}

/// A published queue of an enabled stream.
///
/// Keeps the queue's backing file alive and removes its directory on drop.
struct StreamQueue<E: Event> {
    broadcast: Broadcast<E::QueueCell>,
    event_stream_directory: Box<Path>,
    // keeps the anonymous file alive
    _queue_file: File,
}

impl<E: Event> StreamQueue<E> {
    /// Creates a new queue for `stream` and publishes it in the event system directory.
    fn create(stream: &EventStream<E>) -> Result<Self, CreateStreamError> {
        let event_stream_directory = stream
            .event_system_directory
            .path
            .join(STREAMS_DIRECTORY_NAME)
            .join(stream.stream_name.as_str());

        let staging_directory = stream
            .event_system_directory
            .path
            .join(STAGING_DIRECTORY_NAME)
            .join(stream.stream_name.as_str());

        let temporary_event_stream_directory = StagingDirectory::new(staging_directory)?;

        let schema_file_path = temporary_event_stream_directory
            .path()
            .join(SCHEMA_FILE_NAME);
        let mut schema_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(schema_file_path)?;
        let encoded_schema =
            wincode::serialize(&E::schema()).map_err(CreateStreamError::FailedToSerializeSchema)?;
        schema_file.write_all(&encoded_schema)?;

        let queue_identifier = getrandom::u64()
            .map_err(io::Error::from)
            .map_err(CreateStreamError::OsRngFailure)?;
        let (broadcast, queue_file) =
            create_sealed_queue::<E>(stream.stream_config, queue_identifier)?;

        let queue_file_name = format!("{QUEUE_FILE_NAME_PREFIX}{queue_identifier}");
        let queue_file_path = temporary_event_stream_directory
            .path()
            .join(queue_file_name);
        let process_id = std::process::id();
        let queue_fd = queue_file.as_raw_fd();
        let proc_fd_path = format!("/proc/{process_id}/fd/{queue_fd}");
        symlink(proc_fd_path, queue_file_path)?;

        temporary_event_stream_directory.publish(&event_stream_directory)?;

        Ok(Self {
            broadcast,
            event_stream_directory: event_stream_directory.into(),
            _queue_file: queue_file,
        })
    }
}

impl<E: Event> Drop for StreamQueue<E> {
    fn drop(&mut self) {
        let _ = remove_dir_all(&self.event_stream_directory);
    }
}

/// The directory layout of an event system.
///
/// Shared by the [`EventSystem`] and its streams, so the layout is removed
/// only after the last of them is dropped.
#[derive(Debug)]
struct EventSystemDirectory {
    path: Box<Path>,
}

impl EventSystemDirectory {
    fn create(path: &Path) -> io::Result<Self> {
        create_dir_all(path)?;
        let path = path.canonicalize()?;
        create_dir(path.join(STAGING_DIRECTORY_NAME))?;
        if let Err(error) = create_dir(path.join(STREAMS_DIRECTORY_NAME)) {
            let _ = remove_dir(path.join(STAGING_DIRECTORY_NAME));
            return Err(error);
        }
        Ok(Self { path: path.into() })
    }
}

impl Drop for EventSystemDirectory {
    fn drop(&mut self) {
        // Both directories were created by `create`, so anything left in them
        // belongs to this event system.
        let _ = remove_dir_all(self.path.join(STAGING_DIRECTORY_NAME));
        let _ = remove_dir_all(self.path.join(STREAMS_DIRECTORY_NAME));
    }
}

struct StagingDirectory {
    path: PathBuf,
    cleanup: bool,
}

impl StagingDirectory {
    fn new(path: PathBuf) -> io::Result<Self> {
        create_dir(&path)?;
        Ok(Self {
            path,
            cleanup: true,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publish(mut self, destination: &Path) -> io::Result<()> {
        // Publishing the directory atomically prevents readers from observing
        // a queue without its schema, or vice versa.
        //
        // Published streams are nonempty, so rename cannot replace them.
        std::fs::rename(&self.path, destination)?;
        self.cleanup = false;
        Ok(())
    }
}

impl Drop for StagingDirectory {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{REQUIRED_SEALS, StagingDirectory, create_sealed_queue},
        crate::{StreamConfig, event},
        std::{io, os::fd::AsRawFd},
        tempfile::TempDir,
    };

    #[event]
    struct TestEvent {
        value: u64,
    }

    const TEST_CONFIG: StreamConfig = StreamConfig {
        capacity: 2,
        publisher_slots: 1,
        subscriber_slots: 1,
    };

    #[test]
    fn staging_directory_publish_preserves_contents() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("staging");
        let destination = directory.path().join("published");

        let staging = StagingDirectory::new(path.clone()).unwrap();
        std::fs::write(staging.path().join("file"), b"published contents").unwrap();
        staging.publish(&destination).unwrap();
        assert!(!path.exists());
        assert_eq!(
            std::fs::read(destination.join("file")).unwrap(),
            b"published contents"
        );
    }

    #[test]
    fn unpublished_staging_directory_removes_contents_on_drop() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("staging");
        let staging = StagingDirectory::new(path.clone()).unwrap();
        std::fs::write(staging.path().join("file"), b"unpublished contents").unwrap();
        drop(staging);
        assert!(!path.exists());
    }

    #[test]
    fn queue_file_is_sealed_against_resizing_and_additional_seals() {
        let (_broadcast, queue_file) = create_sealed_queue::<TestEvent>(TEST_CONFIG, 0).unwrap();
        let queue_fd = queue_file.as_raw_fd();
        // SAFETY: queue_file owns a valid descriptor and F_GET_SEALS takes no extra argument.
        let seals = unsafe { libc::fcntl(queue_fd, libc::F_GET_SEALS) };
        assert_ne!(seals, -1);
        assert_eq!(seals & REQUIRED_SEALS, REQUIRED_SEALS,);

        let queue_size = queue_file.metadata().unwrap().len();
        assert_eq!(
            queue_file.set_len(0).unwrap_err().raw_os_error(),
            Some(libc::EPERM),
        );
        assert_eq!(
            queue_file
                .set_len(queue_size.saturating_add(1))
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EPERM)
        );

        // SAFETY: queue_file owns a valid descriptor and F_ADD_SEALS accepts this seal.
        let seal_result =
            unsafe { libc::fcntl(queue_fd, libc::F_ADD_SEALS, libc::F_SEAL_FUTURE_WRITE) };
        let seal_error = io::Error::last_os_error();
        assert_eq!(seal_result, -1);
        assert_eq!(seal_error.raw_os_error(), Some(libc::EPERM));
    }
}
