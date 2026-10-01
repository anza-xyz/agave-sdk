use {
    crate::{
        Event,
        event_system::{
            CreateEventSystemError, CreateStreamError, EventQueueError as PublicEventQueueError,
            StreamConfig,
        },
        stream_name::StreamName,
        stream_policy::StreamPolicy,
    },
    shaq::broadcast::{Broadcast, BroadcastConfig, ProducerId},
    std::{
        fs::{File, OpenOptions, create_dir, create_dir_all, remove_dir, remove_dir_all},
        io::{self, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::fs::symlink,
        },
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    },
    stream_policy::{AtomicStreamRule, StreamPolicyManager},
};
pub(crate) use {
    publisher::Publisher,
    subscriber::{AvailableStream, PublisherMetadata, StreamExplorer, StreamMessage, Subscriber},
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
// ├── lock
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
// Locked for the lifetime of an event system, so a directory left behind by an
// exited process can be told apart from one in use.
const LOCK_FILE_NAME: &str = "lock";

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
    /// - If the directory path already exists, it must be empty, or contain
    ///   only the layout of an event system whose process has exited, which
    ///   is then removed.
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

    /// Creates a stream named `stream_name` for event type `E`
    /// and returns its [`PublisherFactory`].
    pub(crate) fn create_stream<E: Event>(
        &self,
        stream_name: StreamName,
        stream_config: StreamConfig,
    ) -> Result<PublisherFactory<E>, CreateStreamError> {
        let event_stream_directory = self
            .event_system_directory
            .path
            .join(STREAMS_DIRECTORY_NAME)
            .join(stream_name.as_str());

        let staging_directory = self
            .event_system_directory
            .path
            .join(STAGING_DIRECTORY_NAME)
            .join(stream_name.as_str());

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
        let (broadcast, queue_file) = create_sealed_queue::<E>(stream_config, queue_identifier)?;

        let queue_file_name = format!("{QUEUE_FILE_NAME_PREFIX}{queue_identifier}");
        let queue_file_path = temporary_event_stream_directory
            .path()
            .join(queue_file_name);
        let process_id = std::process::id();
        let queue_fd = queue_file.as_raw_fd();
        let proc_fd_path = format!("/proc/{process_id}/fd/{queue_fd}");
        symlink(proc_fd_path, queue_file_path)?;

        temporary_event_stream_directory.publish(&event_stream_directory)?;

        let stream_guard = Arc::new(StreamGuard {
            event_stream_directory: event_stream_directory.into(),
            stream_name: Arc::new(stream_name),
            _queue_file: queue_file,
            _event_system_directory: Arc::clone(&self.event_system_directory),
        });

        let mut stream_policy_manager_guard = self.stream_policy_manager.lock().unwrap();
        let atomic_stream_rule = stream_policy_manager_guard.register_new_stream(&stream_guard);
        drop(stream_policy_manager_guard);

        Ok(PublisherFactory::new(
            broadcast,
            stream_guard,
            atomic_stream_rule,
        ))
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
    broadcast: Broadcast<E::QueueCell>,
    stream_guard: Arc<StreamGuard>,
    stream_rule: Arc<AtomicStreamRule>,
}

impl<E: Event> PublisherFactory<E> {
    pub(crate) fn try_create_publisher(&self) -> Option<Publisher<E>> {
        let stream_guard = self.stream_guard.clone();
        // SAFETY: gettid id is always safe to call
        let thread_id: i32 = unsafe { libc::gettid() };

        let thread_id = u64::try_from(thread_id).expect(
            "gettid man page: `call is always sucessful`, meaning a positive i32 is returned",
        );

        let producer_id = ProducerId::new(thread_id);
        let broadcast_sender = self.broadcast.producer(producer_id).ok()?;

        let publisher = Publisher::new(broadcast_sender, stream_guard, self.stream_rule.clone());

        Some(publisher)
    }

    fn new(
        broadcast: Broadcast<E::QueueCell>,
        stream_guard: Arc<StreamGuard>,
        stream_rule: Arc<AtomicStreamRule>,
    ) -> Self {
        Self {
            broadcast,
            stream_guard,
            stream_rule,
        }
    }
}

impl<E: Event> Clone for PublisherFactory<E> {
    fn clone(&self) -> Self {
        Self {
            broadcast: self.broadcast.clone(),
            stream_guard: self.stream_guard.clone(),
            stream_rule: self.stream_rule.clone(),
        }
    }
}

impl<E: Event> std::fmt::Debug for PublisherFactory<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublisherFactory")
            .field("broadcast", &self.broadcast)
            .finish_non_exhaustive()
    }
}

/// Keeps a stream's backing file alive and removes its directory on drop.
#[derive(Debug)]
struct StreamGuard {
    event_stream_directory: Box<Path>,
    stream_name: Arc<StreamName>,
    // keeps the anonymous file alive
    _queue_file: File,
    // keeps the event-system directory alive until the stream directory is removed
    _event_system_directory: Arc<EventSystemDirectory>,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        let _ = remove_dir_all(&self.event_stream_directory);
    }
}

/// The root of an event system's directory layout.
///
/// Shared by the [`EventSystem`] and its streams, so the layout is removed
/// only after the last of them is dropped. The root directory is left in
/// place, since it may have been set up by the caller (e.g. with permissions
/// for subscribers), and an empty directory can be reused by
/// [`EventSystem::new`].
#[derive(Debug)]
struct EventSystemDirectory {
    path: Box<Path>,
    /// Holds the lock on [`LOCK_FILE_NAME`]. The kernel releases it when the
    /// process exits, even if it crashes.
    _lock_file: File,
}

impl EventSystemDirectory {
    fn create(path: &Path) -> io::Result<Self> {
        create_dir_all(path)?;
        let path = path.canonicalize()?;
        // Checked before creating the lock file, so that a directory which is
        // not an event system is left untouched.
        check_layout_entries(&path)?;
        let lock_file = lock(&path.join(LOCK_FILE_NAME))?;
        remove_stale_layout(&path)?;
        create_dir(path.join(STAGING_DIRECTORY_NAME))?;
        if let Err(error) = create_dir(path.join(STREAMS_DIRECTORY_NAME)) {
            let _ = remove_dir(path.join(STAGING_DIRECTORY_NAME));
            return Err(error);
        }
        Ok(Self {
            path: path.into(),
            _lock_file: lock_file,
        })
    }
}

impl Drop for EventSystemDirectory {
    fn drop(&mut self) {
        // Every stream has removed its own directory by now, so these are
        // expected to be empty. Both were created by `create`, so anything
        // left in them is removed too, so that a later `create` can succeed.
        let _ = remove_dir_all(self.path.join(STAGING_DIRECTORY_NAME));
        let _ = remove_dir_all(self.path.join(STREAMS_DIRECTORY_NAME));
        // Removed while still locked, so no other event system can have
        // claimed the directory in the meantime.
        let _ = std::fs::remove_file(self.path.join(LOCK_FILE_NAME));
    }
}

/// Takes the lock on the event-system directory, creating the lock file if needed.
fn lock(lock_file_path: &Path) -> io::Result<File> {
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_file_path)?;
    match lock_file.try_lock() {
        Ok(()) => Ok(lock_file),
        Err(std::fs::TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::ResourceBusy,
            format!(
                "{} is in use by another event system",
                lock_file_path.display()
            ),
        )),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

/// Returns the subdirectories of an event-system directory, or an error if it
/// contains anything that is not part of the event-system layout.
fn check_layout_entries(path: &Path) -> io::Result<Vec<PathBuf>> {
    let mut layout_directories = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let file_type = entry.file_type()?;
        if name == LOCK_FILE_NAME && file_type.is_file() {
            continue;
        }
        if !(name == STAGING_DIRECTORY_NAME || name == STREAMS_DIRECTORY_NAME)
            || !file_type.is_dir()
        {
            return Err(io::Error::new(
                io::ErrorKind::DirectoryNotEmpty,
                format!(
                    "{} is not an event-system directory: unexpected entry {:?}",
                    path.display(),
                    name,
                ),
            ));
        }
        layout_directories.push(entry.path());
    }
    Ok(layout_directories)
}

/// Removes the layout left behind by an event system whose process exited
/// without dropping it, e.g. after a crash.
///
/// Must be called while holding the directory's lock. The layout is only
/// removed if it contains nothing unexpected, and none of its queues belong
/// to a live process. Otherwise, it is left untouched and an error is returned.
fn remove_stale_layout(path: &Path) -> io::Result<()> {
    let layout_directories = check_layout_entries(path)?;
    for layout_directory in &layout_directories {
        for stream_directory in std::fs::read_dir(layout_directory)? {
            check_stream_directory_is_stale(&stream_directory?.path())?;
        }
    }

    for layout_directory in layout_directories {
        remove_dir_all(layout_directory)?;
    }
    Ok(())
}

/// Checks that a stream directory only contains a schema and queue links,
/// and that every queue link points to a file descriptor that no longer exists.
fn check_stream_directory_is_stale(stream_directory: &Path) -> io::Result<()> {
    let unexpected_entry = |name: &std::ffi::OsStr| {
        io::Error::new(
            io::ErrorKind::DirectoryNotEmpty,
            format!(
                "{} is not an event-stream directory: unexpected entry {:?}",
                stream_directory.display(),
                name,
            ),
        )
    };

    for entry in std::fs::read_dir(stream_directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let file_type = entry.file_type()?;
        if name == SCHEMA_FILE_NAME && file_type.is_file() {
            continue;
        }
        let is_queue_name = name
            .to_str()
            .is_some_and(|name| name.starts_with(QUEUE_FILE_NAME_PREFIX));
        if !(is_queue_name && file_type.is_symlink()) {
            return Err(unexpected_entry(&name));
        }
        // Queue links point to `/proc/<pid>/fd/<fd>`, which only resolves while
        // the publishing process keeps the queue open.
        if entry.path().try_exists()? {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                format!("{} is in use by a live process", entry.path().display()),
            ));
        }
    }
    Ok(())
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
