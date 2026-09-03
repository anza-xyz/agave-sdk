#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg(target_os = "linux")]

use {
    shaq::broadcast::{Broadcast, BroadcastConfig, ProducerId},
    std::{
        fs::{OpenOptions, create_dir, rename},
        io::Write,
        path::{Component, Path, PathBuf},
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    },
    thiserror::Error,
    wincode::{SchemaWrite, config::DefaultConfig},
    wincode_dynamic::SchemaDynamic,
};

pub use {
    crate::{
        event_handle::EventHandle,
        producer::{EmitEventError, EventProducer},
        queue_cell::event_queue_cell_size,
        subscriber::{EventSubscriber, NewSubscriberError},
    },
    agave_event_system_derive::event,
};

#[doc(hidden)]
pub mod __private {
    pub use {wincode, wincode_dynamic};

    pub mod event_macro {
        pub use {wincode::*, wincode_dynamic::*};
    }
}

pub mod event_handle;
pub mod producer;
pub mod subscriber;

pub(crate) mod event_filter;
pub(crate) mod queue_cell;
pub(crate) mod queue_file;

// Layout of the event directory:
//
// event-system-directory/
// ├── tmp/
// │   └── transaction_events/    # Event stream still being initialized
// │       ├── queue
// │       └── schema
// └── event-streams/
//     ├── shred_events/
//     │   ├── queue    # Symlink to a sealed shared-memory queue
//     │   └── schema   # Event schema, including its event names
//     └── slot_events/
//         ├── queue
//         └── schema
//
const EVENT_QUEUE_FILE_NAME: &str = "queue";
const EVENT_SCHEMA_FILE_NAME: &str = "schema";
const EVENT_STAGING_DIRECTORY_NAME: &str = "tmp";
const EVENT_STREAMS_DIRECTORY_NAME: &str = "event-streams";

/// An event type that can be sent on an event stream.
///
/// The [`Event`] trait should only be implemented with [`event`] derivation macro.
///
/// ```
/// # use agave_event_system::event;
/// #[event]
/// #[derive(Debug, PartialEq, Eq)]
/// enum SlotEvents {
///     Completed { slot: u64 },
/// }
/// ```
///
/// Events containing dynamically sized values must declare a
/// `max_serialized_size` strictly greater than the statically known portion of
/// their encoding. Publishing a value whose encoding exceeds the bound returns
/// an [`EmitEventError::Serialization`] error.
///
/// ```
/// # use agave_event_system::event;
/// #[event(max_serialized_size = 1024)]
/// struct Message {
///     contents: String,
/// }
/// ```
///
/// Omitting the bound for a dynamically sized event is a compile-time error.
///
/// ```compile_fail
/// # use agave_event_system::event;
/// #[event]
/// struct UnboundedMessage {
///     contents: String,
/// }
/// ```
///
/// The bound must also exceed wincode's dynamic serialized-size lower bound.
///
/// ```compile_fail
/// # use agave_event_system::event;
/// #[event(max_serialized_size = 8)]
/// struct UndersizedMessage {
///     fixed: u64,
///     contents: String,
/// }
/// ```
///
/// # Safety
///
/// [`Event::QueueCell`] must be valid for every bit pattern, contain no
/// uninitialized padding, and expose its complete representation through
/// [`AsMut<[u8]>`]. The [`event`] macro satisfies these requirements by using
/// `[u8; N]`.
pub unsafe trait Event:
    Sized + 'static + SchemaDynamic + SchemaWrite<DefaultConfig, Src = Self>
{
    /// [generic-const-exprs](https://doc.rust-lang.org/beta/unstable-book/language-features/generic-const-exprs.html)
    /// is not stabilized on current latest stable version of rust. Thus we can't derive the size in const expressions
    /// based on the `SchemaDynamic::SERIALIZED_SIZE` size.
    ///
    /// Instead we have this QueueCell associated type as a workaround, which in practice is always a [u8; _], from the
    /// derive macro implementation.
    type QueueCell: Copy + AsMut<[u8]>;
}

/// Creates a subscriber listening to all event streams.
pub fn new_subscriber(
    event_system_directory: PathBuf,
) -> Result<EventSubscriber, NewSubscriberError> {
    // TODO: skip this wrapper function
    EventSubscriber::new(event_system_directory)
}

#[derive(Debug, Clone)]
pub struct EventSystem {
    // Might want a drop implementation where when ALL producers and handles
    // to the event system have been dropped, then the directory gets wiped out or moved
    // somewhere else if we want to keep it for debugging.
    event_system_directory: Arc<Path>,
}

impl EventSystem {
    /// Creates a new event system rooted at `event_system_directory`.
    ///
    /// The directory must not already exist, and its parent directory must
    /// exist. On success, the event system owns the directory and everything
    /// created beneath it.
    pub fn create(
        event_system_directory: impl AsRef<Path>,
    ) -> Result<Self, CreateEventSystemError> {
        let event_system_directory = event_system_directory.as_ref();
        create_dir(event_system_directory)?;

        let staging_directory = event_system_directory.join(EVENT_STAGING_DIRECTORY_NAME);
        create_dir(staging_directory)?;
        let event_streams_directory = event_system_directory.join(EVENT_STREAMS_DIRECTORY_NAME);
        create_dir(event_streams_directory)?;

        Ok(Self {
            event_system_directory: event_system_directory.into(),
        })
    }

    /// Creates a queue named `event_stream_name` for event type `E`.
    pub fn create_event_handle<E>(
        &self,
        event_stream_name: &str,
        event_stream_config: EventStreamConfig,
    ) -> Result<EventHandle<E>, CreateEventHandleError>
    where
        E: Event,
    {
        let event_stream_directory =
            self.event_stream_directory(event_stream_name)
                .ok_or_else(|| {
                    CreateEventHandleError::InvalidEventStreamName(event_stream_name.to_owned())
                })?;

        let broadcast_config = BroadcastConfig {
            capacity: event_stream_config.capacity,
            producer_slots: event_stream_config.producer_slots,
            consumer_slots: event_stream_config.consumer_slots,
        };

        // proxy for a unique number
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let staging_directory = self
            .event_system_directory
            .join(EVENT_STAGING_DIRECTORY_NAME);

        // write to a tmp directory in the event directory and then rename
        // so we get an atomic write of both the schema and event file.
        let tmp_event_directory_name = format!(".{event_stream_name}-{timestamp}.tmp");
        let tmp_event_directory_path = staging_directory.join(tmp_event_directory_name);
        create_dir(&tmp_event_directory_path)?;

        let encoded_schema = wincode::serialize(&E::schema())
            .map_err(CreateEventHandleError::FailedToSerializeSchema)?;
        let schema_file_path = tmp_event_directory_path.join(EVENT_SCHEMA_FILE_NAME);
        let mut schema_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(schema_file_path)?;
        schema_file.write_all(&encoded_schema)?;

        let queue_file = queue_file::create_anonymous_file()?;
        // SAFETY:
        // - memfd_create returned a new anonymous file, so this call uniquely
        //   initializes it.
        // - the file is sealed against resizing before it is published below.
        // - Event's safety contract guarantees the QueueCell representation
        //   requirements expected by shaq.
        let broadcast =
            unsafe { Broadcast::<E::QueueCell>::create(&queue_file, broadcast_config) }?;
        queue_file::seal(&queue_file)?;
        let queue_file_path = tmp_event_directory_path.join(EVENT_QUEUE_FILE_NAME);
        queue_file::publish_anonymous_file(&queue_file, &queue_file_path)?;

        // atomic move of event stream + schema
        rename(tmp_event_directory_path, event_stream_directory)?;

        Ok(EventHandle::new(broadcast, Arc::new(queue_file)))
    }

    fn event_stream_directory(&self, event_stream_name: &str) -> Option<PathBuf> {
        let event_stream_name = Path::new(event_stream_name);
        let mut components = event_stream_name.components();

        let starts_with_normal_component = matches!(components.next(), Some(Component::Normal(_)));
        let has_additional_components = components.next().is_some();

        if !starts_with_normal_component || has_additional_components {
            return None;
        }

        Some(
            self.event_system_directory
                .join(EVENT_STREAMS_DIRECTORY_NAME)
                .join(event_stream_name),
        )
    }
}

pub struct EventStreamConfig {
    /// Number of events retained in each producer queue.
    pub capacity: usize,
    /// Maximum number of concurrent producers per event stream.
    pub producer_slots: usize,
    /// Maximum number of concurrent consumers per event stream.
    pub consumer_slots: usize,
}

#[derive(Debug, Error)]
#[error("Failed to create the event-system directory")]
pub struct CreateEventSystemError(#[from] std::io::Error);

#[derive(Debug, Error)]
pub enum CreateEventHandleError {
    #[error("Event streams can only be created from a newly created event system")]
    CannotCreateFromJoinedSystem,
    #[error("Event stream name `{0}` is not a valid persistent queue identifier")]
    InvalidEventStreamName(String),
    #[error("Failed to serialize the event-stream schema")]
    FailedToSerializeSchema(#[source] wincode::WriteError),
    #[error("Failed to create the event-stream files")]
    FileSystem(#[from] std::io::Error),
    #[error("Failed to create the event-stream queue")]
    Queue(#[from] shaq::error::Error),
}

pub(crate) fn create_producer_id_for_current_thread() -> ProducerId {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: no caller requirements for gettid
        let thread_id = unsafe { libc::gettid() };
        ProducerId::new(thread_id as u64)
    }

    #[cfg(target_os = "macos")]
    {
        let mut thread_id = 0;

        // SAFETY: Passing 0 requests the calling thread's ID, and `thread_id`
        // points to valid writable memory.
        let error = unsafe { libc::pthread_threadid_np(0, &mut thread_id) };
        assert_eq!(error, 0, "pthread_threadid_np failed with error {error}");

        ProducerId::new(thread_id)
    }
}
