use {
    crate::{
        Event, PublisherFactory,
        backend::{self},
        stream_name::StreamName,
        stream_policy::StreamPolicy,
    },
    std::path::Path,
    thiserror::Error,
};

/// Owns an event-system directory and creates typed event streams within it.
#[derive(Clone)]
pub struct EventSystem {
    backend: backend::EventSystem,
}

impl EventSystem {
    /// Creates an event system directory in the given path, `event_system_directory`.
    ///
    /// ### Note:
    /// - If the directory path already exists, it must be empty, or contain
    ///   only the layout of an event system whose process has exited (e.g.
    ///   after a crash), which is then removed. A directory in use by another
    ///   event system is never removed, and fails with
    ///   [`std::io::ErrorKind::ResourceBusy`].
    /// - This functions creates the given directory and any missing parents.
    /// - The given path is canonicalized.
    /// - The directory is emptied once this [`EventSystem`], its clones, and
    ///   all of its streams (including their [`PublisherFactory`]s and
    ///   publishers) are dropped. The directory itself is left in place, and
    ///   can be reused by a later call to [`EventSystem::new`].
    pub fn new(event_system_directory: impl AsRef<Path>) -> Result<Self, CreateEventSystemError> {
        Ok(Self {
            backend: backend::EventSystem::new(event_system_directory)?,
        })
    }

    /// Creates a stream named `stream_name` for event type `E`
    /// and returns its [`PublisherFactory`].
    pub fn create_stream<E: Event>(
        &self,
        stream_name: StreamName,
        stream_config: StreamConfig,
    ) -> Result<PublisherFactory<E>, CreateStreamError> {
        self.backend
            .create_stream::<E>(stream_name, stream_config)
            .map(PublisherFactory::new)
    }

    /// Applies the given [`StreamPolicy`] on the streams created by this
    /// [`EventSystem`].
    ///
    /// The applied stream policy will also be applied to future stream creations
    /// of this event system.
    pub fn set_stream_policy(&self, stream_policy: StreamPolicy) {
        self.backend.set_stream_policy(stream_policy)
    }
}

impl std::fmt::Debug for EventSystem {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.backend.fmt(formatter)
    }
}

/// Capacity and participant limits for an event stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamConfig {
    /// Number of events retained in each publisher queue.
    pub capacity: usize,
    /// Maximum number of publishers that can be created for the stream.
    ///
    /// This slot count is a lifetime budget. Dropping a publisher permanently retires
    /// that slot forever.
    pub publisher_slots: usize,
    /// Maximum number of concurrent subscribers.
    pub subscriber_slots: usize,
}

#[derive(Debug, Error)]
#[error("failed to create the event-system directory")]
pub struct CreateEventSystemError(#[from] std::io::Error);

/// An error reported by the platform event-queue implementation.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct EventQueueError(#[source] pub(crate) backend::EventQueueError);

#[derive(Debug, Error)]
pub enum CreateStreamError {
    #[error("failed to serialize the event-stream schema")]
    FailedToSerializeSchema(#[source] wincode::WriteError),
    #[error("failed to create the event-stream files")]
    FileSystem(#[from] std::io::Error),
    #[error("failed to create the event-stream queue")]
    Queue(#[source] EventQueueError),
    #[error("failed to produce a random number for the queue identifier")]
    OsRngFailure(std::io::Error),
}
