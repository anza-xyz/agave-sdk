use {
    crate::{
        Event,
        event_system::{CreateEventSystemError, CreateStreamError, StreamConfig},
        publisher::PublishError,
        stream_name::StreamName,
        stream_policy::StreamPolicy,
        subscriber::{RecvTimeoutError, TryConnectError, TryRecvError},
    },
    std::{
        fmt::Debug,
        marker::PhantomData,
        path::{Path, PathBuf},
        time::Duration,
    },
    wincode_dynamic::RootSchema,
};

pub(crate) struct Publisher<E> {
    _data: PhantomData<E>,
}

impl<E> Debug for Publisher<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher").finish()
    }
}

impl<E> Publisher<E> {
    pub(crate) fn new() -> Self {
        Self { _data: PhantomData }
    }

    pub(crate) fn publish(&mut self, _event: &E) -> Result<(), PublishError> {
        Ok(())
    }

    pub(crate) fn publish_batch(&mut self, _events: &[E]) -> Result<(), PublishError> {
        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("infallible error case for stub implementation.")]
pub(crate) struct EventQueueError;

#[derive(Debug, Clone)]
pub(crate) struct EventSystem;

impl EventSystem {
    pub(crate) fn new(
        _event_system_directory: impl AsRef<Path>,
    ) -> Result<Self, CreateEventSystemError> {
        Ok(Self)
    }

    pub(crate) fn create_stream<E: Event>(
        &self,
        _stream_name: StreamName,
        _stream_config: StreamConfig,
    ) -> Result<PublisherFactory<E>, CreateStreamError> {
        Ok(PublisherFactory::new())
    }

    pub(crate) fn set_stream_policy(&self, _new_stream_policy: StreamPolicy) {}
}

pub(crate) struct PublisherFactory<E: Event> {
    _queue_cell: PhantomData<E::QueueCell>,
}

impl<E: Event> PublisherFactory<E> {
    fn new() -> Self {
        Self {
            _queue_cell: PhantomData,
        }
    }

    pub(crate) fn try_create_publisher(&self) -> Option<Publisher<E>> {
        Some(Publisher::new())
    }
}

impl<E: Event> Clone for PublisherFactory<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Event> std::fmt::Debug for PublisherFactory<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PublisherFactory").finish()
    }
}

#[derive(Debug)]
pub(crate) struct StreamExplorer;

impl StreamExplorer {
    pub(crate) fn new(_path: PathBuf) -> Self {
        Self
    }
}

impl StreamExplorer {
    pub(crate) fn available_streams(&self) -> impl Iterator<Item = AvailableStream> + '_ {
        std::iter::empty()
    }
}

#[derive(Debug)]
pub(crate) struct Subscriber {
    stream_name: StreamName,
}

impl Subscriber {
    pub(crate) fn stream_name(&self) -> &StreamName {
        &self.stream_name
    }

    pub(crate) fn type_name(&self) -> &str {
        ""
    }

    pub(crate) fn try_recv(&mut self) -> Result<StreamMessage<'_>, TryRecvError> {
        Err(TryRecvError::Empty)
    }

    pub(crate) fn recv_timeout(
        &mut self,
        _timeout: Duration,
    ) -> Result<StreamMessage<'_>, RecvTimeoutError> {
        Err(RecvTimeoutError)
    }
}

#[derive(Debug)]
pub(crate) struct StreamMessage<'a> {
    schema: &'a RootSchema,
    payload: &'a [u8],
}

impl<'a> StreamMessage<'a> {
    pub(crate) fn schema(&self) -> &'a RootSchema {
        self.schema
    }

    pub(crate) fn payload(&self) -> &[u8] {
        self.payload
    }

    pub(crate) fn publisher_metadata(&self) -> PublisherMetadata<'_> {
        PublisherMetadata(PhantomData)
    }
}

// 'a lifetime is there to match the `linux` backend, where the metadata is
// borrowed from the queue's shared memory.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PublisherMetadata<'a>(PhantomData<&'a ()>);

impl PublisherMetadata<'_> {
    pub(crate) fn lane(&self) -> usize {
        0
    }

    pub(crate) fn thread_id(&self) -> u64 {
        0
    }

    pub(crate) fn rejected_items(&self) -> u64 {
        0
    }
}

#[derive(Debug)]
pub(crate) struct AvailableStream {
    stream_name: StreamName,
    type_name: String,
    dummy_schema: RootSchema,
}

impl AvailableStream {
    pub(crate) fn stream_name(&self) -> &StreamName {
        &self.stream_name
    }

    pub(crate) fn type_name(&self) -> &str {
        &self.type_name
    }

    pub(crate) fn stream_schema(&self) -> &RootSchema {
        &self.dummy_schema
    }

    pub(crate) fn try_connect(self) -> Result<Subscriber, TryConnectError> {
        Ok(Subscriber {
            stream_name: self.stream_name,
        })
    }
}
