use {
    crate::{
        Event,
        event_system::{CreateEventSystemError, CreateStreamError, StreamConfig},
        producer::EmitEventError,
        subscriber::{TryConnectError, TryRecvError},
    },
    std::{
        fmt::Debug,
        marker::PhantomData,
        path::{Path, PathBuf},
    },
    wincode_dynamic::RootSchema,
};

pub(crate) struct Producer<E> {
    _data: PhantomData<E>,
}

impl<E> Debug for Producer<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer").finish()
    }
}

impl<E> Producer<E> {
    pub(crate) fn emit_event(&mut self, _event: &E) -> Result<(), EmitEventError> {
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
        _stream_name: &str,
        _stream_config: StreamConfig,
    ) -> Result<ProducerFactory<E>, CreateStreamError> {
        Ok(ProducerFactory::new())
    }
}

pub(crate) struct ProducerFactory<E: Event> {
    _queue_cell: PhantomData<E::QueueCell>,
}

impl<E: Event> ProducerFactory<E> {
    fn new() -> Self {
        Self {
            _queue_cell: PhantomData,
        }
    }

    pub(crate) fn try_create_producer(&self) -> Option<Producer<E>> {
        Some(Producer { _data: PhantomData })
    }
}

impl<E: Event> Clone for ProducerFactory<E> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

impl<E: Event> std::fmt::Debug for ProducerFactory<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ProducerFactory").finish()
    }
}

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
pub(crate) struct StreamSubscriber;

impl StreamSubscriber {
    pub(crate) fn stream_name(&self) -> &str {
        ""
    }

    pub(crate) fn type_name(&self) -> &str {
        ""
    }

    pub(crate) fn try_recv(&mut self) -> Result<StreamMessage<'_>, TryRecvError> {
        Err(TryRecvError::Empty)
    }
}

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

    pub(crate) fn lane_metadata(&self) -> ProducerMetadata<'_> {
        ProducerMetadata(PhantomData)
    }
}

// 'a lifetime is there to match the `linux` backend, where the metadata is
// borrowed from the queue's shared memory.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ProducerMetadata<'a>(PhantomData<&'a ()>);

impl ProducerMetadata<'_> {
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

// 'a lifetime is there to match `linux` backend
pub(crate) struct AvailableStream {
    stream_name: String,
    type_name: String,
    dummy_schema: RootSchema,
}

impl AvailableStream {
    pub(crate) fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub(crate) fn type_name(&self) -> &str {
        &self.type_name
    }

    pub(crate) fn stream_schema(&self) -> &RootSchema {
        &self.dummy_schema
    }

    pub(crate) fn try_connect(self) -> Result<StreamSubscriber, TryConnectError> {
        Ok(StreamSubscriber)
    }
}
