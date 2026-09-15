use {
    crate::{Event, backend},
    std::{fmt::Debug, marker::PhantomData, rc::Rc},
};

/// A producer which can emit events of a specific type.
///
/// [`Producer<T>`] is [`!Send`](Send) + [`!Sync`](Sync), as a producer is associated
/// with a thread for its entire lifetime.
pub struct Producer<E: Event> {
    inner: backend::Producer<E>,
    //  `Rc` is !Send + !Sync, which makes Producer<E> also neither
    _not_send_or_sync: PhantomData<Rc<()>>,
}

impl<E: Event> Producer<E> {
    pub fn emit_event(&mut self, event: &E) -> Result<(), EmitEventError> {
        self.emit_events_batched(std::array::from_ref(event))
    }

    /// Emits the given batch of events on the stream.
    ///
    /// # Errors
    /// If any event in the batch fails to send, [`EmitEventError`] is returned
    /// and the remaining events in the batch are dropped.
    ///
    /// The events previous to the failing event are all sent.
    pub fn emit_events_batched(&mut self, events: &[E]) -> Result<(), EmitEventError> {
        self.inner.emit_events_batched(events)
    }

    pub(crate) fn new(inner: backend::Producer<E>) -> Self {
        Self {
            inner,
            _not_send_or_sync: PhantomData,
        }
    }
}

impl<E: Event> Debug for Producer<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.inner, f)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum EmitEventError {
    #[error("Failed to serialize the event")]
    Serialization(wincode::WriteError),
    #[error("Failed to send the event. Back-pressured by event subscribers.")]
    FailedToSend,
}
