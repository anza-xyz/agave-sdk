use {
    crate::{
        Event,
        backend::{AtomicStreamRule, StreamGuard},
        producer::EmitEventError,
    },
    std::{fmt::Debug, num::NonZeroUsize, sync::Arc},
};

/// A producer which can emit events of a specific type.
pub(crate) struct Producer<E: Event> {
    broadcast_sender: shaq::broadcast::Producer<E::QueueCell>,
    stream_guard: Arc<StreamGuard>,
    stream_rule: Arc<AtomicStreamRule>,
}

impl<E: Event> Producer<E> {
    pub(crate) fn emit_event(&mut self, event: &E) -> Result<(), EmitEventError> {
        if !self.stream_rule.is_on() {
            return Ok(());
        }

        // SAFETY: write_guard is initialized below before it is dropped by going out of scope.
        let mut write_guard = unsafe { self.broadcast_sender.try_reserve_write() }
            .ok_or(EmitEventError::FailedToSend)?;

        let write_guard_cell = write_guard.as_mut();
        // SAFETY: the inner cell contains [u8; N] which is valid for every bit pattern.
        let cell = unsafe { write_guard_cell.assume_init_mut() };

        // if serialization fails we still send incomplete bytes, as drop implementation of
        // write_guard does the sending.
        wincode::serialize_into(cell.as_mut(), &event).map_err(EmitEventError::Serialization)?;

        Ok(())
    }

    /// Emits the given batch of events.
    ///
    /// # Errors
    /// If any event in the batch fails to send, [`EmitEventError`] is returned
    /// and the remaining events in the batch are dropped.
    ///
    /// The events previous to the failing event are all sent.
    pub(crate) fn emit_events_batched(&mut self, events: &[E]) -> Result<(), EmitEventError> {
        if !self.stream_rule.is_on() {
            return Ok(());
        }

        let Ok(event_count) = NonZeroUsize::try_from(events.len()) else {
            // nothing to write
            return Ok(());
        };
        // SAFETY: write_guard cells are initialized in the loop below before it is dropped by going out of scope.
        let mut write_guard = unsafe { self.broadcast_sender.try_reserve_write_batch(event_count) }
            .ok_or(EmitEventError::FailedToSend)?;

        for (i, event) in events.iter().enumerate() {
            // SAFETY: i < events.len() which is the batch size
            let write_guard_cell = unsafe { write_guard.as_mut(i) };
            // SAFETY: the inner cell contains [u8; N] which is valid for every bit pattern.
            let cell = unsafe { write_guard_cell.assume_init_mut() };

            // if serialization fails we still send incomplete bytes, as drop implementation of
            // write_guard does the sending.
            wincode::serialize_into(cell.as_mut(), &event)
                .map_err(EmitEventError::Serialization)?;
        }

        Ok(())
    }
}

impl<E: Event> Producer<E> {
    pub(super) fn new(
        broadcast_sender: shaq::broadcast::Producer<E::QueueCell>,
        stream_guard: Arc<StreamGuard>,
        stream_rule: Arc<AtomicStreamRule>,
    ) -> Self {
        Self {
            broadcast_sender,
            stream_guard,
            stream_rule,
        }
    }
}
impl<E: Event> Debug for Producer<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("broadcast_sender", &self.broadcast_sender)
            .field("stream_guard", &self.stream_guard)
            .finish()
    }
}
