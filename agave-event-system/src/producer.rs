use {
    crate::{Event, event_filter::EventFilter},
    std::{fmt::Debug, fs::File, marker::PhantomData, rc::Rc, sync::Arc},
    thiserror::Error,
};

/// A producer which can emit events of a specific type.
///
/// [`EventProducer<T>`] is ! [`Send`] + ! [`Sync`], as events emitted by a specific instance of an event producer
/// is associated with a specific thread.
#[derive(Debug)]
pub struct EventProducer<E: Event> {
    broadcast_sender: shaq::broadcast::Producer<E::QueueCell>,
    event_filter: EventFilter,
    // Keeps the descriptor targeted by the published /proc/<pid>/fd/<fd>
    // alive.
    _queue_file: Arc<File>,
    // Makes EventProducer !Send + !Sync as `Rc<_>` is also neither.
    not_send_or_sync: PhantomData<Rc<()>>,
}

impl<E: Event> EventProducer<E> {
    pub fn publish_event(&mut self, event: &E) -> Result<(), EmitEventError> {
        if !self.event_filter.enabled() {
            return Ok(());
        }

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
}

impl<E: Event> EventProducer<E> {
    pub(crate) fn new(
        broadcast_sender: shaq::broadcast::Producer<E::QueueCell>,
        event_filter: EventFilter,
        queue_file: Arc<File>,
    ) -> Self {
        Self {
            broadcast_sender,
            event_filter,
            _queue_file: queue_file,
            not_send_or_sync: PhantomData,
        }
    }
}

#[derive(Error, Debug)]
pub enum EmitEventError {
    #[error("Failed to get host system time. Error code {0}.")]
    FailedToGetTime(i32),
    #[error("Failed to serialize the event")]
    Serialization(wincode::WriteError),
    #[error("Failed to send the event. Back-pressured by event subscribers.")]
    FailedToSend,
}
