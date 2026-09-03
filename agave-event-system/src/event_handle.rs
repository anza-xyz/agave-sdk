use {
    crate::{
        Event, create_producer_id_for_current_thread, event_filter::EventFilter,
        producer::EventProducer,
    },
    shaq::broadcast::Broadcast,
    std::{fs::File, sync::Arc},
};

pub struct EventHandle<E: Event> {
    pub(crate) broadcast: Broadcast<E::QueueCell>,
    pub(crate) event_filter: EventFilter,
    pub(crate) queue_file: Arc<File>,
}

impl<E: Event> EventHandle<E> {
    pub(crate) fn new(broadcast: Broadcast<E::QueueCell>, queue_file: Arc<File>) -> Self {
        Self {
            broadcast,
            event_filter: EventFilter::default(),
            queue_file,
        }
    }
}

impl<E: Event> EventHandle<E> {
    /// Creates a producer if this event stream has a free producer slot.
    pub fn try_create_producer(&self) -> Option<EventProducer<E>> {
        let producer_id = create_producer_id_for_current_thread();
        let broadcast_sender = self.broadcast.producer(producer_id).ok()?;

        Some(EventProducer::new(
            broadcast_sender,
            self.event_filter.clone(),
            Arc::clone(&self.queue_file),
        ))
    }
}

impl<E: Event> Clone for EventHandle<E> {
    fn clone(&self) -> Self {
        Self {
            broadcast: self.broadcast.clone(),
            event_filter: self.event_filter.clone(),
            queue_file: Arc::clone(&self.queue_file),
        }
    }
}

impl<E: Event> std::fmt::Debug for EventHandle<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventHandle")
            .field("event_filter", &self.event_filter)
            .field("broadcast", &self.broadcast)
            .finish()
    }
}
