use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone)]
pub(crate) struct EventFilter(Arc<AtomicBool>);

impl EventFilter {
    pub(crate) fn enabled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    fn new_enabled() -> Self {
        EventFilter(Arc::new(AtomicBool::new(true)))
    }
}

impl Default for EventFilter {
    fn default() -> Self {
        Self::new_enabled()
    }
}
