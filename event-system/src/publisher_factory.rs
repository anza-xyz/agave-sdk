use crate::{Event, backend, publisher::Publisher};

/// A handle to a typed [`Event`] stream that can create publishers
/// on demand.
pub struct PublisherFactory<E: Event> {
    backend: Backend<E>,
}

enum Backend<E: Event> {
    Platform(backend::PublisherFactory<E>),
    Stub(backend::stub::PublisherFactory<E>),
}

impl<E: Event> PublisherFactory<E> {
    pub fn try_create_publisher(&self) -> Option<Publisher<E>> {
        match &self.backend {
            Backend::Platform(backend) => backend.try_create_publisher().map(Publisher::new),
            Backend::Stub(backend) => backend.try_create_publisher().map(Publisher::from_stub),
        }
    }

    pub(crate) fn new(backend: backend::PublisherFactory<E>) -> Self {
        Self {
            backend: Backend::Platform(backend),
        }
    }
    pub(crate) fn stub(backend: backend::stub::PublisherFactory<E>) -> Self {
        Self {
            backend: Backend::Stub(backend),
        }
    }
}

impl<E: Event> Clone for PublisherFactory<E> {
    fn clone(&self) -> Self {
        Self {
            backend: match &self.backend {
                Backend::Platform(backend) => Backend::Platform(backend.clone()),
                Backend::Stub(backend) => Backend::Stub(backend.clone()),
            },
        }
    }
}

impl<E: Event> std::fmt::Debug for PublisherFactory<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.backend {
            Backend::Platform(backend) => backend.fmt(formatter),
            Backend::Stub(backend) => backend.fmt(formatter),
        }
    }
}
