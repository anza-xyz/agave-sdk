use {
    super::StreamGuard,
    crate::{
        stream_name::StreamName,
        stream_policy::{StreamPolicy, StreamRule},
    },
    std::sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

/// Manages the stream rules for producer end of streams.
#[derive(Debug, Default)]
pub(super) struct StreamPolicyManager {
    stream_refs: Vec<StreamRef>,
    stream_policy: StreamPolicy,
}

impl StreamPolicyManager {
    /// Registers the new stream policy and updates all active streams to follow it.
    pub(super) fn set_stream_policy(&mut self, new_stream_policy: StreamPolicy) {
        self.prune_dead_streams();

        self.stream_policy = new_stream_policy;

        for stream_ref in self.stream_refs.iter_mut() {
            let stream_rule = self.stream_policy.stream_rule(&stream_ref.stream_name);
            stream_ref.atomic_stream_rule.store(stream_rule);
        }
    }

    /// Registers a new stream and returns its [`AtomicStreamRule`].
    pub(super) fn register_new_stream(
        &mut self,
        stream: &Arc<StreamGuard>,
    ) -> Arc<AtomicStreamRule> {
        self.prune_dead_streams();

        let stream_rule = self.stream_policy.stream_rule(&stream.stream_name);
        let atomic_stream_rule = Arc::new(AtomicStreamRule::from(stream_rule));

        let stream_ref = StreamRef {
            stream_guard: Arc::downgrade(stream),
            atomic_stream_rule: atomic_stream_rule.clone(),
            stream_name: stream.stream_name.clone(),
        };

        self.stream_refs.push(stream_ref);

        atomic_stream_rule
    }

    fn prune_dead_streams(&mut self) {
        self.stream_refs.retain(StreamRef::is_alive);
    }
}

#[derive(Debug)]
pub(super) struct AtomicStreamRule {
    is_on: AtomicBool,
}

impl From<StreamRule> for AtomicStreamRule {
    #[inline]
    fn from(stream_rule: StreamRule) -> Self {
        let is_on = match stream_rule {
            StreamRule::On => true,
            StreamRule::Off => false,
        };

        Self {
            is_on: AtomicBool::new(is_on),
        }
    }
}

impl AtomicStreamRule {
    #[inline]
    pub(super) fn is_on(&self) -> bool {
        self.is_on.load(Ordering::Relaxed)
    }

    fn store(&self, stream_rule: StreamRule) {
        let is_on = match stream_rule {
            StreamRule::On => true,
            StreamRule::Off => false,
        };

        self.is_on.store(is_on, Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
struct StreamRef {
    stream_guard: Weak<StreamGuard>,
    stream_name: Arc<StreamName>,
    atomic_stream_rule: Arc<AtomicStreamRule>,
}

impl StreamRef {
    fn is_alive(&self) -> bool {
        self.stream_guard.strong_count() > 0
    }
}
