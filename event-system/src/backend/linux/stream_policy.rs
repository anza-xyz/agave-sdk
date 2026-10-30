use {
    crate::{
        CreateStreamError,
        stream_name::StreamName,
        stream_policy::{StreamPolicy, StreamRule},
    },
    std::sync::{Arc, Weak},
};

/// A stream whose queue is created and dropped by its [`StreamRule`].
pub(super) trait PolicyControlledStream: Send + Sync {
    fn stream_name(&self) -> &StreamName;

    /// Creates the stream's queue if `stream_rule` is on, and drops it otherwise.
    fn apply_stream_rule(&self, stream_rule: StreamRule) -> Result<(), CreateStreamError>;
}

/// Manages the stream rules for publisher end of streams.
#[derive(Debug, Default)]
pub(super) struct StreamPolicyManager {
    streams: Vec<Weak<dyn PolicyControlledStream>>,
    stream_policy: StreamPolicy,
}

impl StreamPolicyManager {
    /// Registers the new stream policy and updates all active streams to follow it.
    pub(super) fn set_stream_policy(&mut self, new_stream_policy: StreamPolicy) {
        self.prune_dead_streams();

        self.stream_policy = new_stream_policy;

        for stream in self.streams.iter().filter_map(Weak::upgrade) {
            let stream_rule = self.stream_policy.stream_rule(stream.stream_name());
            // a stream whose queue can not be created stays disabled
            let _ = stream.apply_stream_rule(stream_rule);
        }
    }

    /// Applies the stream policy to a new stream and registers it to follow
    /// future stream policies.
    pub(super) fn register_new_stream(
        &mut self,
        stream: Arc<dyn PolicyControlledStream>,
    ) -> Result<(), CreateStreamError> {
        self.prune_dead_streams();

        let stream_rule = self.stream_policy.stream_rule(stream.stream_name());
        stream.apply_stream_rule(stream_rule)?;
        self.streams.push(Arc::downgrade(&stream));

        Ok(())
    }

    fn prune_dead_streams(&mut self) {
        self.streams.retain(|stream| stream.strong_count() > 0);
    }
}
