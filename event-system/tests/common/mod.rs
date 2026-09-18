#![expect(
    dead_code,
    reason = "each integration test file compiles its own copy this module. Thus if any of them
              don't use all helpers, `dead_code` is false positively linted"
)]

use {
    agave_event_system::{
        EventSystem, StreamConfig, event,
        stream_name::StreamName,
        stream_policy::StreamPolicy,
        subscriber::{StreamSubscriber, TryRecvError, Typed},
    },
    std::{assert_matches, path::PathBuf},
    tempfile::TempDir,
};

pub(crate) const TEST_CONFIG: StreamConfig = StreamConfig {
    capacity: 5,
    producer_slots: 1,
    consumer_slots: 1,
};

pub(crate) const TEST_STREAM_NAME: StreamName = agave_event_system::stream_name!("test-stream");

pub(crate) const TEST_EVENT: TestEvent = TestEvent { value: 42 };

pub(crate) fn assert_is_empty<const N: usize, Mode>(subscribers: [&mut StreamSubscriber<Mode>; N]) {
    for subscriber in subscribers {
        assert_matches!(subscriber.try_recv(), Err(TryRecvError::Empty));
    }
}

pub(crate) fn assert_received<const N: usize>(
    subscribers: [&mut StreamSubscriber<Typed<TestEvent>>; N],
) {
    for subscriber in subscribers {
        let received_message = subscriber.try_recv().unwrap().decode().unwrap();
        assert_eq!(received_message, TEST_EVENT);
    }
}

#[event]
#[derive(Debug, PartialEq, PartialOrd)]
pub(crate) struct TestEvent {
    pub(crate) value: u64,
}

#[event]
pub(crate) enum TestEnumEvent {
    Value { value: u64 },
}

#[derive(Default)]
pub(crate) struct TestContextBuilder {
    stream_policy: Option<StreamPolicy>,
}

impl TestContextBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_policy_enabling_all_streams(mut self) -> Self {
        self.stream_policy = Some("on".parse().unwrap());
        self
    }

    pub(crate) fn build(self) -> TestContext {
        let directory = TempDir::new().unwrap();
        let event_system = EventSystem::new(directory.path()).unwrap();

        event_system.set_stream_policy(self.stream_policy.unwrap_or_default());

        TestContext {
            event_system,
            directory,
        }
    }
}

pub(crate) struct TestContext {
    pub(crate) event_system: EventSystem,
    directory: TempDir,
}

impl TestContext {
    pub(crate) fn event_system_path(&self) -> PathBuf {
        self.directory.path().to_path_buf()
    }
}
