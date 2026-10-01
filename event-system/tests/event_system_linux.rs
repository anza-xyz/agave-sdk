#![cfg(target_os = "linux")]

mod common;

use {
    crate::common::TestContextBuilder,
    agave_event_system::{
        CreateStreamError, Event, EventSystem, PublisherFactory, StreamConfig,
        stream_name::StreamName,
        subscriber::{
            self, AvailableStream, DecodedMessage, TryConnectError, TryConnectTypedError,
        },
    },
    common::{TEST_CONFIG, TEST_STREAM_NAME, TestEnumEvent, TestEvent},
    rstest::rstest,
    std::{assert_matches, io::ErrorKind},
    tempfile::TempDir,
    wincode_dynamic::Value,
};

#[test]
fn create_event_system_fails_when_path_is_a_file() {
    const EXISTING_CONTENTS: &[u8] = b"existing file contents are preserved";

    let event_system_directory = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(&event_system_directory, EXISTING_CONTENTS).unwrap();

    assert_matches!(EventSystem::new(&event_system_directory), Err(_));
    assert_eq!(
        std::fs::read(&event_system_directory).unwrap(),
        EXISTING_CONTENTS
    );
}

#[test]
fn create_event_system_reuses_directory_only_after_drop() {
    let directory = TempDir::new().unwrap();
    let path = directory.path();

    let event_system = EventSystem::new(path).unwrap();

    let event_system_with_reused_path_result = EventSystem::new(path);
    assert_matches!(event_system_with_reused_path_result, Err(_));

    drop(event_system);
    let _event_system =
        EventSystem::new(path).expect("the path can be reused after the event system is dropped");
}

fn io_error_kind(error: &agave_event_system::CreateEventSystemError) -> ErrorKind {
    std::error::Error::source(error)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .expect("CreateEventSystemError wraps an io::Error")
        .kind()
}

/// Lays out a stream directory as an event system whose process has exited
/// would leave it, with a queue link to a file descriptor that no longer exists.
fn write_stale_stream(path: &std::path::Path, layout_directory: &str, stream_name: &str) {
    let stream_directory = path.join(layout_directory).join(stream_name);
    std::fs::create_dir_all(&stream_directory).unwrap();
    std::fs::write(stream_directory.join("schema"), b"schema").unwrap();
    std::os::unix::fs::symlink(
        "/proc/self/fd/does-not-exist",
        stream_directory.join("queue-1"),
    )
    .unwrap();
}

#[test]
fn create_event_system_removes_layout_left_by_exited_process() {
    let directory = TempDir::new().unwrap();
    let path = directory.path();
    std::fs::write(path.join("lock"), b"").unwrap();
    write_stale_stream(path, "event-streams", TEST_STREAM_NAME.as_str());
    write_stale_stream(path, "tmp", "staged-stream");

    let event_system = EventSystem::new(path).expect("stale layout is removed");
    assert_eq!(
        std::fs::read_dir(path.join("event-streams"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(std::fs::read_dir(path.join("tmp")).unwrap().count(), 0);

    let _publisher_factory = event_system
        .create_stream::<TestEvent>(TEST_STREAM_NAME, TEST_CONFIG)
        .expect("the stale stream name can be reused");
}

#[test]
fn create_event_system_fails_when_directory_is_in_use() {
    let directory = TempDir::new().unwrap();
    let path = directory.path();
    let event_system = EventSystem::new(path).unwrap();
    let _publisher_factory = event_system
        .create_stream::<TestEvent>(TEST_STREAM_NAME, TEST_CONFIG)
        .unwrap();

    let error = EventSystem::new(path).unwrap_err();
    assert_eq!(io_error_kind(&error), ErrorKind::ResourceBusy);
    assert!(
        path.join("event-streams")
            .join(TEST_STREAM_NAME.as_str())
            .is_dir()
    );
}

#[test]
fn create_event_system_fails_when_a_queue_is_still_open() {
    let directory = TempDir::new().unwrap();
    let path = directory.path();
    let stream_directory = path.join("event-streams").join(TEST_STREAM_NAME.as_str());
    std::fs::create_dir_all(&stream_directory).unwrap();
    // A queue link that resolves belongs to a live process, even without a lock.
    let open_queue = tempfile::NamedTempFile::new().unwrap();
    std::os::unix::fs::symlink(open_queue.path(), stream_directory.join("queue-1")).unwrap();

    let error = EventSystem::new(path).unwrap_err();
    assert_eq!(io_error_kind(&error), ErrorKind::ResourceBusy);
    assert!(stream_directory.join("queue-1").exists());
}

#[rstest]
#[case::unexpected_root_entry("unrelated-file")]
#[case::unexpected_stream_entry("event-streams/test-stream/unrelated-file")]
fn create_event_system_leaves_unexpected_contents_untouched(#[case] unexpected_entry: &str) {
    let directory = TempDir::new().unwrap();
    let path = directory.path();
    let unexpected_entry = path.join(unexpected_entry);
    std::fs::create_dir_all(unexpected_entry.parent().unwrap()).unwrap();
    std::fs::write(&unexpected_entry, b"unrelated contents").unwrap();

    let error = EventSystem::new(path).unwrap_err();
    assert_eq!(io_error_kind(&error), ErrorKind::DirectoryNotEmpty);
    assert_eq!(
        std::fs::read(&unexpected_entry).unwrap(),
        b"unrelated contents"
    );
}

#[test]
fn create_stream_reserves_names_only_after_success() {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    const REUSED_STREAM_NAME: StreamName = agave_event_system::stream_name!("reused-stream-name");

    let invalid_config = StreamConfig {
        capacity: 0,
        ..TEST_CONFIG
    };

    assert_matches!(
        test_context
            .event_system
            .create_stream::<TestEvent>(REUSED_STREAM_NAME, invalid_config),
        Err(CreateStreamError::Queue(_))
    );
    let _publisher_factory = test_context
        .event_system
        .create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG)
        .expect("test-events is unused stream name as it failed above");

    assert_matches!(
        test_context.event_system.create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG),
        Err(CreateStreamError::FileSystem(error))
            if matches!(
                error.kind(),
                // Linux permits EEXIST or ENOTEMPTY for a nonempty destination.
                ErrorKind::AlreadyExists | ErrorKind::DirectoryNotEmpty
            ),
            "creation of the same stream name must now fail, since it succeeded above."
    );
}

#[test]
fn stream_can_be_recreated_after_dropping_all_handles() {
    const REUSED_STREAM_NAME: StreamName = TEST_STREAM_NAME;
    let directory = TempDir::new().unwrap();
    let event_system = EventSystem::new(directory.path()).unwrap();

    let factory_1 = event_system
        .create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG)
        .unwrap();
    let factory_2 = factory_1.clone();

    drop(factory_1);

    assert_matches!(
        event_system.create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG),
        Err(_),
        "factory_2 is still alive preventing re-creation"
    );

    drop(factory_2);
    assert_matches!(
        event_system.create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG),
        Ok(PublisherFactory { .. }),
        "all factory handles are dropped, recycling the stream name to be reused."
    );
}

#[rstest]
fn publisher_creation_respects_slot_limit(#[values(1, 2, 4)] publisher_slots: usize) {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();

    let stream_config = StreamConfig {
        publisher_slots,
        ..TEST_CONFIG
    };

    let publisher_factory: PublisherFactory<TestEvent> = test_context
        .event_system
        .create_stream(TEST_STREAM_NAME, stream_config)
        .unwrap();

    for _ in 0..publisher_slots {
        publisher_factory
            .try_create_publisher()
            .expect("publisher slot is available");
    }

    assert_matches!(
        publisher_factory.try_create_publisher(),
        None,
        "publisher slots are exhausted"
    );
}

#[rstest]
fn typed_subscribers_can_connect_and_receive_events(#[values(1, 2)] subscriber_slots: usize) {
    const TEST_EVENT: TestEvent = TestEvent { value: 42 };

    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let stream_config = StreamConfig {
        subscriber_slots,
        ..TEST_CONFIG
    };
    let publisher_factory: PublisherFactory<TestEvent> = test_context
        .event_system
        .create_stream(TEST_STREAM_NAME, stream_config)
        .unwrap();
    let mut publisher = publisher_factory.try_create_publisher().unwrap();

    let explorer = subscriber::StreamExplorer::new(test_context.event_system_path());
    let connect_subscriber = || {
        explorer
            .available_streams()
            .next()
            .unwrap()
            .try_connect_typed::<TestEvent>()
    };
    let mut subscribers: Vec<_> = (0..subscriber_slots)
        .map(|_| connect_subscriber().unwrap())
        .collect();

    assert_matches!(
        connect_subscriber(),
        Err(TryConnectTypedError::Connection(
            TryConnectError::SubscriberSlotsExhausted
        ))
    );

    publisher.publish(&TEST_EVENT).unwrap();
    for subscriber in &mut subscribers {
        assert_eq!(&TEST_STREAM_NAME, subscriber.stream_name());
        assert_eq!("TestEvent", subscriber.type_name());

        let received_event = subscriber.try_recv().unwrap().decode().unwrap();
        assert_eq!(TEST_EVENT, received_event);
    }
}

#[rstest]
#[case::struct_event(TestEvent { value: 42 }, None)]
#[case::enum_event(TestEnumEvent::Value { value: 42 }, Some("Value"))]
fn dynamic_subscriber_can_connect_and_decode_events<E: Event>(
    #[case] event: E,
    #[case] expected_variant_name: Option<&str>,
) {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let publisher_factory: PublisherFactory<E> = test_context
        .event_system
        .create_stream(TEST_STREAM_NAME, TEST_CONFIG)
        .unwrap();

    let mut publisher = publisher_factory.try_create_publisher().unwrap();

    let subscriber = subscriber::StreamExplorer::new(test_context.event_system_path());
    let mut available_streams: Vec<AvailableStream> = subscriber.available_streams().collect();

    assert_eq!(available_streams.len(), 1, "only one stream is published");
    let available_stream = available_streams.pop().unwrap();
    let mut subscriber = available_stream.try_connect_dynamic().unwrap();

    publisher.publish(&event).unwrap();
    let received_message = subscriber.try_recv().unwrap();
    let (variant_name, mut fields) = match received_message.decode().unwrap() {
        DecodedMessage::Struct { fields } => (None, fields),
        DecodedMessage::Enum {
            variant_name,
            fields,
        } => (Some(variant_name), fields),
    };
    assert_eq!(variant_name, expected_variant_name);

    // Fields decode lazily, so consume the iterator to exercise payload decoding.
    let field = fields.next().unwrap().unwrap();
    assert_eq!(field.name(), "value");
    assert_eq!(field.value(), &Value::U64(42));
    assert!(fields.next().is_none(), "no extra fields are present");
}
